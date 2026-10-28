"""
MotorStateOrderAdapter -- ordering against Motor State's API (see
src.integrations.clients.motorstate.order_client for the transport and the live findings).

Richest of the distributors wired so far, and the first that can genuinely price freight before
an order:

  get_shipping_quote      OrderInquiry   line prices, availability AND per-method freight rates
  list_shipping_methods   OrderOptions   17 real codes for this dealer -- so a real picker
  submit_order            Order          returns Motor State's order number
  get_order_status        Order (GET)    looked up by OUR PO number; tracking, ship date
  get_invoices            Order (GET)    synthesised; there is no invoice endpoint
  cancel_order            --             no cancel operation exists in the API

Credentials are the ORDER namespace's ``api_key``, not the feed's FTP login. The key is issued
with the FTP credentials on the same subscription, but separately: Motor State approves some
accounts with FTP only, so a connected feed does not imply a usable key.

**A payment method is mandatory on every priced request.** Nothing in our order flow asks a
dealer to choose one per order, so the adapter resolves it once: the order account's pinned
``payment_method_code`` when set, else the best of the account's own OrderOptions (terms before
card before COD -- see the client's _PAYMENT_PREFERENCE, and note Motor State refuses COD for
drop shipments and FedEx retired COD entirely). Getting this wrong is not a subtle failure: an
inquiry without it returns no rates at all, and ShippingEstimate returns HTTP 500.
"""
import datetime
import logging
import typing
from decimal import Decimal, InvalidOperation

from src import constants as src_constants
from src import enums as src_enums
from src import models as src_models
from src.integrations import credentials as credentials_helper
from src.integrations.clients.motorstate import exceptions as ms_exceptions
from src.integrations.clients.motorstate import order_client as ms_order_client
from src.integrations.orders import base
from src.integrations.orders import exceptions as order_exceptions

logger = logging.getLogger(__name__)
_LOG_PREFIX = "[MOTOR-STATE-ORDER-ADAPTER]"

_PAYMENT_METHOD_FIELD = "payment_method_code"
_CREDIT_CARD_LAST4_FIELD = "credit_card_last4"


def _decimal(value: typing.Any) -> typing.Optional[Decimal]:
    if value is None or str(value).strip() == "":
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _int(value: typing.Any) -> int:
    try:
        return int(float(str(value).strip()))
    except (TypeError, ValueError):
        return 0


def _date(value: typing.Any) -> typing.Optional[datetime.date]:
    text = str(value).strip() if value is not None else ""
    if not text:
        return None
    try:
        return datetime.datetime.fromisoformat(text.replace("Z", "+00:00")).date()
    except ValueError:
        pass
    for fmt in ("%Y-%m-%d", "%m/%d/%Y"):
        try:
            return datetime.datetime.strptime(text[:10], fmt).date()
        except ValueError:
            continue
    return None


def _error_text(e: Exception) -> str:
    """
    MotorStateAPIBadResponseCodeError keeps its text on ``.message`` and never passes it to
    Exception, so ``str(e)`` on one is empty. Everything user-facing goes through here.
    """
    return str(getattr(e, "message", "") or "") or str(e) or e.__class__.__name__


def _header_errors(payload: typing.Dict) -> typing.List[str]:
    """Motor State's order-level rejections, e.g. 'COD payment is not available for drop shipment
    orders.' (code 17). Returned on HTTP 422 alongside HasErrors."""
    out = []
    for err in (payload or {}).get("HeaderErrors") or []:
        message = (err or {}).get("Message")
        if message:
            out.append(str(message).strip())
    return out


def _line_errors(line: typing.Dict) -> typing.List[str]:
    """
    Per-line rejections, which arrive in ``Lines[].Errors`` and NOT in HeaderErrors.

    Easy to miss and silent when missed: a truck-freight-only item quoted under a parcel method
    returns HeaderErrors empty, HasErrors unset, and an EMPTY rate list, with the only
    explanation sitting on the line -- "The selected Shipping Method is not valid for the
    product ALL22202." Without reading these, the dealer gets a quote with no shipping options
    and no reason for it. (Naming tkpp on the same cart rates it fine.)
    """
    out = []
    for err in (line or {}).get("Errors") or []:
        message = (err or {}).get("Message")
        if message:
            out.append(str(message).strip())
    return out


class MotorStateOrderAdapter(base.DistributorOrderAdapter):
    provider_kind = src_enums.BrandProviderKind.MOTOR_STATE_DISTRIBUTING.value
    fulfillment_channel_name = "api"

    def __init__(
        self,
        company_provider: src_models.CompanyProviders,
        order_account: typing.Optional[src_models.CompanyProviderOrderAccount] = None,
    ) -> None:
        base.DistributorOrderAdapter.__init__(self, company_provider, order_account)
        self._order_credentials = (
            credentials_helper.get_order_credentials(company_provider, order_account) or {}
        )
        self._client = ms_order_client.MotorStateOrderClient(credentials=self._order_credentials)
        self._payment: typing.Optional[typing.Tuple[str, typing.Optional[str]]] = None

    # -- helpers -------------------------------------------------------------

    def _payment_method(self) -> typing.Tuple[str, typing.Optional[str]]:
        """(code, credit_card_last4). Pinned on the order account if the dealer set one, else
        the best option the account itself reports. Resolved once per adapter."""
        if self._payment is not None:
            return self._payment
        pinned = str(self._order_credentials.get(_PAYMENT_METHOD_FIELD) or "").strip()
        if pinned:
            last4 = str(self._order_credentials.get(_CREDIT_CARD_LAST4_FIELD) or "").strip() or None
            self._payment = (pinned, last4)
            return self._payment
        code, last4 = self._client.default_payment_method()
        if not code:
            raise order_exceptions.OrderValidationError(
                "Motor State reports no payment methods for this account, and every priced "
                "request needs one. Ask Motor State to enable a payment method, or set one on "
                "this ordering account."
            )
        self._payment = (code, last4)
        return self._payment

    @staticmethod
    def motorstate_part_number(provider_part: src_models.ProviderPart) -> str:
        """
        Motor State's own part number, unpacked from ``provider_external_id``.

        That field is NOT the part number: master_parts keys it as
        ``"{motorstate_brand_id}_{part_number}"`` (see _motorstate_provider_external_id), the
        same compound shape NTW and AllPro use. Sending it verbatim would put
        "322_MAH4065MS-112-1" in the Lines array and the order would be rejected -- or worse,
        silently match nothing.

        Split on the FIRST underscore only: the brand id is digits, and Motor State's own part
        numbers contain hyphens and underscores of their own.
        """
        external_id = (provider_part.provider_external_id or "").strip()
        brand_id, _, part_number = external_id.partition("_")
        if brand_id.isdigit() and part_number:
            return part_number
        # Nothing to unpack -- treat the whole value as the part number rather than failing,
        # so a row written by a different path still quotes.
        return external_id

    @staticmethod
    def _cart_lines(
        line_items: typing.Sequence[base.OrderLineItemRequest],
    ) -> typing.Tuple[typing.List[typing.Dict], typing.Dict[str, int]]:
        """
        Our line items as Motor State lines, plus part number -> our line_item_id. Motor State
        echoes ``LineNumber`` back, but it also renumbers on partial fills, so the part number
        is the reliable key home.
        """
        lines: typing.List[typing.Dict] = []
        index: typing.Dict[str, int] = {}
        for position, item in enumerate(line_items, start=1):
            part_number = MotorStateOrderAdapter.motorstate_part_number(item.provider_part)
            if not part_number:
                raise order_exceptions.OrderValidationError(
                    "ProviderPart {} has no Motor State part number.".format(item.provider_part.id)
                )
            lines.append({
                "line_number": position,
                "part_number": part_number,
                "quantity": item.quantity,
            })
            index[part_number.upper()] = item.line_item_id
        return lines, index

    @staticmethod
    def _drop_ship_address(ship_to: base.ShipToAddress) -> typing.Optional[typing.Dict]:
        """
        DropShipAddress describes an end customer. Shipping to the shop's own address means
        Motor State already has it from the account, and sending one anyway turns a stock order
        into a drop shipment -- which is both priced differently (a DropShipmentFees line
        appears) and refused outright for COD.
        """
        if ship_to.is_shop_address:
            return None
        return {
            "Name": ship_to.name,
            "Contact": ship_to.attention or ship_to.name,
            "Line1": ship_to.address1,
            "Line2": ship_to.address2,
            "City": ship_to.city,
            "State": ship_to.state,
            "PostalCode": ship_to.postal_code,
            "Country": ship_to.country or "US",
            "Phone": ship_to.phone,
            "Email": ship_to.email,
        }

    def _handle(self, e: Exception, request_payload: typing.Optional[typing.Dict] = None):
        if isinstance(e, ms_exceptions.MotorStatePermissionError):
            raise order_exceptions.OrderValidationError(
                "Motor State rejected this API key. It is issued separately from the FTP feed "
                "login -- check the key on this ordering account is the one they emailed.",
                request_payload=request_payload,
            ) from e
        if isinstance(e, ms_exceptions.MotorStateAPIConnectionError):
            raise order_exceptions.OrderAdapterError(_error_text(e)) from e
        if isinstance(e, ms_exceptions.MotorStateAPIException):
            raise order_exceptions.OrderValidationError(
                _error_text(e), request_payload=request_payload
            ) from e
        raise e

    @staticmethod
    def _ship_options(payload: typing.Dict) -> typing.List[base.ShipOption]:
        """
        Freight options off a quote. A method with no ``Rate`` is one Motor State cannot price
        for this cart -- it is DROPPED rather than offered at zero, because an option showing
        free shipping that the carrier will not carry is worse than one method fewer.
        """
        options = []
        for rate in (payload or {}).get("AvailableShippingMethodRates") or []:
            amount = _decimal(rate.get("Rate"))
            if amount is None:
                continue
            fees = _decimal(rate.get("DropShipmentFees")) or Decimal("0")
            options.append(base.ShipOption(
                service_level_code=str(rate.get("ShippingMethodCode") or ""),
                service_level_name=str(rate.get("Description") or rate.get("ShippingMethodCode") or ""),
                cost=amount + fees,
            ))
        return options

    # -- quote ---------------------------------------------------------------

    def get_shipping_quote(
        self,
        line_items: typing.List[base.OrderLineItemRequest],
        ship_to: base.ShipToAddress,
        ship_method: typing.Optional[str] = None,
    ) -> base.ShippingQuoteResult:
        cart, index = self._cart_lines(line_items)
        payment_code, last4 = self._payment_method()
        # Motor State demands a PurchaseOrderNumber even on an inquiry, which creates nothing.
        # A throwaway reference keeps quotes out of their order history as real PO numbers.
        quote_ref = ms_order_client.quote_reference()
        request_payload = {
            "operation": "OrderInquiry",
            "PurchaseOrderNumber": quote_ref,
            "PaymentMethodCode": payment_code,
            "ShippingMethodCode": ship_method,
            "Lines": cart,
        }

        try:
            # One call: OrderInquiry returns the freight options too, so there is no separate
            # ShippingEstimate round trip here.
            result = self._client.order_inquiry(
                cart,
                payment_method_code=payment_code,
                credit_card_last4=last4,
                purchase_order_number=quote_ref,
                shipping_method_code=ship_method,
                drop_ship_address=self._drop_ship_address(ship_to),
            )
        except Exception as e:
            self._handle(e, request_payload)

        errors = _header_errors(result)
        if result.get("HasErrors") and errors:
            raise order_exceptions.OrderValidationError(
                "; ".join(errors), request_payload=request_payload
            )

        ship_options = self._ship_options(result)
        lines: typing.List[base.ShippingQuoteLine] = []
        all_line_errors: typing.List[str] = []
        for line in result.get("Lines") or []:
            part_number = str(line.get("PartNumber") or "").strip()
            line_item_id = index.get(part_number.upper())
            if line_item_id is None:
                continue
            shipped = _int(line.get("QuantityShip"))
            line_errors = _line_errors(line)
            all_line_errors.extend(line_errors)
            lines.append(base.ShippingQuoteLine(
                line_item_id=line_item_id,
                provider_external_id=part_number,
                quantity_available=shipped,
                quantity_backordered=_int(line.get("QuantityBackOrder")),
                distributor_unit_price=_decimal(line.get("Price")),
                distributor_line_total=_decimal(line.get("LineTotal")),
                # Every line shares the order-level freight options: Motor State rates the
                # shipment, not the line.
                ship_options=ship_options,
                flags=line_errors,
            ))

        if all_line_errors and not ship_options:
            # Priced, but with nothing that can carry it -- which is not a quote a dealer can
            # act on. Motor State's own wording says what to do ("select an allowable shipping
            # method"), so it is raised rather than returned as an empty picker.
            raise order_exceptions.OrderValidationError(
                "; ".join(dict.fromkeys(all_line_errors)), request_payload=request_payload
            )

        fees: typing.List[base.LineFee] = []
        for label, key in (
            ("sales_tax", "SalesTaxAmount"),
            ("drop_ship_fee", "DropShipmentFees"),
            ("duty", "TotalDuty"),
            ("tariff", "TotalTariff"),
        ):
            amount = _decimal(result.get(key))
            if amount and amount > 0:
                fees.append(base.LineFee(fee_type=label, description=key, amount=amount))

        return base.ShippingQuoteResult(
            lines=lines,
            # InvoiceAmount is the all-in figure Motor State would bill, freight included.
            distributor_total=_decimal(result.get("InvoiceAmount")),
            fees=fees,
            raw_response=result,
            request_payload=request_payload,
        )

    # -- shipping methods ----------------------------------------------------

    def supports_shipping_method_selection(self) -> bool:
        return True

    def list_shipping_methods(self) -> typing.List[base.ShippingMethod]:
        """This dealer's own codes, from OrderOptions -- not a hardcoded list."""
        try:
            methods = self._client.shipping_methods()
        except Exception as e:
            self._handle(e)
        return [
            base.ShippingMethod(
                code=str(m.get("Code") or ""),
                name=str(m.get("Description") or m.get("Code") or ""),
            )
            for m in methods
            if m.get("Code")
        ]

    # -- submit --------------------------------------------------------------

    def submit_order(
        self,
        purchase_order: src_models.PurchaseOrder,
        line_items: typing.List[base.OrderLineItemRequest],
        ship_to: base.ShipToAddress,
    ) -> base.DistributorOrderResult:
        cart, index = self._cart_lines(line_items)
        payment_code, last4 = self._payment_method()
        po_number = base.resolve_po_number(purchase_order)
        request_payload = {
            "operation": "Order",
            "PurchaseOrderNumber": po_number,
            "PaymentMethodCode": payment_code,
            "ShippingMethodCode": purchase_order.ship_method or None,
            "Lines": cart,
        }

        try:
            result = self._client.place_order(
                cart,
                payment_method_code=payment_code,
                credit_card_last4=last4,
                purchase_order_number=po_number,
                shipping_method_code=purchase_order.ship_method or None,
                drop_ship_address=self._drop_ship_address(ship_to),
            )
        except Exception as e:
            self._handle(e, request_payload)

        errors = _header_errors(result)
        if result.get("HasErrors") and errors:
            raise order_exceptions.OrderValidationError(
                "; ".join(errors), request_payload=request_payload
            )

        order_number = str(result.get("OrderNumber") or "").strip()
        if not order_number:
            raise order_exceptions.OrderValidationError(
                "Motor State accepted the request but returned no order number{}.".format(
                    " ({})".format("; ".join(errors)) if errors else ""
                ),
                request_payload=request_payload,
            )

        placements = []
        for line in result.get("Lines") or []:
            part_number = str(line.get("PartNumber") or "").strip()
            line_item_id = index.get(part_number.upper())
            if line_item_id is None:
                continue
            placements.append(base.LineItemPlacement(
                line_item_id=line_item_id,
                distributor_order_number=order_number,
                quantity_confirmed=_int(line.get("QuantityShip")),
                quantity_backordered=_int(line.get("QuantityBackOrder")),
            ))

        for warning in result.get("Warnings") or []:
            logger.warning("{} PO {} warning: {} {}".format(
                _LOG_PREFIX, po_number, warning.get("Code"), warning.get("Message")))

        return base.DistributorOrderResult(
            distributor_order_numbers=[order_number],
            line_item_placements=placements,
            raw_response=result,
            request_payload=request_payload,
            # Held means Motor State accepted it but parked it for review, so it is not yet a
            # confirmed order from their side.
            distributor_confirmed=not bool(result.get("Held")),
        )

    # -- status --------------------------------------------------------------

    def _orders_for(self, purchase_order: src_models.PurchaseOrder) -> typing.List[typing.Dict]:
        po_number = base.resolve_po_number(purchase_order)
        try:
            responses = self._client.get_orders(po_numbers=[po_number])
        except Exception as e:
            self._handle(e)
        return [
            r.get("Order") or {}
            for r in responses
            if isinstance(r, dict) and r.get("Found") and r.get("Order")
        ]

    def get_order_status(self, purchase_order: src_models.PurchaseOrder) -> base.OrderStatusResult:
        statuses = []
        for order in self._orders_for(purchase_order):
            # TrackingNumbers is a single comma-joined string, not a list.
            tracking = [
                t.strip() for t in str(order.get("TrackingNumbers") or "").split(",") if t.strip()
            ]
            statuses.append(base.DistributorOrderStatus(
                distributor_order_number=str(order.get("OrderNumber") or ""),
                status_code=str(order.get("StatusDescription") or order.get("StatusCode") or "").strip(),
                tracking_numbers=tracking,
                carrier=str(order.get("ShippingMethod") or "").strip() or None,
                ship_date=_date(order.get("ShipDate")),
                raw_response=order,
            ))
        return base.OrderStatusResult(orders=statuses)

    # -- cancel --------------------------------------------------------------

    def supports_cancel(self) -> bool:
        return False

    def cancel_order(self, purchase_order: src_models.PurchaseOrder) -> bool:
        raise order_exceptions.OrderNotSupportedError(
            "Motor State's API has no cancel operation; cancellations go through your rep."
        )

    # -- invoices ------------------------------------------------------------

    def supports_invoices(self) -> bool:
        return True

    def get_invoices(self, purchase_order: src_models.PurchaseOrder) -> typing.List[base.DistributorInvoice]:
        """
        Synthesised from the order itself -- Motor State exposes no invoice document or invoice
        endpoint, only InvoiceDate / InvoiceAmount on the order, so an invoice appears here only
        once they have invoiced it.
        """
        invoices = []
        for order in self._orders_for(purchase_order):
            invoice_date = _date(order.get("InvoiceDate"))
            amount = _decimal(order.get("InvoiceAmount"))
            if invoice_date is None and amount is None:
                continue
            order_number = str(order.get("OrderNumber") or "")
            line_items = [
                base.InvoiceLineItem(
                    part_number=str(line.get("PartNumber") or ""),
                    quantity=_int(line.get("QuantityShip")),
                    unit_price=_decimal(line.get("Price")),
                    total_price=_decimal(line.get("LineTotal")),
                )
                for line in order.get("Lines") or []
            ]
            invoices.append(base.DistributorInvoice(
                invoice_number=order_number,
                invoice_date=invoice_date,
                distributor_order_number=order_number,
                website_order_number=str(order.get("PurchaseOrderNumber") or "") or None,
                total_price=amount,
                freight=_decimal(order.get("ShippingRate")),
                line_items=line_items,
                comments=(
                    "Synthesised from Motor State's order detail -- they expose no invoice "
                    "document or invoice endpoint, so the order number stands in as the "
                    "invoice number."
                ),
                raw_response=order,
            ))
        return invoices
