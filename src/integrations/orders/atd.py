"""
ATDOrderAdapter -- ordering through ATD Connect's SOAP order service.

Credentials are the SAME username/password/clientId/location the catalog feed uses, read from
``credentials["feed"]`` like Turn14 does. Entitlement is not the same thing, though: ATD grants
order access to an account separately from catalog access, so a connection can ingest 34,000
products happily and still be refused here. That refusal arrives as
ATDOrderNotEntitledError and is surfaced as a configuration problem rather than a credential
one, because telling a dealer to re-check a password that is already correct sends them nowhere.

The capability set, read off the live WSDL rather than inferred:

  get_shipping_quote   previewOrder   -- prices AND shipping-quotes in one call, commits nothing
  submit_order         placeOrder     -- returns only a confirmation number
  get_order_status     getOrderDetail -- full per-fulfillment tracking
  get_invoices         getOrderDetail -- invoice NUMBERS only; ATD exposes no invoice document
  cancel_order         not offered    -- there is no cancel operation in the schema
  list_shipping_methods  no source    -- 3_6 takes `shippingmethod` as an order INPUT, but
                                        nothing in the API enumerates valid values

previewOrder is the reason this adapter can be finished and exercised before anyone is willing
to place a real order: same cart, same prices, no commitment.
"""
import datetime
import logging
import typing
from decimal import Decimal, InvalidOperation

from src import enums as src_enums
from src import models as src_models
from src.integrations import credentials as credentials_helper
from src.integrations.clients.atd import exceptions as atd_exceptions
from src.integrations.clients.atd import order_client as atd_order_client
from src.integrations.orders import base
from src.integrations.orders import exceptions as order_exceptions

logger = logging.getLogger(__name__)
_LOG_PREFIX = "[ATD-ORDER-ADAPTER]"

# ATD fulfillment status -> whether that quantity actually ships. Their vocabulary is not
# documented anywhere we have; these are the values seen in the schema and in Flxpoint's
# description of the same feed. Anything unrecognised is treated as shipping, because the
# alternative -- silently moving a real quantity into "backordered" -- is the worse error.
_BACKORDER_STATUSES = frozenset({"backordered", "backorder", "unavailable", "out of stock"})


def _decimal(value: typing.Any) -> typing.Optional[Decimal]:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None


def _int(value: typing.Any) -> int:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return 0


def _date(value: typing.Any) -> typing.Optional[datetime.date]:
    """ATD sends xs:date on a preview and xs:dateTime on a status, so both must parse."""
    text = (str(value) or "").strip() if value is not None else ""
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


def _ships_now(status: typing.Optional[str]) -> bool:
    return (status or "").strip().lower() not in _BACKORDER_STATUSES


class ATDOrderAdapter(base.DistributorOrderAdapter):
    provider_kind = src_enums.BrandProviderKind.ATD.value
    fulfillment_channel_name = "api"

    def __init__(
        self,
        company_provider: src_models.CompanyProviders,
        order_account: typing.Optional[src_models.CompanyProviderOrderAccount] = None,
    ) -> None:
        base.DistributorOrderAdapter.__init__(self, company_provider, order_account)
        # The feed namespace on purpose: ATD issues ONE credential set per dealer and it covers
        # the REST catalog and the SOAP order service alike, so there is no separate order
        # credential to resolve here. (Turn 14 is the opposite case -- its Order API credentials
        # really are distinct from its feed's, which is why that adapter reads the order
        # namespace.) The catalog entry therefore declares no required API order fields.
        credentials = dict(credentials_helper.get_feed_credentials(company_provider) or {})

        # ...with one exception: the order service takes a locationnumber of its own, so an order
        # account may override which ATD DC the order is placed against while still authenticating
        # as the same dealer. Empty (the normal case) leaves the feed's location in place -- note
        # this reads the order account only for `location`, nothing else.
        override = (credentials_helper.get_order_credentials(company_provider, order_account) or {})
        location_override = str(override.get("location") or "").strip()
        if location_override:
            credentials["location"] = location_override

        self._client = atd_order_client.ATDOrderClient(credentials=credentials)

    # -- helpers -------------------------------------------------------------

    @staticmethod
    def _cart_lines(line_items: typing.Sequence[base.OrderLineItemRequest]) -> typing.List[typing.Dict]:
        """
        Our line items as ATD cart lines. ``provider_external_id`` IS the ATD product number --
        see master_parts.py's ATD ingest, which keys ProviderPart on exactly that.
        """
        return [
            {
                "cart_line_number": index,
                "atd_product_number": item.provider_part.provider_external_id,
                "quantity": item.quantity,
            }
            for index, item in enumerate(line_items, start=1)
        ]

    @staticmethod
    def _ship_to_dict(ship_to: base.ShipToAddress) -> typing.Optional[typing.Dict]:
        """
        consumerdata is for a drop-ship recipient. Shipping to the shop's own address means
        ATD already knows where that is from the location number, and sending a consumer block
        would describe the dealer as an end customer.
        """
        if ship_to.is_shop_address:
            return None
        return {
            "name": ship_to.name,
            "phone": ship_to.phone,
            "email": ship_to.email,
            "address1": ship_to.address1,
            "address2": ship_to.address2,
            "city": ship_to.city,
            "state": ship_to.state,
            "postal_code": ship_to.postal_code,
        }

    def _handle(self, e: Exception, request_payload: typing.Optional[typing.Dict] = None):
        if isinstance(e, atd_exceptions.ATDOrderNotEntitledError):
            raise order_exceptions.OrderAdapterNotImplementedError(
                "ATD has not enabled ordering on this account. The same credentials work for "
                "the catalog -- order access is granted separately, after a sandbox test order. "
                "ATD said: {}".format(str(e))
            ) from e
        if isinstance(e, atd_exceptions.ATDConnectionError):
            raise order_exceptions.OrderAdapterError(str(e)) from e
        raise order_exceptions.OrderValidationError(str(e), request_payload=request_payload) from e

    # -- quote ---------------------------------------------------------------

    def get_shipping_quote(
        self,
        line_items: typing.List[base.OrderLineItemRequest],
        ship_to: base.ShipToAddress,
        ship_method: typing.Optional[str] = None,
    ) -> base.ShippingQuoteResult:
        """
        previewOrder: ATD prices the cart and routes it to source DCs without committing. One
        call covers what most distributors split between a price quote and a shipping quote.
        """
        cart = self._cart_lines(line_items)
        request_payload = {
            "operation": "previewOrder",
            "locationnumber": self._client.location,
            "lineitems": cart,
            "pickup": bool(ship_to.is_shop_address),
        }
        try:
            preview = self._client.preview_order(
                cart,
                customer_po_number=None,
                pickup=False,
                ship_to=self._ship_to_dict(ship_to),
                shipping_method=ship_method,
            )
        except Exception as e:
            self._handle(e, request_payload)

        by_cart_line = {str(line.get("cart_line_number")): line for line in preview["lines"]}
        lines: typing.List[base.ShippingQuoteLine] = []
        distributor_total = Decimal("0")
        total_freight = Decimal("0")
        saw_price = False

        for index, item in enumerate(line_items, start=1):
            line = by_cart_line.get(str(index))
            external_id = item.provider_part.provider_external_id or ""
            if line is None:
                # ATD dropped the line entirely -- it cannot be ordered at this location.
                lines.append(
                    base.ShippingQuoteLine(
                        line_item_id=item.line_item_id,
                        provider_external_id=external_id,
                        quantity_available=0,
                        quantity_backordered=item.quantity,
                        flags=["blocked"],
                    )
                )
                continue

            unit_price = _decimal(line.get("cost"))
            line_total = _decimal(line.get("line_total"))
            if line_total is not None:
                distributor_total += line_total
                saw_price = True

            # A line can be split across several DCs, each with its own freight and ETA. Our
            # quote line carries one warehouse, so the DC that ships the most of it wins and
            # the rest of the detail is preserved in flags/ship options.
            fulfillments = line.get("fulfillments") or []
            shipping_now = sum(_int(f.get("quantity")) for f in fulfillments if _ships_now(f.get("status")))
            backordered = sum(_int(f.get("quantity")) for f in fulfillments if not _ships_now(f.get("status")))
            for f in fulfillments:
                freight = _decimal(f.get("freight"))
                if freight:
                    total_freight += freight

            primary = max(fulfillments, key=lambda f: _int(f.get("quantity")), default=None)
            ship_options = []
            if primary and primary.get("ship_method"):
                ship_options.append(
                    base.ShipOption(
                        service_level_code=str(primary["ship_method"]),
                        service_level_name=str(primary["ship_method"]),
                        estimated_delivery_date=_date(primary.get("estimated_delivery")),
                        cost=_decimal(primary.get("freight")),
                        quote_option_id=str(primary["ship_method"]),
                    )
                )

            flags = []
            if backordered:
                flags.append("backorder")
            if str(line.get("discontinued") or "").lower() == "true":
                flags.append("discontinued")

            lines.append(
                base.ShippingQuoteLine(
                    line_item_id=item.line_item_id,
                    provider_external_id=external_id,
                    quantity_available=min(shipping_now, item.quantity),
                    quantity_backordered=max(0, item.quantity - shipping_now),
                    warehouse_code=(primary or {}).get("source_dc"),
                    warehouse_name=(primary or {}).get("source_dc_name"),
                    ship_options=ship_options,
                    flags=flags,
                    distributor_unit_price=unit_price,
                    distributor_line_total=line_total,
                )
            )

        fees = []
        if total_freight:
            fees.append(
                base.LineFee(
                    fee_type="freight",
                    description="ATD freight",
                    amount=total_freight,
                )
            )

        return base.ShippingQuoteResult(
            lines=lines,
            raw_response=preview,
            distributor_total=_decimal(preview.get("order_total")) or (
                distributor_total if saw_price else None
            ),
            fees=fees,
            request_payload=request_payload,
        )

    # -- submit --------------------------------------------------------------

    def submit_order(
        self,
        purchase_order: src_models.PurchaseOrder,
        line_items: typing.List[base.OrderLineItemRequest],
        ship_to: base.ShipToAddress,
    ) -> base.DistributorOrderResult:
        cart = self._cart_lines(line_items)
        request_payload = {
            "operation": "placeOrder",
            "locationnumber": self._client.location,
            "customerponumber": base.resolve_po_number(purchase_order),
            "lineitems": cart,
        }
        try:
            result = self._client.place_order(
                cart,
                customer_po_number=base.resolve_po_number(purchase_order),
                pickup=False,
                ship_to=self._ship_to_dict(ship_to),
                shipping_method=purchase_order.ship_method or None,
            )
        except Exception as e:
            self._handle(e, request_payload)

        confirmation = (result or {}).get("confirmation_number")
        if not confirmation:
            raise order_exceptions.OrderValidationError(
                "ATD accepted the request but returned no confirmation number.",
                request_payload=request_payload,
            )

        # placeOrder returns ONLY a confirmation number -- no per-line detail at all -- so every
        # line is recorded against it as requested, and the real split arrives on the first
        # getOrderDetail poll.
        placements = [
            base.LineItemPlacement(
                line_item_id=item.line_item_id,
                distributor_order_number=confirmation,
                quantity_confirmed=item.quantity,
            )
            for item in line_items
        ]
        return base.DistributorOrderResult(
            distributor_order_numbers=[confirmation],
            line_item_placements=placements,
            raw_response=result,
            request_payload=request_payload,
        )

    # -- status --------------------------------------------------------------

    def _details_for(self, purchase_order: src_models.PurchaseOrder) -> typing.List[typing.Dict]:
        details = []
        for distributor_order in purchase_order.distributor_orders.all():
            try:
                details.append(self._client.get_order_detail(distributor_order.distributor_order_number))
            except Exception as e:
                self._handle(e)
        return details

    def get_order_status(self, purchase_order: src_models.PurchaseOrder) -> base.OrderStatusResult:
        statuses: typing.List[base.DistributorOrderStatus] = []
        for detail in self._details_for(purchase_order):
            fulfillments = [f for line in detail["lines"] for f in line["fulfillments"]]
            tracking = sorted({f["tracking_number"] for f in fulfillments if f.get("tracking_number")})
            carrier = next((f.get("carrier_name") for f in fulfillments if f.get("carrier_name")), None)
            shipped = [_date(f.get("shipped_date")) for f in fulfillments if f.get("shipped_date")]
            delivered = [f for f in fulfillments if f.get("actual_delivery_date")]
            etas = [_date(f.get("estimated_delivery_date")) for f in fulfillments if f.get("estimated_delivery_date")]

            if delivered and len(delivered) == len(fulfillments):
                delivery_status = "delivered"
            elif tracking or shipped:
                delivery_status = "in_transit"
            else:
                delivery_status = None

            statuses.append(
                base.DistributorOrderStatus(
                    distributor_order_number=detail.get("confirmation_number") or "",
                    status_code=(fulfillments[0].get("status") if fulfillments else "") or "",
                    tracking_numbers=tracking,
                    carrier=carrier,
                    raw_response=detail,
                    ship_date=min([d for d in shipped if d], default=None),
                    estimated_delivery_date=max([d for d in etas if d], default=None),
                    delivery_status=delivery_status,
                )
            )
        return base.OrderStatusResult(orders=statuses)

    # -- invoices ------------------------------------------------------------

    def supports_invoices(self) -> bool:
        return True

    def get_invoices(self, purchase_order: src_models.PurchaseOrder) -> typing.List[base.DistributorInvoice]:
        """
        Synthesized from getOrderDetail. ATD exposes no invoice document and no invoice
        endpoint, but each fulfillment carries an ``invoicenumber`` once it bills -- so an
        invoice here is the group of fulfillments sharing one number.

        Line amounts come from the fulfillment's own ``linetotal``, which is ATD's figure, not
        ours. Freight is the order's, so it is attached to the first invoice only rather than
        repeated -- repeating it would double-count on a split shipment.
        """
        invoices: typing.List[base.DistributorInvoice] = []
        for detail in self._details_for(purchase_order):
            by_invoice: typing.Dict[str, typing.List[typing.Dict]] = {}
            for line in detail["lines"]:
                for f in line["fulfillments"]:
                    number = f.get("invoice_number")
                    if number:
                        by_invoice.setdefault(number, []).append({**f, "_line": line})

            freight_applied = False
            for number, entries in by_invoice.items():
                line_items = [
                    base.InvoiceLineItem(
                        part_number=e["_line"].get("atd_product_number"),
                        description=e["_line"].get("product_name"),
                        quantity=_int(e.get("quantity_shipped")) or _int(e.get("quantity")) or None,
                        total_price=_decimal(e.get("line_total")),
                        warehouse_code=e.get("source_dc"),
                    )
                    for e in entries
                ]
                totals = [li.total_price for li in line_items if li.total_price is not None]
                invoices.append(
                    base.DistributorInvoice(
                        invoice_number=number,
                        distributor_order_number=detail.get("confirmation_number"),
                        total_price=(sum(totals, Decimal("0")) if totals else None),
                        freight=(None if freight_applied else _decimal(detail.get("freight"))),
                        tracking=[
                            base.InvoiceTrackingEntry(
                                ship_method=e.get("ship_method"), tracking_number=e.get("tracking_number")
                            )
                            for e in entries if e.get("tracking_number")
                        ],
                        line_items=line_items,
                        comments=(
                            "Synthesized from ATD getOrderDetail fulfillments, not an invoice "
                            "document -- ATD exposes none. Amounts are ATD's own line totals; "
                            "invoice date and amount due are not reported by this API."
                        ),
                        raw_response={"invoice_number": number, "fulfillments": entries},
                    )
                )
                freight_applied = True
        return invoices

    # -- capabilities --------------------------------------------------------

    def cancel_order(self, purchase_order: src_models.PurchaseOrder) -> bool:
        raise order_exceptions.OrderNotSupportedError(
            "ATD Connect has no cancel operation; cancellations go through your ATD rep."
        )

    def supports_cancel(self) -> bool:
        return False

    def supports_shipping_method_selection(self) -> bool:
        # 3_6 accepts `shippingmethod` as an order input, but nothing in the API enumerates the
        # valid values, so a picker would be a free-text box pretending to be a catalog.
        return False
