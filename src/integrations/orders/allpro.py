"""
AllProOrderAdapter -- ordering against AllPro's Fulfillment Master API (see
src.integrations.clients.allpro.order_client for the transport and the live findings).

Capability map, and where each one honestly stands:

  get_shipping_quote     stockcheck   availability + cost + core. NO FREIGHT -- see below.
  submit_order           ordersend    one order per branch; see the branch note below.
  get_order_status       gettracking  per-line status, carrier, tracking number, ship date.
  cancel_order           ordercancel  real cancel, refused once a tracking number exists.
  cancel_line_items      itemcancel   exposed for callers that cancel part of an order.
  get_invoices           billing      invoiced lines, grouped into invoices here.
  list_shipping_methods  --           not offered; AllPro publishes no code list (below).

**No freight, anywhere, before the order.** Nothing in this API prices shipping up front:
stockcheck returns cost/list/core only. So a quote here carries item cost and availability and
no shipping total, unlike NTW (freight in the quote) and ATD (freight on previewOrder). Freight
first exists after shipment, as ``cost`` on tracking and ``ship_cost`` on billing, which is why
get_invoices can report it and get_shipping_quote cannot.

**One order, one branch.** AllPro rejects a request whose <part> elements carry different
``branch`` values. Our feed spans three warehouses (600 Virginia / 800 Florida / 900 Alabama),
so submit_order sends a blank branch and lets AllPro source the whole order itself rather than
silently splitting a dealer's PO into several distributor orders behind their back.

**No shipping-method picker.** The method travels as a free-text <comment type="ship"> and
AllPro's own documentation asks the integrator to agree the codes with them ("let us know what
code is tied to what services"). Inventing a list would be a box pretending to be a catalog, so
supports_shipping_method_selection() is False until they publish one.
"""
import datetime
import logging
import typing
from decimal import Decimal, InvalidOperation

from src import enums as src_enums
from src import models as src_models
from src.integrations import credentials as credentials_helper
from src.integrations.clients.allpro import exceptions as allpro_exceptions
from src.integrations.clients.allpro import order_client as allpro_client
from src.integrations.orders import base
from src.integrations.orders import exceptions as order_exceptions

logger = logging.getLogger(__name__)
_LOG_PREFIX = "[ALLPRO-ORDER-ADAPTER]"

# status values gettracking returns, per their spec: shipped, new, cancel, returned.
_SHIPPED = "shipped"
_CANCELLED = "cancel"
_RETURNED = "returned"


def _decimal(value: typing.Any) -> typing.Optional[Decimal]:
    if value is None or str(value).strip() == "":
        return None
    try:
        return Decimal(str(value).strip())
    except (InvalidOperation, ValueError):
        return None


def _int(value: typing.Any) -> int:
    try:
        return int(float(str(value).strip()))
    except (TypeError, ValueError):
        return 0


class AllProOrderAdapter(base.DistributorOrderAdapter):
    provider_kind = src_enums.BrandProviderKind.ALLPRO_DISTRIBUTING.value
    fulfillment_channel_name = "api"

    def __init__(
        self,
        company_provider: src_models.CompanyProviders,
        order_account: typing.Optional[src_models.CompanyProviderOrderAccount] = None,
    ) -> None:
        base.DistributorOrderAdapter.__init__(self, company_provider, order_account)
        # The ORDER namespace, not the feed's: AllPro's FTP catalog login is rejected by this
        # API outright (measured -- "Invalid username/password"), and Fulfillment Master issues
        # a separate per-dealer account. ValueError when it is missing, which the registry turns
        # into "no adapter" and the UI into a disabled action.
        self._client = allpro_client.AllProFulfillmentClient(
            credentials=credentials_helper.get_order_credentials(company_provider, order_account)
        )

    # -- helpers -------------------------------------------------------------

    @staticmethod
    def _cart_lines(
        line_items: typing.Sequence[base.OrderLineItemRequest],
    ) -> typing.Tuple[typing.List[typing.Dict], typing.Dict[str, int]]:
        """
        Our line items as AllPro <part> dicts, plus a map from "LINECODE|PARTNO" back to our own
        line_item_id -- AllPro echoes parts by linecode/partno and assigns its own ``linenum``,
        so that pair is the only stable way home.

        ``provider_external_id`` is "<allpro_brand_id>_<part_number>" (see
        master_parts.sync_provider_inventory_from_allpro), and the API wants the LINE CODE rather
        than our brand id, so the AllProParts row is the source for both halves.
        """
        cart: typing.List[typing.Dict] = []
        index: typing.Dict[str, int] = {}
        for item in line_items:
            part = AllProOrderAdapter._allpro_part_for(item.provider_part)
            cart.append({
                "linecode": part.line_code,
                "partno": part.part_number,
                "qtyreq": item.quantity,
            })
            index[AllProOrderAdapter._key(part.line_code, part.part_number)] = item.line_item_id
        return cart, index

    @staticmethod
    def _key(linecode: typing.Any, partno: typing.Any) -> str:
        return "{}|{}".format(str(linecode or "").strip().upper(), str(partno or "").strip().upper())

    @staticmethod
    def _allpro_part_for(provider_part: src_models.ProviderPart) -> src_models.AllProParts:
        external_id = (provider_part.provider_external_id or "").strip()
        brand_id, _, part_number = external_id.partition("_")
        part = None
        if brand_id.isdigit() and part_number:
            part = src_models.AllProParts.objects.filter(
                brand_id=int(brand_id), part_number=part_number
            ).first()
        if not part:
            raise order_exceptions.OrderValidationError(
                "No AllPro catalog row for provider_external_id {!r}.".format(external_id)
            )
        return part

    @staticmethod
    def _ship_to_dict(ship_to: base.ShipToAddress) -> typing.Dict:
        return {
            "customer": ship_to.name,
            "address1": ship_to.address1,
            "address2": ship_to.address2,
            "city": ship_to.city,
            "state": ship_to.state,
            "zip": ship_to.postal_code,
            "phonenumber": ship_to.phone,
            "email": ship_to.email,
        }

    def _handle(self, e: Exception, request_payload: typing.Optional[typing.Dict] = None):
        if isinstance(e, allpro_exceptions.AllProUpstreamNotProvisionedError):
            raise order_exceptions.OrderAdapterNotImplementedError(
                "AllPro's ordering system is not finished connecting this account. Their API "
                "accepts the login and then fails reaching their own inventory system, which "
                "only AllPro can fix -- ask them to provision the Fulfillment Master account. "
                # OrderAdapterNotImplementedError carries no request_payload (it takes a message
                # only); the payload rides on the OrderValidationError paths below instead.
                "(Their message: {})".format(str(e))
            ) from e
        if isinstance(e, allpro_exceptions.AllProAPIAuthError):
            raise order_exceptions.OrderValidationError(
                "AllPro rejected these ordering credentials. Note they are issued separately "
                "from the FTP feed login and are not the same values.",
                request_payload=request_payload,
            )
        if isinstance(e, allpro_exceptions.AllProException):
            raise order_exceptions.OrderValidationError(str(e), request_payload=request_payload)
        raise e

    # -- quote ---------------------------------------------------------------

    def get_shipping_quote(
        self,
        line_items: typing.List[base.OrderLineItemRequest],
        ship_to: base.ShipToAddress,
        ship_method: typing.Optional[str] = None,
    ) -> base.ShippingQuoteResult:
        cart, index = self._cart_lines(line_items)
        request_payload = {"operation": "stockcheck", "branch": "", "lineitems": cart}

        try:
            parts = self._client.stock_check(cart, ship_method=ship_method)
        except Exception as e:
            self._handle(e, request_payload)

        requested = {
            self._key(c["linecode"], c["partno"]): c["qtyreq"] for c in cart
        }
        lines: typing.List[base.ShippingQuoteLine] = []
        fees: typing.List[base.LineFee] = []
        distributor_total = Decimal("0")
        saw_price = False

        for part in parts:
            key = self._key(part.get("linecode"), part.get("partno"))
            line_item_id = index.get(key)
            if line_item_id is None:
                continue
            want = _int(requested.get(key))
            available = _int(part.get("qtyavail"))
            confirmed = min(available, want) if want else available
            cost = _decimal(part.get("cost"))
            core = _decimal(part.get("core"))

            if cost is not None:
                saw_price = True
                distributor_total += cost * confirmed
            if core and core > 0:
                # A core charge is a real per-unit cost AllPro bills on top of the part, so it
                # belongs in the total rather than only in the line detail.
                distributor_total += core * confirmed
                fees.append(base.LineFee(
                    fee_type="core_charge",
                    description="Core charge -- {} {}".format(part.get("linecode"), part.get("partno")),
                    amount=core * confirmed,
                ))

            branch = part.get("branch") or None
            lines.append(base.ShippingQuoteLine(
                line_item_id=line_item_id,
                provider_external_id=str(part.get("partno") or ""),
                quantity_available=confirmed,
                quantity_backordered=max(want - available, 0) if want else 0,
                warehouse_code=branch,
                warehouse_name=allpro_client.BRANCH_NAMES.get(branch or ""),
                distributor_unit_price=cost,
                distributor_line_total=(cost * confirmed) if cost is not None else None,
                # No ship_options on purpose: AllPro prices no shipping before an order exists,
                # so an option here would be an invented number.
                ship_options=[],
            ))

        return base.ShippingQuoteResult(
            lines=lines,
            distributor_total=distributor_total if saw_price else None,
            fees=fees,
            raw_response={"parts": parts},
            request_payload=request_payload,
        )

    # -- submit --------------------------------------------------------------

    def submit_order(
        self,
        purchase_order: src_models.PurchaseOrder,
        line_items: typing.List[base.OrderLineItemRequest],
        ship_to: base.ShipToAddress,
    ) -> base.DistributorOrderResult:
        cart, index = self._cart_lines(line_items)
        po_number = base.resolve_po_number(purchase_order)
        request_payload = {"operation": "ordersend", "ponumber": po_number, "lineitems": cart}

        try:
            result = self._client.place_order(
                cart,
                po_number=po_number,
                ship_to=self._ship_to_dict(ship_to),
                ship_method=purchase_order.ship_method or None,
            )
        except Exception as e:
            self._handle(e, request_payload)

        placements: typing.List[base.LineItemPlacement] = []
        order_numbers: typing.List[str] = []
        failures: typing.List[str] = []

        for line in result.get("lines") or []:
            key = self._key(line.get("linecode"), line.get("partno"))
            line_item_id = index.get(key)
            order_no = line.get("orderno") or ""
            errcode = (line.get("errcode") or "").strip().lower()
            if errcode and errcode != "success":
                failures.append("{} {}: {}".format(
                    line.get("linecode"), line.get("partno"), line.get("errcode")))
            if order_no and order_no not in order_numbers:
                order_numbers.append(order_no)
            if line_item_id is None:
                continue
            placements.append(base.LineItemPlacement(
                line_item_id=line_item_id,
                distributor_order_number=order_no,
                quantity_confirmed=_int(line.get("qtysup")),
                quantity_backordered=max(_int(line.get("qtyreq")) - _int(line.get("qtysup")), 0),
                warehouse_code=line.get("branch"),
                status_code=line.get("errcode"),
            ))

        if not order_numbers:
            raise order_exceptions.OrderValidationError(
                "AllPro accepted the request but returned no order number{}.".format(
                    " ({})".format("; ".join(failures)) if failures else ""
                ),
                request_payload=request_payload,
            )
        if failures:
            # Partial acceptance is real here: AllPro confirms line by line, so some lines can
            # carry an order number while others report an error on the same submission.
            logger.warning("{} PO {} partially accepted: {}".format(
                _LOG_PREFIX, po_number, "; ".join(failures)))

        return base.DistributorOrderResult(
            distributor_order_numbers=order_numbers,
            line_item_placements=placements,
            raw_response=result,
            request_payload=request_payload,
        )

    # -- status --------------------------------------------------------------

    def get_order_status(self, purchase_order: src_models.PurchaseOrder) -> base.OrderStatusResult:
        # gettracking is keyed by OUR PO number, not by AllPro's order numbers -- the request
        # attribute is spelled invoicenumber but holds the PO (their own spec flags the holdover).
        po_number = base.resolve_po_number(purchase_order)
        try:
            tracking = self._client.get_tracking([po_number])
        except Exception as e:
            self._handle(e)

        entry = tracking.get(po_number) or {}
        items = entry.get("items") or []

        statuses: typing.List[base.DistributorOrderStatus] = []
        for distributor_order in purchase_order.distributor_orders.all():
            tracking_numbers = []
            carrier = None
            ship_date = None
            statuses_seen = set()
            for item in items:
                number = item.get("trackingnumber")
                if number and number not in tracking_numbers:
                    tracking_numbers.append(number)
                carrier = carrier or item.get("service")
                ship_date = ship_date or item.get("ship_date")
                statuses_seen.add((item.get("status") or "").strip().lower())

            statuses.append(base.DistributorOrderStatus(
                distributor_order_number=distributor_order.distributor_order_number,
                status_code=self._roll_up(statuses_seen, entry.get("status")),
                tracking_numbers=tracking_numbers,
                carrier=carrier or entry.get("service"),
                ship_date=ship_date,
                raw_response=entry,
            ))
        return base.OrderStatusResult(orders=statuses)

    @staticmethod
    def _roll_up(statuses: typing.Set[str], order_level: typing.Optional[str]) -> str:
        """
        One status for the order out of its lines. Anything still unshipped keeps the order out
        of "shipped" -- reporting a partially shipped order as shipped is the error that stops a
        dealer chasing the rest of it.
        """
        statuses = {s for s in statuses if s}
        if not statuses:
            return (order_level or "new").strip().lower() or "new"
        if statuses == {_CANCELLED}:
            return _CANCELLED
        if statuses == {_RETURNED}:
            return _RETURNED
        if statuses <= {_SHIPPED, _CANCELLED, _RETURNED}:
            return _SHIPPED
        return "new"

    # -- cancel --------------------------------------------------------------

    def supports_cancel(self) -> bool:
        return True

    def cancel_order(self, purchase_order: src_models.PurchaseOrder) -> bool:
        po_number = base.resolve_po_number(purchase_order)
        try:
            results = self._client.cancel_order([po_number])
        except Exception as e:
            self._handle(e)

        if not results:
            raise order_exceptions.OrderValidationError(
                "AllPro returned no cancellation result for PO {}.".format(po_number)
            )
        refused = [r for r in results if r.get("error")]
        if refused:
            # Their refusals are business outcomes, not transport failures: an order that already
            # has a tracking number cannot be cancelled, and the dealer needs to hear why.
            raise order_exceptions.OrderValidationError(
                "; ".join(r.get("msg") or "AllPro refused the cancellation" for r in refused)
            )
        return True

    def cancel_line_items(
        self, purchase_order: src_models.PurchaseOrder,
        line_items: typing.List[base.OrderLineItemRequest],
    ) -> typing.List[typing.Dict]:
        """
        Cancels individual lines and leaves the rest of the order standing. Not part of
        DistributorOrderAdapter -- no other distributor here offers it -- so callers reach for it
        explicitly. AllPro re-packages whatever remains into a new shipment record automatically.
        """
        cart, _ = self._cart_lines(line_items)
        try:
            return self._client.cancel_line_items(base.resolve_po_number(purchase_order), cart)
        except Exception as e:
            self._handle(e)

    # -- shipping methods ----------------------------------------------------

    def supports_shipping_method_selection(self) -> bool:
        return False

    # -- invoices ------------------------------------------------------------

    def supports_invoices(self) -> bool:
        return True

    def get_invoices(self, purchase_order: src_models.PurchaseOrder) -> typing.List[base.DistributorInvoice]:
        """
        Invoices for this PO, assembled from billing's flat per-line JSON.

        AllPro returns one row per invoiced line item with the invoice number repeated, so the
        grouping into invoices happens here. Two of their fields need handling rather than
        copying: ``extended_amount`` is documented as always null, so the line total is computed
        as unit_cost x qty; and a negative-quantity line is a credit, reported by them as
        ``credit_qty`` with ``qty`` empty.
        """
        po_number = base.resolve_po_number(purchase_order)
        try:
            rows = self._client.get_billing(customer_po=po_number)
        except Exception as e:
            self._handle(e)

        grouped: typing.Dict[str, typing.List[typing.Dict]] = {}
        for row in rows or []:
            grouped.setdefault(str(row.get("invoice_number") or "").strip() or po_number, []).append(row)

        invoices = []
        for invoice_number, entries in grouped.items():
            line_items = []
            total = Decimal("0")
            freight = Decimal("0")
            invoice_date = None
            for row in entries:
                invoice_date = invoice_date or _date_or_none(row.get("invoice_date"))
                unit_cost = _decimal(row.get("unit_cost"))
                core = _decimal(row.get("unit_core_cost"))
                credit_qty = _int(row.get("credit_qty"))
                # credit_qty is populated INSTEAD of qty on a return, and is the negative side of
                # the invoice -- counting it as a positive shipment would overstate what is owed.
                quantity = -credit_qty if credit_qty else _int(row.get("qty"))
                unit_total = (unit_cost or Decimal("0")) + (core or Decimal("0"))
                line_total = unit_total * quantity
                total += line_total
                ship_cost = _decimal(row.get("ship_cost"))
                if ship_cost:
                    freight += ship_cost
                line_items.append(base.InvoiceLineItem(
                    part_number=row.get("partno"),
                    description=row.get("sku"),
                    quantity=quantity,
                    unit_price=unit_cost,
                    total_price=line_total,
                ))
            invoices.append(base.DistributorInvoice(
                invoice_number=invoice_number,
                invoice_date=invoice_date,
                website_order_number=po_number,
                total_price=total + freight,
                freight=freight or None,
                line_items=line_items,
                comments=(
                    "Assembled from AllPro's billing API, which returns one row per invoiced "
                    "line. Line totals are unit_cost x qty because their extended_amount field "
                    "is always null."
                ),
                raw_response={"rows": entries},
            ))
        return invoices


def _date_or_none(value: typing.Any) -> typing.Optional[datetime.date]:
    return allpro_client._date(value)
