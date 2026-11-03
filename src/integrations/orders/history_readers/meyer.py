"""
MeyerOrderHistoryReader -- discovers Meyer orders, using the one native origin flag any of the
four in-scope distributors actually has: SalesCurrent/SalesHistory's "API Order" field states
directly whether an order went through our API ("Yes") or not ("No"). See
docs/tasks/external-orders/reference/meyer-order-reads.md for the vendor-doc spec this is built
against.

VERIFIED LIVE from the production host on 2026-11-03, which corrected four field names the
vendor PDF gets wrong. Trust the list below over the PDF; the PDF cannot be reached from a dev
machine to check, since both calls need a SOCKS5 proxy only production can reach.

  - ``CreateDate``, not ``DocDate``. DocDate does not exist on either call.
  - ``ExtendedPrice``, not ``ExtPrice``.
  - ``CustomerPO`` IS returned by both calls; the PDF documents it only on
    SalesOrderDetail/Invoice.
  - A structured ship-to IS returned by both -- ``ShipToName``, ``Address1``, ``Address2``,
    ``City``, ``State``, ``ZipCode``, ``Country``. The PDF lists none of them here.
  - ``LocationCode`` is documented but absent from every live response observed.
  - Line fields (``ItemNumber``/``Quantity``/``UnitPrice``/``ExtendedPrice``) come back from
    SalesHistory ONLY. SalesCurrent returns headers alone, though the PDF documents lines on
    both.
  - Tracking is ``TrackingNumbers`` (a LIST) on SalesHistory but ``Tracking_Number`` (singular)
    on SalesCurrent. Both are read; one order really does ship in several packages.
  - Neither listing carries an invoice number, freight, tax or a real order total. GET /Invoice
    does, which is what enrich_new_order goes after.
  - SalesCurrent gives a header and nothing else, so an OPEN order has no lines until
    GET /SalesOrderDetail is asked for them -- Items[] with ItemNumber, LineSequence, Quantity,
    UnitPrice, TotalPrice and Kit.

Believing the PDF cost 29 Meyer orders ingested as empty shells -- right order numbers and
origin flags, every other field null -- before the first production run exposed it.
"""
import datetime
import decimal
import logging
import typing

from django.conf import settings

from src import models as src_models
from src.integrations import credentials as credentials_helper
from src.integrations.clients.meyer import exceptions as meyer_client_exceptions
from src.integrations.clients.meyer.order_client import MeyerOrderApiClient
from src.integrations.orders import history
from src.integrations.orders import meyer as meyer_orders

logger = logging.getLogger(__name__)
_LOG_PREFIX = "[MEYER-ORDER-HISTORY]"


def _to_decimal(value: typing.Any) -> typing.Optional[decimal.Decimal]:
    if value in (None, ""):
        return None
    try:
        return decimal.Decimal(str(value))
    except (decimal.InvalidOperation, ValueError, TypeError):
        return None


def _quantity(value: typing.Any) -> typing.Optional[typing.Union[int, float]]:
    """Meyer sends Quantity as a fixed-point STRING ("1.00000"), where the other three readers
    send a real number. contract.md types ExternalOrderLine.quantity as a number, and the UI
    renders it straight into a table cell -- so left alone it would read "1.00000" to a customer.
    Whole values come back as int so the common case renders as "1", not "1.0"."""
    if value is None or value == "":
        return None
    try:
        number = decimal.Decimal(str(value))
    except (decimal.InvalidOperation, ValueError):
        return None
    return int(number) if number == number.to_integral_value() else float(number)


def _parse_create_date(value: typing.Optional[str]) -> typing.Optional[datetime.date]:
    """``CreateDate`` is an ISO datetime whose time component is always midnight -- only the date
    part is ever meaningful, matching ExternalPurchaseOrder.order_date's DateField.

    The field is CreateDate, NOT the DocDate the vendor PDF documents: measured against the live
    production API, SalesCurrent and SalesHistory both return CreateDate and no DocDate at all.
    The PDF is wrong about several of these names -- see this module's docstring."""
    if not value:
        return None
    try:
        return datetime.date.fromisoformat(value.strip()[:10])
    except ValueError:
        return None


def _warehouse_names() -> typing.Dict[str, str]:
    """{LocationCode: "City, ST"} from MeyerLocation (populated by fetch_meyer_locations from
    GET /Warehouses) -- same decode source src.integrations.orders.meyer's own, module-private
    _load_meyer_warehouse_names() uses for the write-side adapter; duplicated here (a 3-line
    query) rather than importing that private helper across modules, since this is the only
    distributor the research dossier calls out as having a usable warehouse-name table for this
    feature (see contract.md's ExternalOrderLine.warehouse_name note)."""
    names: typing.Dict[str, str] = {}
    for row in src_models.MeyerLocation.objects.all().values("external_id", "city", "state"):
        external_id = (row.get("external_id") or "").strip()
        if not external_id:
            continue
        label = ", ".join(part for part in (row.get("city"), row.get("state")) if part)
        if label:
            names[external_id] = label
    return names


def _group_rows_by_order(rows: typing.List[typing.Dict]) -> typing.Dict[str, typing.List[typing.Dict]]:
    """SalesCurrent/SalesHistory return one row per line item with header fields repeated --
    group by OrderNumber before building one ExternalOrderResult per order (see the vendor
    doc's Response shape section)."""
    by_order: typing.Dict[str, typing.List[typing.Dict]] = {}
    for row in rows:
        order_number = (row.get("OrderNumber") or "").strip()
        if not order_number:
            continue
        by_order.setdefault(order_number, []).append(row)
    return by_order


def _build_order_result(
    order_number: str, rows: typing.List[typing.Dict], status_label: str, warehouse_names: typing.Dict[str, str]
) -> history.ExternalOrderResult:
    header = rows[0]
    # "API Order": "No" is authoritative for Meyer -- the whole reason this distributor doesn't
    # need PO-number-match inference like the other three (see src.enums.
    # ExternalOrderOriginSignal / external_order_sync._apply_results).
    api_order_flag = (header.get("API Order") or "").strip().lower() == "yes"

    # LineSequence, not ItemNumber, is the real per-line key -- the same item can appear twice on
    # one order (e.g. split across two shipments), so this groups by OrderNumber only and keeps
    # every row as its own line rather than merging same-ItemNumber rows together.
    #
    # Only SalesHistory carries line fields. SalesCurrent (open orders) returns headers ONLY --
    # no ItemNumber/Quantity/UnitPrice/ExtendedPrice at all, measured live, contrary to the
    # vendor PDF which documents them on both. So a row with no ItemNumber is skipped rather
    # than emitted as an all-null line: 29 Meyer orders shipped as rows of all-None line items
    # the first time this ran, which is worse than an honestly empty list.
    line_items = []
    for row in rows:
        part_number = row.get("ItemNumber")
        if not part_number:
            continue
        unit_price = _to_decimal(row.get("UnitPrice"))
        # ExtendedPrice, not the vendor PDF's ExtPrice -- that key does not exist live.
        line_total = _to_decimal(row.get("ExtendedPrice"))
        # LocationCode is documented by the PDF but absent from every live response observed, so
        # this is normally None; kept (and decoded when present) rather than dropped, since it
        # costs one small query and starts working the moment Meyer includes it.
        warehouse_code = row.get("LocationCode")
        line_items.append(
            {
                "part_number": part_number,
                "description": None,
                "quantity": _quantity(row.get("Quantity")),
                "unit_price": float(unit_price) if unit_price is not None else None,
                "line_total": float(line_total) if line_total is not None else None,
                "warehouse_code": warehouse_code,
                "warehouse_name": warehouse_names.get(warehouse_code) if warehouse_code else None,
            }
        )

    line_totals = [li["line_total"] for li in line_items if li["line_total"] is not None]
    subtotal = round(sum(line_totals), 2) if line_totals else None

    # Both endpoints DO carry the customer PO and a structured ship-to, contrary to the vendor
    # PDF, which documents neither on these two calls. Meyer still never needs PO-matching for
    # dedup ("API Order" is authoritative), but the PO is what a human recognises an order by --
    # Trident's read real things like "TRU107550" and "FRIDAY DELIVERY".
    ship_to = {
        "name": header.get("ShipToName"),
        "address1": header.get("Address1"),
        "address2": header.get("Address2") or None,
        "city": header.get("City"),
        "state": header.get("State"),
        "postal_code": header.get("ZipCode"),
        "country": header.get("Country"),
        "phone": None,
    }
    if not any(value for value in ship_to.values()):
        ship_to = None

    # Tracking rides on the listing itself -- no extra call. SalesHistory names it
    # "TrackingNumbers" and gives a LIST (one order genuinely ships in several packages:
    # ['879615150664', '879615150675'] measured live); SalesCurrent names it "Tracking_Number"
    # and gives a single string. Both shapes are read, and the carrier is the order's own
    # ShipMethod, which Meyer gives as a plain name ("FEDEX HOME", "UPS GRND RES") rather than a
    # code needing a lookup.
    ship_method = (header.get("ShipMethod") or "").strip() or None
    tracking = []
    seen_tracking = set()
    for row in rows:
        numbers = row.get("TrackingNumbers")
        if isinstance(numbers, str):
            numbers = [numbers]
        for candidate in list(numbers or []) + [row.get("Tracking_Number")]:
            number = str(candidate or "").strip()
            if not number or number in seen_tracking:
                continue
            seen_tracking.add(number)
            tracking.append({"tracking_number": number, "carrier": ship_method})

    return history.ExternalOrderResult(
        distributor_order_number=order_number,
        customer_po_number=header.get("CustomerPO") or None,
        order_date=_parse_create_date(header.get("CreateDate")),
        status=status_label,
        ship_to=ship_to,
        subtotal=decimal.Decimal(str(subtotal)) if subtotal is not None else None,
        # Neither listing carries freight or a real order total -- the subtotal stands in until
        # enrich_new_order's GET /Invoice fills both in with Meyer's own figures.
        freight=None,
        total=decimal.Decimal(str(subtotal)) if subtotal is not None else None,
        line_items=line_items,
        raw_response={"rows": rows},
        api_order_flag=api_order_flag,
        tracking=tracking,
        ship_method_code=ship_method,
        ship_method=ship_method,
    )


class MeyerOrderHistoryReader(history.OrderHistoryReader):
    def __init__(
        self,
        company_provider: src_models.CompanyProviders,
        order_account: src_models.CompanyProviderOrderAccount,
    ) -> None:
        history.OrderHistoryReader.__init__(self, company_provider, order_account)
        environment = getattr(settings, "MEYER_ORDER_ENVIRONMENT", "testing")
        self._client = MeyerOrderApiClient(
            credentials=credentials_helper.get_order_credentials(company_provider, order_account),
            environment=environment,
        )

    def list_orders(
        self, start_date: datetime.date, end_date: datetime.date
    ) -> typing.List[history.ExternalOrderResult]:
        """SalesHistory -- all INVOICED orders whose DocDate falls in [start_date, end_date].
        This is the only one of Meyer's two listing calls the watermark window actually bounds
        (see external_order_sync._collect_results and research-be.md's "Meyer is a genuine
        special case")."""
        try:
            rows = self._client.get_sales_history(
                customer_number=self._client.customer_number,
                start_date=start_date.strftime("%Y-%m-%d"),
                end_date=end_date.strftime("%Y-%m-%d"),
            )
        except meyer_client_exceptions.MeyerException as e:
            logger.error(
                "{} get_sales_history failed for company_provider_id={} order_account_id={}: {}".format(
                    _LOG_PREFIX, self.company_provider.id, self.order_account.id, e
                )
            )
            raise
        warehouse_names = _warehouse_names()
        return [
            _build_order_result(order_number, order_rows, "Invoiced", warehouse_names)
            for order_number, order_rows in _group_rows_by_order(rows).items()
        ]

    def _apply_order_detail(self, result: history.ExternalOrderResult) -> history.ExternalOrderResult:
        """Status, delivery estimate, and -- for an order SalesCurrent could only give a header
        for -- its line items and freight.

        Meyer has no order-status field. The listing can only say which endpoint found the order:
        SalesHistory means invoiced, SalesCurrent means open. SalesOrderDetail's DeliveryETA says
        something better, and says it for almost every order: the literal "Delivered", or a date,
        or a business-day range, or "Unavailable" for a non-Meyer shipment it cannot see.

        So status becomes Delivered where Meyer says so, and falls back to the listing's own
        Invoiced/Open otherwise -- a billing fact, which is all there is to say until it arrives.

        Best-effort: a failure leaves the order exactly as the listing described it."""
        try:
            details = self._client.get_sales_order_detail(result.distributor_order_number, self._client.customer_number)
        except meyer_client_exceptions.MeyerException:
            logger.info(
                "{} get_sales_order_detail failed for newly-discovered order {}; no line items.".format(
                    _LOG_PREFIX, result.distributor_order_number
                )
            )
            return result
        if not details:
            return result

        detail = details[0]
        line_items = []
        subtotal = decimal.Decimal("0")
        for item in detail.get("Items") or []:
            part_number = item.get("ItemNumber")
            if not part_number:
                continue
            unit_price = _to_decimal(item.get("UnitPrice"))
            line_total = _to_decimal(item.get("TotalPrice"))
            if line_total is not None:
                subtotal += line_total
            line_items.append(
                {
                    "part_number": part_number,
                    "description": None,
                    "quantity": _quantity(item.get("Quantity")),
                    "unit_price": float(unit_price) if unit_price is not None else None,
                    "line_total": float(line_total) if line_total is not None else None,
                    # SalesOrderDetail carries no warehouse either -- same LocationCode absence
                    # as the listings (see this module's docstring).
                    "warehouse_code": None,
                    "warehouse_name": None,
                }
            )

        # Status and ETA apply whether or not this order needed lines.
        eta_raw = (detail.get("DeliveryETA") or "").strip()
        if eta_raw.lower() == "delivered":
            result.status = "Delivered"
        elif (detail.get("Invoiced") or "").strip().lower() == "yes":
            result.status = "Invoiced"
        elif detail.get("Invoiced"):
            result.status = "Open"
        if eta_raw and eta_raw.lower() not in ("delivered", "unavailable"):
            # Reuses the write-side parser, which already handles both shapes Meyer sends here --
            # a literal date and an "N-M Business Days" range.
            estimated, _days = meyer_orders._parse_meyer_delivery(eta_raw)
            result.estimated_delivery_date = estimated
        result.raw_response = dict(result.raw_response or {}, sales_order_detail=detail)

        if not line_items:
            return result

        result.line_items = line_items
        if result.subtotal is None:
            result.subtotal = subtotal
        freight = _to_decimal(detail.get("Freight"))
        if freight is not None:
            result.freight = freight
        if result.total is None:
            result.total = subtotal + (freight or decimal.Decimal("0"))
        return result

    def enrich_new_order(self, result: history.ExternalOrderResult) -> history.ExternalOrderResult:
        """
        One GET /Invoice per newly-discovered order, never on a refresh of one already stored.

        SalesCurrent/SalesHistory carry no invoice number, no freight, no tax and no order total
        -- the "total" a listing yields is just its own lines added up, which is short by
        whatever Meyer charged to ship it. GET /Invoice carries Meyer's own figures: InvoiceNumber,
        Freight, Tax, MiscCharge, TradeDiscount and Total.

        Best-effort, like every other enrichment here. A 70202 ("No invoice found") is the normal
        answer for an order that has not invoiced yet, not a failure -- the order keeps the
        listing's own numbers and picks the invoice up on a later poll once it exists.
        """
        # GET /SalesOrderDetail is called for every newly-discovered order, not just the
        # line-less ones. It is the only place Meyer reports DeliveryETA, and that turns out to
        # be the status worth showing: measured across all 28 of Trident's Meyer orders, 27 read
        # "Delivered" while the listing could only say "Invoiced". It also carries Items[] for an
        # open order, whose listing gives a header and nothing else.
        result = self._apply_order_detail(result)

        try:
            invoices = self._client.get_invoice(result.distributor_order_number, self._client.customer_number)
        except meyer_client_exceptions.MeyerException:
            logger.info(
                "{} no invoice yet for newly-discovered order {}; keeping listing totals.".format(
                    _LOG_PREFIX, result.distributor_order_number
                )
            )
            return result
        if not invoices:
            return result

        invoice = invoices[0]
        invoice_numbers = []
        for entry in invoices:
            number = str(entry.get("InvoiceNumber") or "").strip()
            if number and number not in invoice_numbers:
                invoice_numbers.append(number)

        freight = _to_decimal(invoice.get("Freight"))
        total = _to_decimal(invoice.get("Total"))
        result.distributor_invoice_numbers = invoice_numbers
        if freight is not None:
            result.freight = freight
        # Meyer's own Total already includes freight, tax and any trade discount, so it replaces
        # the summed-lines figure outright rather than being added to it.
        if total is not None:
            result.total = total
        return result

    def list_current_orders(self) -> typing.List[history.ExternalOrderResult]:
        """SalesCurrent -- ALL open (non-invoiced) orders, no date parameter at all. Called
        unconditionally on every run regardless of watermark (see external_order_sync's module
        docstring) -- there is nothing to make this one incremental."""
        try:
            rows = self._client.get_sales_current(customer_number=self._client.customer_number)
        except meyer_client_exceptions.MeyerException as e:
            logger.error(
                "{} get_sales_current failed for company_provider_id={} order_account_id={}: {}".format(
                    _LOG_PREFIX, self.company_provider.id, self.order_account.id, e
                )
            )
            raise
        warehouse_names = _warehouse_names()
        return [
            _build_order_result(order_number, order_rows, "Open", warehouse_names)
            for order_number, order_rows in _group_rows_by_order(rows).items()
        ]
