"""
Turn14OrderHistoryReader -- discovers Turn 14 orders we did not place, via the existing
``get_orders(start_date, end_date, page)`` bulk listing (already implemented on
Turn14OrderApiClient; no new client method needed -- see research-be.md's Files-to-change
table). Live-confirmed against company 16 (company_provider_id=14): a 60-day window returned 5
orders, 0 of which matched any stored PurchaseOrder/PurchaseOrderDistributorOrder row -- the
``attributes`` shape below (keys, field names, ``ship_to_address``/``bill_to_address`` being
single opaque strings) is read directly off that live response, not guessed from docs.
"""
import datetime
import decimal
import logging
import typing

from django.conf import settings

from src import models as src_models
from src.integrations import credentials as credentials_helper
from src.integrations.clients.turn_14 import exceptions as turn14_client_exceptions
from src.integrations.clients.turn_14.order_client import Turn14OrderApiClient
from src.integrations.orders import history
from src.integrations.orders import turn_14 as turn14_orders

logger = logging.getLogger(__name__)
_LOG_PREFIX = "[TURN14-ORDER-HISTORY]"


def _to_decimal(value: typing.Any) -> typing.Optional[decimal.Decimal]:
    if value is None:
        return None
    try:
        return decimal.Decimal(str(value))
    except (decimal.InvalidOperation, ValueError, TypeError):
        return None


def _parse_turn14_date(value: typing.Optional[str]) -> typing.Optional[datetime.date]:
    """attributes.date is "YYYY-MM-DD HH:MM:SS" (confirmed live -- the time component observed
    was always "00:00:00", same as Meyer's DocDate) -- only the date part is ever meaningful,
    matching ExternalPurchaseOrder.order_date's DateField."""
    if not value:
        return None
    try:
        return datetime.date.fromisoformat(value.strip()[:10])
    except ValueError:
        return None


def _build_line_items(
    lines: typing.List[typing.Dict], warehouse_names: typing.Optional[typing.Dict[str, str]] = None
) -> typing.List[typing.Dict]:
    """attributes.lines[] keys confirmed live: part_number, part_description, quantity, price,
    total, open_quantity, delivered_quantity, total_fulfilled_quantity, location_id, item_id,
    line_num, invoiced_quantity, esd. An invoice's own lines are a subset of the same shape
    (item_id, location_id, part_description, part_number, quantity, total_price, unit_price),
    so both sources build lines through here.

    location_id decodes through the write-side module's own Turn14Location lookup, so a
    discovered order names the same warehouse an order we placed does."""
    warehouse_names = warehouse_names if warehouse_names is not None else turn14_orders.load_warehouse_names()
    line_items = []
    for line in lines or []:
        # Orders call them price/total; invoices call them unit_price/total_price.
        unit_price = _to_decimal(line.get("price") if line.get("price") is not None else line.get("unit_price"))
        line_total = _to_decimal(line.get("total") if line.get("total") is not None else line.get("total_price"))
        line_items.append(
            {
                "part_number": line.get("part_number"),
                "description": line.get("part_description"),
                "quantity": line.get("quantity"),
                "unit_price": float(unit_price) if unit_price is not None else None,
                "line_total": float(line_total) if line_total is not None else None,
                "warehouse_code": line.get("location_id"),
                "warehouse_name": warehouse_names.get(str(line.get("location_id") or "").strip()),
            }
        )
    return line_items


def _tracking_entries(raw: typing.Any) -> typing.List[typing.Dict]:
    """attributes.tracking[] is [{ship_method, tracking_number}] -- present on 158 of 163 live
    invoices and on none of the open orders, which have not shipped yet."""
    entries = []
    seen = set()
    for item in raw or []:
        number = str((item or {}).get("tracking_number") or "").strip()
        if not number or number in seen:
            continue
        seen.add(number)
        entries.append({"tracking_number": number, "carrier": (item or {}).get("ship_method") or None})
    return entries


def _build_from_invoices(
    website_order_number: str, invoices: typing.List[typing.Dict], warehouse_names: typing.Dict[str, str]
) -> history.ExternalOrderResult:
    """One order built from every invoice Turn 14 raised against it.

    GET /invoices is where Turn 14's order history actually lives: over 90 live days on Trident's
    account it returned 163 invoices against 4 orders from GET /orders, because that endpoint
    only lists orders still OPEN. The invoice carries what the order needs -- date, freight,
    total, purchase_order_number, the ship-to, priced lines with a warehouse, and tracking with
    a carrier on 158 of the 163.

    An order can invoice more than once when it ships in parts, so lines and tracking are
    collected across all of them and the money is summed rather than taken from one.
    """
    primary = invoices[0]
    first = primary.get("attributes") or {}

    line_items = []
    tracking = []
    seen_tracking = set()
    invoice_numbers = []
    subtotal = decimal.Decimal("0")
    freight = decimal.Decimal("0")
    total = decimal.Decimal("0")
    ship_method = None
    for invoice in invoices:
        attrs = invoice.get("attributes") or {}
        line_items.extend(_build_line_items(attrs.get("lines") or [], warehouse_names))
        for entry in _tracking_entries(attrs.get("tracking")):
            if entry["tracking_number"] in seen_tracking:
                continue
            seen_tracking.add(entry["tracking_number"])
            tracking.append(entry)
            ship_method = ship_method or entry["carrier"]
        number = str(attrs.get("invoice_number") or "").strip()
        if number and number not in invoice_numbers:
            invoice_numbers.append(number)
        freight += _to_decimal(attrs.get("freight")) or decimal.Decimal("0")
        total += _to_decimal(attrs.get("total_price")) or decimal.Decimal("0")

    subtotal_candidates = [li["line_total"] for li in line_items if li["line_total"] is not None]
    subtotal = decimal.Decimal(str(round(sum(subtotal_candidates), 2))) if subtotal_candidates else None

    return history.ExternalOrderResult(
        distributor_order_number=str(website_order_number),
        customer_po_number=first.get("purchase_order_number"),
        order_date=_parse_turn14_date(first.get("date")),
        # Turn 14's orders listing has a real status field; an invoice does not, because being
        # invoiced IS the status. Said in its own word rather than mapped.
        status="Invoiced",
        ship_to={"address1": first.get("ship_to_address")} if first.get("ship_to_address") else None,
        subtotal=subtotal,
        freight=freight or None,
        total=total or None,
        line_items=line_items,
        raw_response={"invoices": invoices},
        distributor_invoice_numbers=invoice_numbers,
        tracking=tracking,
        ship_method_code=ship_method,
        ship_method=ship_method,
    )


def _build_order_result(
    attrs: typing.Dict, entry: typing.Dict, warehouse_names: typing.Optional[typing.Dict[str, str]] = None
) -> typing.Optional[history.ExternalOrderResult]:
    website_order_number = attrs.get("website_order_number")
    if website_order_number is None:
        return None

    line_items = _build_line_items(attrs.get("lines") or [], warehouse_names)
    subtotal_candidates = [li["line_total"] for li in line_items if li["line_total"] is not None]
    subtotal = round(sum(subtotal_candidates), 2) if subtotal_candidates else None

    return history.ExternalOrderResult(
        distributor_order_number=str(website_order_number),
        customer_po_number=attrs.get("purchase_order_number"),
        order_date=_parse_turn14_date(attrs.get("date")),
        status=attrs.get("status"),
        # ship_to_address is one opaque "\r"-delimited string (confirmed live --
        # "Name\rAddress1\rAddress2-or-company\rCity State  Zip\rCOUNTRY\rPhone", no field
        # marker between a possible company-name line and the real address lines) -- stored as
        # address1 verbatim rather than mis-parsed into the wrong structured field, per
        # contract.md's explicit instruction for this exact shape.
        ship_to={"address1": attrs.get("ship_to_address")} if attrs.get("ship_to_address") else None,
        subtotal=decimal.Decimal(str(subtotal)) if subtotal is not None else None,
        freight=_to_decimal(attrs.get("freight")),
        total=_to_decimal(attrs.get("total")),
        line_items=line_items,
        raw_response=entry,
        tracking=_tracking_entries(attrs.get("tracking")),
    )


class Turn14OrderHistoryReader(history.OrderHistoryReader):
    def __init__(
        self,
        company_provider: src_models.CompanyProviders,
        order_account: src_models.CompanyProviderOrderAccount,
    ) -> None:
        history.OrderHistoryReader.__init__(self, company_provider, order_account)
        environment = getattr(settings, "TURN14_ORDER_ENVIRONMENT", "testing")
        self._client = Turn14OrderApiClient(
            credentials=credentials_helper.get_order_credentials(company_provider, order_account),
            environment=environment,
        )

    def list_orders(
        self, start_date: datetime.date, end_date: datetime.date
    ) -> typing.List[history.ExternalOrderResult]:
        """
        Invoices first, then the open orders they do not cover.

        GET /orders lists only orders still OPEN -- 4 of them over 90 live days on Trident's
        account, against 163 invoices for the same window. So the invoice listing is where the
        history actually is, and GET /orders contributes the handful that have not invoiced yet.
        Both are paginated and both are walked to the last page.
        """
        start_str, end_str = start_date.isoformat(), end_date.isoformat()
        warehouse_names = turn14_orders.load_warehouse_names()

        by_order: typing.Dict[str, typing.List[typing.Dict]] = {}
        try:
            for invoice in self._paged(self._client.get_invoices, start_str, end_str, "get_invoices"):
                attrs = invoice.get("attributes") or {}
                website_order_number = str(attrs.get("website_order_number") or "").strip()
                if not website_order_number:
                    # 5 of 163 live invoices carry no order number -- a credit or an adjustment
                    # with no order behind it. Nothing to attach them to.
                    continue
                by_order.setdefault(website_order_number, []).append(invoice)
        except turn14_client_exceptions.Turn14APIException as e:
            # Not fatal: the open-order listing below still discovers orders, just fewer.
            logger.warning(
                "{} get_invoices failed for company_provider_id={} order_account_id={}: {} -- "
                "falling back to open orders only.".format(
                    _LOG_PREFIX, self.company_provider.id, self.order_account.id, e
                )
            )

        results = [
            _build_from_invoices(website_order_number, invoices, warehouse_names)
            for website_order_number, invoices in by_order.items()
        ]

        try:
            entries = list(self._paged(self._client.get_orders, start_str, end_str, "get_orders"))
        except turn14_client_exceptions.Turn14APIException:
            if results:
                return results
            raise
        for entry in entries:
            attrs = entry.get("attributes") or {}
            if str(attrs.get("website_order_number") or "").strip() in by_order:
                continue
            result = _build_order_result(attrs, entry, warehouse_names)
            if result is not None:
                results.append(result)
        return results

    def _paged(self, fetch: typing.Callable, start_str: str, end_str: str, label: str) -> typing.Iterator[typing.Dict]:
        """Walks a Turn 14 listing to its last page. meta.total_pages is authoritative; both
        endpoints page at 100."""
        page, total_pages = 1, 1
        while page <= total_pages:
            try:
                response = fetch(start_date=start_str, end_date=end_str, page=page)
            except turn14_client_exceptions.Turn14APIException as e:
                logger.error(
                    "{} {} failed for company_provider_id={} order_account_id={} page={}: {}".format(
                        _LOG_PREFIX, label, self.company_provider.id, self.order_account.id, page, e
                    )
                )
                raise
            for entry in response.get("data", []) or []:
                yield entry
            total_pages = (response.get("meta") or {}).get("total_pages", 1)
            page += 1
