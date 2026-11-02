"""
KeystoneOrderHistoryReader -- discovers Keystone orders we did not place, via the existing
``get_order_history(po_number, from_date, to_date)`` SOAP call (already implemented on
KeystoneOrderApiClient; no new client method needed -- see research-be.md's Files-to-change
table). Called with ``po_number=""`` and a date range, which Keystone's own docs document as a
valid "list everything in this window" call (PONumber alone is what narrows it to one order;
blank + a date range is the discovery case this feature needs).

UNVERIFIED LIVE: this dev machine's IP isn't on Keystone's allowlist (SOAP fault "*** Illegal
use of this web service !!! ***", confirmed live -- contract.md's "Verification blockers").
Every parsing decision below reuses field names already confirmed live by the WRITE-side
adapter's own GetOrderHistory parsing (src.integrations.orders.keystone -- EKORD#/EKKEY#/EKSTAT/
etc, including the "#"-as-"_x0023_" XML name-mangling that module's client already handles), not
re-confirmed against a fresh discovery-shaped call. Must be re-run from an allowlisted host
before this is trusted unattended.
"""
import datetime
import decimal
import logging
import typing

from src import models as src_models
from src.integrations import credentials as credentials_helper
from src.integrations.clients.keystone import exceptions as keystone_client_exceptions
from src.integrations.clients.keystone.order_client import KeystoneOrderApiClient
from src.integrations.orders import history

logger = logging.getLogger(__name__)
_LOG_PREFIX = "[KEYSTONE-ORDER-HISTORY]"

# Same as orders.keystone._ORDER_HISTORY_ERROR_VALUES -- GetOrderHistory rows whose EKORD# holds
# one of these mean "no matching order", not a real one.
_ORDER_HISTORY_ERROR_VALUES = {"NoData", "BadDate", "BadInDt"}


def _to_decimal(value: typing.Optional[str]) -> typing.Optional[decimal.Decimal]:
    if not value:
        return None
    try:
        return decimal.Decimal(value)
    except decimal.InvalidOperation:
        return None


def _to_int(value: typing.Optional[str]) -> typing.Optional[int]:
    if not value:
        return None
    try:
        return int(decimal.Decimal(value))
    except (decimal.InvalidOperation, ValueError):
        return None


def _parse_ekdate(value: typing.Optional[str]) -> typing.Optional[datetime.date]:
    """EKDATE is YYYYMMDD, no separators (same format GetOrderHistory's own FromDate/ToDate
    params take) -- confirmed against orders.keystone._decode_order_history_timestamp, which
    parses the identical field for the write-side adapter's own order-detail view."""
    if not value or len(value) != 8:
        return None
    try:
        return datetime.date(int(value[0:4]), int(value[4:6]), int(value[6:8]))
    except ValueError:
        return None


def _group_rows_by_order(
    rows: typing.List[typing.Dict[str, str]]
) -> typing.Dict[str, typing.List[typing.Dict[str, str]]]:
    """GetOrderHistory returns one row per (line item, status transition) pair, with EKORD#
    (Keystone's own order-number field, confirmed live by the write-side adapter -- see
    src.integrations.orders.keystone's own "po_number": row.get("EKORD#") mapping) repeated on
    every one of an order's rows -- group by it the same way Meyer's SalesCurrent/SalesHistory
    rows are grouped by OrderNumber."""
    by_order: typing.Dict[str, typing.List[typing.Dict[str, str]]] = {}
    for row in rows:
        order_number = (row.get("EKORD#") or "").strip()
        if not order_number or order_number in _ORDER_HISTORY_ERROR_VALUES:
            continue
        by_order.setdefault(order_number, []).append(row)
    return by_order


def _build_order_result(order_number: str, rows: typing.List[typing.Dict[str, str]]) -> history.ExternalOrderResult:
    # Keystone's write-side submission has no separate order id of its own -- EKORD# IS the
    # PONumber, ours or the distributor rep's (see confirmed_purchase_order_sync.py's own "Keystone
    # has no separate order id -- distributor_order_number already IS the po_number"). Stored as
    # BOTH identifiers here (rather than picking one), since dedup checks distributor_order_number
    # first and customer_po_number second -- for Keystone those are, in reality, the same value,
    # and storing it under both lets either check catch a real match instead of depending on which
    # one external_order_sync happens to try first.
    customer_po_number = order_number

    # Most recent row's EKSTAT (last in Keystone's own chronological order -- GetOrderHistory
    # rows for one line item are already in stage-progression order per the vendor's own docs)
    # is this order's current raw status; unlike Turn14/Meyer, Keystone has no single
    # order-level status field, so this is a best-effort "most advanced stage seen" rather than
    # a literal field read.
    status = None
    for row in reversed(rows):
        if row.get("EKSTAT"):
            status = row["EKSTAT"]
            break

    line_items = []
    for row in rows:
        unit_price = _to_decimal(row.get("EKPRIC"))
        line_total = _to_decimal(row.get("EKEXTD"))
        warehouse_code = row.get("EKSWHS") or None
        vend = row.get("EKVEND", "")
        part = row.get("EKPART", "")
        line_items.append(
            {
                "part_number": "{}-{}".format(vend, part) if vend or part else None,
                "description": None,
                "quantity": _to_int(row.get("EKXQTY")),
                "unit_price": float(unit_price) if unit_price is not None else None,
                "line_total": float(line_total) if line_total is not None else None,
                "warehouse_code": warehouse_code,
                "warehouse_name": None,
            }
        )

    line_totals = [li["line_total"] for li in line_items if li["line_total"] is not None]
    subtotal = round(sum(line_totals), 2) if line_totals else None
    freight = _to_decimal(rows[0].get("EKFRTD")) if rows else None
    total = (
        decimal.Decimal(str(subtotal)) + freight
        if subtotal is not None and freight is not None
        else (decimal.Decimal(str(subtotal)) if subtotal is not None else None)
    )

    order_date = None
    for row in rows:
        order_date = _parse_ekdate(row.get("EKDATE"))
        if order_date:
            break

    return history.ExternalOrderResult(
        distributor_order_number=order_number,
        customer_po_number=customer_po_number,
        order_date=order_date,
        status=status,
        ship_to=None,  # GetOrderHistory carries no ship-to address at all.
        subtotal=decimal.Decimal(str(subtotal)) if subtotal is not None else None,
        freight=freight,
        total=total,
        line_items=line_items,
        raw_response={"rows": rows},
    )


class KeystoneOrderHistoryReader(history.OrderHistoryReader):
    def __init__(
        self,
        company_provider: src_models.CompanyProviders,
        order_account: src_models.CompanyProviderOrderAccount,
    ) -> None:
        history.OrderHistoryReader.__init__(self, company_provider, order_account)
        self._client = KeystoneOrderApiClient(
            credentials=credentials_helper.get_order_credentials(company_provider, order_account)
        )

    def list_orders(
        self, start_date: datetime.date, end_date: datetime.date
    ) -> typing.List[history.ExternalOrderResult]:
        try:
            tables = self._client.get_order_history(
                po_number="",
                from_date=start_date.strftime("%Y%m%d"),
                to_date=end_date.strftime("%Y%m%d"),
            )
        except keystone_client_exceptions.KeystoneException as e:
            logger.error(
                "{} get_order_history failed for company_provider_id={} order_account_id={}: {}".format(
                    _LOG_PREFIX, self.company_provider.id, self.order_account.id, e
                )
            )
            raise
        # _call_dataset returns {table_name: [rows]} -- GetOrderHistory's rows live under
        # whichever single table name the response's DataSet actually used (see
        # orders.keystone's own "a row carrying EKORD#" lookup for the identical situation).
        rows: typing.List[typing.Dict[str, str]] = []
        for table_rows in tables.values():
            if table_rows and "EKORD#" in table_rows[0]:
                rows = table_rows
                break
        return [
            _build_order_result(order_number, order_rows)
            for order_number, order_rows in _group_rows_by_order(rows).items()
        ]
