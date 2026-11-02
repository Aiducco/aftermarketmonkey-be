"""
MeyerOrderHistoryReader -- discovers Meyer orders, using the one native origin flag any of the
four in-scope distributors actually has: SalesCurrent/SalesHistory's "API Order" field states
directly whether an order went through our API ("Yes") or not ("No"). See
docs/tasks/external-orders/reference/meyer-order-reads.md for the vendor-doc spec this is built
against.

UNVERIFIED LIVE: SalesCurrent/SalesHistory both time out connecting through the required SOCKS5
proxy from this dev machine (contract.md's "Verification blockers") -- every parsing decision
below is justified from the vendor doc, not confirmed against a real response. Must be re-run
from a host the proxy actually accepts connections from before this is trusted unattended.
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

logger = logging.getLogger(__name__)
_LOG_PREFIX = "[MEYER-ORDER-HISTORY]"


def _to_decimal(value: typing.Any) -> typing.Optional[decimal.Decimal]:
    if value in (None, ""):
        return None
    try:
        return decimal.Decimal(str(value))
    except (decimal.InvalidOperation, ValueError, TypeError):
        return None


def _parse_doc_date(value: typing.Optional[str]) -> typing.Optional[datetime.date]:
    """DocDate is an ISO datetime whose time component is always "00:00:00.000Z" per the vendor
    doc -- only the date part is ever meaningful, matching
    ExternalPurchaseOrder.order_date's DateField."""
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

    # LineSequence, not ItemNumber, is the real per-line key -- the vendor doc notes the same
    # item can appear twice on one order (e.g. split across two shipments), so this groups by
    # OrderNumber only and keeps every row as its own line rather than merging same-ItemNumber
    # rows together.
    line_items = []
    for row in rows:
        unit_price = _to_decimal(row.get("UnitPrice"))
        line_total = _to_decimal(row.get("ExtPrice"))
        warehouse_code = row.get("LocationCode")
        line_items.append(
            {
                "part_number": row.get("ItemNumber"),
                "description": None,
                "quantity": row.get("Quantity"),
                "unit_price": float(unit_price) if unit_price is not None else None,
                "line_total": float(line_total) if line_total is not None else None,
                "warehouse_code": warehouse_code,
                "warehouse_name": warehouse_names.get(warehouse_code) if warehouse_code else None,
            }
        )

    line_totals = [li["line_total"] for li in line_items if li["line_total"] is not None]
    subtotal = round(sum(line_totals), 2) if line_totals else None

    return history.ExternalOrderResult(
        distributor_order_number=order_number,
        # SalesCurrent/SalesHistory's row shape carries no CustPO field of its own (only
        # SalesOrderDetail/Invoice do, per the vendor doc) -- left None rather than guessed.
        # Meyer doesn't need PO-matching for dedup anyway, since "API Order" is authoritative.
        customer_po_number=None,
        order_date=_parse_doc_date(header.get("DocDate")),
        status=status_label,
        # Neither endpoint's row shape carries a ship-to address at all.
        ship_to=None,
        subtotal=decimal.Decimal(str(subtotal)) if subtotal is not None else None,
        freight=None,
        total=decimal.Decimal(str(subtotal)) if subtotal is not None else None,
        line_items=line_items,
        raw_response={"rows": rows},
        api_order_flag=api_order_flag,
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
