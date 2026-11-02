"""
PremierOrderHistoryReader -- discovers Premier orders we did not place, via the bulk
``GET /tracking/date`` endpoint (new ``get_tracking_by_date`` client method; see
PremierOrderApiClient). Live-confirmed against company 16 (company_provider_id=80): a 14-day
window returned 1 order, which matched our own stored PurchaseOrder -- real end-to-end
validation of both the dedup design and the "SO-" prefix mismatch (see
``external_order_sync._normalize_order_number``). Also live-confirmed (same session): a
follow-up ``GET /sales-orders/{salesOrderNumber}`` call, keyed by the NORMALIZED (no "SO-"
prefix) number, returns ``shipToAddress`` (name/addressLine1/addressLine2/city/regionCode/
postalCode/countryCode/phone) and ``salesOrderLines`` (itemNumber/quantity/unitPrice/
warehouseCode, no line-level description or total) -- see ``enrich_new_order`` below, which is
the only place those field names are read.
"""
import datetime
import decimal
import logging
import typing

from django.conf import settings

from src import models as src_models
from src.integrations import credentials as credentials_helper
from src.integrations.clients.premier import exceptions as premier_client_exceptions
from src.integrations.clients.premier.order_client import PremierOrderApiClient
from src.integrations.orders import history

logger = logging.getLogger(__name__)
_LOG_PREFIX = "[PREMIER-ORDER-HISTORY]"


def _to_decimal(value: typing.Any) -> typing.Optional[decimal.Decimal]:
    if value is None:
        return None
    try:
        return decimal.Decimal(str(value))
    except (decimal.InvalidOperation, ValueError, TypeError):
        return None


def _parse_quantity(value: typing.Any) -> typing.Optional[typing.Union[int, float]]:
    """Premier's own quantity fields come back as decimal strings (e.g. ``Decimal("1.00000000")``
    -- confirmed live on both packageItems[].quantity and salesOrderLines[].quantity) -- same
    issue, same fix, as orders.premier._parse_quantity: collapsed to a plain int for the
    whole-unit case every other distributor's quantity field already uses, rather than storing
    (and the contract requiring a bare JSON number for) a Decimal that ``simplejson`` would
    otherwise round-trip as a string."""
    parsed = _to_decimal(value)
    if parsed is None:
        return None
    as_float = float(parsed)
    return int(as_float) if as_float == int(as_float) else as_float


def _build_order_result(row: typing.Dict) -> typing.Optional[history.ExternalOrderResult]:
    sales_order_number = row.get("salesOrderNumber")
    if not sales_order_number:
        return None

    tracking_entries = row.get("tracking") or []
    # GET /tracking/date carries no order date, status, ship-to, or line pricing at all --
    # tracking-specific fields only, per Premier's own docs (confirmed live). order_date/status/
    # ship_to/subtotal/freight/total all stay None here; line_items is built from whatever
    # packageItems tracking gives (item number + quantity only, no price) so a brand-new row
    # still shows SOMETHING before enrich_new_order's follow-up call fills in the rest.
    line_items = [
        {
            "part_number": item.get("itemNumber"),
            "description": None,
            "quantity": _parse_quantity(item.get("quantity")),
            "unit_price": None,
            "line_total": None,
            "warehouse_code": None,
            "warehouse_name": None,
        }
        for entry in tracking_entries
        for item in (entry.get("packageItems") or [])
        if item.get("itemNumber")
    ]

    return history.ExternalOrderResult(
        distributor_order_number=sales_order_number,
        customer_po_number=row.get("customerPurchaseOrderNumber"),
        order_date=None,
        status=None,
        ship_to=None,
        subtotal=None,
        freight=None,
        total=None,
        line_items=line_items,
        raw_response=row,
    )


class PremierOrderHistoryReader(history.OrderHistoryReader):
    def __init__(
        self,
        company_provider: src_models.CompanyProviders,
        order_account: src_models.CompanyProviderOrderAccount,
    ) -> None:
        history.OrderHistoryReader.__init__(self, company_provider, order_account)
        environment = getattr(settings, "PREMIER_ORDER_ENVIRONMENT", "production")
        self._client = PremierOrderApiClient(
            credentials=credentials_helper.get_order_credentials(company_provider, order_account),
            environment=environment,
        )

    def list_orders(
        self, start_date: datetime.date, end_date: datetime.date
    ) -> typing.List[history.ExternalOrderResult]:
        try:
            rows = self._client.get_tracking_by_date(start_date=start_date.isoformat(), end_date=end_date.isoformat())
        except premier_client_exceptions.PremierException as e:
            logger.error(
                "{} get_tracking_by_date failed for company_provider_id={} order_account_id={}: {}".format(
                    _LOG_PREFIX, self.company_provider.id, self.order_account.id, e
                )
            )
            raise
        results = []
        for row in rows:
            result = _build_order_result(row)
            if result is not None:
                results.append(result)
        return results

    def enrich_new_order(self, result: history.ExternalOrderResult) -> history.ExternalOrderResult:
        """
        Only called by external_order_sync for a genuinely NEW external order (never on a
        refresh of one already stored) -- list_orders carries no ship-to address or line
        pricing at all (see its own docstring), so this one extra
        ``GET /sales-orders/{salesOrderNumber}`` call per newly-discovered order fills those in.
        Keyed by result.distributor_order_number, which external_order_sync already normalizes
        (strips "SO-") before this is called -- confirmed live that the un-normalized,
        "SO-"-prefixed form 404s against this endpoint. Best-effort: a failure here just means
        the new row is stored with only list_orders' tracking-level fields, same as every other
        best-effort enrichment in this codebase (e.g. MeyerOrderAdapter._get_item_info).
        """
        try:
            full_order = self._client.get_sales_order(result.distributor_order_number)
        except premier_client_exceptions.PremierException:
            logger.warning(
                "{} get_sales_order failed enriching newly-discovered order {}; storing with "
                "tracking-level fields only.".format(_LOG_PREFIX, result.distributor_order_number)
            )
            return result
        if not full_order:
            return result

        ship_to_raw = full_order.get("shipToAddress") or {}
        ship_to = (
            {
                "name": ship_to_raw.get("name"),
                "address1": ship_to_raw.get("addressLine1"),
                "address2": ship_to_raw.get("addressLine2"),
                "city": ship_to_raw.get("city"),
                "state": ship_to_raw.get("regionCode"),
                "postal_code": ship_to_raw.get("postalCode"),
                "country": ship_to_raw.get("countryCode"),
                "phone": ship_to_raw.get("phone"),
            }
            if ship_to_raw
            else result.ship_to
        )

        line_items = result.line_items
        subtotal = result.subtotal
        raw_lines = full_order.get("salesOrderLines")
        if raw_lines:
            built_lines = []
            for line in raw_lines:
                raw_quantity = _to_decimal(line.get("quantity"))
                unit_price = _to_decimal(line.get("unitPrice"))
                line_total = (
                    (raw_quantity * unit_price) if raw_quantity is not None and unit_price is not None else None
                )
                built_lines.append(
                    {
                        "part_number": line.get("itemNumber"),
                        "description": None,
                        # Premier's own quantity is a decimal string (e.g. "1.00000000") -- see
                        # _parse_quantity. line_total above is still computed from the RAW
                        # decimal for precision; only the field stored/returned here is
                        # collapsed to a plain number.
                        "quantity": _parse_quantity(line.get("quantity")),
                        "unit_price": float(unit_price) if unit_price is not None else None,
                        "line_total": float(line_total) if line_total is not None else None,
                        "warehouse_code": line.get("warehouseCode"),
                        "warehouse_name": None,
                    }
                )
            line_items = built_lines
            totals = [li["line_total"] for li in built_lines if li["line_total"] is not None]
            subtotal = decimal.Decimal(str(round(sum(totals), 2))) if totals else None

        freight = _to_decimal(full_order.get("freightAmt"))
        discount = _to_decimal(full_order.get("tradeDiscountAmount"))
        total = subtotal
        if total is not None and freight is not None:
            total += freight
        if total is not None and discount is not None:
            total -= discount

        return history.ExternalOrderResult(
            distributor_order_number=result.distributor_order_number,
            customer_po_number=result.customer_po_number or full_order.get("customerPurchaseOrderNumber"),
            order_date=result.order_date,
            status=result.status,
            ship_to=ship_to,
            subtotal=subtotal,
            freight=freight,
            total=total,
            line_items=line_items,
            raw_response={"tracking": result.raw_response, "order": full_order},
            api_order_flag=result.api_order_flag,
        )
