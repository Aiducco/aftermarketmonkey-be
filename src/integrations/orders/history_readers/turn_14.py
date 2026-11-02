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


def _build_line_items(lines: typing.List[typing.Dict]) -> typing.List[typing.Dict]:
    """attributes.lines[] keys confirmed live: part_number, part_description, quantity, price,
    total, open_quantity, delivered_quantity, total_fulfilled_quantity, location_id, item_id,
    line_num, invoiced_quantity, esd. warehouse_name is left None here -- unlike Meyer's
    MeyerLocation table, decoding location_id into a human-readable place isn't needed for this
    feature's stated goal (showing the order existed and what was ordered; see research-be.md's
    "Not doing") and would just duplicate turn_14.py's own private warehouse-name lookup."""
    line_items = []
    for line in lines or []:
        unit_price = _to_decimal(line.get("price"))
        line_total = _to_decimal(line.get("total"))
        line_items.append(
            {
                "part_number": line.get("part_number"),
                "description": line.get("part_description"),
                "quantity": line.get("quantity"),
                "unit_price": float(unit_price) if unit_price is not None else None,
                "line_total": float(line_total) if line_total is not None else None,
                "warehouse_code": line.get("location_id"),
                "warehouse_name": None,
            }
        )
    return line_items


def _build_order_result(attrs: typing.Dict, entry: typing.Dict) -> typing.Optional[history.ExternalOrderResult]:
    website_order_number = attrs.get("website_order_number")
    if website_order_number is None:
        return None

    line_items = _build_line_items(attrs.get("lines") or [])
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
        results: typing.List[history.ExternalOrderResult] = []
        page, total_pages = 1, 1
        start_str, end_str = start_date.isoformat(), end_date.isoformat()
        while page <= total_pages:
            try:
                response = self._client.get_orders(start_date=start_str, end_date=end_str, page=page)
            except turn14_client_exceptions.Turn14APIException as e:
                logger.error(
                    "{} get_orders failed for company_provider_id={} order_account_id={} page={}: {}".format(
                        _LOG_PREFIX, self.company_provider.id, self.order_account.id, page, e
                    )
                )
                raise
            for entry in response.get("data", []) or []:
                attrs = entry.get("attributes") or {}
                result = _build_order_result(attrs, entry)
                if result is not None:
                    results.append(result)
            total_pages = (response.get("meta") or {}).get("total_pages", 1)
            page += 1
        return results
