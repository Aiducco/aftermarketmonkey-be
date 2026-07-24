"""
PremierOrderAdapter — DistributorOrderAdapter implementation for Premier (APG Wholesale)'s v5
REST Order API. Wraps ``src.integrations.clients.premier.order_client.PremierOrderApiClient``.

Re-confirmed against a full read of https://developer.premierwd.com/ (v0.5.0) — contrary to an
earlier pass's note, the docs are actually complete for every endpoint used here. Two real
limitations remain, both confirmed directly from the docs rather than inferred:

1. There is NO shipping/freight-quote endpoint at all. Premier's docs state orders are
   committed immediately on POST with no dry-run/preview mode, so get_shipping_quote() can only
   report availability (via GET /inventory) and pricing (via GET /pricing) — every
   ShippingQuoteLine's ship_options comes from the static, documented Ship Method List instead
   (see _static_ship_options) and is never priced/scheduled like Turn14/Keystone/Meyer's. This
   is a hard API limitation, not something this adapter is missing.
2. The order-creation response does NOT include Premier's own order number — it only echoes
   back what was submitted (confirmed directly from the docs' own POST /sales-orders/ example
   response). Since GET /sales-orders/{salesOrderNumber} needs exactly that number, it's
   unusable right after submission. This adapter uses ``purchase_order.po_number`` (Premier's
   ``customerPurchaseOrderNumber``) as the distributor_order_number throughout, and polls status
   via GET /tracking?purchaseOrderNumber=... instead — the only documented lookup keyed by
   something we actually have.

SAFETY: submit_order() places a REAL order against Premier — their docs describe no dry-run
mode at all, even for a "testing" environment. Must only ever be invoked from an explicit,
user-approved submission. There is no cancel endpoint — see cancel_order() below.
"""
import datetime
import decimal
import logging
import typing

from django.conf import settings

from src import enums as src_enums
from src import models as src_models
from src.integrations import credentials as credentials_helper
from src.integrations.clients.premier import exceptions as premier_client_exceptions
from src.integrations.clients.premier.order_client import PremierOrderApiClient
from src.integrations.orders import base
from src.integrations.orders import exceptions as order_exceptions

logger = logging.getLogger(__name__)
_LOG_PREFIX = "[PREMIER-ORDER-ADAPTER]"

# Premier's own "Warehouse Code List" (https://developer.premierwd.com/#sales-orders) — unlike
# Turn14/Meyer, this is a small, static, fully-documented table, so no location-sync job/table
# is needed here; "City, ST"-style label to match the convention Turn14/Keystone already use.
# *Idaho was renamed Utah per Premier's docs — the API still accepts the old code and converts
# it, so it's mapped here too for display purposes.
# CONFIRMED INCOMPLETE: a live quote returned "NV-1-US" (Nevada), which isn't in Premier's own
# documented table at all — added here from the same "<State> Warehouse" naming pattern as the
# rest, but treat this list as best-effort; any other undocumented code just falls back to None
# (see get_shipping_quote) rather than guessing further.
_WAREHOUSE_NAMES = {
    "UT-1-US": "Utah Warehouse",
    "ID-1-US": "Utah Warehouse",
    "KY-1-US": "Kentucky Warehouse",
    "CA-1-US": "California Warehouse",
    "TX-1-US": "Texas Warehouse",
    "WA-1-US": "Washington Warehouse",
    "CO-1-US": "Colorado Warehouse",
    "AB-1-CA": "Alberta Warehouse",
    "NV-1-US": "Nevada Warehouse",
}

# Verbatim from Premier's docs (https://developer.premierwd.com/#sales-orders) — a mix of named
# and numeric carrier service codes, no carrier/name breakdown given for the numeric ones.
_SHIP_METHOD_CODES = [
    "GROUND", "2ND DAY", "2ND DAY AM", "2ND DAY LETTER", "3 DAY SELECT", "FEDEX 2ND DAY",
    "FEDEX 3 DAY", "FEDEX GROUND", "FEDEX INT 1 DAY", "FEDEX INT 3 DAY", "FEDEX PRIORITY",
    "FEDEX STD OVRNT", "NEXT DAY", "NEXT DAY AM", "NEXT DAY LETTER", "NEXT DAY SAT",
    "NEXT DAY SAVER", "ONTRAC", "P-UP", "Priority Mail", "Express Mail", "EXPRESS INT",
    "PRIORITY INT", "EXPEDITED INT", "STANDARD INT", "EXPRESS INT PLS", "EXPRESS INT SVR",
    "FIRST OVERNIGHT", "EXPRESS SAVER", "HOME DELIVERY", "PARCEL POST", "Global Express",
    "Exp Mail Inter.", "1st Class Int.", "1st Class",
    "20101", "20501", "20102", "20103", "20104", "20105", "20106", "20107", "20108", "20109",
    "20201", "20202", "20203", "20204", "20205", "20401", "20402", "20403", "20404", "20405",
    "20406", "20407", "20408", "20801", "20802", "20803", "20804", "20805", "20806", "20206",
    "FEDEX 2 DAY AM", "20207", "20208", "20209", "20210", "FEDEX INT ECON", "FEDEX INT FIRST",
    "20211", "20212", "ONTRAC SUNRISE", "ONTRAC GOLD",
]


def _to_decimal(value: typing.Optional[typing.Any]) -> typing.Optional[decimal.Decimal]:
    if value in (None, ""):
        return None
    try:
        return decimal.Decimal(str(value))
    except decimal.InvalidOperation:
        return None


def _parse_premier_date(value: typing.Optional[str]) -> typing.Optional[datetime.date]:
    """Premier's invoice-filter query params use MM/DD/YYYY (per the docs' own examples), but
    the response body's own date fields (e.g. header.transactionDate) aren't shown in an example
    anywhere — try ISO 8601 first (the more common API-response convention), then MM/DD/YYYY,
    else give up rather than guess further."""
    if not value or not value.strip():
        return None
    value = value.strip()
    for fmt in ("%Y-%m-%d", "%m/%d/%Y"):
        try:
            return datetime.datetime.strptime(value[:10], fmt).date()
        except ValueError:
            continue
    return None


def _premier_item_number(provider_part: src_models.ProviderPart) -> str:
    """Premier's real itemNumber — NOT ProviderPart.provider_external_id, which for Premier is a
    composite ``"{premier_brand_id}_{premier_part_number}"`` key (see
    master_parts._premier_provider_external_id), needed only for our own DB uniqueness since the
    same premier_part_number can recur across different Premier brands. Sending that composite
    key straight to Premier's API would never match a real item — every quote/submit call would
    silently fail to find anything. product_details' "sku" entry (see
    master_parts._premier_product_details) carries the raw number directly; fall back to
    splitting provider_external_id on its first "_" (the brand id prefix is always purely
    numeric, so this is safe) if product_details is ever missing/stale."""
    for entry in provider_part.product_details or []:
        if entry.get("key") == "sku" and entry.get("value"):
            return str(entry["value"]).strip()
    ext_id = provider_part.provider_external_id or ""
    _, _, remainder = ext_id.partition("_")
    return (remainder or ext_id).strip()


def _static_ship_options(ship_method: typing.Optional[str]) -> typing.List[base.ShipOption]:
    """Premier's GET /inventory carries no per-method cost/date at all — there is no
    shipping/freight-quote endpoint (see module docstring) — so, unlike Turn14/Keystone/Meyer,
    these options can never be priced or scheduled. But POST /sales-orders/'s shipMethod field
    does accept any of the documented Ship Method List codes (_SHIP_METHOD_CODES, the same
    catalog list_shipping_methods() already exposes), so ship_options is populated from that
    static list rather than left empty — otherwise the FE has nothing to let a user pick from
    at all, despite supports_shipping_method_selection() being True. Once ship_method is set
    (a previous selection, or a re-quote), narrow to just that one code, matching every other
    adapter's _filter_options."""
    options = [
        base.ShipOption(service_level_code=code, service_level_name=code, quote_option_id=code)
        for code in _SHIP_METHOD_CODES
    ]
    if not ship_method:
        return options
    filtered = [o for o in options if o.service_level_code == ship_method]
    return filtered or options


def _best_candidate_warehouse(inventory_rows: typing.List[typing.Dict], country: typing.Optional[str]) -> typing.Optional[str]:
    """Best-effort fulfillment-warehouse guess for display at quote time: the matching-country
    warehouse (codes end "-US"/"-CA") with the most available stock. Premier's own "Warehouse
    Auto-selection" (docs: closest domestic warehouse with inventory) depends on geocoding we
    don't have, so this can't be authoritative — the real warehouse actually used is only known
    once the order is placed (see LineItemPlacement.warehouse_code, taken straight from
    Premier's POST /sales-orders/ response)."""
    suffix = "-CA" if (country or "").upper() in ("CA", "CAN", "CANADA") else "-US"
    candidates = [
        (w.get("warehouseCode"), w.get("quantityAvailable") or 0)
        for w in inventory_rows
        if (w.get("warehouseCode") or "").upper().endswith(suffix) and (w.get("quantityAvailable") or 0) > 0
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda c: c[1])[0]


class PremierOrderAdapter(base.DistributorOrderAdapter):
    provider_kind = src_enums.BrandProviderKind.PREMIER_PERFORMANCE.value

    def __init__(self, company_provider: src_models.CompanyProviders) -> None:
        base.DistributorOrderAdapter.__init__(self, company_provider)
        # Was previously omitted entirely, so every call silently defaulted to
        # PremierOrderApiClient's own hardcoded "testing" regardless of
        # settings.PREMIER_ORDER_ENVIRONMENT (already "production" in settings_base.py).
        # Fallback here is also "production" (unlike Turn14/Meyer's "testing" fallback) since
        # Premier's own setting has always defaulted to production, not testing.
        environment = getattr(settings, "PREMIER_ORDER_ENVIRONMENT", "production")
        self._client = PremierOrderApiClient(
            credentials=credentials_helper.get_order_credentials(company_provider),
            environment=environment,
        )

    def _handle_error(self, e: Exception, request_payload: typing.Optional[typing.Dict] = None) -> None:
        code = getattr(e, "code", None)
        raise order_exceptions.OrderValidationError(
            message=str(e), code=str(code) if code else None, request_payload=request_payload
        )

    def _get_prices(self, item_numbers: typing.List[str], currency: str) -> typing.Dict[str, decimal.Decimal]:
        """{item_number: cost} in the given currency ("USD"/"CAD") via GET /pricing — Premier's
        own quoted dealer cost (what we pay Premier, not jobber/map/retail — same role Meyer's
        ItemInformation/Turn14's inline quote pricing play for those adapters). Best-effort: a
        failure here degrades to no distributor pricing for this quote rather than failing it
        outright, matching MeyerOrderAdapter._get_item_info."""
        try:
            rows = self._client.get_pricing(item_numbers)
        except premier_client_exceptions.PremierException:
            logger.exception(
                "{} get_pricing failed; quote will have no distributor pricing this round.".format(_LOG_PREFIX)
            )
            return {}

        prices: typing.Dict[str, decimal.Decimal] = {}
        for row in rows:
            item_number = row.get("itemNumber", "")
            if not item_number:
                continue
            for p in row.get("pricing") or []:
                if (p.get("currency") or "").upper() != currency:
                    continue
                cost = p.get("cost")
                if cost is not None:
                    try:
                        prices[item_number] = decimal.Decimal(str(cost))
                    except decimal.InvalidOperation:
                        pass
                break
        return prices

    # -- DistributorOrderAdapter ------------------------------------------------------------

    def get_shipping_quote(
        self,
        line_items: typing.List[base.OrderLineItemRequest],
        ship_to: base.ShipToAddress,
        ship_method: typing.Optional[str] = None,
    ) -> base.ShippingQuoteResult:
        item_numbers = [_premier_item_number(li.provider_part) for li in line_items]
        data = {"itemNumbers": item_numbers}
        try:
            inventory = self._client.get_inventory(item_numbers)
        except premier_client_exceptions.PremierException as e:
            self._handle_error(e, request_payload=data)

        logger.info("{} Inventory response: {}".format(_LOG_PREFIX, repr(inventory)[:4000]))

        currency = "CAD" if (ship_to.country or "").upper() in ("CA", "CAN", "CANADA") else "USD"
        prices = self._get_prices(item_numbers, currency)
        ship_options = _static_ship_options(ship_method)

        try:
            by_external_id = {_premier_item_number(li.provider_part): li for li in line_items}
            lines: typing.List[base.ShippingQuoteLine] = []
            seen: typing.Set[str] = set()

            for entry in inventory:
                external_id = entry.get("itemNumber", "")
                li = by_external_id.get(external_id)
                seen.add(external_id)
                inventory_rows = entry.get("inventory") or []
                total_available = sum(w.get("quantityAvailable", 0) or 0 for w in inventory_rows)
                requested = li.quantity if li else 0
                quantity_available = min(total_available, requested)
                warehouse_code = _best_candidate_warehouse(inventory_rows, ship_to.country)
                unit_price = prices.get(external_id)
                # quantity_available is capped at what was actually requested; any shortfall is
                # reported as backordered. ship_options is the static method catalog (see
                # _static_ship_options) — never priced/scheduled, since no freight-quote
                # endpoint exists, but still selectable.
                lines.append(
                    base.ShippingQuoteLine(
                        line_item_id=li.line_item_id if li else 0,
                        provider_external_id=external_id,
                        quantity_available=quantity_available,
                        quantity_backordered=max(requested - total_available, 0),
                        warehouse_code=warehouse_code,
                        warehouse_name=_WAREHOUSE_NAMES.get(warehouse_code) if warehouse_code else None,
                        ship_options=ship_options,
                        flags=[] if total_available >= requested else ["backorder"],
                        distributor_unit_price=unit_price,
                        distributor_line_total=(
                            unit_price * quantity_available if unit_price is not None else None
                        ),
                    )
                )

            for external_id, li in by_external_id.items():
                if external_id in seen:
                    continue
                lines.append(
                    base.ShippingQuoteLine(
                        line_item_id=li.line_item_id,
                        provider_external_id=external_id,
                        quantity_available=0,
                        flags=["not_returned_in_quote"],
                    )
                )

            if not lines:
                raise order_exceptions.OrderValidationError(
                    "Unexpected/empty inventory response shape from Premier. Raw response: {}".format(
                        repr(inventory)[:2000]
                    ),
                    request_payload=data,
                )
        except order_exceptions.OrderValidationError:
            raise
        except (AttributeError, TypeError, KeyError, IndexError) as e:
            raise order_exceptions.OrderValidationError(
                "Unexpected inventory response shape from Premier ({}: {}). Raw response: {}".format(
                    type(e).__name__, e, repr(inventory)[:2000]
                ),
                request_payload=data,
            )

        return base.ShippingQuoteResult(lines=lines, raw_response={"inventory": inventory}, request_payload=data)

    def submit_order(
        self,
        purchase_order: src_models.PurchaseOrder,
        line_items: typing.List[base.OrderLineItemRequest],
        ship_to: base.ShipToAddress,
    ) -> base.DistributorOrderResult:
        phone_digits = "".join(ch for ch in (ship_to.phone or "") if ch.isdigit())
        data = {
            "customerPurchaseOrderNumber": purchase_order.po_number,
            "shipToAddress": {
                "name": ship_to.name,
                "addressLine1": ship_to.address1,
                "addressLine2": ship_to.address2 or "",
                "city": ship_to.city,
                "regionCode": (ship_to.state or "")[:2].upper(),
                "postalCode": ship_to.postal_code,
                "countryCode": (ship_to.country or "US")[:2].upper(),
                "phone": phone_digits,
            },
            "salesOrderLines": [
                {"itemNumber": _premier_item_number(li.provider_part), "quantity": li.quantity}
                for li in line_items
            ],
        }
        if purchase_order.ship_method:
            data["shipMethod"] = purchase_order.ship_method

        try:
            response = self._client.create_sales_order(data)
        except premier_client_exceptions.PremierException as e:
            self._handle_error(e, request_payload=data)

        return self._parse_submit_response(response, purchase_order, line_items, request_payload=data)

    @staticmethod
    def _parse_submit_response(
        response: typing.Dict,
        purchase_order: src_models.PurchaseOrder,
        line_items: typing.List[base.OrderLineItemRequest],
        request_payload: typing.Optional[typing.Dict] = None,
    ) -> base.DistributorOrderResult:
        try:
            by_external_id = {_premier_item_number(li.provider_part): li for li in line_items}
            placements: typing.List[base.LineItemPlacement] = []
            # Premier's response only echoes back what was submitted (no confirmed-vs-requested
            # signal, no per-line pricing) — see module docstring. Fall back to our own request
            # data if the response is missing/malformed rather than failing a placed order.
            response_lines = response.get("salesOrderLines") or [
                {"itemNumber": _premier_item_number(li.provider_part), "quantity": li.quantity}
                for li in line_items
            ]
            for line in response_lines:
                external_id = line.get("itemNumber", "")
                li = by_external_id.get(external_id)
                placements.append(
                    base.LineItemPlacement(
                        line_item_id=li.line_item_id if li else 0,
                        distributor_order_number=purchase_order.po_number,
                        quantity_confirmed=line.get("quantity", li.quantity if li else 0),
                        warehouse_code=line.get("warehouseCode"),
                    )
                )
        except (AttributeError, TypeError, KeyError, IndexError) as e:
            raise order_exceptions.OrderValidationError(
                "Unexpected order response shape from Premier ({}: {}). Raw response: {}".format(
                    type(e).__name__, e, repr(response)[:2000]
                ),
                request_payload=request_payload,
            )

        # No distributor order number is ever returned — see module docstring.
        return base.DistributorOrderResult(
            distributor_order_numbers=[purchase_order.po_number],
            line_item_placements=placements,
            raw_response=response,
            request_payload=request_payload,
        )

    def get_order_status(self, purchase_order: src_models.PurchaseOrder) -> base.OrderStatusResult:
        try:
            tracking_entries = self._client.get_tracking_by_purchase_order_number(purchase_order.po_number)
        except premier_client_exceptions.PremierOrderValidationError:
            # Undocumented whether "no tracking yet" (order placed but not shipped) returns an
            # empty result or an error — treated as "still open" rather than a hard failure,
            # since that's the far more common case for a routine status-poll on a fresh order.
            # Revisit once Premier's actual behavior here is confirmed.
            return base.OrderStatusResult(
                orders=[
                    base.DistributorOrderStatus(
                        distributor_order_number=purchase_order.po_number,
                        status_code="OPEN",
                        tracking_numbers=[],
                    )
                ]
            )
        except premier_client_exceptions.PremierException as e:
            self._handle_error(e)

        try:
            orders: typing.List[base.DistributorOrderStatus] = []
            for entry in tracking_entries:
                tracking_number = entry.get("trackingNumber")
                orders.append(
                    base.DistributorOrderStatus(
                        distributor_order_number=purchase_order.po_number,
                        status_code="SHIPPED" if tracking_number else "OPEN",
                        tracking_numbers=[tracking_number] if tracking_number else [],
                        carrier=entry.get("carrier"),
                        raw_response=entry,
                    )
                )
            if not orders:
                orders = [
                    base.DistributorOrderStatus(
                        distributor_order_number=purchase_order.po_number, status_code="OPEN", tracking_numbers=[]
                    )
                ]
        except (AttributeError, TypeError, KeyError, IndexError) as e:
            raise order_exceptions.OrderValidationError(
                "Unexpected tracking response shape from Premier ({}: {}). Raw response: {}".format(
                    type(e).__name__, e, repr(tracking_entries)[:2000]
                )
            )
        return base.OrderStatusResult(orders=orders)

    def supports_invoices(self) -> bool:
        return True

    def get_invoices(self, purchase_order: src_models.PurchaseOrder) -> typing.List[base.DistributorInvoice]:
        """
        Premier's Invoice API (GET invoices) can only be filtered by invoiceNumber/
        salesOrderNumber/itemNumber/date-range — never by customerPurchaseOrderNumber (see
        module docstring's order-number gap, which applies here too). So invoice numbers must be
        discovered first, from whichever tracking entries (already fetched by get_order_status,
        re-fetched here since adapters are called independently) carry one — Premier's bulk
        tracking/date endpoint is confirmed to return an invoiceNumber per sales order, but the
        purchaseOrderNumber-filtered form used here isn't confirmed to carry the same field on
        each entry; read defensively (.get) so this degrades to "no invoices yet" rather than
        breaking if it's genuinely absent. Revisit once confirmed against Premier's test
        environment, same caveat get_order_status already carries for this same endpoint.
        """
        try:
            tracking_entries = self._client.get_tracking_by_purchase_order_number(purchase_order.po_number)
        except premier_client_exceptions.PremierOrderValidationError:
            return []
        except premier_client_exceptions.PremierException as e:
            self._handle_error(e)

        invoice_numbers = sorted(
            {entry.get("invoiceNumber") for entry in tracking_entries if entry.get("invoiceNumber")}
        )
        if not invoice_numbers:
            return []

        invoices: typing.List[base.DistributorInvoice] = []
        for invoice_number in invoice_numbers:
            try:
                rows = self._client.get_invoices(invoice_number=invoice_number)
            except premier_client_exceptions.PremierException:
                logger.warning(
                    "{} get_invoices failed for invoice_number={}; skipped this round.".format(
                        _LOG_PREFIX, invoice_number
                    )
                )
                continue
            invoices.extend(self._parse_invoice(row) for row in rows)
        return invoices

    @staticmethod
    def _parse_invoice(row: typing.Dict) -> base.DistributorInvoice:
        try:
            header = row.get("header") or {}
            line_items = [
                base.InvoiceLineItem(
                    part_number=(line.get("item") or {}).get("itemNumber"),
                    description=next(
                        (
                            d.get("shortDescription") or d.get("longDescription")
                            for d in ((line.get("item") or {}).get("descriptions") or [])
                        ),
                        None,
                    ),
                    quantity=line.get("quantityShipped"),
                    unit_price=_to_decimal(line.get("unitPrice")),
                    total_price=_to_decimal(line.get("total")),
                    warehouse_code=line.get("warehouseCode"),
                )
                for line in (row.get("lines") or [])
            ]
            return base.DistributorInvoice(
                invoice_number=str(header.get("invoiceNumber", "")),
                invoice_date=_parse_premier_date(header.get("transactionDate")),
                total_price=_to_decimal(header.get("transactionAmount")),
                freight=_to_decimal(header.get("shipAmount")),
                discount_amount=_to_decimal(header.get("discountAmount")),
                # Premier's invoice header has "balance" (amount still owed), no separate
                # paid_amount field — payments[] (also on this response, in raw_response) has
                # the itemized payment history if that's ever needed beyond the running balance.
                amount_due=_to_decimal(header.get("balance")),
                line_items=line_items,
                raw_response=row,
            )
        except (AttributeError, TypeError, KeyError, IndexError) as e:
            raise order_exceptions.OrderValidationError(
                "Unexpected invoice response shape from Premier ({}: {}). Raw response: {}".format(
                    type(e).__name__, e, repr(row)[:2000]
                )
            )

    def cancel_order(self, purchase_order: src_models.PurchaseOrder) -> bool:
        raise order_exceptions.OrderNotSupportedError(
            "Premier's Order API does not expose a cancel-order endpoint."
        )

    def supports_cancel(self) -> bool:
        return False

    def supports_shipping_method_selection(self) -> bool:
        return True

    def list_shipping_methods(self) -> typing.List[base.ShippingMethod]:
        # Static reference list — Premier's docs give no live "list methods" endpoint, just this
        # fixed catalog of valid shipMethod codes for the Sales Orders request.
        return [base.ShippingMethod(code=code, name=code) for code in _SHIP_METHOD_CODES]
