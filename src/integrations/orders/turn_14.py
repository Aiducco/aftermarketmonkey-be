"""
Turn14OrderAdapter — the reference DistributorOrderAdapter implementation (see the Purchase
Orders plan for why Turn14 was chosen first). Wraps
``src.integrations.clients.turn_14.order_client.Turn14OrderApiClient``.

SAFETY: submit_order() places a REAL order against Turn 14 (even in their "testing"
environment, per their docs this creates a real test order in their system, not a dry-run).
It must only ever be invoked from an explicit, user-approved submission — never from
exploratory/dev code, automated tests, or ad-hoc scripts.
"""
import datetime
import decimal
import logging
import re
import typing

from django.conf import settings

from src import enums as src_enums
from src import models as src_models
from src.integrations import credentials as credentials_helper
from src.integrations.clients.turn_14 import exceptions as turn14_client_exceptions
from src.integrations.clients.turn_14.order_client import Turn14OrderApiClient
from src.integrations.orders import base
from src.integrations.orders import exceptions as order_exceptions

logger = logging.getLogger(__name__)
_LOG_PREFIX = "[TURN14-ORDER-ADAPTER]"

# Turn 14's "default" location lets them auto-select the best warehouse(s) to fulfill from,
# instead of us pinning specific location codes — matches their own recommended approach for
# integrators who don't need manual per-warehouse control.
_DEFAULT_LOCATION = "default"

# Turn 14's own warehouses plus their two special values. "default" lets Turn 14 pick the
# best location(s) itself -- prioritising availability, then proximity to the ship-to zip, then
# restock priorities -- which is the right answer for most orders and why it stays the default.
# "ds" designates a manufacturer drop-shipment. A numeric code forces one specific warehouse,
# which a dealer may want in order to consolidate freight or keep inventory in one region even
# when it is not the closest.
VALID_LOCATIONS = {"default", "ds", "01", "02", "03", "59"}


def resolve_preferred_location(
    company_provider: src_models.CompanyProviders,
    order_account: typing.Optional[src_models.CompanyProviderOrderAccount] = None,
) -> str:
    """
    The warehouse this connection wants its Turn 14 orders fulfilled from.

    Read from the order account first and the connection second, so a company running separate
    accounts for its own shop and for drop-shipping can point them at different warehouses --
    which is the case the setting mainly exists for. Anything unrecognised falls back to
    "default" with a warning rather than being sent through: an invalid location code fails the
    whole quote at Turn 14, and silently degrading to their own (good) warehouse selection beats
    breaking a customer's checkout over a bad config value.
    """
    candidates = []
    if order_account is not None:
        candidates.append((order_account.credentials or {}).get("preferred_location"))
    candidates.append((company_provider.credentials or {}).get("preferred_location"))

    for value in candidates:
        if not value:
            continue
        normalised = str(value).strip().lower()
        if normalised in VALID_LOCATIONS:
            return normalised
        logger.warning(
            "{} Ignoring unrecognised preferred_location {!r} for company_provider={}; "
            "falling back to {!r}.".format(_LOG_PREFIX, value, company_provider.id, _DEFAULT_LOCATION)
        )
    return _DEFAULT_LOCATION

# "Best Premium Ground" shipping group (UPS Ground / FedEx Ground / OnTrac Ground, cheapest of
# the group). Used only when a PurchaseOrder has no ship_method set yet — see submit_order().
_DEFAULT_SHIPPING_GROUP_CODE = "7002"


def _normalize_warehouse_code(location: typing.Any) -> typing.Optional[str]:
    """Turn14's ``location`` comes back as a JSON string for some warehouses ("02") but a bare
    number for others (59, confirmed against a live quote) — normalize to str so warehouse_code
    is consistently typed everywhere it's compared/keyed on (our own JSON responses, the FE)."""
    return str(location) if location is not None else None


def _sanitize_order_notes(raw: typing.Optional[str]) -> typing.Optional[str]:
    """
    Turn14 has 400'd a whole submit over order_notes alone: {"errors":[{"status":"400",
    "title":"Invalid string","source":{"pointer":"/data/order_notes/"},"detail":"The characters
    provided were not valid for a string."}]}. purchase_order.notes is free text a dealer typed
    or pasted (often from a phone/Word doc, so smart quotes/em dashes/emoji are common) — non-
    ASCII characters are a plausible trigger for that error, so they're stripped rather than
    rejected client-side, so a note with one bad character doesn't block the whole order.

    Returns None (no notes at all, or notes that were entirely non-ASCII) when there's nothing
    left to send — callers should omit order_notes from the request body entirely rather than
    send "" or null, since either is itself a plausible trigger for the same error, and
    Turn14's own field-mapping docs list "Don't Map" as valid for this field, implying it's
    genuinely optional.
    """
    if not raw:
        return None
    collapsed = re.sub(r"\s+", " ", raw)
    return collapsed.encode("ascii", "ignore").decode("ascii").strip() or None


def _load_warehouse_names() -> typing.Dict[str, str]:
    """
    {external_id: "City, ST"} from Turn14Location (populated by fetch_turn14_locations from
    GET /v1/locations), so quote responses can show a real place instead of a bare code — this
    is exactly what Turn14's own ordering portal does (it groups shipping options by state, e.g.
    "PA In Stock Items" for warehouse "01"). Same zero-pad tolerance as
    master_parts._get_turn14_location_map, since a quote's ``location`` can come back
    unpadded (e.g. int 59) after _normalize_warehouse_code.
    """
    names = {}
    for row in src_models.Turn14Location.objects.all().values("external_id", "name", "state"):
        external_id = (row.get("external_id") or "").strip()
        if not external_id:
            continue
        label = ", ".join(part for part in (row.get("name"), row.get("state")) if part)
        names[external_id] = label
        if external_id.isdigit():
            names[external_id.zfill(2)] = label
            names[str(int(external_id))] = label
    return names


def extract_po_reference(raw_response: typing.Optional[typing.Dict]) -> typing.Optional[str]:
    """
    The customer PO reference Turn14 recorded an order under, read from whatever shape a
    PurchaseOrderDistributorOrder.raw_response currently holds. Two different attribute names
    carry the same value depending on which Turn14 endpoint produced the response: the
    order-creation/submit response uses ``attributes.po_number``, while a later
    GET /v1/orders/po/{ref} lookup's entries use ``attributes.purchase_order_number`` instead
    (confirmed directly from live examples of each).
    """
    if not isinstance(raw_response, dict):
        return None
    data = raw_response.get("data")
    if isinstance(data, dict):
        attrs = data.get("attributes") or {}
    elif isinstance(data, list) and data:
        attrs = data[0].get("attributes") or {}
    else:
        # Already-unwrapped shape: a bare orders/po/{ref} list entry, as stored back by
        # confirmed_purchase_order_sync after a refresh — not wrapped in a "data" key at all.
        attrs = raw_response.get("attributes") or {}
    return attrs.get("po_number") or attrs.get("purchase_order_number") or None


def translate_order_status(raw_status: typing.Optional[str]) -> typing.Optional["src_enums.DistributorOrderRawStatus"]:
    """
    Turn14's orders/po/{ref} lookup reports attributes.status as one of exactly these two
    strings today -- a direct 1:1 mapping, not a heuristic. Unknown/future values return None
    rather than guessing, so callers can log and leave the field unset instead of recording a
    wrong status.
    """
    if not raw_status:
        return None
    try:
        return src_enums.DistributorOrderRawStatus[raw_status.strip().upper()]
    except KeyError:
        return None


def _to_float(value: typing.Any) -> typing.Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_order_lines(attrs: typing.Dict, warehouse_names: typing.Dict[str, str]) -> typing.List[typing.Dict]:
    """
    The orders/po/{ref} lookup (what confirmed_purchase_order_sync stores long-term into
    PurchaseOrderDistributorOrder.raw_response) reports items as a flat ``lines`` array, not
    nested under ``shipment`` the way a fresh create/promote response does (see
    _parse_order_shipment_items for that shape) -- confirmed against a live refreshed order.
    """
    line_items = []
    for line in attrs.get("lines", []) or []:
        open_quantity = line.get("open_quantity") or 0
        delivered_quantity = line.get("delivered_quantity") or line.get("total_fulfilled_quantity") or 0
        if open_quantity:
            status = "backordered"
        elif delivered_quantity:
            status = "shipped"
        else:
            status = "confirmed"
        warehouse_code = _normalize_warehouse_code(line.get("location_id"))
        line_items.append(
            {
                "part_number": line.get("part_number"),
                "quantity": line.get("quantity"),
                "unit_price": _to_float(line.get("price")),
                "line_total": _to_float(line.get("total")),
                "warehouse_code": warehouse_code,
                "warehouse_name": warehouse_names.get(warehouse_code) if warehouse_code else None,
                "status": status,
            }
        )
    return line_items


def _parse_invoice_lines(attrs: typing.Dict, warehouse_names: typing.Dict[str, str]) -> typing.List[typing.Dict]:
    """A bulk GET /v1/invoices entry's ``lines`` shape -- confirmed live to use unit_price/
    total_price (not orders/po/{ref}'s price/total) and carry no open_quantity/delivered_quantity
    at all, since every line on an invoice has, by definition, already shipped."""
    line_items = []
    for line in attrs.get("lines", []) or []:
        warehouse_code = _normalize_warehouse_code(line.get("location_id"))
        line_items.append(
            {
                "part_number": line.get("part_number"),
                "quantity": line.get("quantity"),
                "unit_price": _to_float(line.get("unit_price")),
                "line_total": _to_float(line.get("total_price")),
                "warehouse_code": warehouse_code,
                "warehouse_name": warehouse_names.get(warehouse_code) if warehouse_code else None,
                "status": "shipped",
            }
        )
    return line_items


def _parse_order_shipment_items(attrs: typing.Dict, warehouse_names: typing.Dict[str, str]) -> typing.List[typing.Dict]:
    """The create_order/promote_quote_to_order response's shape -- items nested under
    ``shipment``, before any refresh has replaced it with the flatter ``lines`` shape above."""
    line_items = []
    for shipment in attrs.get("shipment", []) or []:
        is_backordered = shipment.get("type") == "out_of_stock"
        warehouse_code = _normalize_warehouse_code(shipment.get("location"))
        for item in shipment.get("items", []) or []:
            line_items.append(
                {
                    "part_number": item.get("part_number"),
                    "quantity": item.get("quantity"),
                    "unit_price": _to_float(item.get("unit_price")),
                    "line_total": _to_float(item.get("line_total")),
                    "warehouse_code": warehouse_code,
                    "warehouse_name": warehouse_names.get(warehouse_code) if warehouse_code else None,
                    "status": "backordered" if is_backordered else "in_stock",
                }
            )
    return line_items


_EMPTY_PARSED_ORDER = {
    "distributor_order_number": None,
    "distributor_status": None,
    "distributor_invoice_ids": [],
    "tracking": [],
    "total": None,
    "freight": None,
    "discount": None,
    "subtotal": None,
    "line_items": [],
}


def parse_order_raw_response(raw_response: typing.Optional[typing.Dict]) -> typing.Dict:
    """
    Maps whatever shape PurchaseOrderDistributorOrder.raw_response currently holds for a Turn14
    order (same create/promote vs. orders/po/{ref} shape tolerance as extract_po_reference) into
    the purchase-order detail API's standardized distributor-order fields. Reads the raw JSON
    fresh on every call instead of a separately-synced DB copy, so a status/tracking change on
    Turn14's side shows up here without needing a background job to have already caught up.

    A third shape is possible too: a bulk GET /v1/invoices entry, stored as raw_response by
    confirmed_purchase_order_sync._apply_turn14_invoice_match when a PO is matched via invoice
    rather than via the (always-open, confirmed live) bulk orders endpoint. Its "type" is
    "Invoice", not "Order" -- distinguishing the two mattered here: an invoice entry's money
    fields are named total_price/discount_amount (not total/discount), its lines use
    unit_price/total_price (not price/total) and carry no open_quantity/delivered_quantity at
    all, and its relationships is a list keyed by "order" (not an order entry's own
    {"invoice": [...]} dict) -- naively parsing it with the order-shaped logic below silently
    produced None for total/discount/line pricing despite tracking happening to match by luck.
    """
    if not isinstance(raw_response, dict):
        return dict(_EMPTY_PARSED_ORDER)
    data = raw_response.get("data")
    if isinstance(data, dict):
        entry = data
    elif isinstance(data, list) and data:
        entry = data[0]
    else:
        entry = raw_response
    attrs = entry.get("attributes") or {}

    tracking = [
        {"tracking_number": t.get("tracking_number"), "method": t.get("ship_method")}
        for t in (attrs.get("tracking") or [])
        if t.get("tracking_number")
    ]

    if entry.get("type") == "Invoice":
        warehouse_names = _load_warehouse_names()
        line_items = _parse_invoice_lines(attrs, warehouse_names)
        line_totals = [li["line_total"] for li in line_items if li["line_total"] is not None]
        subtotal = round(sum(line_totals), 2) if line_totals else None
        return {
            # Turn14's invoice payload carries no status field of its own -- an invoice existing
            # at all is our own closed signal (see _apply_turn14_invoice_match), so this is
            # reported directly rather than left None like an unrecognized order status would be.
            "distributor_status": "Closed",
            "distributor_invoice_ids": [attrs["invoice_number"]] if attrs.get("invoice_number") else [],
            "tracking": tracking,
            "total": _to_float(attrs.get("total_price")),
            "freight": _to_float(attrs.get("freight")),
            "discount": _to_float(attrs.get("discount_amount")),
            "subtotal": subtotal,
            "line_items": line_items,
        }

    # relationships.invoice is only present once an invoice has actually been generated against
    # this order (post-refresh shape) -- absent entirely on a freshly-submitted order. It lists
    # one entry per line item billed against the invoice, not one per invoice (confirmed against
    # a live 2-line order sharing a single invoice) -- deduped so a multi-line order doesn't
    # repeat the same invoice id once per line.
    invoice_ids = []
    for rel in (entry.get("relationships", {}) or {}).get("invoice", []) or []:
        invoice_id = rel.get("invoice_id")
        if invoice_id is not None and str(invoice_id) not in invoice_ids:
            invoice_ids.append(str(invoice_id))

    warehouse_names = _load_warehouse_names()
    line_items = (
        _parse_order_lines(attrs, warehouse_names)
        if "lines" in attrs
        else _parse_order_shipment_items(attrs, warehouse_names)
    )
    line_totals = [li["line_total"] for li in line_items if li["line_total"] is not None]
    subtotal = round(sum(line_totals), 2) if line_totals else None

    return {
        "distributor_status": attrs.get("status"),
        "distributor_invoice_ids": invoice_ids,
        "tracking": tracking,
        # freight/discount only appear on the orders/po/{ref} (post-refresh) shape -- absent on
        # a freshly-submitted, not-yet-refreshed order's shipment[]-shaped response.
        "total": _to_float(attrs.get("total")),
        "freight": _to_float(attrs.get("freight")),
        "discount": _to_float(attrs.get("discount")),
        # Our own sum of line_total across every parsed line -- not read off the distributor's
        # response (Turn14 doesn't state a subtotal directly; "total" above is gross, after
        # freight/discount).
        "subtotal": subtotal,
        "line_items": line_items,
    }


class Turn14OrderAdapter(base.DistributorOrderAdapter):
    provider_kind = src_enums.BrandProviderKind.TURN_14.value

    def __init__(
        self,
        company_provider: src_models.CompanyProviders,
        order_account: typing.Optional[src_models.CompanyProviderOrderAccount] = None,
    ) -> None:
        base.DistributorOrderAdapter.__init__(self, company_provider, order_account)
        environment = getattr(settings, "TURN14_ORDER_ENVIRONMENT", "testing")
        self._client = Turn14OrderApiClient(
            credentials=credentials_helper.get_order_credentials(company_provider, order_account),
            environment=environment,
        )
        self._location = resolve_preferred_location(company_provider, order_account)

    # -- Request building -----------------------------------------------------------------

    @staticmethod
    def _build_recipient(ship_to: base.ShipToAddress) -> typing.Dict:
        recipient = {
            "company": ship_to.attention or ship_to.name,
            "name": ship_to.name,
            "address": ship_to.address1,
            "city": ship_to.city,
            "state": ship_to.state,
            "country": ship_to.country,
            "zip": ship_to.postal_code,
            "phone_number": ship_to.phone or "",
            # Set from the FE's review-cart request (ship_to.ship_to_my_shop) — see
            # base.ShipToAddress.is_shop_address. Informational to Turn 14, not
            # fulfillment-blocking; defaults False (drop-ship) when the FE doesn't send it.
            "is_shop_address": ship_to.is_shop_address,
        }
        if ship_to.address2:
            recipient["address_2"] = ship_to.address2
        if ship_to.email:
            recipient["email_address"] = ship_to.email
        return recipient

    def _build_locations(
        self,
        line_items: typing.List[base.OrderLineItemRequest],
        shipping_code: typing.Optional[str] = None,
    ) -> typing.List[typing.Dict]:
        items = [
            {
                "item_identifier": li.provider_part.provider_external_id,
                "item_identifier_type": "item_id",
                "quantity": li.quantity,
            }
            for li in line_items
        ]
        # One block per location, and item_ids must be unique within it -- Turn 14 rejects the
        # order otherwise. Both hold here: a single block, and the cart cannot contain the same
        # provider part twice.
        location = {
            "location": self._location,
            "combine_in_out_stock": False,
            "items": items,
        }
        if shipping_code is not None:
            location["shipping"] = {
                "shipping_code": shipping_code,
                "saturday_delivery": False,
                "signature_required": False,
            }
        return [location]

    def _handle_error(
        self,
        e: turn14_client_exceptions.Turn14APIBadResponseCodeError,
        request_payload: typing.Optional[typing.Dict] = None,
    ) -> None:
        raise order_exceptions.OrderValidationError(message=e.message, code=str(e.code), request_payload=request_payload)

    # -- DistributorOrderAdapter ------------------------------------------------------------

    def get_shipping_quote(
        self,
        line_items: typing.List[base.OrderLineItemRequest],
        ship_to: base.ShipToAddress,
        ship_method: typing.Optional[str] = None,
    ) -> base.ShippingQuoteResult:
        # Per Turn 14's docs: on a "default" location quote, providing a shipping_code
        # doesn't select it outright — it filters the computed rate to that carrier only
        # (e.g. shipping_code=3 -> only UPS rates considered). There is no single call that
        # returns multiple priced options; comparing methods means re-quoting per method.
        data = {
            "environment": self._client.environment,
            "locations": self._build_locations(line_items, shipping_code=ship_method),
            "recipient": self._build_recipient(ship_to),
        }
        try:
            response = self._client.create_quote(data)
        except turn14_client_exceptions.Turn14APIBadResponseCodeError as e:
            self._handle_error(e, request_payload=data)

        logger.info("{} Quote response: {}".format(_LOG_PREFIX, repr(response)[:4000]))

        try:
            attrs = response.get("data", {}).get("attributes", {})
            by_external_id = {li.provider_part.provider_external_id: li for li in line_items}

            # Per-item pricing promos (e.g. "6% Added Overstock Item Discount") are a top-level
            # array keyed by item_id, not nested under each shipment — collect them once and
            # attach the same list to every shipment line for that item below.
            promotions_by_item_id: typing.Dict[str, typing.List[base.LinePromotion]] = {}
            for promo in attrs.get("promos", []):
                item_id = str(promo.get("item_id", ""))
                # promo_amount is PER-UNIT; promo_total (= promo_amount * promo_qty) is the real
                # total to deduct from this item's gross line total. Using promo_amount alone (as
                # if it were the whole line's discount) undercounts the deduction by a factor of
                # promo_qty — confirmed against a live quote: a 9-unit line with a $22.61/unit,
                # 7% promo has promo_total=$203.49, not $22.61; using promo_amount alone left
                # po.subtotal $180.88 higher than Turn14's own distributor_quoted_total.
                if promo.get("promo_total") is not None:
                    promo_amount = decimal.Decimal(str(promo["promo_total"]))
                elif promo.get("promo_amount") is not None and promo.get("promo_qty") is not None:
                    promo_amount = decimal.Decimal(str(promo["promo_amount"])) * decimal.Decimal(
                        str(promo["promo_qty"])
                    )
                elif promo.get("promo_amount") is not None:
                    promo_amount = decimal.Decimal(str(promo["promo_amount"]))
                else:
                    promo_amount = None
                promotions_by_item_id.setdefault(item_id, []).append(
                    base.LinePromotion(
                        description=promo.get("promo_description") or "",
                        amount=promo_amount,
                    )
                )

            # California Prop 65 warning applicability is also a top-level array keyed by
            # item_id (only present when shipping to a California address, per Turn14's docs) —
            # folded into each matching line's flags below rather than tracked separately.
            prop_65_item_ids = {str(row.get("item_id", "")) for row in attrs.get("prop_65", [])}

            # Order-level fees (e.g. a dropship fee) aren't tied to any specific item.
            fees = [
                base.LineFee(
                    fee_type=fee.get("fee_type") or "",
                    description=fee.get("fee_description") or "",
                    amount=(
                        decimal.Decimal(str(fee["fee_amount"])) if fee.get("fee_amount") is not None else None
                    ),
                )
                for fee in attrs.get("fees", [])
            ]

            distributor_total = (
                decimal.Decimal(str(attrs["total"])) if attrs.get("total") is not None else None
            )

            warehouse_names = _load_warehouse_names()

            lines: typing.List[base.ShippingQuoteLine] = []
            for shipment in attrs.get("shipment", []):
                # Contrary to the docs' single-object example, an unfiltered quote actually
                # returns a LIST of priced shipping options per shipment here (confirmed
                # against a live response) — this is what lets the FE offer a real picker
                # with live prices from one quote call, no re-quoting needed.
                ship_options = [
                    base.ShipOption(
                        service_level_code=str(opt.get("shipping_code", "")),
                        # Turn14's quote response doesn't name most options at all (see
                        # verbose_eta below) — left blank here and backfilled from the
                        # distributor's method-name catalog downstream (purchase_order_jobs.py),
                        # same as every other adapter.
                        service_level_name="",
                        cost=(decimal.Decimal(str(opt["cost"])) if opt.get("cost") is not None else None),
                        days_in_transit=opt.get("days_in_transit"),
                        # Only present on some options (e.g. the flat-rate "Preferred Access"
                        # one) — a marketing/eligibility blurb, not a clean method name.
                        verbose_eta=opt.get("verbose_eta") or None,
                        # The id submit_order must send back to select THIS exact priced
                        # option — distinct from shipping_code, which recurs across shipments.
                        quote_option_id=(
                            str(opt["shipping_quote_id"]) if opt.get("shipping_quote_id") is not None else None
                        ),
                    )
                    for opt in (shipment.get("shipping") or [])
                ]
                is_out_of_stock = shipment.get("type") == "out_of_stock"
                for item in shipment.get("items", []):
                    external_id = str(item.get("item_id", ""))
                    li = by_external_id.get(external_id)
                    quantity = item.get("quantity", 0)
                    warehouse_code = _normalize_warehouse_code(shipment.get("location"))
                    flags = ["backorder"] if is_out_of_stock else []
                    if external_id in prop_65_item_ids:
                        flags.append("prop_65")
                    lines.append(
                        base.ShippingQuoteLine(
                            line_item_id=li.line_item_id if li else 0,
                            provider_external_id=external_id,
                            quantity_available=0 if is_out_of_stock else quantity,
                            quantity_backordered=quantity if is_out_of_stock else 0,
                            warehouse_code=warehouse_code,
                            warehouse_name=warehouse_names.get(warehouse_code) if warehouse_code else None,
                            ship_options=ship_options,
                            flags=flags,
                            promotions=promotions_by_item_id.get(external_id, []),
                            distributor_unit_price=(
                                decimal.Decimal(str(item["unit_price"]))
                                if item.get("unit_price") is not None
                                else None
                            ),
                            distributor_line_total=(
                                decimal.Decimal(str(item["line_total"]))
                                if item.get("line_total") is not None
                                else None
                            ),
                        )
                    )
        except (AttributeError, TypeError, KeyError, IndexError) as e:
            # Response came back 200 (no Turn14APIBadResponseCodeError), but didn't match the
            # shape we expected to parse — surface the actual payload instead of a bare
            # AttributeError, so the real shape is visible in PurchaseOrder.error_message
            # without needing server-log access to diagnose it.
            raise order_exceptions.OrderValidationError(
                "Unexpected quote response shape from Turn14 ({}: {}). Raw response: {}".format(
                    type(e).__name__, e, repr(response)[:2000]
                ),
                request_payload=data,
            )

        return base.ShippingQuoteResult(
            lines=lines, raw_response=response, distributor_total=distributor_total, fees=fees, request_payload=data
        )

    def submit_order(
        self,
        purchase_order: src_models.PurchaseOrder,
        line_items: typing.List[base.OrderLineItemRequest],
        ship_to: base.ShipToAddress,
    ) -> base.DistributorOrderResult:
        quote_raw = purchase_order.quote_raw_response or {}
        quote_id = quote_raw.get("data", {}).get("id")
        # Real Prop 65 acknowledgement, not a hardcoded False — is_prop_65 is set at quote time
        # from Turn14's own top-level "prop_65" array (see get_shipping_quote). acknowledge_epa/
        # acknowledge_carb stay hardcoded False for now — no confirmed evidence yet that
        # Turn14's quote response carries a parallel array for either, and CARB would need a
        # separate Turn14Items catalog lookup; flagged as a follow-up, not silently guessed at.
        acknowledge_prop_65 = purchase_order.line_items.filter(is_prop_65=True).exists()
        order_notes = _sanitize_order_notes(purchase_order.notes)
        phone_number = ship_to.phone or ""
        po_number = base.resolve_po_number(purchase_order)

        data: typing.Dict = {}
        try:
            if quote_id:
                shipping_ids = self._build_shipping_selection(purchase_order)
                data = {
                    "environment": self._client.environment,
                    "quote_id": quote_id,
                    "po_number": po_number,
                    "acknowledge_prop_65": acknowledge_prop_65,
                    "acknowledge_epa": False,
                    "acknowledge_carb": False,
                    "phone_number": phone_number,
                    "shipping": shipping_ids,
                }
                if order_notes is not None:
                    data["order_notes"] = order_notes
                response = self._client.promote_quote_to_order(data)
            else:
                shipping_code = purchase_order.ship_method or _DEFAULT_SHIPPING_GROUP_CODE
                data = {
                    "environment": self._client.environment,
                    "po_number": po_number,
                    "locations": self._build_locations(line_items, shipping_code=shipping_code),
                    "acknowledge_prop_65": acknowledge_prop_65,
                    "acknowledge_epa": False,
                    "acknowledge_carb": False,
                    "phone_number": phone_number,
                    "recipient": self._build_recipient(ship_to),
                }
                if order_notes is not None:
                    data["order_notes"] = order_notes
                response = self._client.create_order(data)
        except turn14_client_exceptions.Turn14APIBadResponseCodeError as e:
            self._handle_error(e, request_payload=data)

        return self._parse_order_response(response, line_items, request_payload=data)

    @staticmethod
    def _build_shipping_selection(purchase_order: src_models.PurchaseOrder) -> typing.List[typing.Dict]:
        """
        Builds promote_quote_to_order's "shipping" array directly from
        ``purchase_order.shipments`` (the normalized, PO-level shipment list built at quote time
        by ``_run_quote`` — see ``PurchaseOrder.shipments``). Each shipment's
        ``selected_ship_option_id`` IS the distributor's real ``shipping_quote_id`` (see
        ``base.ShipOption.quote_option_id``) — defaulted at quote time (match ``ship_method``'s
        code when offered there, else cheapest) and possibly overridden per shipment via
        ``POST .../shipments/select/`` before this runs. No re-deriving/guessing from
        ``quote_raw_response`` needed anymore — this was the previous approach's whole problem:
        it could only apply one global method choice, not a distinct one per shipment.
        """
        shipping_ids = []
        for shipment in purchase_order.shipments or []:
            selected_id = shipment.get("selected_ship_option_id")
            if not selected_id:
                continue
            # Stored as a string throughout po.shipments (JSON-safe, consistent across
            # distributors), but Turn14's API expects shipping_id as a bare number (confirmed
            # against their raw request example) — cast back before sending.
            try:
                shipping_id = int(selected_id)
            except (TypeError, ValueError):
                shipping_id = selected_id
            shipping_ids.append(
                {"shipping_id": shipping_id, "saturday_delivery": False, "signature_required": False}
            )
        return shipping_ids

    @staticmethod
    def _parse_order_response(
        response: typing.Dict,
        line_items: typing.List[base.OrderLineItemRequest],
        request_payload: typing.Optional[typing.Dict] = None,
    ) -> base.DistributorOrderResult:
        try:
            by_external_id = {li.provider_part.provider_external_id: li for li in line_items}
            order_data = response.get("data", {})
            order_id = str(order_data.get("id", ""))
            attrs = order_data.get("attributes", {})

            placements: typing.List[base.LineItemPlacement] = []
            for shipment in attrs.get("shipment", []):
                for item in shipment.get("items", []):
                    external_id = str(item.get("item_id", ""))
                    li = by_external_id.get(external_id)
                    placements.append(
                        base.LineItemPlacement(
                            line_item_id=li.line_item_id if li else 0,
                            distributor_order_number=order_id,
                            quantity_confirmed=item.get("quantity", 0),
                            warehouse_code=_normalize_warehouse_code(shipment.get("location")),
                            status_message=shipment.get("type"),
                        )
                    )
        except (AttributeError, TypeError, KeyError, IndexError) as e:
            raise order_exceptions.OrderValidationError(
                "Unexpected order response shape from Turn14 ({}: {}). Raw response: {}".format(
                    type(e).__name__, e, repr(response)[:2000]
                ),
                request_payload=request_payload,
            )

        return base.DistributorOrderResult(
            distributor_order_numbers=[order_id] if order_id else [],
            line_item_placements=placements,
            raw_response=response,
            request_payload=request_payload,
        )

    def get_order_status(self, purchase_order: src_models.PurchaseOrder) -> base.OrderStatusResult:
        try:
            response = self._client.get_orders_by_po_number(base.resolve_po_number(purchase_order))
        except turn14_client_exceptions.Turn14APIBadResponseCodeError as e:
            self._handle_error(e)

        try:
            raw_orders = response.get("data", [])
            if isinstance(raw_orders, dict):
                raw_orders = [raw_orders]

            orders = []
            for order in raw_orders:
                attrs = order.get("attributes", {}) or {}
                # Turn14's Order.attributes carries tracking as a list of {ship_method,
                # tracking_number} objects, not a flat "tracking_numbers" array or a top-level
                # "carrier" field (neither key exists anywhere in Turn14's documented Order
                # shape) — reading those non-existent keys meant tracking was silently never
                # captured. Turn14 doesn't give a real carrier code at the order level either;
                # ship_method (e.g. "UPS Ground") is the closest available label, used here as a
                # best-effort stand-in rather than left blank.
                tracking_entries = attrs.get("tracking") or []
                tracking_numbers = [
                    t.get("tracking_number") for t in tracking_entries if t.get("tracking_number")
                ]
                ship_methods = sorted({t.get("ship_method") for t in tracking_entries if t.get("ship_method")})
                orders.append(
                    base.DistributorOrderStatus(
                        distributor_order_number=str(order.get("id", "")),
                        status_code=str(attrs.get("status", "")),
                        tracking_numbers=tracking_numbers,
                        carrier=", ".join(ship_methods) if ship_methods else None,
                        raw_response=order,
                    )
                )
        except (AttributeError, TypeError, KeyError, IndexError) as e:
            raise order_exceptions.OrderValidationError(
                "Unexpected order-status response shape from Turn14 ({}: {}). Raw response: {}".format(
                    type(e).__name__, e, repr(response)[:2000]
                )
            )
        return base.OrderStatusResult(orders=orders)

    def get_orders_by_reference(self, po_reference: str) -> typing.Dict:
        """GET /v1/orders/po/{po_reference} for an arbitrary reference value, not necessarily
        derived from a PurchaseOrder via base.resolve_po_number — used by
        confirmed_purchase_order_sync.refresh_confirmed_purchase_orders, which already has the
        reference to query stored on PurchaseOrderDistributorOrder.raw_response (the value
        Turn14 actually recorded the order under at submit time) rather than needing to
        re-derive it from the PurchaseOrder itself."""
        try:
            return self._client.get_orders_by_po_number(po_reference)
        except turn14_client_exceptions.Turn14APIBadResponseCodeError as e:
            self._handle_error(e)

    def get_orders(self, start_date: str, end_date: str, page: int = 1) -> typing.Dict:
        """GET /v1/orders?start_date=&end_date=&page= -- bulk order list for this account, used
        by confirmed_purchase_order_sync's Turn14 refresh path instead of one
        GET /v1/orders/po/{ref} call per confirmed PO."""
        try:
            return self._client.get_orders(start_date=start_date, end_date=end_date, page=page)
        except turn14_client_exceptions.Turn14APIBadResponseCodeError as e:
            self._handle_error(e)

    def get_invoices_bulk(self, start_date: str, end_date: str, page: int = 1) -> typing.Dict:
        """GET /v1/invoices?start_date=&end_date=&page= -- bulk invoice list for this account.
        Confirmed live: Turn 14's bulk GET /v1/orders never returns a closed order at all (a
        known-Closed, in-range order was completely absent from it), so an invoice match is the
        only bulk signal confirmed_purchase_order_sync has for that transition -- an order with
        at least one invoice is treated as CLOSED, same "invoiced == closed" rule already applied
        to Premier's own per-PO invoice check. Distinct from get_invoices(purchase_order) above,
        which is the older per-PO invoices/po/{ref} lookup."""
        try:
            return self._client.get_invoices(start_date=start_date, end_date=end_date, page=page)
        except turn14_client_exceptions.Turn14APIBadResponseCodeError as e:
            self._handle_error(e)

    def supports_invoices(self) -> bool:
        return True

    def get_invoices(self, purchase_order: src_models.PurchaseOrder) -> typing.List[base.DistributorInvoice]:
        try:
            response = self._client.get_invoices_by_po_number(base.resolve_po_number(purchase_order))
        except turn14_client_exceptions.Turn14APIBadResponseCodeError as e:
            self._handle_error(e)

        try:
            raw_invoices = response.get("data", [])
            if isinstance(raw_invoices, dict):
                raw_invoices = [raw_invoices]

            invoices = []
            for invoice in raw_invoices:
                attrs = invoice.get("attributes", {}) or {}
                invoice_date = attrs.get("date")
                # relationships is a list of single-key dicts (see the raw example in Turn14's
                # docs) — order_id is the distributor's own order id this invoice was billed
                # against, informational only (see DistributorInvoice.distributor_order_number).
                distributor_order_number = None
                for rel in attrs.get("relationships", []) or []:
                    order_rel = rel.get("order") if isinstance(rel, dict) else None
                    if order_rel and order_rel.get("order_id") is not None:
                        distributor_order_number = str(order_rel["order_id"])
                        break
                tracking = [
                    base.InvoiceTrackingEntry(
                        ship_method=t.get("ship_method"), tracking_number=t.get("tracking_number")
                    )
                    for t in (attrs.get("tracking") or [])
                ]
                invoices.append(
                    base.DistributorInvoice(
                        invoice_number=str(attrs.get("invoice_number", "")),
                        invoice_date=(
                            datetime.date.fromisoformat(invoice_date) if invoice_date else None
                        ),
                        distributor_order_number=distributor_order_number,
                        website_order_number=attrs.get("website_order_number"),
                        total_price=(
                            decimal.Decimal(str(attrs["total_price"])) if attrs.get("total_price") is not None else None
                        ),
                        freight=decimal.Decimal(str(attrs["freight"])) if attrs.get("freight") is not None else None,
                        discount_amount=(
                            decimal.Decimal(str(attrs["discount_amount"]))
                            if attrs.get("discount_amount") is not None
                            else None
                        ),
                        paid_amount=(
                            decimal.Decimal(str(attrs["paid_amount"])) if attrs.get("paid_amount") is not None else None
                        ),
                        amount_due=(
                            decimal.Decimal(str(attrs["amount_due"])) if attrs.get("amount_due") is not None else None
                        ),
                        tracking=tracking,
                        comments=attrs.get("comments"),
                        raw_response=invoice,
                    )
                )
        except (AttributeError, TypeError, KeyError, IndexError, ValueError) as e:
            raise order_exceptions.OrderValidationError(
                "Unexpected invoice response shape from Turn14 ({}: {}). Raw response: {}".format(
                    type(e).__name__, e, repr(response)[:2000]
                )
            )
        return invoices

    def cancel_order(self, purchase_order: src_models.PurchaseOrder) -> bool:
        raise order_exceptions.OrderNotSupportedError(
            "Turn 14's Electronic Order API does not expose a cancel-order endpoint."
        )

    def supports_cancel(self) -> bool:
        return False

    def supports_shipping_method_selection(self) -> bool:
        return True

    def list_shipping_methods(self) -> typing.List[base.ShippingMethod]:
        try:
            response = self._client.get_shipping_options()
        except turn14_client_exceptions.Turn14APIBadResponseCodeError as e:
            self._handle_error(e)

        methods = []
        for row in response.get("data", []):
            attrs = row.get("attributes", {}) or {}
            methods.append(
                base.ShippingMethod(
                    code=str(row.get("id", "")),
                    name=attrs.get("transportation_name", ""),
                    carrier_name=attrs.get("carrier_name"),
                )
            )
        return methods
