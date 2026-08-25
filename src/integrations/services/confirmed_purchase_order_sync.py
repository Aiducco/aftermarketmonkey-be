"""
refresh_confirmed_purchase_orders — a distinct, lighter-weight distributor-order refresh for
CONFIRMED purchase orders, separate from the general STATUS_CHECK job (see
sync_purchase_order_statuses / purchase_order_jobs._run_status_check, currently paused).

Policy: a fresh order needs checking often at first, then less often --
  - Within the first hour after submission: check every time this command runs, so a new PO
    gets checked as often as cron invokes this command (e.g. every 5-10 minutes).
  - After that: check at most once every _STALE_CHECK_INTERVAL, tracked via
    PurchaseOrder.distributor_status_checked_at. A rolling interval rather than "once per
    calendar day" -- the latter had up to ~24h of visible staleness for an order that finishes
    processing on the distributor's side shortly after the calendar day's one check already ran
    (confirmed live: a Turn14 order that closed a few hours after its fresh window ended sat as
    "still open" here until the next day's check, even though a manual refresh at any point in
    between showed the true CLOSED status immediately -- same underlying refresh logic either
    way, just gated by how long since the calendar day flipped instead of how long since the
    last real check).

Turn14, Keystone, Meyer, Premier, and Wheel Pros are implemented so far (per-provider dispatch
in _refresh_purchase_order, via _REFRESH_HANDLERS) -- any other provider kind is logged and
skipped, not crashed on, so this command is safe to run against a mixed-distributor CONFIRMED
queue today and grows adapter-by-adapter later.

For Turn14 specifically: _refresh_turn14_orders_for_company replaces one GET /v1/orders/po/{ref}
call per PO with a handful of bulk, paginated GET /v1/orders and GET /v1/invoices calls per
company. Confirmed live that these two aren't redundant: GET /v1/orders?start_date&end_date
never returns a closed order at all (a real, verified-Closed order sat completely absent from
it for a date range that fully contained it), while every one of a live 20-PO test batch's
already-closed orders showed up in GET /v1/invoices for the same window. So orders is read as
"currently open" and invoices is read as "closed" (an order with at least one invoice is done --
the same "invoiced == closed" rule Premier's own per-PO invoice check already applies) -- an
invoice match takes priority over an order match when a reference appears in both.

The reference each PurchaseOrderDistributorOrder is matched against is read from
PurchaseOrderDistributorOrder.raw_response -- either the original submit response's
attributes.po_number, or (once this command has already run once) a previous run's stored
attributes.purchase_order_number, since Turn14's orders/po/{ref} lookup response uses a
different attribute name for the same value than the order-creation response does (confirmed
directly from live examples of each). Both bulk endpoints return every order/invoice ever
placed under that PO reference (there can be several over time, and Turn14 can split one
reference into several distributor orders and even several invoices per distributor order --
confirmed live), so the specific entry this row's distributor_order_number was assigned to is
found by matching each candidate entry's attributes.website_order_number (confirmed live: a
submit response's data.id, e.g. "20927114", equals website_order_number in both the orders and
invoices lookups, NOT either lookup's own outer "id" field, which is a different, unrelated
numbering the general status-check path incorrectly compares against instead).

An order match's matched entry's raw JSON entirely replaces this row's raw_response;
attributes.order_number (Turn14's own internal order id) is saved to
distributor_internal_order_number, and attributes.status is translated via
turn_14.translate_order_status into distributor_order_status/distributor_order_status_name
(src.enums.DistributorOrderRawStatus) -- always "Open" in practice, since GET /v1/orders never
returns anything else. An invoice match instead sets distributor_order_status/_name directly to
CLOSED (Turn14's invoice payload has no status field of its own to translate), stores the
invoice entry as raw_response, and additionally persists the invoice into PurchaseOrderInvoice
(_persist_turn14_invoice) -- something the old per-PO Turn14 path never did at all.

For Keystone: distributor_order_number IS the po_number we submitted (Keystone hands back no
separate order id at submit time -- see KeystoneOrderAdapter.submit_order). This calls
get_order_status_by_reference(pdo.distributor_order_number) -- NOT the adapter's own
get_order_status(purchase_order) -- because that re-derives the query PONumber from the
PurchaseOrder's *current* po_name/po_number (base.resolve_po_number), which can drift from what
was actually submitted if po_name is set/changed afterward (confirmed live: a PO submitted under
PONumber "AMS-000036" whose po_name was later set to "30747" made get_order_status silently
query the wrong PO and find nothing -- the same "don't re-derive the reference from mutable
PurchaseOrder state" lesson as Turn14's extraction above, just triggered a different way).
Pulls the matching entry's EKKEY# (Keystone's own internal order number) into
distributor_internal_order_number, translating its EKSTAT via keystone.translate_order_status
into distributor_order_status/distributor_order_status_name. Also runs the PO's full raw row
set (every line item x every status transition) through keystone.decode_and_merge_order_history
into PurchaseOrderDistributorOrder.processed_order -- one human-readable, merged entry per line
item (by VCPN) instead of GetOrderHistory's one-row-per-status-transition shape.

For Meyer: distributor_order_number IS Meyer's own real OrderNumber, assigned at submit time
(see MeyerOrderAdapter.submit_order) -- querying SalesOrderDetail by that exact value always
returns exactly one order (its single-object response mode), so there's no searching/matching
among several entries like Turn14/Keystone need. Calls
get_order_status_by_reference(pdo.distributor_order_number) for the same reference-drift reason
as Keystone's own fix. The response's CustomerPO field (what we actually submitted) is saved to
po_number if not already set, and its "Invoiced" Yes/No (already normalized by
get_order_status_by_reference into status_code "INVOICED"/"OPEN") is translated via
meyer.translate_order_status into distributor_order_status/distributor_order_status_name.

For Premier: distributor_order_number IS Premier's real salesOrderNumber, confirmed live to
actually be present in a POST /sales-orders/ response despite the adapter's own module
docstring previously believing it never was (see PremierOrderAdapter.submit_order/
_parse_submit_response). Order status/tracking still has to go through
adapter.get_order_status(purchase_order) using base.resolve_po_number, since Premier's
/tracking endpoint has no salesOrderNumber filter at all (confirmed against
developer.premierwd.com) -- unlike Turn14/Keystone/Meyer, there's no drift-safe alternative
reference available for that specific lookup, and Premier never fans one PO out into multiple
distributor orders, so there's no searching/matching several entries by number either; every
tracking entry returned belongs to this PO's single PurchaseOrderDistributorOrder row -- but
GET /tracking only ever carries shipping-specific fields (trackingNumber/carrier/isDropShip/
packageItems, confirmed against developer.premierwd.com/#tracking), never full order details,
so GET /sales-orders/{salesOrderNumber} (get_full_order(pdo.distributor_order_number), usable
now that the real number is captured) is also fetched and merged into raw_response alongside
tracking, instead of raw_response ending up with only the narrow tracking shape. That full
order's customerPurchaseOrderNumber backfills po_number too, same as Keystone/Meyer's own
backfill-on-refresh. Invoices, however, ARE filterable by salesOrderNumber (confirmed against
developer.premierwd.com/#invoice), so those are fetched directly via
get_invoices_by_sales_order_number(pdo.distributor_order_number) -- no discovery step needed,
unlike get_invoices(purchase_order)'s tracking-then-invoice-number two-step lookup for the older
general status-check path. Fetched invoices are persisted the same way _sync_invoices does (see
purchase_order_jobs.py) via _persist_invoices, and their presence is used to derive
distributor_order_status/distributor_order_status_name (src.enums.DistributorOrderRawStatus) --
Premier gives no OPEN/CLOSED field of its own, so an order with at least one invoice is
considered CLOSED, matching the same "invoiced == closed" rule Keystone/Meyer/Turn14 apply to
their own literal status fields.

For Wheel Pros: distributor_order_number IS the real supplierOrderNumber from the create
response (see WheelProsOrderAdapter.submit_order/_parse_submit_response) -- like Premier/
Keystone, Wheel Pros never fans one submission out into several distributor orders (no per-line
confirmation detail at all, per that adapter's own module docstring), so there's no
searching/matching among several entries sharing one PO reference either. adapter.
get_order_status(purchase_order) already resolves the query by pdo.distributor_order_number
whenever at least one PurchaseOrderDistributorOrder exists for this PO (see
WheelProsOrderAdapter.get_order_status), the same drift-safe pattern Keystone/Meyer's own
get_order_status_by_reference enforces, so no separate by-reference call is needed here. Wheel
Pros has no invoice API at all (supports_invoices() stays False) and no order-level status field
of its own either (confirmed against its live OrderTrackingResponseSchema) -- delivery_status
(already normalized from trackingInfo[].statusCode by get_order_status) is the only completion
signal available, so distributor_order_status is derived from that instead: CLOSED once every
order's delivery_status is a terminal one ("delivered" or "cancelled"), OPEN while any is still
missing/"in_transit".
"""
import datetime
import logging
import typing

from django.utils import timezone

from src import enums as src_enums
from src import models as src_models
from src.integrations.orders import exceptions as order_exceptions
from src.integrations.orders import keystone as keystone_adapter
from src.integrations.orders import meyer as meyer_adapter
from src.integrations.orders import premier as premier_adapter
from src.integrations.orders import registry as order_registry
from src.integrations.orders import turn_14 as turn_14_adapter
from src.integrations.orders import wheelpros as wheelpros_adapter

logger = logging.getLogger(__name__)
_LOG_PREFIX = "[CONFIRMED-PO-SYNC]"

# How long after submission a PO counts as "fresh" and gets checked on every run.
_FREQUENT_CHECK_WINDOW = datetime.timedelta(hours=1)

# How long a PO can go between checks once it's past the fresh window. A rolling interval
# (see the module docstring) rather than "once per calendar day" -- was hours=24 (as
# once-per-calendar-day), narrowed after confirming live that a PO whose distributor-side
# status changed shortly after the calendar day's single check already ran could sit visibly
# stale here for up to ~24h despite being genuinely done, until either the next day's check or
# a manual refresh (refresh_purchase_order_now) caught up.
_STALE_CHECK_INTERVAL = datetime.timedelta(hours=4)

# Turn14-specific: a tighter rolling interval than every other provider's _STALE_CHECK_INTERVAL,
# affordable only because the bulk GET /v1/orders refresh (_refresh_turn14_orders_for_company)
# replaced one GET /v1/orders/po/{ref} call per PO with one/a few paginated calls per company --
# checking more often no longer means more requests per PO checked.
_TURN14_STALE_CHECK_INTERVAL = datetime.timedelta(hours=1)


def _should_check(po: src_models.PurchaseOrder, now: datetime.datetime) -> bool:
    """True if this PO is due for a distributor refresh right now, per the module's
    frequent-then-rolling-interval policy."""
    if po.submitted_at and (now - po.submitted_at) <= _FREQUENT_CHECK_WINDOW:
        return True
    if po.distributor_status_checked_at is None:
        return True
    return (now - po.distributor_status_checked_at) >= _STALE_CHECK_INTERVAL


def _should_check_turn14(po: src_models.PurchaseOrder, now: datetime.datetime) -> bool:
    """Same frequent-window rule as _should_check, but _TURN14_STALE_CHECK_INTERVAL (1h) instead
    of _STALE_CHECK_INTERVAL (4h) once past it."""
    if po.submitted_at and (now - po.submitted_at) <= _FREQUENT_CHECK_WINDOW:
        return True
    if po.distributor_status_checked_at is None:
        return True
    return (now - po.distributor_status_checked_at) >= _TURN14_STALE_CHECK_INTERVAL


def _refresh_turn14_distributor_order(
    adapter: turn_14_adapter.Turn14OrderAdapter,
    po: src_models.PurchaseOrder,
    pdo: src_models.PurchaseOrderDistributorOrder,
) -> None:
    # po is unused here (Turn14's reference comes from pdo.raw_response, not the PurchaseOrder
    # itself) -- accepted anyway so every _REFRESH_HANDLERS entry shares one call signature.
    # pdo.po_number is captured up front at submit time (see purchase_order_jobs._run_submit)
    # so this normally doesn't need to touch raw_response at all; the extraction fallback only
    # matters for rows created before that field existed.
    reference = pdo.po_number or turn_14_adapter.extract_po_reference(pdo.raw_response)
    if not reference:
        logger.warning(
            "{} No po_number/purchase_order_number found for PurchaseOrderDistributorOrder "
            "id={}; skipping.".format(_LOG_PREFIX, pdo.id)
        )
        return

    response = adapter.get_orders_by_reference(reference)
    entries = response.get("data", [])
    if isinstance(entries, dict):
        entries = [entries]

    matched = None
    for entry in entries:
        attrs = entry.get("attributes") or {}
        website_order_number = attrs.get("website_order_number")
        if website_order_number is not None and str(website_order_number) == str(pdo.distributor_order_number):
            matched = entry
            break

    if matched is None:
        logger.info(
            "{} No entry in orders/po/{} matched distributor_order_number={} for "
            "PurchaseOrderDistributorOrder id={}.".format(
                _LOG_PREFIX, reference, pdo.distributor_order_number, pdo.id
            )
        )
        return

    update_fields = ["raw_response", "updated_at"]
    pdo.raw_response = matched
    if not pdo.po_number:
        # Backfills rows created before po_number was captured at submit time.
        pdo.po_number = reference
        update_fields.append("po_number")

    # attrs here is still the matched entry's attributes (loop breaks right after assigning
    # `matched`) -- order_number is Turn14's own internal order id, distinct from both
    # distributor_order_number (website_order_number) and po_number.
    pdo.distributor_internal_order_number = attrs.get("order_number")
    update_fields.append("distributor_internal_order_number")

    raw_status = turn_14_adapter.translate_order_status(attrs.get("status"))
    if raw_status is not None:
        pdo.distributor_order_status = raw_status.value
        pdo.distributor_order_status_name = raw_status.name
        update_fields += ["distributor_order_status", "distributor_order_status_name"]
    else:
        logger.warning(
            "{} Unrecognized Turn14 order status {!r} for PurchaseOrderDistributorOrder "
            "id={}; leaving distributor_order_status unset.".format(
                _LOG_PREFIX, attrs.get("status"), pdo.id
            )
        )

    pdo.save(update_fields=update_fields)
    logger.info(
        "{} Updated raw_response for PurchaseOrderDistributorOrder id={} (distributor_order_number={}).".format(
            _LOG_PREFIX, pdo.id, pdo.distributor_order_number
        )
    )


def _index_by_po_reference(data: typing.Dict, index: typing.Dict[str, typing.List[typing.Dict]]) -> None:
    """Add one GET /v1/orders or GET /v1/invoices page's rows to ``index``, keyed by
    attributes.purchase_order_number -- the same field name and the same reference
    _refresh_turn14_distributor_order queries orders/po/{ref} with, on both endpoints
    (confirmed live)."""
    for entry in data.get("data", []):
        attrs = entry.get("attributes") or {}
        ref = attrs.get("purchase_order_number")
        if ref:
            index.setdefault(str(ref), []).append(entry)


def _find_by_website_order_number(
    entries: typing.List[typing.Dict], distributor_order_number: typing.Optional[str]
) -> typing.Optional[typing.Dict]:
    for entry in entries:
        attrs = entry.get("attributes") or {}
        website_order_number = attrs.get("website_order_number")
        if website_order_number is not None and str(website_order_number) == str(distributor_order_number):
            return entry
    return None


def _persist_turn14_invoice(po: src_models.PurchaseOrder, attrs: typing.Dict) -> None:
    """Writes one bulk GET /v1/invoices entry into PurchaseOrderInvoice -- the old per-PO Turn14
    refresh never persisted invoices at all (unlike Premier/Meyer/Keystone's own paths), it only
    used invoice presence as a status signal. Reads the raw attrs dict directly rather than going
    through base.DistributorInvoice/_persist_invoices, since the bulk path has no adapter-level
    parsing step for invoices the way the old per-PO get_invoices() does."""
    invoice_number = str(attrs.get("invoice_number") or "")
    if not invoice_number:
        return

    order_id = None
    for rel in attrs.get("relationships", []) or []:
        order_rel = rel.get("order") if isinstance(rel, dict) else None
        if order_rel and order_rel.get("order_id") is not None:
            order_id = str(order_rel["order_id"])
            break

    invoice_date = attrs.get("date")
    src_models.PurchaseOrderInvoice.objects.update_or_create(
        purchase_order=po,
        invoice_number=invoice_number,
        defaults={
            "invoice_date": datetime.date.fromisoformat(invoice_date) if invoice_date else None,
            "distributor_order_number": order_id,
            "website_order_number": attrs.get("website_order_number"),
            "total_price": attrs.get("total_price"),
            "freight": attrs.get("freight"),
            "discount_amount": attrs.get("discount_amount"),
            "paid_amount": attrs.get("paid_amount"),
            "amount_due": attrs.get("amount_due"),
            "tracking": attrs.get("tracking") or [],
            "line_items": attrs.get("lines") or [],
            "comments": attrs.get("comments"),
            "raw_response": {"type": "Invoice", "attributes": attrs},
        },
    )


def _apply_turn14_order_match(
    pdo: src_models.PurchaseOrderDistributorOrder, matched: typing.Dict, reference: str
) -> None:
    attrs = matched.get("attributes") or {}
    update_fields = ["raw_response", "updated_at"]
    pdo.raw_response = matched
    if not pdo.po_number:
        pdo.po_number = reference
        update_fields.append("po_number")

    pdo.distributor_internal_order_number = attrs.get("order_number")
    update_fields.append("distributor_internal_order_number")

    raw_status = turn_14_adapter.translate_order_status(attrs.get("status"))
    if raw_status is not None:
        pdo.distributor_order_status = raw_status.value
        pdo.distributor_order_status_name = raw_status.name
        update_fields += ["distributor_order_status", "distributor_order_status_name"]
    else:
        logger.warning(
            "{} Unrecognized Turn14 order status {!r} for PurchaseOrderDistributorOrder "
            "id={}; leaving distributor_order_status unset.".format(
                _LOG_PREFIX, attrs.get("status"), pdo.id
            )
        )

    pdo.save(update_fields=update_fields)
    logger.info(
        "{} (bulk orders) Updated raw_response for PurchaseOrderDistributorOrder id={} "
        "(distributor_order_number={}).".format(_LOG_PREFIX, pdo.id, pdo.distributor_order_number)
    )


def _apply_turn14_invoice_match(
    po: src_models.PurchaseOrder,
    pdo: src_models.PurchaseOrderDistributorOrder,
    matched: typing.Dict,
    reference: str,
) -> None:
    """An invoice match means this distributor order shipped -- GET /v1/orders never surfaces a
    closed order at all (confirmed live), so this is the only bulk signal available for that
    transition. Same "invoiced == closed" rule _refresh_premier_distributor_order already
    applies to its own per-PO invoice check."""
    attrs = matched.get("attributes") or {}
    update_fields = ["raw_response", "distributor_order_status", "distributor_order_status_name", "updated_at"]
    pdo.raw_response = matched
    if not pdo.po_number:
        pdo.po_number = reference
        update_fields.append("po_number")

    order_id = None
    for rel in attrs.get("relationships", []) or []:
        order_rel = rel.get("order") if isinstance(rel, dict) else None
        if order_rel and order_rel.get("order_id") is not None:
            order_id = str(order_rel["order_id"])
            break
    if order_id:
        pdo.distributor_internal_order_number = order_id
        update_fields.append("distributor_internal_order_number")

    pdo.distributor_order_status = src_enums.DistributorOrderRawStatus.CLOSED.value
    pdo.distributor_order_status_name = src_enums.DistributorOrderRawStatus.CLOSED.name
    pdo.save(update_fields=update_fields)

    _persist_turn14_invoice(po, attrs)
    logger.info(
        "{} (bulk invoices) Marked PurchaseOrderDistributorOrder id={} CLOSED "
        "(distributor_order_number={}).".format(_LOG_PREFIX, pdo.id, pdo.distributor_order_number)
    )


def _match_and_update_turn14_pdo(
    po: src_models.PurchaseOrder,
    pdo: src_models.PurchaseOrderDistributorOrder,
    orders_index: typing.Dict[str, typing.List[typing.Dict]],
    invoices_index: typing.Dict[str, typing.List[typing.Dict]],
) -> None:
    """Same matching + field-update logic as _refresh_turn14_distributor_order, sourced from
    pre-fetched bulk indices instead of live per-PO calls. An invoices_index match wins over an
    orders_index match when a reference appears in both -- invoiced is a stronger, later signal
    than merely appearing in the (always-open) bulk orders list."""
    reference = pdo.po_number or turn_14_adapter.extract_po_reference(pdo.raw_response)
    if not reference:
        logger.warning(
            "{} No po_number/purchase_order_number found for PurchaseOrderDistributorOrder "
            "id={}; skipping.".format(_LOG_PREFIX, pdo.id)
        )
        return

    invoice_match = _find_by_website_order_number(
        invoices_index.get(str(reference), []), pdo.distributor_order_number
    )
    if invoice_match is not None:
        _apply_turn14_invoice_match(po, pdo, invoice_match, reference)
        return

    order_match = _find_by_website_order_number(
        orders_index.get(str(reference), []), pdo.distributor_order_number
    )
    if order_match is not None:
        _apply_turn14_order_match(pdo, order_match, reference)
        return

    logger.info(
        "{} No entry in the bulk orders or invoices index matched reference={} "
        "distributor_order_number={} for PurchaseOrderDistributorOrder id={}.".format(
            _LOG_PREFIX, reference, pdo.distributor_order_number, pdo.id
        )
    )


# How many days back a "recent" fetch (fresh-submission or never-checked POs) looks -- generous
# enough that a just-submitted order is always inside it, narrow enough to usually be one page.
_RECENT_ORDERS_LOOKBACK_DAYS = 7

# Fallback lookback for a reconciliation fetch when no due PO in the batch has a submitted_at
# (shouldn't normally happen -- every PO gets one at submit time -- but a wide, bounded default
# beats an unbounded "since forever" scan.
_RECONCILE_ORDERS_LOOKBACK_DAYS = 90


def _fetch_all_pages(
    fetch_page: typing.Callable[[int], typing.Dict],
) -> typing.Iterator[typing.Dict]:
    """Keep calling ``fetch_page`` with successive page numbers (1, 2, 3, ...) until Turn 14's
    own meta.total_pages says there's nothing left, yielding each page's raw response. Shared by
    both the orders and the invoices reconciliation fetch below -- same JSON:API {data,
    meta: {total_pages}} pagination shape on both endpoints (confirmed live)."""
    page, total_pages = 1, 1
    while page <= total_pages:
        data = fetch_page(page)
        yield data
        total_pages = (data.get("meta") or {}).get("total_pages", 1)
        page += 1


def _refresh_turn14_orders_for_company(
    company_provider: src_models.CompanyProviders,
    pos: typing.List[src_models.PurchaseOrder],
    now: datetime.datetime,
) -> None:
    """
    Bulk Turn14 refresh for every due CONFIRMED PO belonging to one company: a handful of
    paginated GET /v1/orders + GET /v1/invoices calls instead of one GET /v1/orders/po/{ref}
    call per PO. Both are needed -- confirmed live that GET /v1/orders never returns a closed
    order at all, so invoices is the only bulk source for that transition (see the module
    docstring and _apply_turn14_invoice_match).

    Splits the batch in two by why each PO is due (see _should_check):
      - fresh (submitted within the last hour) or never-checked: cheap, page-1-only fetch of
        both endpoints over a narrow recent window -- a just-submitted order is always on the
        newest page (Turn 14 returns both newest-first, confirmed live).
      - due only via the rolling _STALE_CHECK_INTERVAL: a full paginated fetch of both endpoints
        back to the oldest PO being reconciled this run ("keep calling" through every page, not
        just the first), so an order or invoice that's slipped past page 1 since it was
        submitted is still found.
    Both fetches land in the same two reference->entries indices; every due PO is then matched
    exactly the same way _refresh_turn14_distributor_order already does, just against
    pre-fetched data.
    """
    adapter = order_registry.get_adapter(company_provider)
    if adapter is None or not isinstance(adapter, turn_14_adapter.Turn14OrderAdapter):
        logger.info(
            "{} No Turn14 order adapter for company_provider_id={}; skipping {} PO(s).".format(
                _LOG_PREFIX, company_provider.id, len(pos)
            )
        )
        return

    recent_pos, reconcile_pos = [], []
    for po in pos:
        is_fresh = po.submitted_at and (now - po.submitted_at) <= _FREQUENT_CHECK_WINDOW
        is_never_checked = po.distributor_status_checked_at is None
        (recent_pos if (is_fresh or is_never_checked) else reconcile_pos).append(po)

    orders_index: typing.Dict[str, typing.List[typing.Dict]] = {}
    invoices_index: typing.Dict[str, typing.List[typing.Dict]] = {}
    end = now.date().isoformat()

    if recent_pos:
        start = (now - datetime.timedelta(days=_RECENT_ORDERS_LOOKBACK_DAYS)).date().isoformat()
        orders_data = adapter.get_orders(start_date=start, end_date=end, page=1)
        _index_by_po_reference(orders_data, orders_index)
        invoices_data = adapter.get_invoices_bulk(start_date=start, end_date=end, page=1)
        _index_by_po_reference(invoices_data, invoices_index)
        logger.info(
            "{} company_provider_id={}: recent fetch (page 1, since {}) -> {} order(s), "
            "{} invoice(s) indexed.".format(
                _LOG_PREFIX, company_provider.id, start,
                len(orders_data.get("data", [])), len(invoices_data.get("data", [])),
            )
        )

    if reconcile_pos:
        earliest = min(
            (po.submitted_at for po in reconcile_pos if po.submitted_at),
            default=now - datetime.timedelta(days=_RECONCILE_ORDERS_LOOKBACK_DAYS),
        )
        start = earliest.date().isoformat()

        orders_seen, orders_pages = 0, 0
        for data in _fetch_all_pages(lambda page: adapter.get_orders(start_date=start, end_date=end, page=page)):
            _index_by_po_reference(data, orders_index)
            orders_seen += len(data.get("data", []))
            orders_pages += 1

        invoices_seen, invoices_pages = 0, 0
        for data in _fetch_all_pages(
            lambda page: adapter.get_invoices_bulk(start_date=start, end_date=end, page=page)
        ):
            _index_by_po_reference(data, invoices_index)
            invoices_seen += len(data.get("data", []))
            invoices_pages += 1

        logger.info(
            "{} company_provider_id={}: reconciliation fetch (since {}) -> {} order(s) across "
            "{} page(s), {} invoice(s) across {} page(s) indexed.".format(
                _LOG_PREFIX, company_provider.id, start,
                orders_seen, orders_pages, invoices_seen, invoices_pages,
            )
        )

    for po in recent_pos + reconcile_pos:
        for pdo in po.distributor_orders.all():
            try:
                _match_and_update_turn14_pdo(po, pdo, orders_index, invoices_index)
            except Exception:
                logger.exception(
                    "{} Failed matching PurchaseOrderDistributorOrder id={} for "
                    "purchase_order_id={}.".format(_LOG_PREFIX, pdo.id, po.id)
                )


def _refresh_keystone_distributor_order(
    adapter: keystone_adapter.KeystoneOrderAdapter,
    po: src_models.PurchaseOrder,
    pdo: src_models.PurchaseOrderDistributorOrder,
) -> None:
    # Queries by pdo.distributor_order_number itself (the PONumber actually submitted), not by
    # re-deriving one from the PurchaseOrder's current po_name/po_number (get_order_status would)
    # -- po_name can be set/changed after submission, which would silently query the wrong PO.
    result = adapter.get_order_status_by_reference(pdo.distributor_order_number)
    matched = next(
        (o for o in result.orders if o.distributor_order_number == pdo.distributor_order_number), None
    )
    if matched is None:
        logger.info(
            "{} No GetOrderHistory entry matched distributor_order_number={} for "
            "PurchaseOrderDistributorOrder id={}.".format(_LOG_PREFIX, pdo.distributor_order_number, pdo.id)
        )
        return

    rows = (matched.raw_response or {}).get("rows") or []
    # get_order_status already sorts rows chronologically before returning, so the last row is
    # the most recent transaction for this PO.
    latest = rows[-1] if rows else {}

    update_fields = ["raw_response", "processed_order", "updated_at"]
    pdo.raw_response = matched.raw_response
    pdo.processed_order = keystone_adapter.decode_and_merge_order_history(rows)
    if not pdo.po_number:
        # Keystone has no separate order id -- distributor_order_number already IS the po_number.
        pdo.po_number = pdo.distributor_order_number
        update_fields.append("po_number")

    internal_order_number = latest.get("EKKEY#")
    if internal_order_number:
        pdo.distributor_internal_order_number = internal_order_number
        update_fields.append("distributor_internal_order_number")

    raw_status = keystone_adapter.translate_order_status(matched.status_code)
    if raw_status is not None:
        pdo.distributor_order_status = raw_status.value
        pdo.distributor_order_status_name = raw_status.name
        update_fields += ["distributor_order_status", "distributor_order_status_name"]
    else:
        logger.warning(
            "{} Unrecognized Keystone EKSTAT {!r} for PurchaseOrderDistributorOrder id={}; "
            "leaving distributor_order_status unset.".format(_LOG_PREFIX, matched.status_code, pdo.id)
        )

    pdo.save(update_fields=update_fields)
    logger.info(
        "{} Updated raw_response for PurchaseOrderDistributorOrder id={} (distributor_order_number={}).".format(
            _LOG_PREFIX, pdo.id, pdo.distributor_order_number
        )
    )


def _refresh_meyer_distributor_order(
    adapter: meyer_adapter.MeyerOrderAdapter,
    po: src_models.PurchaseOrder,
    pdo: src_models.PurchaseOrderDistributorOrder,
) -> None:
    """
    Meyer's distributor_order_number IS its own real OrderNumber, assigned at submit time (see
    MeyerOrderAdapter.submit_order/_parse_submit_response) -- querying SalesOrderDetail by that
    exact value always returns exactly one order (its single-object response mode), unlike
    Turn14/Keystone, which both have to search/match among several entries sharing one PO
    reference.
    """
    result = adapter.get_order_status_by_reference(pdo.distributor_order_number)
    matched = next(
        (o for o in result.orders if o.distributor_order_number == pdo.distributor_order_number), None
    )
    if matched is None:
        logger.info(
            "{} No SalesOrderDetail entry matched distributor_order_number={} for "
            "PurchaseOrderDistributorOrder id={}.".format(_LOG_PREFIX, pdo.distributor_order_number, pdo.id)
        )
        return

    update_fields = ["raw_response", "updated_at"]
    pdo.raw_response = matched.raw_response
    if not pdo.po_number:
        # CustomerPO is what we actually submitted (base.resolve_po_number at submit time),
        # distinct from Meyer's own OrderNumber (distributor_order_number) -- same two-numbering
        # shape as Turn14's po_number/website_order_number, unlike Keystone's single numbering.
        customer_po = (matched.raw_response or {}).get("CustomerPO")
        if customer_po:
            pdo.po_number = customer_po
            update_fields.append("po_number")

    raw_status = meyer_adapter.translate_order_status(matched.status_code)
    if raw_status is not None:
        pdo.distributor_order_status = raw_status.value
        pdo.distributor_order_status_name = raw_status.name
        update_fields += ["distributor_order_status", "distributor_order_status_name"]
    else:
        logger.warning(
            "{} Unrecognized Meyer status_code {!r} for PurchaseOrderDistributorOrder id={}; "
            "leaving distributor_order_status unset.".format(_LOG_PREFIX, matched.status_code, pdo.id)
        )

    pdo.save(update_fields=update_fields)
    logger.info(
        "{} Updated raw_response for PurchaseOrderDistributorOrder id={} (distributor_order_number={}).".format(
            _LOG_PREFIX, pdo.id, pdo.distributor_order_number
        )
    )


def _persist_invoices(po: src_models.PurchaseOrder, invoices: typing.List) -> None:
    """Same persistence shape as purchase_order_jobs._sync_invoices (the older general
    status-check path) -- duplicated rather than shared to avoid a cross-module dependency for
    what's a handful of straightforward field assignments."""
    for invoice in invoices:
        src_models.PurchaseOrderInvoice.objects.update_or_create(
            purchase_order=po,
            invoice_number=invoice.invoice_number,
            defaults={
                "invoice_date": invoice.invoice_date,
                "distributor_order_number": invoice.distributor_order_number,
                "website_order_number": invoice.website_order_number,
                "total_price": invoice.total_price,
                "freight": invoice.freight,
                "discount_amount": invoice.discount_amount,
                "paid_amount": invoice.paid_amount,
                "amount_due": invoice.amount_due,
                "tracking": [
                    {"ship_method": t.ship_method, "tracking_number": t.tracking_number}
                    for t in invoice.tracking
                ],
                "line_items": [
                    {
                        "part_number": li.part_number,
                        "description": li.description,
                        "quantity": li.quantity,
                        "unit_price": li.unit_price,
                        "total_price": li.total_price,
                        "warehouse_code": li.warehouse_code,
                    }
                    for li in invoice.line_items
                ],
                "comments": invoice.comments,
                "raw_response": invoice.raw_response,
            },
        )


def _refresh_premier_distributor_order(
    adapter: premier_adapter.PremierOrderAdapter,
    po: src_models.PurchaseOrder,
    pdo: src_models.PurchaseOrderDistributorOrder,
) -> None:
    """
    Premier never fans one PurchaseOrder out into multiple distributor orders (see
    PremierOrderAdapter.submit_order), so unlike Turn14/Keystone/Meyer there's no
    searching/matching a specific entry among several sharing one PO reference -- every
    tracking entry get_order_status returns for this PO belongs to this one row. That lookup is
    still keyed by base.resolve_po_number (Premier's /tracking endpoint has no salesOrderNumber
    filter at all and never returns full order details -- only shipping-specific fields, per
    developer.premierwd.com/#tracking), unlike invoices below, which ARE filterable by
    salesOrderNumber and so are fetched directly via pdo.distributor_order_number -- no
    discovery step needed. GET /sales-orders/{salesOrderNumber} (get_full_order) is what
    actually carries the full order (customer, ship-to, priced/warehoused line items) -- fetched
    here and merged into raw_response alongside tracking, instead of raw_response ending up with
    only GET /tracking's narrow shipping fields (its previous behavior, which silently discarded
    the full order data every refresh replaced it with).
    """
    try:
        result = adapter.get_order_status(po)
    except order_exceptions.OrderAdapterError:
        logger.exception(
            "{} get_order_status failed for PurchaseOrderDistributorOrder id={}.".format(_LOG_PREFIX, pdo.id)
        )
        return

    tracking_numbers = sorted({t for o in result.orders for t in o.tracking_numbers if t})
    carriers = sorted({o.carrier for o in result.orders if o.carrier})
    full_order = adapter.get_full_order(pdo.distributor_order_number)

    update_fields = ["raw_response", "tracking_numbers", "updated_at"]
    pdo.raw_response = {
        "order": full_order,
        "tracking": [o.raw_response for o in result.orders if o.raw_response],
    }
    pdo.tracking_numbers = tracking_numbers
    if carriers:
        pdo.carrier = ", ".join(carriers)
        update_fields.append("carrier")
    if not pdo.po_number:
        customer_po = full_order.get("customerPurchaseOrderNumber")
        if customer_po:
            pdo.po_number = customer_po
            update_fields.append("po_number")

    try:
        invoices = adapter.get_invoices_by_sales_order_number(pdo.distributor_order_number)
    except order_exceptions.OrderAdapterError:
        logger.warning(
            "{} Invoice fetch failed for PurchaseOrderDistributorOrder id={}; distributor_order_status "
            "left unset this round.".format(_LOG_PREFIX, pdo.id)
        )
        invoices = None

    if invoices is not None:
        _persist_invoices(po, invoices)
        # Premier gives no OPEN/CLOSED field of its own (unlike Keystone/Meyer/Turn14's literal
        # status) -- having at least one invoice is the only signal available that the order is
        # actually done, same "invoiced == closed" rule the other three apply to their own field.
        raw_status = (
            src_enums.DistributorOrderRawStatus.CLOSED if invoices else src_enums.DistributorOrderRawStatus.OPEN
        )
        pdo.distributor_order_status = raw_status.value
        pdo.distributor_order_status_name = raw_status.name
        update_fields += ["distributor_order_status", "distributor_order_status_name"]

    pdo.save(update_fields=update_fields)
    logger.info(
        "{} Updated raw_response for PurchaseOrderDistributorOrder id={} (distributor_order_number={}).".format(
            _LOG_PREFIX, pdo.id, pdo.distributor_order_number
        )
    )


# Delivery statuses that mean "nothing further will happen to this order" -- see
# _refresh_wheelpros_distributor_order.
_WHEELPROS_TERMINAL_DELIVERY_STATUSES = {"delivered", "cancelled"}


def _refresh_wheelpros_distributor_order(
    adapter: wheelpros_adapter.WheelProsOrderAdapter,
    po: src_models.PurchaseOrder,
    pdo: src_models.PurchaseOrderDistributorOrder,
) -> None:
    """
    Wheel Pros never fans one PurchaseOrder out into multiple distributor orders (single
    supplierOrderNumber per submission, no per-line confirmation detail -- see
    WheelProsOrderAdapter.submit_order), so like Premier/Keystone there's no searching/matching a
    specific entry among several sharing one PO reference. adapter.get_order_status(purchase_order)
    already keys its lookup off pdo.distributor_order_number whenever at least one
    PurchaseOrderDistributorOrder exists for this PO, the same drift-safe pattern Keystone/Meyer's
    own get_order_status_by_reference enforces -- no separate by-reference call needed here.

    Wheel Pros has no invoice API (supports_invoices() stays False) and no order-level
    OPEN/CLOSED field of its own either -- confirmed against its live OrderTrackingResponseSchema,
    salesOrders[] carries no status/orderStatus field at all (see get_order_status). The only
    completion signal available is each result order's own delivery_status (already normalized
    from trackingInfo[].statusCode) -- CLOSED once every returned order's delivery_status is
    terminal ("delivered" or "cancelled"), OPEN while any order has none yet or is still
    "in_transit". No delivery_status at all (e.g. a fresh order Wheel Pros hasn't shipped yet)
    leaves distributor_order_status unset this round rather than guessing OPEN vs. CLOSED from
    nothing.
    """
    try:
        result = adapter.get_order_status(po)
    except order_exceptions.OrderAdapterError:
        logger.exception(
            "{} get_order_status failed for PurchaseOrderDistributorOrder id={}.".format(_LOG_PREFIX, pdo.id)
        )
        return

    tracking_numbers = sorted({t for o in result.orders for t in o.tracking_numbers if t})
    carriers = sorted({o.carrier for o in result.orders if o.carrier})

    update_fields = ["raw_response", "tracking_numbers", "updated_at"]
    pdo.raw_response = {"orders": [o.raw_response for o in result.orders if o.raw_response]}
    pdo.tracking_numbers = tracking_numbers
    if carriers:
        pdo.carrier = ", ".join(carriers)
        update_fields.append("carrier")

    delivery_statuses = [o.delivery_status for o in result.orders if o.delivery_status]
    if delivery_statuses:
        raw_status = (
            src_enums.DistributorOrderRawStatus.CLOSED
            if all(s in _WHEELPROS_TERMINAL_DELIVERY_STATUSES for s in delivery_statuses)
            else src_enums.DistributorOrderRawStatus.OPEN
        )
        pdo.distributor_order_status = raw_status.value
        pdo.distributor_order_status_name = raw_status.name
        update_fields += ["distributor_order_status", "distributor_order_status_name"]

    pdo.save(update_fields=update_fields)
    logger.info(
        "{} Updated raw_response for PurchaseOrderDistributorOrder id={} (distributor_order_number={}).".format(
            _LOG_PREFIX, pdo.id, pdo.distributor_order_number
        )
    )


# Per-adapter-type refresh handler, all sharing the (adapter, po, pdo) signature -- add an entry
# here (and its own _refresh_<x>_distributor_order function) as each new distributor is wired up.
_REFRESH_HANDLERS = {
    turn_14_adapter.Turn14OrderAdapter: _refresh_turn14_distributor_order,
    keystone_adapter.KeystoneOrderAdapter: _refresh_keystone_distributor_order,
    meyer_adapter.MeyerOrderAdapter: _refresh_meyer_distributor_order,
    wheelpros_adapter.WheelProsOrderAdapter: _refresh_wheelpros_distributor_order,
    premier_adapter.PremierOrderAdapter: _refresh_premier_distributor_order,
}


def _refresh_purchase_order(po: src_models.PurchaseOrder) -> None:
    adapter = order_registry.get_adapter(po.company_provider)
    if adapter is None:
        logger.info(
            "{} No order adapter available for purchase_order_id={} (company_provider_id={}); "
            "skipping.".format(_LOG_PREFIX, po.id, po.company_provider_id)
        )
        return

    handler = _REFRESH_HANDLERS.get(type(adapter))
    if handler is None:
        logger.info(
            "{} {} not implemented yet for purchase_order_id={}; skipping.".format(
                _LOG_PREFIX, po.company_provider.provider.kind_name, po.id
            )
        )
        return

    for pdo in po.distributor_orders.all():
        try:
            handler(adapter, po, pdo)
        except Exception:
            logger.exception(
                "{} Failed refreshing PurchaseOrderDistributorOrder id={} for "
                "purchase_order_id={}.".format(_LOG_PREFIX, pdo.id, po.id)
            )


def refresh_purchase_order_now(po: src_models.PurchaseOrder) -> None:
    """
    Public single-PO entry point for on-demand refreshes -- see the
    .../purchase-orders/<id>/refresh-status/ API endpoint (purchase_orders_services.
    refresh_purchase_order_status). Runs the exact same per-distributor refresh logic
    refresh_confirmed_purchase_orders' batch sweep uses, but bypasses _should_check's
    frequent-then-rolling-interval cadence gate entirely, since this is an explicit, user-initiated
    request rather than a scheduled poll -- a user clicking "refresh" should always get a real
    refresh, not "not due yet".
    """
    _refresh_purchase_order(po)
    po.distributor_status_checked_at = timezone.now()
    po.save(update_fields=["distributor_status_checked_at", "updated_at"])


def refresh_confirmed_purchase_orders() -> typing.Dict[str, int]:
    now = timezone.now()
    checked = 0
    skipped = 0
    qs = (
        src_models.PurchaseOrder.objects.filter(status=src_enums.PurchaseOrderStatus.CONFIRMED.value)
        .select_related("company_provider__provider", "company_provider__company")
        .prefetch_related("distributor_orders")
    )

    # Turn14 POs are pulled out and grouped by company so _refresh_turn14_orders_for_company can
    # do one bulk GET /v1/orders fetch per company instead of _refresh_purchase_order's one
    # GET /v1/orders/po/{ref} call per PO. Every other provider is untouched -- same per-PO
    # _refresh_purchase_order path as before.
    turn14_pos_by_company: typing.Dict[int, typing.List[src_models.PurchaseOrder]] = {}
    turn14_company_providers: typing.Dict[int, src_models.CompanyProviders] = {}
    other_pos: typing.List[src_models.PurchaseOrder] = []

    for po in qs:
        is_turn14 = po.company_provider.provider.kind == src_enums.BrandProviderKind.TURN_14.value
        due = _should_check_turn14(po, now) if is_turn14 else _should_check(po, now)
        if not due:
            skipped += 1
            continue
        if is_turn14:
            turn14_pos_by_company.setdefault(po.company_provider_id, []).append(po)
            turn14_company_providers[po.company_provider_id] = po.company_provider
        else:
            other_pos.append(po)

    for po in other_pos:
        _refresh_purchase_order(po)
        po.distributor_status_checked_at = now
        po.save(update_fields=["distributor_status_checked_at", "updated_at"])
        checked += 1

    for company_provider_id, pos in turn14_pos_by_company.items():
        _refresh_turn14_orders_for_company(turn14_company_providers[company_provider_id], pos, now)
        for po in pos:
            po.distributor_status_checked_at = now
            po.save(update_fields=["distributor_status_checked_at", "updated_at"])
            checked += 1

    logger.info("{} Checked {} PO(s), skipped {} (not due yet).".format(_LOG_PREFIX, checked, skipped))
    return {"checked": checked, "skipped": skipped}
