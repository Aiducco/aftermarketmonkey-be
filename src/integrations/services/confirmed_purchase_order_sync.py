"""
refresh_confirmed_purchase_orders — a distinct, lighter-weight distributor-order refresh for
CONFIRMED purchase orders, separate from the general STATUS_CHECK job (see
sync_purchase_order_statuses / purchase_order_jobs._run_status_check, currently paused).

Policy: a fresh order needs checking often at first, then rarely --
  - Within the first hour after submission: check every time this command runs, so a new PO
    gets checked as often as cron invokes this command (e.g. every 5-10 minutes).
  - After that: check at most once per calendar day, tracked via
    PurchaseOrder.distributor_status_checked_at.

Turn14, Keystone, Meyer, and Premier are implemented so far (per-provider dispatch in
_refresh_purchase_order, via _REFRESH_HANDLERS) -- any other provider kind is logged and
skipped, not crashed on, so this command is safe to run against a mixed-distributor CONFIRMED
queue today and grows adapter-by-adapter later.

For Turn14 specifically: rather than re-deriving the customer PO reference from the
PurchaseOrder (see base.resolve_po_number, used by the general status-check path), this reads
it straight from each PurchaseOrderDistributorOrder.raw_response -- either the original submit
response's attributes.po_number, or (once this command has already run once) a previous run's
stored attributes.purchase_order_number, since Turn14's orders/po/{ref} lookup response uses a
different attribute name for the same value than the order-creation response does (confirmed
directly from live examples of each). The lookup returns every order ever placed under that PO
reference (there can be several over time), so the specific one this row's
distributor_order_number was assigned to (matched against each entry's attributes.
website_order_number -- confirmed live: a submit response's data.id, e.g. "20927114", equals
website_order_number in the later orders/po lookup, NOT that lookup's own outer "id" field,
which is a different, unrelated numbering the general status-check path incorrectly compares
against instead) is found and its raw JSON entirely replaces this row's raw_response. That
same matched entry's attributes.order_number (Turn14's own internal order id) is saved to
distributor_internal_order_number, and attributes.status is translated via
turn_14.translate_order_status into distributor_order_status/distributor_order_status_name
(src.enums.DistributorOrderRawStatus) -- currently a direct OPEN/CLOSED passthrough.

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
tracking entry returned belongs to this PO's single PurchaseOrderDistributorOrder row. Invoices,
however, ARE filterable by salesOrderNumber (confirmed against developer.premierwd.com/
#invoice), so those are fetched directly via
get_invoices_by_sales_order_number(pdo.distributor_order_number) -- no discovery step needed,
unlike get_invoices(purchase_order)'s tracking-then-invoice-number two-step lookup for the older
general status-check path. Fetched invoices are persisted the same way _sync_invoices does (see
purchase_order_jobs.py) via _persist_invoices, and their presence is used to derive
distributor_order_status/distributor_order_status_name (src.enums.DistributorOrderRawStatus) --
Premier gives no OPEN/CLOSED field of its own, so an order with at least one invoice is
considered CLOSED, matching the same "invoiced == closed" rule Keystone/Meyer/Turn14 apply to
their own literal status fields.
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

logger = logging.getLogger(__name__)
_LOG_PREFIX = "[CONFIRMED-PO-SYNC]"

# How long after submission a PO counts as "fresh" and gets checked on every run.
_FREQUENT_CHECK_WINDOW = datetime.timedelta(hours=1)


def _should_check(po: src_models.PurchaseOrder, now: datetime.datetime) -> bool:
    """True if this PO is due for a distributor refresh right now, per the module's
    frequent-then-daily policy."""
    if po.submitted_at and (now - po.submitted_at) <= _FREQUENT_CHECK_WINDOW:
        return True
    if po.distributor_status_checked_at is None:
        return True
    return po.distributor_status_checked_at.date() < now.date()


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
    filter at all), unlike invoices below, which ARE filterable by salesOrderNumber and so are
    fetched directly via pdo.distributor_order_number -- no discovery step needed.
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
    pdo.raw_response = {"tracking": [o.raw_response for o in result.orders if o.raw_response]}
    pdo.tracking_numbers = tracking_numbers
    if carriers:
        pdo.carrier = ", ".join(carriers)
        update_fields.append("carrier")

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


# Per-adapter-type refresh handler, all sharing the (adapter, po, pdo) signature -- add an entry
# here (and its own _refresh_<x>_distributor_order function) as each new distributor is wired up.
_REFRESH_HANDLERS = {
    turn_14_adapter.Turn14OrderAdapter: _refresh_turn14_distributor_order,
    keystone_adapter.KeystoneOrderAdapter: _refresh_keystone_distributor_order,
    meyer_adapter.MeyerOrderAdapter: _refresh_meyer_distributor_order,
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
    frequent-then-daily cadence gate entirely, since this is an explicit, user-initiated
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
        .select_related("company_provider__provider")
        .prefetch_related("distributor_orders")
    )
    for po in qs:
        if not _should_check(po, now):
            skipped += 1
            continue
        _refresh_purchase_order(po)
        po.distributor_status_checked_at = now
        po.save(update_fields=["distributor_status_checked_at", "updated_at"])
        checked += 1
    logger.info("{} Checked {} PO(s), skipped {} (not due yet).".format(_LOG_PREFIX, checked, skipped))
    return {"checked": checked, "skipped": skipped}
