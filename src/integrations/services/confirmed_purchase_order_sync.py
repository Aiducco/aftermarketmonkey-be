"""
refresh_confirmed_purchase_orders — a distinct, lighter-weight distributor-order refresh for
CONFIRMED purchase orders, separate from the general STATUS_CHECK job (see
sync_purchase_order_statuses / purchase_order_jobs._run_status_check, currently paused).

Policy: a fresh order needs checking often at first, then rarely --
  - Within the first hour after submission: check every time this command runs, so a new PO
    gets checked as often as cron invokes this command (e.g. every 5-10 minutes).
  - After that: check at most once per calendar day, tracked via
    PurchaseOrder.distributor_status_checked_at.

Only Turn14 is implemented so far (per-provider dispatch in _refresh_purchase_order) -- any
other provider kind is logged and skipped, not crashed on, so this command is safe to run
against a mixed-distributor CONFIRMED queue today and grows adapter-by-adapter later.

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
against instead) is found and its raw JSON entirely replaces this row's raw_response.
"""
import datetime
import logging
import typing

from django.utils import timezone

from src import enums as src_enums
from src import models as src_models
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


def _extract_turn14_reference(raw_response: typing.Optional[typing.Dict]) -> typing.Optional[str]:
    """The customer PO reference to query GET /v1/orders/po/{ref} with, read from whatever
    shape PurchaseOrderDistributorOrder.raw_response currently holds — see module docstring
    for why two different attribute names both have to be checked."""
    if not isinstance(raw_response, dict):
        return None
    data = raw_response.get("data")
    if isinstance(data, dict):
        attrs = data.get("attributes") or {}
    elif isinstance(data, list) and data:
        attrs = data[0].get("attributes") or {}
    else:
        # Already-unwrapped shape: what this same command stores back after a previous run
        # (see _refresh_turn14_distributor_order) — a bare orders/po/{ref} list entry, not
        # wrapped in a "data" key at all.
        attrs = raw_response.get("attributes") or {}
    return attrs.get("po_number") or attrs.get("purchase_order_number") or None


def _refresh_turn14_distributor_order(
    adapter: turn_14_adapter.Turn14OrderAdapter, pdo: src_models.PurchaseOrderDistributorOrder
) -> None:
    reference = _extract_turn14_reference(pdo.raw_response)
    if not reference:
        logger.warning(
            "{} No po_number/purchase_order_number found in raw_response for "
            "PurchaseOrderDistributorOrder id={}; skipping.".format(_LOG_PREFIX, pdo.id)
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

    pdo.raw_response = matched
    pdo.save(update_fields=["raw_response", "updated_at"])
    logger.info(
        "{} Updated raw_response for PurchaseOrderDistributorOrder id={} (distributor_order_number={}).".format(
            _LOG_PREFIX, pdo.id, pdo.distributor_order_number
        )
    )


def _refresh_purchase_order(po: src_models.PurchaseOrder) -> None:
    adapter = order_registry.get_adapter(po.company_provider)
    if adapter is None:
        logger.info(
            "{} No order adapter available for purchase_order_id={} (company_provider_id={}); "
            "skipping.".format(_LOG_PREFIX, po.id, po.company_provider_id)
        )
        return

    if not isinstance(adapter, turn_14_adapter.Turn14OrderAdapter):
        logger.info(
            "{} {} not implemented yet for purchase_order_id={}; skipping.".format(
                _LOG_PREFIX, po.company_provider.provider.kind_name, po.id
            )
        )
        return

    for pdo in po.distributor_orders.all():
        try:
            _refresh_turn14_distributor_order(adapter, pdo)
        except Exception:
            logger.exception(
                "{} Failed refreshing PurchaseOrderDistributorOrder id={} for "
                "purchase_order_id={}.".format(_LOG_PREFIX, pdo.id, po.id)
            )


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
