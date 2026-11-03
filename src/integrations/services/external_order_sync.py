"""
sync_external_orders -- discovers distributor-placed ("external") orders we did NOT place
through this app, by polling each capable distributor's own order-history API (see
``src.integrations.orders.history_registry``), de-duplicating against orders we DID place, and
upserting the rest into ``ExternalPurchaseOrder``. See docs/tasks/external-orders/research-be.md
for the full design (Dedup, Incremental polling, Adapters sections) -- this module implements
that plan; read it before changing anything here.

READ-ONLY against every distributor API -- no adapter/client write method is ever called from
this module, only ``OrderHistoryReader.list_orders``/``list_current_orders``/``enrich_new_order``.

Safe to run repeatedly: ``ExternalPurchaseOrder``'s
``unique_together = ["order_account", "distributor_order_number"]`` makes every write here an
``update_or_create``, so running this command three times in a row creates nothing new on the
2nd/3rd run -- see ``_apply_results``.
"""
import datetime
import logging
import typing

from django.db.models import Q
from django.utils import timezone

from src import enums as src_enums
from src import models as src_models
from src.integrations.orders import history, history_registry

logger = logging.getLogger(__name__)
_LOG_PREFIX = "[EXTERNAL-ORDER-SYNC]"

# Narrower than confirmed_purchase_order_sync.py's own lookback constants
# (_RECENT_ORDERS_LOOKBACK_DAYS=7, _RECONCILE_ORDERS_LOOKBACK_DAYS=90) -- this job runs "a few
# times a day" per the brief, so the gap between runs is hours, not a day. 3 days still
# comfortably covers a backdated order or a status flip that lands a day or two after the order
# date.
OVERLAP_DAYS = 3
# First-ever run for an account (no watermark row yet) with no --since given.
DEFAULT_BACKFILL_DAYS = 30
# GET /tracking/date has no documented pagination and a hard 1000-row cap -- a window that
# could plausibly return more than that must be split BEFORE calling, not detected after (see
# research-be.md's "Premier's 1000-row cap").
PREMIER_MAX_WINDOW_DAYS = 14
# The only observable signal /tracking/date gives that a window may have been truncated.
_PREMIER_TRUNCATION_ROW_COUNT = 1000


def _normalize_order_number(value: typing.Optional[str]) -> str:
    """
    Strips Premier's "SO-" prefix (see contract.md's "Premier prefix mismatch is real") -- a
    no-op for every other distributor's own order-number format. Shared here (rather than only
    inside the Premier reader) because dedup (``_match_our_order_account_id``) must normalize
    the SAME way before comparing against
    ``PurchaseOrderDistributorOrder.distributor_order_number``.
    """
    value = (value or "").strip()
    if value.upper().startswith("SO-"):
        return value[3:]
    return value


def _match_our_order_account_id(
    company_provider_id: int,
    distributor_order_number: str,
    customer_po_number: typing.Optional[str],
) -> typing.Tuple[bool, typing.Optional[int]]:
    """
    (matched, order_account_id) -- order_account_id is the account the matching
    PurchaseOrder/PurchaseOrderDistributorOrder was actually submitted through (None means the
    connection's implicit default, same meaning as PurchaseOrder.order_account itself carries).
    Checked by two independent identifiers, in priority order (see research-be.md's Dedup
    section):

      1. distributor_order_number (normalized) against
         PurchaseOrderDistributorOrder.distributor_order_number.
      2. customer_po_number against PurchaseOrderDistributorOrder.po_number (the value actually
         submitted) AND PurchaseOrder.po_number/po_name directly (covers a PO that submitted
         seconds ago and has no PurchaseOrderDistributorOrder row yet).

    Neither identifier present at all (shouldn't happen for any of the four distributors per
    their documented shapes, but defensively) returns (False, None) -- an order we can't
    identify as ours is not safe to assume is ours.
    """
    normalized = _normalize_order_number(distributor_order_number)
    pdo = None
    if normalized:
        pdo = (
            src_models.PurchaseOrderDistributorOrder.objects.filter(
                purchase_order__company_provider_id=company_provider_id,
                distributor_order_number=normalized,
            )
            .select_related("purchase_order")
            .first()
        )
    if pdo is None and customer_po_number:
        pdo = (
            src_models.PurchaseOrderDistributorOrder.objects.filter(
                purchase_order__company_provider_id=company_provider_id,
                po_number=customer_po_number,
            )
            .select_related("purchase_order")
            .first()
        )
    if pdo is not None:
        return True, pdo.purchase_order.order_account_id

    if customer_po_number:
        po = (
            src_models.PurchaseOrder.objects.filter(company_provider_id=company_provider_id)
            .filter(Q(po_number=customer_po_number) | Q(po_name=customer_po_number))
            .first()
        )
        if po is not None:
            return True, po.order_account_id

    return False, None


def _is_ours(
    company_provider_id: int,
    order_account: src_models.CompanyProviderOrderAccount,
    distributor_order_number: str,
    customer_po_number: typing.Optional[str],
) -> bool:
    """
    True if this incoming row is an order WE placed through the app. See
    ``_match_our_order_account_id`` for the two-identifier matching priority this follows.

    A match anywhere on the SAME ``company_provider`` counts as ours, whichever of that
    connection's accounts the PurchaseOrder was submitted through. ``_match_our_order_account_id``
    is already scoped to company_provider_id, so a match is never cross-distributor; within one
    connection the accounts are just credential sets, and a distributor hands the same customer's
    order history to all of them.

    This used to additionally require the matched account to BE the account being polled, on the
    reasoning that a visible duplicate beats silently losing a genuine external order. Trident's
    real data says otherwise: 21 of its own Keystone orders were classed external purely because
    the PO was submitted through the TRU account and rediscovered through the Trident one (or the
    reverse) -- both see the same history. Every single cross-account match was ours; none was a
    genuine external order.

    The coincidence that reasoning feared cannot really happen here either, because the identifier
    being matched is a PO number WE generated and sent at order placement. An order someone put in
    by phone or on the distributor's own site carries whatever reference the rep typed, not one of
    our numbers. The one genuinely risky case -- the same PO number reused against a DIFFERENT
    distributor -- is already excluded by the company_provider scoping.
    """
    matched, matched_order_account_id = _match_our_order_account_id(
        company_provider_id, distributor_order_number, customer_po_number
    )
    if not matched:
        return False
    if matched_order_account_id is not None and matched_order_account_id != order_account.id:
        logger.info(
            "{} distributor_order_number={} matched a PurchaseOrder submitted through "
            "order_account_id={} while polling order_account_id={} on the same connection "
            "(company_provider_id={}); suppressing as ours -- the accounts share one order "
            "history.".format(
                _LOG_PREFIX,
                distributor_order_number,
                matched_order_account_id,
                order_account.id,
                company_provider_id,
            )
        )
    return True


def _self_heal_existing_rows(
    company_provider_id: int,
    order_account: src_models.CompanyProviderOrderAccount,
    results: typing.List[history.ExternalOrderResult],
) -> int:
    """
    Re-checks every ExternalPurchaseOrder row already stored for this account against this
    poll's own freshly-fetched result set, deleting any that NOW match one of our own orders --
    see research-be.md's "Self-healing re-check (race with our own submission)": a customer's
    own order can transiently look external if it's polled before
    confirmed_purchase_order_sync has had a chance to backfill the real distributor order number
    onto our own row. No extra query -- this window's rows are already in memory. Not applied
    for Meyer (handled entirely by the "API Order" flag at ingestion time, with no PO-matching
    inference to ever need correcting).
    """
    normalized_numbers = {_normalize_order_number(r.distributor_order_number) for r in results}
    normalized_numbers.discard("")
    po_numbers = {r.customer_po_number for r in results if r.customer_po_number}
    if not normalized_numbers and not po_numbers:
        return 0

    query = Q(distributor_order_number__in=normalized_numbers) if normalized_numbers else Q(pk__in=[])
    if po_numbers:
        query |= Q(customer_po_number__in=po_numbers)
    candidates = src_models.ExternalPurchaseOrder.objects.filter(order_account=order_account).filter(query)

    healed = 0
    for row in candidates:
        if _is_ours(company_provider_id, order_account, row.distributor_order_number, row.customer_po_number):
            logger.info(
                "{} Self-healing: ExternalPurchaseOrder id={} now matches one of our own orders; "
                "deleting.".format(_LOG_PREFIX, row.id)
            )
            row.delete()
            healed += 1
    return healed


def _window_for_account(
    watermark: typing.Optional[src_models.ExternalOrderSyncWatermark],
    since: typing.Optional[datetime.date],
    now: datetime.datetime,
) -> typing.Tuple[datetime.date, datetime.date]:
    end = now.date()
    if watermark is not None and watermark.last_synced_through is not None:
        start = (watermark.last_synced_through - datetime.timedelta(days=OVERLAP_DAYS)).date()
    elif since is not None:
        start = since
    else:
        start = (now - datetime.timedelta(days=DEFAULT_BACKFILL_DAYS)).date()
    return start, end


def _premier_windows(
    start: datetime.date, end: datetime.date
) -> typing.List[typing.Tuple[datetime.date, datetime.date]]:
    """Splits [start, end] into <=PREMIER_MAX_WINDOW_DAYS-wide sub-windows (see the module-level
    PREMIER_MAX_WINDOW_DAYS constant's own docstring)."""
    if start > end:
        return []
    windows = []
    window_start = start
    step = datetime.timedelta(days=PREMIER_MAX_WINDOW_DAYS)
    while window_start <= end:
        window_end = min(window_start + step - datetime.timedelta(days=1), end)
        windows.append((window_start, window_end))
        window_start = window_end + datetime.timedelta(days=1)
    return windows


def _collect_results(
    reader: history.OrderHistoryReader,
    company_provider: src_models.CompanyProviders,
    start: datetime.date,
    end: datetime.date,
) -> typing.List[history.ExternalOrderResult]:
    kind = company_provider.provider.kind
    results: typing.List[history.ExternalOrderResult] = []

    if kind == src_enums.BrandProviderKind.MEYER.value:
        # SalesCurrent takes no date parameter at all -- always a full re-fetch of every
        # currently-open order, every run, unconditionally (see research-be.md's "Meyer is a
        # genuine special case"). SalesHistory (list_orders) is the only call the watermark
        # window actually bounds.
        results.extend(reader.list_current_orders())
        results.extend(reader.list_orders(start, end))
    elif kind == src_enums.BrandProviderKind.PREMIER_PERFORMANCE.value:
        for window_start, window_end in _premier_windows(start, end):
            window_results = reader.list_orders(window_start, window_end)
            if len(window_results) >= _PREMIER_TRUNCATION_ROW_COUNT:
                logger.warning(
                    "{} Premier /tracking/date returned {} rows for window {}..{} -- possible "
                    "truncation at the documented 1000-row cap.".format(
                        _LOG_PREFIX, len(window_results), window_start, window_end
                    )
                )
            results.extend(window_results)
    else:
        results.extend(reader.list_orders(start, end))

    return results


def _result_to_defaults(
    result: history.ExternalOrderResult,
    company_provider: src_models.CompanyProviders,
    origin_signal: "src_enums.ExternalOrderOriginSignal",
) -> typing.Dict:
    ship_to = result.ship_to or {}
    return {
        "company": company_provider.company,
        "company_provider": company_provider,
        "customer_po_number": result.customer_po_number or None,
        "origin_signal": origin_signal.value,
        "origin_signal_name": origin_signal.name,
        "distributor_status": result.status,
        "order_date": result.order_date,
        "ship_to_name": ship_to.get("name"),
        "ship_to_address1": ship_to.get("address1"),
        "ship_to_address2": ship_to.get("address2"),
        "ship_to_city": ship_to.get("city"),
        "ship_to_state": ship_to.get("state"),
        "ship_to_postal_code": ship_to.get("postal_code"),
        "ship_to_country": ship_to.get("country"),
        "ship_to_phone": ship_to.get("phone"),
        "subtotal": result.subtotal,
        "freight": result.freight,
        "total": result.total,
        "line_items": result.line_items or [],
        "raw_response": result.raw_response,
    }


def _apply_results(
    company_provider: src_models.CompanyProviders,
    account: src_models.CompanyProviderOrderAccount,
    reader: history.OrderHistoryReader,
    results: typing.List[history.ExternalOrderResult],
    dry_run: bool,
) -> typing.Tuple[int, int, int]:
    discovered = updated = healed = 0
    is_meyer = company_provider.provider.kind == src_enums.BrandProviderKind.MEYER.value

    # Collapse to one result per distinct (normalized) order before anything else -- Premier's
    # own window-splitting (_premier_windows) calls list_orders() once per <=14-day sub-window,
    # and a single poll's overall [start, end] can be split into more than one such call; a
    # duplicate occurrence of the same order within ONE run (whatever the cause -- distributor
    # API quirk, or windowing) must never be processed twice in the same pass: the second pass
    # would see `existing` already created by the first, skip enrich_new_order (correctly, per
    # contract.md's "never on every poll"), then overwrite that row with the SECOND
    # occurrence's un-enriched defaults, silently discarding what the first pass just enriched.
    # Last occurrence wins (arbitrary among duplicates -- they're expected to be identical).
    deduped_by_number: typing.Dict[str, history.ExternalOrderResult] = {}
    for result in results:
        key = _normalize_order_number(result.distributor_order_number)
        if key:
            deduped_by_number[key] = result
    results = list(deduped_by_number.values())

    # Meyer is excluded deliberately, matching _self_heal_existing_rows' own docstring: its rows
    # are classified by the authoritative "API Order" flag, never by PO-matching inference, so
    # there is nothing for a re-check to correct -- and letting an inference delete a row Meyer
    # itself told us was external would override the authoritative signal with a weaker one.
    if not dry_run and results and not is_meyer:
        healed = _self_heal_existing_rows(company_provider.id, account, results)

    for result in results:
        if is_meyer:
            if result.api_order_flag:
                # "API Order": "Yes" -- WE placed it; it's already in purchase_orders with its
                # own distributor_order_number set by our own submit flow. Skipped
                # unconditionally, regardless of whether it happens to also PO-match -- Meyer's
                # flag is authoritative on its own, no cross-check needed (see research-be.md's
                # Dedup section).
                continue
            origin_signal = src_enums.ExternalOrderOriginSignal.MEYER_API_ORDER_NO
        else:
            if _is_ours(company_provider.id, account, result.distributor_order_number, result.customer_po_number):
                continue
            origin_signal = src_enums.ExternalOrderOriginSignal.NO_MATCH

        normalized_number = _normalize_order_number(result.distributor_order_number)
        if not normalized_number:
            logger.warning(
                "{} Discovered row with no distributor_order_number at all for "
                "company_provider_id={} order_account_id={}; skipping (can't key an upsert on "
                "an empty identifier).".format(_LOG_PREFIX, company_provider.id, account.id)
            )
            continue
        # Normalize up front so every downstream consumer (the enrichment hook below, and the
        # defaults built from it) sees the same stored value -- Premier's enrich_new_order is
        # keyed by this exact field and 404s against the un-normalized "SO-"-prefixed form
        # (confirmed live).
        result.distributor_order_number = normalized_number

        existing = src_models.ExternalPurchaseOrder.objects.filter(
            order_account=account, distributor_order_number=normalized_number
        ).first()

        if existing is None:
            # Only a genuinely NEW external order gets the extra enrichment call (e.g.
            # Premier's follow-up get_sales_order) -- never on a refresh of one already stored,
            # per contract.md's explicit "never on every poll".
            result = reader.enrich_new_order(result)

        if dry_run:
            logger.info(
                "{} [dry-run] Would {} ExternalPurchaseOrder(order_account_id={}, "
                "distributor_order_number={}).".format(
                    _LOG_PREFIX, "create" if existing is None else "update", account.id, normalized_number
                )
            )
            if existing is None:
                discovered += 1
            else:
                updated += 1
            continue

        defaults = _result_to_defaults(result, company_provider, origin_signal)
        _, created = src_models.ExternalPurchaseOrder.objects.update_or_create(
            order_account=account,
            distributor_order_number=normalized_number,
            defaults=defaults,
        )
        if created:
            discovered += 1
        else:
            updated += 1

    return discovered, updated, healed


def _record_watermark_success(
    account: src_models.CompanyProviderOrderAccount, now: datetime.datetime, synced_through_date: datetime.date
) -> None:
    src_models.ExternalOrderSyncWatermark.objects.update_or_create(
        order_account=account,
        defaults={
            # Naive, like every other DateTimeField in this codebase -- USE_TZ=False project-
            # wide (see conf.settings_base), so timezone.now() itself returns naive "now" in
            # whatever timezone the server runs (UTC in production); combine() here matches
            # that same convention rather than introducing a tz-aware value this backend (and
            # SQLite under conf.settings_test, which outright rejects tz-aware datetimes) can't
            # store.
            "last_synced_through": datetime.datetime.combine(synced_through_date, datetime.time.min),
            "last_run_at": now,
            "last_run_ok": True,
            "last_error": None,
        },
    )


def _record_watermark_failure(
    account: src_models.CompanyProviderOrderAccount, now: datetime.datetime, error_message: str
) -> None:
    src_models.ExternalOrderSyncWatermark.objects.update_or_create(
        order_account=account,
        defaults={
            "last_run_at": now,
            "last_run_ok": False,
            "last_error": (error_message or "")[:2000],
        },
    )


def _active_accounts(
    company_id: typing.Optional[int], provider_kind: typing.Optional[int]
) -> typing.List[src_models.CompanyProviderOrderAccount]:
    qs = src_models.CompanyProviderOrderAccount.objects.filter(
        active=True, company_provider__active=True
    ).select_related("company_provider__provider", "company_provider__company")
    if company_id is not None:
        qs = qs.filter(company_provider__company_id=company_id)
    if provider_kind is not None:
        qs = qs.filter(company_provider__provider__kind=provider_kind)
    return list(qs.order_by("company_provider_id", "id"))


def sync_external_orders(
    company_id: typing.Optional[int] = None,
    provider_kind: typing.Optional[int] = None,
    since: typing.Optional[datetime.date] = None,
    limit: typing.Optional[int] = None,
    dry_run: bool = False,
) -> typing.Dict[str, int]:
    """
    Polls every active order account (optionally narrowed by company_id/provider_kind) whose
    distributor kind has a registered OrderHistoryReader (see
    history_registry.supports_order_discovery), discovers orders we did not place, and upserts
    them into ExternalPurchaseOrder. Safe to run repeatedly -- see the module docstring.

    dry_run=True runs every read and every dedup/match decision exactly as a real run would,
    logs what it WOULD upsert, and returns without touching the database at all (no
    update_or_create, no watermark write, no self-heal delete) -- see research-be.md's Plan step
    5 for how this is verified (two real runs back to back, counting rows before/after each).

    ``limit`` caps how many accounts are polled this invocation (accounts are processed in the
    order ``_active_accounts`` returns them, grouped by company_provider_id) -- for a bounded
    manual run against a large company.
    """
    now = timezone.now()
    accounts = _active_accounts(company_id, provider_kind)
    if limit is not None:
        accounts = accounts[:limit]

    stats = {
        "accounts_polled": 0,
        "accounts_skipped": 0,
        "accounts_failed": 0,
        "orders_discovered": 0,
        "orders_updated": 0,
        "orders_healed": 0,
    }

    for account in accounts:
        company_provider = account.company_provider
        if not history_registry.supports_order_discovery(company_provider.provider.kind):
            # Not an error -- this account simply can't be polled (Motor State et al.). Surfaced
            # to the FE via PolledConnection.status="not_supported", not via this command's
            # stats (see src.api.services.purchase_orders._polled_connections).
            continue

        reader = history_registry.get_history_reader(company_provider, account)
        if reader is None:
            logger.info(
                "{} No history reader constructable for company_provider_id={} order_account_id={} "
                "(credentials likely missing/invalid); skipping.".format(_LOG_PREFIX, company_provider.id, account.id)
            )
            stats["accounts_skipped"] += 1
            continue

        watermark = src_models.ExternalOrderSyncWatermark.objects.filter(order_account=account).first()
        start, end = _window_for_account(watermark, since, now)

        try:
            results = _collect_results(reader, company_provider, start, end)
        except Exception as e:
            logger.exception(
                "{} Poll failed for company_provider_id={} order_account_id={}.".format(
                    _LOG_PREFIX, company_provider.id, account.id
                )
            )
            stats["accounts_failed"] += 1
            if not dry_run:
                _record_watermark_failure(account, now, str(e))
            continue

        discovered, updated, healed = _apply_results(company_provider, account, reader, results, dry_run)
        stats["orders_discovered"] += discovered
        stats["orders_updated"] += updated
        stats["orders_healed"] += healed
        stats["accounts_polled"] += 1

        if not dry_run:
            _record_watermark_success(account, now, end)

    logger.info("{} {}".format(_LOG_PREFIX, stats))
    return stats
