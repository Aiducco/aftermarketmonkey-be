"""
Plan expiry / downgrade lifecycle.

A comp'd Hunter trial (Company.manual_trial_granted_at set, subscription_status="trialing",
subscription_period_end = trial end) ends in a *downgrade*, not a cancellation: the company drops to
Scout limits (see conf/settings_base.py PLANS["scout"]) but nothing is deleted. Distributors over
the Scout cap are *paused* (CompanyProviders.paused_at) and members over the seat cap are
*suspended* (UserProfile.suspended_at), credentials and config untouched, so an upgrade -- or an
admin extending the trial -- brings everything back instantly (restore_company).

Account state (plan_state) is derived from existing columns rather than stored twice:
  trialing   -> subscription_status "trialing" with manual_trial_granted_at set
  downgraded -> subscription_status "downgraded" (not an active Stripe status, so
                billing._plan_key already resolves the plan to "scout" with no change there)
  active     -> everything else (paid, or plain free Scout)

This module owns the rules; views, the demote_expired_trials command and the Stripe webhook only
call into it. All datetimes are naive UTC (USE_TZ = False) -- always timezone.now().
"""
import collections
import datetime
import logging
import math
import typing

from django.conf import settings
from django.db import transaction
from django.db.models import Count, Q
from django.utils import timezone

from src import enums as src_enums
from src import models as src_models
from src.api.services import billing as billing_services
from src.api.services import company_settings as company_settings_services
from src.api.services import integrations as integrations_services
from src.api.services import spaces as spaces_services
from src.integrations.services import integration_pricing_sync_jobs
from src.integrations.services import notifications as notifications_services

logger = logging.getLogger(__name__)

STATE_TRIALING = "trialing"
STATE_ACTIVE = "active"
STATE_DOWNGRADED = "downgraded"

SOURCE_USER = "user"
SOURCE_AUTO = "auto"

# Written to CompanyProviders.paused_reason / UserProfile.suspended_reason. The only reason today;
# kept as a column (not a boolean) so a future non-plan pause does not need another migration.
REASON_PLAN_LIMIT = "plan_limit"

# Purchase order statuses that count as "an order placed through this distributor" for the
# auto-pick ranking and the picker's usage line. DRAFT/QUOTED are cart state, CANCELLED/FAILED
# never reached the distributor.
_PLACED_ORDER_STATUSES = [
    src_enums.PurchaseOrderStatus.SUBMITTING.value,
    src_enums.PurchaseOrderStatus.SUBMITTED.value,
    src_enums.PurchaseOrderStatus.CONFIRMED.value,
    src_enums.PurchaseOrderStatus.PARTIALLY_FULFILLED.value,
    src_enums.PurchaseOrderStatus.FULFILLED.value,
]

_FEED_STATUSES = {"connected", "ingesting", "waiting", "failing"}

# How far back "orders in the last 30 days" and the searches proxy look.
_USAGE_WINDOW_DAYS = 30
# Cap on audit rows read for the searches proxy, so one very busy company cannot make the
# plan-status call (fetched on every app-shell load for admins mid-trial) read an unbounded set.
_SEARCH_PROXY_AUDIT_ROW_CAP = 5000

# The d0 email is retried by every cron run for this many days after the downgrade, so a Resend
# outage at the moment of expiry does not mean the "your trial has ended" email is never sent.
_D0_EMAIL_RETRY_DAYS = 7


class PlanLifecycleError(Exception):
    """A rule violation the view maps to an HTTP response (never 401 -- the frontend logs out on
    it -- and never 402, which opens the global upgrade dialog)."""

    def __init__(self, message: str, *, error_code: str, status_code: int, upgrade_required: bool = False):
        super().__init__(message)
        self.message = message
        self.error_code = error_code
        self.status_code = status_code
        self.upgrade_required = upgrade_required


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------


def iso_utc(value: typing.Optional[datetime.datetime]) -> typing.Optional[str]:
    """Naive-UTC datetime -> "YYYY-MM-DDTHH:MM:SSZ". A bare ``.isoformat()`` carries no zone, and
    the frontend's ``new Date()`` would then read it as local time."""
    return value.strftime("%Y-%m-%dT%H:%M:%SZ") if value else None


def is_feed_connected(company_provider: src_models.CompanyProviders) -> bool:
    """The repo-wide "connected" definition: the credentials carry a truthy "feed" namespace.
    Order-only rows are not part of the plan-limit model (they are neither counted nor paused)."""
    return bool((company_provider.credentials or {}).get("feed"))


def is_paused(company_provider: src_models.CompanyProviders) -> bool:
    return company_provider.paused_at is not None


def feed_connected_providers(company_id: int) -> typing.List[src_models.CompanyProviders]:
    return [
        cp
        for cp in src_models.CompanyProviders.objects.filter(company_id=company_id)
        .select_related("provider")
        .order_by("id")
        if is_feed_connected(cp)
    ]


def family_feed_connected_providers(company_id: int) -> typing.List[src_models.CompanyProviders]:
    """Feed-connected connections of the company AND its feed tenants (spec OQ-2: a space's own feeds pause and
    restore with the company's). Only pausing and restoring use it: counts, the keep picker and the status page
    stay on ``feed_connected_providers``, because own feeds do not count toward the connection limit (OQ-6)."""
    return [
        cp
        for cp in src_models.CompanyProviders.objects.filter(
            company_id__in=spaces_services.family_company_ids(company_id)
        )
        .select_related("provider")
        .order_by("id")
        if is_feed_connected(cp)
    ]


def plan_state(company: src_models.Company) -> str:
    if company.subscription_status == STATE_DOWNGRADED:
        return STATE_DOWNGRADED
    # A company with a real Stripe subscription is "active" even if an admin comped it extra trial time
    # (extend_company_trial allows that): it is paying, so it must never show the trial banner or be
    # downgraded.
    if (
        company.subscription_status == "trialing"
        and company.manual_trial_granted_at is not None
        and not company.subscription_id
    ):
        return STATE_TRIALING
    return STATE_ACTIVE


def plan_limits(plan_key: str) -> dict:
    """PLANS features under the spec's names; -1 means unlimited (billing's convention)."""
    features = billing_services._plan_features(plan_key)
    return {
        "max_connections": features.get("distributor_connections", 0),
        "max_seats": features.get("seats", 1),
        "detail_views_per_month": features.get("detail_views_per_month", 0),
        "ordering_enabled": bool(features.get("po_checkout_allowed", False)),
    }


def scout_limits() -> dict:
    return plan_limits("scout")


def _feed_status(company_provider: src_models.CompanyProviders) -> str:
    """Same rule as integrations._order_gating_feed_status_enum: a completed first sync is
    "connected" whatever the narrower status column says."""
    if company_provider.initial_sync_completed:
        return "connected"
    status = (company_provider.status_name or "ingesting").lower()
    return status if status in _FEED_STATUSES else "ingesting"


def _ordering_connected(company_provider: src_models.CompanyProviders) -> bool:
    return company_provider.order_status == src_enums.CompanyProviderOrderConnectionStatus.CONNECTED.value


def _display_name(company_provider: src_models.CompanyProviders) -> str:
    return integrations_services._provider_ui_metadata(company_provider.provider)["provider_display_name"]


# ---------------------------------------------------------------------------
# Usage metrics and ranking
# ---------------------------------------------------------------------------


def _integration_metrics(
    company_id: int, company_providers: typing.List[src_models.CompanyProviders], now: datetime.datetime
) -> typing.Dict[int, dict]:
    """
    ``{company_provider_id: {"orders_30d": int, "searches_30d": int}}``.

    orders_30d: purchase orders actually placed through the distributor in the window, plus
    orders discovered on the distributor's own portal (ExternalPurchaseOrder) -- both are
    "this company really buys here".

    searches_30d has no exact source: PartRequestAudit's ``search`` rows store only the query text,
    not which distributors the search returned. The documented proxy is the company's 30-day
    part-detail views of parts this distributor carries. It is computed in two cheap steps -- the
    audit's master_part_ids (capped), then one lookup on provider_parts' unique (master_part,
    provider) index for those ids -- and never a join or count over the parts tables.
    """
    cutoff = now - datetime.timedelta(days=_USAGE_WINDOW_DAYS)
    cp_ids = [cp.id for cp in company_providers]
    metrics = {cp_id: {"orders_30d": 0, "searches_30d": 0} for cp_id in cp_ids}
    if not cp_ids:
        return metrics

    placed = (
        src_models.PurchaseOrder.objects.filter(
            company_id=company_id,
            company_provider_id__in=cp_ids,
            created_at__gte=cutoff,
            status__in=_PLACED_ORDER_STATUSES,
        )
        .values("company_provider_id")
        .annotate(n=Count("id"))
    )
    for row in placed:
        metrics[row["company_provider_id"]]["orders_30d"] += row["n"]
    external = (
        src_models.ExternalPurchaseOrder.objects.filter(
            company_id=company_id, company_provider_id__in=cp_ids, discovered_at__gte=cutoff
        )
        .values("company_provider_id")
        .annotate(n=Count("id"))
    )
    for row in external:
        metrics[row["company_provider_id"]]["orders_30d"] += row["n"]

    viewed_part_ids = list(
        src_models.PartRequestAudit.objects.filter(
            company_id=company_id, action="detail", created_at__gte=cutoff, master_part_id__isnull=False
        ).values_list("master_part_id", flat=True)[:_SEARCH_PROXY_AUDIT_ROW_CAP]
    )
    if viewed_part_ids:
        views_by_part = collections.Counter(viewed_part_ids)
        cp_id_by_provider_id = {cp.provider_id: cp.id for cp in company_providers}
        carried = src_models.ProviderPart.objects.filter(
            master_part_id__in=list(views_by_part), provider_id__in=list(cp_id_by_provider_id)
        ).values_list("master_part_id", "provider_id")
        for master_part_id, provider_id in carried:
            metrics[cp_id_by_provider_id[provider_id]]["searches_30d"] += views_by_part[master_part_id]
    return metrics


def rank_integrations(
    company_id: int,
    *,
    company_providers: typing.Optional[typing.List[src_models.CompanyProviders]] = None,
    with_metrics: bool = True,
    now: typing.Optional[datetime.datetime] = None,
) -> typing.List[dict]:
    """
    The feed-connected integrations, best first, as ``[{"cp", "feed_status", "ordering_connected",
    "orders_30d", "searches_30d"}]``. Shared by the picker (plan-status) and the expiry cron's
    auto-pick, so what we recommend is exactly what we would pick.

    Order, highest priority first: (spec section 6, rule 5 leads because a half-set-up connection
    must lose to every fully set up one regardless of its other numbers)
      1. feed fully set up (feed_status == "connected")
      2. ordering connected
      3. orders in the last 30 days
      4. searches proxy in the last 30 days
      5. most recently connected, then highest id as the final tie-break

    ``with_metrics=False`` skips the order/audit queries (orders/searches then rank as 0 and are
    reported as None) for the plan-status calls that do not need picker data.
    """
    now = now or timezone.now()
    if company_providers is None:
        company_providers = feed_connected_providers(company_id)
    metrics = _integration_metrics(company_id, company_providers, now) if with_metrics else {}

    rows = []
    for cp in company_providers:
        m = metrics.get(cp.id)
        rows.append(
            {
                "cp": cp,
                "feed_status": _feed_status(cp),
                "ordering_connected": _ordering_connected(cp),
                "orders_30d": m["orders_30d"] if m else None,
                "searches_30d": m["searches_30d"] if m else None,
            }
        )
    rows.sort(
        key=lambda r: (
            r["feed_status"] != "connected",
            not r["ordering_connected"],
            -(r["orders_30d"] or 0),
            -(r["searches_30d"] or 0),
            -r["cp"].created_at.timestamp(),
            -r["cp"].id,
        )
    )
    return rows


# ---------------------------------------------------------------------------
# Plan status (GET /api/account/plan-status/)
# ---------------------------------------------------------------------------


def _viewer(user, company_id: int) -> dict:
    """The requesting user's standing in this company, read from the DB -- never the JWT, which
    lives 12 hours and bakes in is_company_admin."""
    profile = src_models.UserProfile.objects.filter(user_id=user.id, company_id=company_id).first()
    return {
        "id": user.id,
        "is_admin": bool(profile and profile.is_company_admin),
        "state": "suspended" if (profile and profile.suspended_at) else "active",
    }


def get_plan_status(
    company_id: int, user, *, include_picker_data: typing.Optional[bool] = None
) -> typing.Optional[dict]:
    """
    The single payload the frontend drives its banner, modal, picker and billing counts from.
    None when the company does not exist. See docs/tasks/plan-expiry-downgrade/contract.md for
    the shape (-1 = unlimited, timestamps ISO-8601 UTC or null).

    The picker numbers (searches_30d / orders_30d / recommended) cost a few queries and this is
    fetched on every app-shell load, so they are computed only when the viewer can actually open
    the picker (``can_choose_kept_integrations``) and are null otherwise. ``include_picker_data``
    forces them on -- the staff override endpoints, whose viewer is not a company member.
    """
    company = src_models.Company.objects.filter(id=company_id).first()
    if not company:
        return None

    now = timezone.now()
    state = plan_state(company)
    plan_key = billing_services._plan_key(company)
    limits = plan_limits(plan_key)
    scout = scout_limits()
    viewer = _viewer(user, company_id)

    cps = feed_connected_providers(company_id)
    paused_count = sum(1 for cp in cps if is_paused(cp))
    active_count = len(cps) - paused_count
    suspended_count = src_models.UserProfile.objects.filter(company_id=company_id, suspended_at__isnull=False).count()
    total_members = src_models.UserProfile.objects.filter(company_id=company_id).count()

    period_end = company.subscription_period_end
    trial_ends_at = period_end if state in (STATE_TRIALING, STATE_DOWNGRADED) else None
    if state == STATE_TRIALING:
        trial_plan = company.subscription_plan
    elif state == STATE_DOWNGRADED:
        trial_plan = company.downgraded_from_plan
    else:
        trial_plan = None

    # Trial-time picking applies only to a trial that would actually overflow Scout; after expiry
    # the (single) post-expiry change is gated by the can_choose rule below instead.
    needs_distributor_choice = state == STATE_TRIALING and active_count > scout["max_connections"]
    can_choose = (
        viewer["is_admin"]
        and viewer["state"] == "active"
        and (
            needs_distributor_choice
            or (
                state == STATE_DOWNGRADED
                and company.keep_integration_ids_source == SOURCE_AUTO
                and company.connections_locked_at is None
                and paused_count > 0
            )
        )
    )

    with_metrics = can_choose if include_picker_data is None else include_picker_data
    ranked = rank_integrations(company_id, company_providers=cps, with_metrics=with_metrics, now=now)
    recommended_ids = {r["cp"].id for r in ranked[: scout["max_connections"]]} if with_metrics else set()
    kept_ids = set(company.keep_integration_ids or [])

    integrations = []
    for r in ranked:
        cp = r["cp"]
        integrations.append(
            {
                "id": cp.id,
                "provider_id": cp.provider_id,
                "name": _display_name(cp),
                "kind_name": cp.provider.kind_name,
                "icon_url": integrations_services._provider_ui_metadata(cp.provider)["provider_icon_url"],
                "state": "paused" if is_paused(cp) else "active",
                "paused_at": iso_utc(cp.paused_at),
                "paused_reason": cp.paused_reason,
                "ordering_connected": r["ordering_connected"],
                "feed_status": r["feed_status"],
                "connected_at": iso_utc(cp.created_at),
                "kept": cp.id in kept_ids,
                "searches_30d": r["searches_30d"],
                "orders_30d": r["orders_30d"],
                "recommended": (cp.id in recommended_ids) if with_metrics else None,
            }
        )

    detail_views_used = src_models.PartRequestAudit.objects.filter(
        company_id=company_id,
        action="detail",
        created_at__gte=billing_services._period_start(company),
    ).count()

    remaining = (period_end - now) if (state == STATE_TRIALING and period_end) else None
    show_warning_banner = remaining is not None and remaining <= datetime.timedelta(days=settings.TRIAL_WARNING_DAYS)
    if show_warning_banner:
        warning_level = "urgent" if remaining <= datetime.timedelta(days=1) else "warning"
    else:
        warning_level = None

    return {
        "plan": plan_key,
        "plan_name": billing_services._plan_name(plan_key),
        "status": state,
        "trial_plan": trial_plan,
        "trial_plan_name": billing_services._plan_name(trial_plan) if trial_plan else None,
        "trial_ends_at": iso_utc(trial_ends_at),
        "downgraded_at": iso_utc(company.downgraded_at),
        "server_time": iso_utc(now),
        "limits": limits,
        "scout_limits": scout,
        "usage": {
            "detail_views": {"used": detail_views_used, "limit": limits["detail_views_per_month"]},
            "seats": {
                "used": total_members - suspended_count,
                "limit": limits["max_seats"],
                "suspended": suspended_count,
                "total": total_members,
            },
            "connections": {
                "used": active_count,
                "limit": limits["max_connections"],
                "paused": paused_count,
                "total": len(cps),
            },
        },
        "integrations": integrations,
        "keep_integration_ids": company.keep_integration_ids,
        "keep_integration_ids_source": company.keep_integration_ids_source,
        "connections_locked_at": iso_utc(company.connections_locked_at),
        "downgrade_acknowledged_at": iso_utc(company.downgrade_acknowledged_at),
        "paused_integrations_count": paused_count,
        "suspended_members_count": suspended_count,
        "needs_distributor_choice": needs_distributor_choice,
        "can_choose_kept_integrations": can_choose,
        "kept_change_is_final": state == STATE_DOWNGRADED,
        "show_warning_banner": show_warning_banner,
        "warning_level": warning_level,
        "show_downgrade_banner": state == STATE_DOWNGRADED,
        "show_downgrade_modal": (
            state == STATE_DOWNGRADED
            and company.downgrade_acknowledged_at is None
            and viewer["is_admin"]
            and viewer["state"] == "active"
        ),
        "current_user": viewer,
    }


# ---------------------------------------------------------------------------
# Pausing / restoring primitives
# ---------------------------------------------------------------------------


def _apply_pause_state(
    company_id: int, kept_ids: typing.Iterable[int], now: datetime.datetime
) -> typing.List[src_models.CompanyProviders]:
    """
    Makes every feed-connected connection in ``kept_ids`` active and every other one paused. Feed tenants'
    connections are not in the pick (it is the company's own), so they are paused with the rest.

    A row that is already paused keeps its ORIGINAL paused_at (the 60-day sync clock must not
    restart each time the choice is re-applied). Returns the kept rows that were paused for longer
    than PAUSED_INTEGRATION_SYNC_DAYS -- their data went stale while paused, so the caller queues
    an immediate sync for them (restoring a row paused for less has been syncing nightly and is
    fresh). Order-only rows are not feed-connected and are never touched.
    """
    kept_set = set(kept_ids)
    cutoff = now - datetime.timedelta(days=settings.PAUSED_INTEGRATION_SYNC_DAYS)
    to_unpause, to_pause, stale = [], [], []
    for cp in family_feed_connected_providers(company_id):
        if cp.id in kept_set:
            if cp.paused_at is not None:
                to_unpause.append(cp.id)
                if cp.paused_at <= cutoff:
                    stale.append(cp)
        elif cp.paused_at is None:
            to_pause.append(cp.id)
    if to_unpause:
        src_models.CompanyProviders.objects.filter(id__in=to_unpause).update(
            paused_at=None, paused_reason=None, updated_at=now
        )
    if to_pause:
        src_models.CompanyProviders.objects.filter(id__in=to_pause).update(
            paused_at=now, paused_reason=REASON_PLAN_LIMIT, updated_at=now
        )
    return stale


def _enqueue_resync(company_providers: typing.Iterable[src_models.CompanyProviders]) -> int:
    """Queues one immediate full pricing sync per connection (the same call connect_provider
    makes), for connections whose data went stale while paused. Returns how many were queued."""
    queued = 0
    for cp in company_providers:
        if not cp.active or not is_feed_connected(cp):
            continue
        if not integration_pricing_sync_jobs.should_enqueue_pricing_sync(cp.provider.kind):
            continue
        integration_pricing_sync_jobs.enqueue_company_provider_pricing_sync(cp.id)
        queued += 1
    return queued


def _valid_kept_ids(
    company_id: int, ids: typing.Iterable[int], *, limit: int, require_full: bool = False
) -> typing.List[int]:
    """De-duplicated ids (order kept), every one a feed-connected connection of this company,
    at most ``limit`` of them. ``require_full`` (the customer-facing save) additionally demands exactly
    min(limit, connected) ids, so a pick cannot be under-sized and silently pause a distributor the
    plan had room to keep; the staff override leaves it off."""
    deduped = list(dict.fromkeys(ids))
    connected_ids = {cp.id for cp in feed_connected_providers(company_id)}
    bad = [i for i in deduped if i not in connected_ids]
    if bad:
        raise PlanLifecycleError(
            "These are not connected distributors of this company: {}.".format(", ".join(str(i) for i in bad)),
            error_code="invalid_integration_ids",
            status_code=400,
        )
    if limit != -1 and len(deduped) > limit:
        raise PlanLifecycleError(
            "You can keep at most {} distributors on Scout.".format(limit),
            error_code="too_many_integrations",
            status_code=400,
        )
    if require_full:
        needed = len(connected_ids) if limit == -1 else min(limit, len(connected_ids))
        if len(deduped) < needed:
            raise PlanLifecycleError(
                "Choose {} distributor{} to keep.".format(needed, "" if needed == 1 else "s"),
                error_code="too_few_integrations",
                status_code=400,
            )
    return deduped


# ---------------------------------------------------------------------------
# Downgrade (the expiry cron)
# ---------------------------------------------------------------------------


def _is_expired_trial(company: src_models.Company, now: datetime.datetime) -> bool:
    return (
        company.subscription_status == "trialing"
        and company.manual_trial_granted_at is not None
        and company.subscription_period_end is not None
        and company.subscription_period_end < now
        # Never a company with a real Stripe subscription (e.g. an annual customer comped extra time).
        and not company.subscription_id
    )


def _plan_downgrade(company: src_models.Company, now: datetime.datetime) -> dict:
    """
    Works out -- without writing anything -- what a downgrade of ``company`` would do. Shared by
    downgrade_company (which then applies it) and the command's --dry-run, so the preview cannot
    drift from the real run.

    Distributors: with at most Scout's cap connected there is nothing to choose and nothing is
    paused (source/locked stay unset so no "change my 2" is offered). Otherwise the admin's
    saved pick wins -- intersected with what is still connected, since a pick can outlive a
    disconnect -- and becomes final immediately (source "user", locked). With no usable pick the
    top ranked connections are auto-picked and the choice is left unlocked: the admin gets one
    change after expiry.
    Members: the earliest-created admin (there is no owner column; register_user makes the first
    user an admin) stays and every other member is suspended.
    """
    scout = scout_limits()
    ranked = rank_integrations(company.id, now=now)
    all_ids = [r["cp"].id for r in ranked]
    limit = scout["max_connections"]

    if limit == -1 or len(all_ids) <= limit:
        kept_ids, source, locked = list(all_ids), None, False
    else:
        connected = set(all_ids)
        picked = [i for i in dict.fromkeys(company.keep_integration_ids or []) if i in connected][:limit]
        if picked:
            kept_ids, source, locked = picked, SOURCE_USER, True
        else:
            kept_ids, source, locked = all_ids[:limit], SOURCE_AUTO, False
    paused_ids = [i for i in all_ids if i not in set(kept_ids)]

    max_seats = scout["max_seats"]
    profiles = list(
        src_models.UserProfile.objects.filter(company_id=company.id).order_by("-is_company_admin", "created_at", "id")
    )
    suspend_profiles = [] if max_seats == -1 else profiles[max_seats:]

    return {
        "kept_ids": kept_ids,
        "paused_ids": paused_ids,
        "source": source,
        "locked": locked,
        "suspend_user_ids": [p.user_id for p in suspend_profiles],
        "names": {r["cp"].id: _display_name(r["cp"]) for r in ranked},
    }


def preview_downgrade(company_id: int, *, now: typing.Optional[datetime.datetime] = None) -> typing.Optional[dict]:
    """What downgrade_company would do right now, with no writes (--dry-run). None if the company
    is not an expired comp'd trial."""
    now = now or timezone.now()
    company = src_models.Company.objects.filter(id=company_id).first()
    if not company or not _is_expired_trial(company, now):
        return None
    return _plan_downgrade(company, now)


def _no_stripe_subscription() -> Q:
    return Q(subscription_id__isnull=True) | Q(subscription_id="")


def expired_trial_company_ids(now: typing.Optional[datetime.datetime] = None) -> typing.List[int]:
    now = now or timezone.now()
    return list(
        src_models.Company.objects.filter(
            _no_stripe_subscription(),
            subscription_status="trialing",
            manual_trial_granted_at__isnull=False,
            subscription_period_end__lt=now,
        )
        .order_by("id")
        .values_list("id", flat=True)
    )


def downgrade_company(company_id: int, *, now: typing.Optional[datetime.datetime] = None) -> typing.Optional[dict]:
    """
    Downgrades one company whose comp'd trial has ended, in a single transaction. Returns a summary
    dict, or None when there was nothing to do -- which is what makes the cron idempotent: the
    row is locked and the "expired trial" condition re-checked inside the transaction, so a second
    run (or a Stripe webhook / admin extension that raced the first) finds a company that no longer
    qualifies and changes nothing.

    subscription_plan is stored as NULL, not the literal "scout": admin.extend_company_trial only
    re-applies the trial plan when subscription_plan is empty, so "scout" would leave an extended
    company "trialing" on Scout limits. The plan it had is kept in downgraded_from_plan instead.
    subscription_period_end is deliberately kept (the trial end date); billing._period_start
    still resolves to the calendar month because period_end > now is false.

    The "your trial has ended" email is sent after the transaction commits and only stamped
    warning_emails_sent["d0"] once Resend accepts it; a failure is retried by the cron's
    send_pending_downgrade_emails sweep without re-running the downgrade.
    """
    now = now or timezone.now()
    with transaction.atomic():
        company = src_models.Company.objects.select_for_update().filter(id=company_id).first()
        if not company or not _is_expired_trial(company, now):
            return None

        plan = _plan_downgrade(company, now)
        _apply_pause_state(company_id, plan["kept_ids"], now)
        if plan["suspend_user_ids"]:
            src_models.UserProfile.objects.filter(
                company_id=company_id, user_id__in=plan["suspend_user_ids"], suspended_at__isnull=True
            ).update(suspended_at=now, suspended_reason=REASON_PLAN_LIMIT, updated_at=now)

        sent = dict(company.warning_emails_sent or {})
        sent.pop("upgrade", None)  # the next restore must email again
        from_plan = company.subscription_plan or settings.NEW_SIGNUP_TRIAL_PLAN
        company.subscription_status = STATE_DOWNGRADED
        company.subscription_plan = None
        company.manual_trial_granted_at = None
        company.downgraded_from_plan = from_plan
        company.downgraded_at = now
        company.downgrade_acknowledged_at = None
        company.keep_integration_ids = plan["kept_ids"]
        company.keep_integration_ids_source = plan["source"]
        company.connections_locked_at = now if plan["locked"] else None
        company.warning_emails_sent = sent
        company.save(
            update_fields=[
                "subscription_status",
                "subscription_plan",
                "manual_trial_granted_at",
                "downgraded_from_plan",
                "downgraded_at",
                "downgrade_acknowledged_at",
                "keep_integration_ids",
                "keep_integration_ids_source",
                "connections_locked_at",
                "warning_emails_sent",
                "updated_at",
            ]
        )
        _send_after_commit(lambda: send_trial_ended_email(company_id, now=now))

    logger.info(
        "Downgraded company_id=%s: kept=%s paused=%s suspended_users=%s source=%s.",
        company_id,
        plan["kept_ids"],
        plan["paused_ids"],
        plan["suspend_user_ids"],
        plan["source"],
    )
    return {
        "company_id": company_id,
        "kept_ids": plan["kept_ids"],
        "paused_ids": plan["paused_ids"],
        "suspended_user_ids": plan["suspend_user_ids"],
        "source": plan["source"],
        "locked": plan["locked"],
    }


# ---------------------------------------------------------------------------
# Restore (upgrade / admin trial extension)
# ---------------------------------------------------------------------------


def restore_company(
    company_id: int, *, notify_upgrade: bool = False, now: typing.Optional[datetime.datetime] = None
) -> dict:
    """
    Undoes a downgrade's pausing: every paused connection and suspended member is active again
    (members are restored even above the new plan's seat count -- only NEW invites are then blocked,
    see billing.check_seat_limit), and the keep/lock/acknowledged state is cleared so a later
    downgrade starts fresh. downgraded_at / downgraded_from_plan stay as history, and
    warning_emails_sent is untouched except by the next downgrade.

    Connections paused longer than PAUSED_INTEGRATION_SYNC_DAYS had stopped syncing, so each gets an
    immediate sync queued (the process_integration_pricing_sync_jobs cron picks it up).

    Idempotent and cheap when there is nothing to restore (the Stripe renewal webhook calls it on
    every invoice). ``notify_upgrade`` sends the "everything's back on" email -- only for a real
    upgrade, not an admin trial extension.
    """
    now = now or timezone.now()
    cutoff = now - datetime.timedelta(days=settings.PAUSED_INTEGRATION_SYNC_DAYS)
    with transaction.atomic():
        # Row lock only (the value is not needed): serialises concurrent restores / a racing
        # downgrade for this company.
        src_models.Company.objects.select_for_update().filter(id=company_id).first()
        paused = list(
            src_models.CompanyProviders.objects.filter(
                company_id__in=spaces_services.family_company_ids(company_id), paused_at__isnull=False
            ).select_related("provider")
        )
        stale = [cp for cp in paused if cp.paused_at <= cutoff]
        if paused:
            src_models.CompanyProviders.objects.filter(id__in=[cp.id for cp in paused]).update(
                paused_at=None, paused_reason=None, updated_at=now
            )
        members_restored = src_models.UserProfile.objects.filter(
            company_id=company_id, suspended_at__isnull=False
        ).update(suspended_at=None, suspended_reason=None, updated_at=now)
        src_models.Company.objects.filter(id=company_id).update(
            keep_integration_ids=None,
            keep_integration_ids_source=None,
            connections_locked_at=None,
            downgrade_acknowledged_at=None,
            updated_at=now,
        )
        resyncing = _enqueue_resync(stale)

        if notify_upgrade:
            restored_names = [_display_name(cp) for cp in paused]
            _send_after_commit(
                lambda: send_plan_restored_email(
                    company_id,
                    restored_distributors=len(paused),
                    restored_members=members_restored,
                    resyncing=resyncing > 0,
                    now=now,
                )
            )
            logger.info("Restored company_id=%s after upgrade: unpaused %s.", company_id, restored_names)

    return {
        "company_id": company_id,
        "integrations_restored": len(paused),
        "members_restored": members_restored,
        "resync_queued": resyncing,
    }


# ---------------------------------------------------------------------------
# Picker save + acknowledge (POST /api/account/*)
# ---------------------------------------------------------------------------


def _require_admin(user, company_id: int) -> None:
    ok, err = company_settings_services._is_company_admin(user, company_id)
    if not ok:
        raise PlanLifecycleError(err or "Company admin access required", error_code="admin_required", status_code=403)


def save_kept_integrations(company_id: int, user, ids: typing.List[int]) -> dict:
    """
    POST /api/account/kept-integrations/. Checks run in the contract's order: admin, "nothing to
    choose" (paid plan, or at most Scout's cap connected), locked / not-auto, then the ids
    themselves.

    Trialing: the ids are stored (source "user") and nothing is paused; callable any number of times.
    Downgraded with an unlocked auto-pick: applied now -- chosen active, the rest paused -- and
    locked for good (source "user", connections_locked_at set). This is the one post-expiry change.
    """
    _require_admin(user, company_id)
    scout = scout_limits()
    now = timezone.now()
    with transaction.atomic():
        company = src_models.Company.objects.select_for_update().filter(id=company_id).first()
        if not company:
            raise PlanLifecycleError("Company not found", error_code="not_found", status_code=404)

        state = plan_state(company)
        connected = feed_connected_providers(company_id)
        if state == STATE_ACTIVE:
            raise PlanLifecycleError(
                "Your plan has no distributor limit to choose for.", error_code="not_applicable", status_code=409
            )
        if scout["max_connections"] != -1 and len(connected) <= scout["max_connections"]:
            raise PlanLifecycleError(
                "Nothing to choose: all your distributors fit on Scout.",
                error_code="not_applicable",
                status_code=409,
            )
        if company.connections_locked_at is not None or (
            state == STATE_DOWNGRADED and company.keep_integration_ids_source != SOURCE_AUTO
        ):
            raise PlanLifecycleError(
                "Your distributor choice is final. Upgrade to reactivate paused distributors.",
                error_code="choice_locked",
                status_code=409,
                upgrade_required=True,
            )

        kept_ids = _valid_kept_ids(company_id, ids, limit=scout["max_connections"], require_full=True)
        company.keep_integration_ids = kept_ids
        company.keep_integration_ids_source = SOURCE_USER
        update_fields = ["keep_integration_ids", "keep_integration_ids_source", "updated_at"]
        stale = []
        if state == STATE_DOWNGRADED:
            stale = _apply_pause_state(company_id, kept_ids, now)
            company.connections_locked_at = now
            update_fields.append("connections_locked_at")
        company.save(update_fields=update_fields)
        _enqueue_resync(stale)

    return get_plan_status(company_id, user)


def acknowledge_downgrade(company_id: int, user) -> dict:
    """POST /api/account/downgrade-acknowledge/ -- idempotent: an already-acknowledged downgrade
    keeps its original timestamp."""
    _require_admin(user, company_id)
    company = src_models.Company.objects.filter(id=company_id).first()
    if not company:
        raise PlanLifecycleError("Company not found", error_code="not_found", status_code=404)
    if plan_state(company) != STATE_DOWNGRADED:
        raise PlanLifecycleError("This account has not been downgraded.", error_code="not_downgraded", status_code=409)
    src_models.Company.objects.filter(id=company_id, downgrade_acknowledged_at__isnull=True).update(
        downgrade_acknowledged_at=timezone.now()
    )
    return get_plan_status(company_id, user)


# ---------------------------------------------------------------------------
# Staff overrides (admin endpoints)
# ---------------------------------------------------------------------------


def admin_set_kept_integrations(
    company_id: int, user, *, ids: typing.Optional[typing.List[int]] = None, unlock: bool = False
) -> typing.Optional[dict]:
    """
    Staff override of the distributor choice, bypassing the admin-user, lock and trial/downgraded
    checks that bind the customer. ``ids`` stores the choice (source "user") and, if the company is
    already downgraded, applies it immediately. ``unlock`` reopens the one post-expiry change
    (source "auto", unlocked). Returns the company's plan status, or None if it does not exist.
    """
    scout = scout_limits()
    now = timezone.now()
    with transaction.atomic():
        company = src_models.Company.objects.select_for_update().filter(id=company_id).first()
        if not company:
            return None
        update_fields = ["updated_at"]
        stale = []
        if ids is not None:
            kept_ids = _valid_kept_ids(company_id, ids, limit=scout["max_connections"])
            company.keep_integration_ids = kept_ids
            company.keep_integration_ids_source = SOURCE_USER
            update_fields += ["keep_integration_ids", "keep_integration_ids_source"]
            if plan_state(company) == STATE_DOWNGRADED:
                stale = _apply_pause_state(company_id, kept_ids, now)
        if unlock:
            company.connections_locked_at = None
            company.keep_integration_ids_source = SOURCE_AUTO
            update_fields += ["connections_locked_at", "keep_integration_ids_source"]
        company.save(update_fields=sorted(set(update_fields)))
        _enqueue_resync(stale)
    return get_plan_status(company_id, user, include_picker_data=True)


def admin_unpause_integration(company_id: int, company_provider_id: int) -> bool:
    """Staff exception: force-unpause one connection (queues a sync if it was paused past the
    60-day window). The next save of the kept choice would re-pause it. False if no such row."""
    now = timezone.now()
    with transaction.atomic():
        cp = (
            src_models.CompanyProviders.objects.select_for_update()
            .select_related("provider")
            .filter(id=company_provider_id, company_id__in=spaces_services.family_company_ids(company_id))
            .first()
        )
        if not cp:
            return False
        if cp.paused_at is not None:
            stale = cp.paused_at <= now - datetime.timedelta(days=settings.PAUSED_INTEGRATION_SYNC_DAYS)
            src_models.CompanyProviders.objects.filter(id=cp.id).update(
                paused_at=None, paused_reason=None, updated_at=now
            )
            if stale:
                _enqueue_resync([cp])
    return True


# ---------------------------------------------------------------------------
# Emails (warning d7/d1, trial ended d0, plan restored)
# ---------------------------------------------------------------------------


def _frontend_url(path: str) -> str:
    return "{}{}".format(settings.FRONTEND_BASE_URL.rstrip("/"), path)


def _format_date(value: typing.Optional[datetime.datetime]) -> str:
    return "{} {}, {}".format(value.strftime("%B"), value.day, value.year) if value else ""


def _email_context(company: src_models.Company, now: typing.Optional[datetime.datetime] = None) -> dict:
    """The template context shared by all four lifecycle emails, read fresh from the DB at send time."""
    now = now or timezone.now()
    scout = scout_limits()
    days_left = None
    if company.subscription_period_end:
        # Whole days, rounded up (a trial with 2.3 days left "ends in 3 days"), never below 1.
        days_left = max(1, math.ceil((company.subscription_period_end - now).total_seconds() / 86400))
    cps = feed_connected_providers(company.id)
    state = plan_state(company)
    paused = [cp for cp in cps if is_paused(cp)]
    active = [cp for cp in cps if not is_paused(cp)]
    if state == STATE_TRIALING:
        # Before expiry "kept" means what the admin has picked so far; with no pick yet there is
        # nothing to name (the email then says we will choose).
        picked = set(company.keep_integration_ids or [])
        kept_names = [_display_name(cp) for cp in cps if cp.id in picked]
        paused_names = []
    else:
        kept_names = [_display_name(cp) for cp in active]
        # Names come from the family: a space's paused own feeds are named too. Usage counts stay parent-only (OQ-6).
        paused_names = [_display_name(cp) for cp in family_feed_connected_providers(company.id) if is_paused(cp)]
    trial_plan = company.subscription_plan if state == STATE_TRIALING else company.downgraded_from_plan
    plan_key = billing_services._plan_key(company)
    return {
        "company_name": company.name,
        "trial_plan_name": billing_services._plan_name(trial_plan or settings.NEW_SIGNUP_TRIAL_PLAN),
        "trial_ends_at": _format_date(company.subscription_period_end),
        "days_left": days_left,
        "days_left_label": "{} day{}".format(days_left, "" if days_left == 1 else "s") if days_left else "",
        "needs_choice": state == STATE_TRIALING and len(cps) > scout["max_connections"],
        "can_change": (
            state == STATE_DOWNGRADED
            and company.keep_integration_ids_source == SOURCE_AUTO
            and company.connections_locked_at is None
            and bool(paused)
        ),
        "kept_names": kept_names,
        "paused_names": paused_names,
        "paused_count": len(paused_names),
        "suspended_count": src_models.UserProfile.objects.filter(
            company_id=company.id, suspended_at__isnull=False
        ).count(),
        "scout_max_connections": scout["max_connections"],
        "scout_max_seats": scout["max_seats"],
        "scout_detail_views": scout["detail_views_per_month"],
        "ordering_enabled": plan_limits(plan_key)["ordering_enabled"],
        "restored_plan_name": billing_services._plan_name(plan_key),
        # /integrations?choose_distributor=1 opens the picker (contract decision 12); /billing is upgrade.
        "picker_url": _frontend_url("/integrations?choose_distributor=1"),
        "upgrade_url": _frontend_url("/billing"),
        "app_url": _frontend_url("/parts"),
    }


def _send_after_commit(send: typing.Callable[[], bool]) -> None:
    """Runs an email send once the surrounding transaction commits, and never lets it raise: an
    exception from an on_commit callback would surface out of the transaction that already
    committed (a downgrade, or the Stripe webhook) as if that work had failed. A failed or skipped
    send is simply picked up again -- the d0 email by the cron's retry sweep."""

    def _run() -> None:
        try:
            send()
        except Exception:
            logger.exception("Plan lifecycle email failed after commit.")

    transaction.on_commit(_run)


# A claim on an email send (see _send_once) older than this is treated as abandoned -- the process
# that took it died mid-send -- so the next cron run may try again.
_EMAIL_CLAIM_TTL = datetime.timedelta(minutes=15)
_ISO_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


def _claim_email(
    company_id: int,
    key: str,
    *,
    eligible: typing.Callable[[src_models.Company], bool],
    now: datetime.datetime,
) -> typing.Optional[src_models.Company]:
    """Phase 1 of _send_once, under the company row lock: returns the company if this caller may send
    ``key`` now (not yet sent, still eligible, no live claim by another run) and records a claim;
    otherwise None."""
    claim_key = "{}_claim".format(key)
    with transaction.atomic():
        company = src_models.Company.objects.select_for_update().filter(id=company_id).first()
        if not company:
            return None
        sent = dict(company.warning_emails_sent or {})
        if key in sent or not eligible(company):
            return None
        claimed_at = sent.get(claim_key)
        if claimed_at:
            try:
                if datetime.datetime.strptime(claimed_at, _ISO_FORMAT) > now - _EMAIL_CLAIM_TTL:
                    return None
            except (TypeError, ValueError):
                pass  # unreadable claim: treat as abandoned
        sent[claim_key] = iso_utc(now)
        src_models.Company.objects.filter(id=company_id).update(warning_emails_sent=sent)
        return company


def _send_once(
    company_id: int,
    key: str,
    email_type: src_enums.NotificationEmailType,
    *,
    eligible: typing.Callable[[src_models.Company], bool],
    context_extra: typing.Optional[dict] = None,
    now: typing.Optional[datetime.datetime] = None,
) -> bool:
    """
    Sends one lifecycle email at most once per ``key`` in Company.warning_emails_sent. Returns True when
    an email was sent.

    Three short steps so the company row lock is never held across the (up to 10 s) Resend call:
      1. under the lock: check the key is unsent and ``eligible`` still holds (a trial extended or
         upgraded since the caller looked is not emailed), and record a "<key>_claim" so an
         overlapping cron run skips it instead of sending a second copy;
      2. outside any lock: send;
      3. under the lock: stamp ``key`` only if the send was accepted, and drop the claim either way, so
         a failed send is retried on the next run and a successful one never repeats.
    A claim left behind by a crashed run expires after _EMAIL_CLAIM_TTL.
    """
    now = now or timezone.now()
    company = _claim_email(company_id, key, eligible=eligible, now=now)
    if company is None:
        return False

    ok = False
    try:
        context = _email_context(company, now)
        context.update(context_extra or {})
        ok = notifications_services.send_plan_lifecycle_email(company=company, email_type=email_type, context=context)
    finally:
        with transaction.atomic():
            current = src_models.Company.objects.select_for_update().filter(id=company_id).first()
            if current is not None:
                sent = dict(current.warning_emails_sent or {})
                sent.pop("{}_claim".format(key), None)
                if ok:
                    sent[key] = iso_utc(now)
                src_models.Company.objects.filter(id=company_id).update(warning_emails_sent=sent)
    return ok


def _warning_window(company: src_models.Company, now: datetime.datetime) -> typing.Optional[str]:
    """Which warning email a still-trialing company is due: "d1" within the last day, "d7" within
    TRIAL_WARNING_DAYS, else None. The windows are exclusive (a trial created or extended with a
    day left gets only d1, never both)."""
    if plan_state(company) != STATE_TRIALING or not company.subscription_period_end:
        return None
    remaining = company.subscription_period_end - now
    if remaining <= datetime.timedelta(0):
        return None
    if remaining <= datetime.timedelta(days=1):
        return "d1"
    if remaining <= datetime.timedelta(days=settings.TRIAL_WARNING_DAYS):
        return "d7"
    return None


def warning_email_candidates(
    now: typing.Optional[datetime.datetime] = None, *, company_id: typing.Optional[int] = None
) -> typing.List[typing.Tuple[int, str]]:
    """``[(company_id, "d7"|"d1")]`` for trials in a warning window whose email has not gone out."""
    now = now or timezone.now()
    # Internal (staff / demo / test) companies get no warning emails. They ARE still downgraded: the
    # command always demoted them, and --dry-run exists to review that before the first production run.
    qs = src_models.Company.objects.filter(
        _no_stripe_subscription(),
        is_internal=False,
        subscription_status="trialing",
        manual_trial_granted_at__isnull=False,
        subscription_period_end__gt=now,
        subscription_period_end__lte=now + datetime.timedelta(days=settings.TRIAL_WARNING_DAYS),
    ).order_by("id")
    if company_id is not None:
        qs = qs.filter(id=company_id)
    out = []
    for company in qs:
        key = _warning_window(company, now)
        if key and key not in (company.warning_emails_sent or {}):
            out.append((company.id, key))
    return out


def send_trial_warning_emails(
    now: typing.Optional[datetime.datetime] = None, *, company_id: typing.Optional[int] = None
) -> int:
    """Sends every due day-7 / day-1 warning. Returns the number of emails sent."""
    now = now or timezone.now()
    sent = 0
    for cid, key in warning_email_candidates(now, company_id=company_id):
        email_type = (
            src_enums.NotificationEmailType.TRIAL_ENDING_1D
            if key == "d1"
            else src_enums.NotificationEmailType.TRIAL_ENDING_7D
        )
        try:
            if _send_once(
                cid,
                key,
                email_type,
                eligible=lambda c, key=key: not c.is_internal and _warning_window(c, now) == key,
                now=now,
            ):
                sent += 1
        except Exception:
            # One company's failure (DB error, bad template data) must not stop the other warnings or the
            # downgrade phase that runs after this in the command.
            logger.exception("Trial warning email %s failed for company_id=%s.", key, cid)
    return sent


def send_trial_ended_email(company_id: int, *, now: typing.Optional[datetime.datetime] = None) -> bool:
    """The day-0 "your trial has ended" email (stamps "d0")."""
    return _send_once(
        company_id,
        "d0",
        src_enums.NotificationEmailType.TRIAL_ENDED,
        eligible=lambda c: plan_state(c) == STATE_DOWNGRADED,
        now=now,
    )


def pending_downgrade_email_company_ids(
    now: typing.Optional[datetime.datetime] = None, *, company_id: typing.Optional[int] = None
) -> typing.List[int]:
    now = now or timezone.now()
    qs = src_models.Company.objects.filter(
        subscription_status=STATE_DOWNGRADED,
        downgraded_at__gte=now - datetime.timedelta(days=_D0_EMAIL_RETRY_DAYS),
    ).order_by("id")
    if company_id is not None:
        qs = qs.filter(id=company_id)
    return [c.id for c in qs if "d0" not in (c.warning_emails_sent or {})]


def send_pending_downgrade_emails(
    now: typing.Optional[datetime.datetime] = None, *, company_id: typing.Optional[int] = None
) -> int:
    """Retries day-0 emails that failed (or were never reached) for recent downgrades."""
    now = now or timezone.now()
    sent = 0
    for cid in pending_downgrade_email_company_ids(now, company_id=company_id):
        try:
            if send_trial_ended_email(cid, now=now):
                sent += 1
        except Exception:
            logger.exception("Trial-ended email retry failed for company_id=%s.", cid)
    return sent


def send_plan_restored_email(
    company_id: int,
    *,
    restored_distributors: int,
    restored_members: int,
    resyncing: bool,
    now: typing.Optional[datetime.datetime] = None,
) -> bool:
    """The "everything's back on" email after an upgrade (stamps "upgrade"; cleared by the next downgrade)."""
    return _send_once(
        company_id,
        "upgrade",
        src_enums.NotificationEmailType.PLAN_RESTORED,
        eligible=lambda c: True,
        context_extra={
            "restored_distributors": "{} restored".format(restored_distributors) if restored_distributors else "",
            "restored_members": "{} restored".format(restored_members) if restored_members else "",
            "resyncing": resyncing,
        },
        now=now,
    )
