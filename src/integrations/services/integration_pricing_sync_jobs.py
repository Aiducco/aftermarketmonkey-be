"""
Enqueue and run per-company-provider distributor pricing sync (no Celery).
Jobs are stored in IntegrationPricingSyncJob and processed by a management command.

Two job modes controlled by the ``skip_raw_fetch`` field:
  - skip_raw_fetch=False (default / new-company onboarding):
      Full flow — fetch raw pricing data from distributor (API/SFTP/CSV) then sync to
      master parts.  Used when a company first adds credentials or a manual re-sync is needed.
  - skip_raw_fetch=True (nightly pipeline):
      Master-only — raw pricing data was already fetched in ingest_all_providers Phase 1;
      only run the master-parts pricing layer sync for this company.
      Avoids double-downloading large pricing feeds for every active company.
"""
import concurrent.futures
import datetime
import logging
import typing

from django.db import connection, models as django_db_models, transaction
from django.utils import timezone

from src import enums as src_enums
from src import models as src_models
from src.integrations.services import keystone as keystone_services
from src.integrations.services import master_parts
from src.integrations.services import atech as atech_services
from src.integrations.services import dlg as dlg_services
from src.integrations.services import elite_wheel as elite_wheel_services
from src.integrations.services import helmet_house as helmet_house_services
from src.integrations.services import meyer as meyer_services
from src.integrations.services import motorstate as motorstate_services
from src.integrations.services import notifications as notifications_services
from src.integrations.services import premier as premier_services
from src.integrations.services import quadratec as quadratec_services
from src.integrations import credentials as credentials_helper
from src.integrations import rate_limit as rate_limit_base
from src.integrations.services import rough_country as rough_country_services
from src.integrations.services import the_wheel_group as the_wheel_group_services
from src.integrations.services import tirerack as tirerack_services
from src.integrations.services import turn_14 as turn_14_services
from src.integrations.services import turn_14_sweeps
from src.integrations.services import vossen as vossen_services
from src.integrations.services import wheelpros as wheelpros_services
from src.integrations.services import wps as wps_services

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[INTEGRATION-PRICING-SYNC-JOBS]"

_PRICING_SYNC_KINDS = frozenset(
    {
        src_enums.BrandProviderKind.TURN_14.value,
        src_enums.BrandProviderKind.KEYSTONE.value,
        src_enums.BrandProviderKind.ROUGH_COUNTRY.value,
        src_enums.BrandProviderKind.WHEELPROS.value,
        src_enums.BrandProviderKind.MEYER.value,
        src_enums.BrandProviderKind.ATECH.value,
        src_enums.BrandProviderKind.DLG.value,
        src_enums.BrandProviderKind.PREMIER_PERFORMANCE.value,
        src_enums.BrandProviderKind.VOSSEN.value,
        src_enums.BrandProviderKind.TIRERACK.value,
        src_enums.BrandProviderKind.QUADRATEC.value,
        src_enums.BrandProviderKind.ELITE_WHEEL.value,
        src_enums.BrandProviderKind.MOTOR_STATE_DISTRIBUTING.value,
        src_enums.BrandProviderKind.WESTERN_POWER_SPORTS.value,
        src_enums.BrandProviderKind.HELMHOUSE.value,
        src_enums.BrandProviderKind.THE_WHEEL_GROUP.value,
    }
)

# Kinds whose *recurring* pricing refresh is deliberately slower than the nightly cycle,
# mapped to their minimum interval. Motor State has no bulk pricing endpoint -- a refresh is
# ~10.4k sequential /api/Product calls (15 part numbers each), far too heavy to repeat nightly,
# and its dealer prices move slowly. The first sync for a connection is never throttled: it is
# enqueued directly by connect/reconnect (enqueue_company_provider_pricing_sync), which does not
# consult this map, so a newly connected company still gets a full pricing sync immediately.
_RECURRING_PRICING_MIN_INTERVAL = {
    src_enums.BrandProviderKind.MOTOR_STATE_DISTRIBUTING.value: datetime.timedelta(days=7),
}


def _recurring_pricing_sync_is_due(company_provider_id: int, kind: int) -> bool:
    """
    Whether the recurring cycle should enqueue this connection now.

    True for every kind without a configured interval. For throttled kinds, True only when the
    connection has no completed pricing job yet (so a connection that has never synced is always
    due) or the last completed one is older than the interval.
    """
    min_interval = _RECURRING_PRICING_MIN_INTERVAL.get(kind)
    if not min_interval:
        return True

    last_completed_at = (
        src_models.IntegrationPricingSyncJob.objects.filter(
            company_provider_id=company_provider_id,
            status=src_enums.IntegrationPricingSyncJobStatus.COMPLETED.value,
        )
        .exclude(completed_at__isnull=True)
        .order_by("-completed_at")
        .values_list("completed_at", flat=True)
        .first()
    )
    if not last_completed_at:
        return True
    return (timezone.now() - last_completed_at) >= min_interval


def should_enqueue_pricing_sync(provider_kind: int) -> bool:
    return provider_kind in _PRICING_SYNC_KINDS


def enqueue_all_active_company_provider_pricing_jobs() -> int:
    """
    Enqueue a pricing sync job for every active ``CompanyProviders`` row whose provider
    kind is in ``_PRICING_SYNC_KINDS``, EXCEPT Turn 14 -- Turn 14's recurring pricing sync is
    enqueued separately, once daily, from sync_turn14_global_sweep (see
    enqueue_all_active_turn14_pricing_jobs), tied to the same cycle as the fresh catalog sweep
    it should be checked against rather than to this function's every-4-hours cadence. Turn 14
    stays in _PRICING_SYNC_KINDS itself since should_enqueue_pricing_sync (the on-connect gate)
    also reads that set and on-connect enqueue for Turn 14 is unchanged.

    Called by the nightly ``ingest_all_providers`` pipeline after Phase 2 (global catalog
    sync) completes.  All jobs use ``skip_raw_fetch=False`` so each company-provider
    downloads its own fresh per-company pricing data (FTP/SFTP/API) before syncing to
    the master pricing layer.  Phase 1 only fetches global catalog data, never per-company
    pricing, so there is nothing to skip here.

    Idempotent: any existing OPEN job for the same company-provider is removed before
    creating a fresh one (same behaviour as ``enqueue_company_provider_pricing_sync``).

    Returns the number of jobs enqueued.
    """
    # active=True: despite the function's name, this never actually filtered on it until
    # 2026-08-31 -- every inactive/disconnected CompanyProviders row (dead credentials, a
    # customer who disconnected) was enqueued and retried every cycle regardless, which was a
    # real chunk of the noise in a 45%-failure-rate scan of pricing jobs (auth failures, missing
    # SFTP files -- exactly what a stale disconnected connection produces).
    recurring_kinds = [k for k in _PRICING_SYNC_KINDS if k != src_enums.BrandProviderKind.TURN_14.value]
    qs = (
        src_models.CompanyProviders.objects.select_related("provider")
        .filter(provider__kind__in=recurring_kinds, active=True)
        .values_list("id", "provider__kind")
    )
    enqueued = 0
    skipped_not_due = 0
    for cp_id, kind in qs:
        if not _recurring_pricing_sync_is_due(cp_id, kind):
            skipped_not_due += 1
            logger.info(
                "{} Skipping company_provider_id={} (kind={}): recurring pricing sync not due "
                "yet (min interval {}).".format(
                    _LOG_PREFIX, cp_id, kind, _RECURRING_PRICING_MIN_INTERVAL.get(kind),
                )
            )
            continue
        enqueue_company_provider_pricing_sync(cp_id, skip_raw_fetch=False)
        enqueued += 1
    if skipped_not_due:
        logger.info(
            "{} enqueue_all_active_company_provider_pricing_jobs: skipped {} connection(s) not "
            "yet due for a recurring pricing sync.".format(_LOG_PREFIX, skipped_not_due)
        )
    logger.info(
        "{} enqueue_all_active_company_provider_pricing_jobs: enqueued {} job(s).".format(
            _LOG_PREFIX, enqueued
        )
    )
    return enqueued


def enqueue_all_active_turn14_pricing_jobs() -> int:
    """
    Enqueue a pricing sync job for every active Turn 14 ``CompanyProviders`` row.

    Called once daily from sync_turn14_global_sweep, after that command's own catalog sweep +
    MasterPart/ProviderPart propagation complete -- pricing gets checked against the same-day
    fresh catalog it was just swept against, rather than against up to ~20h-stale data if this
    ran on the generic every-4-hours ingest_all_providers cycle instead. Each job still does a
    full per-company flat /v1/pricing fetch (see _fetch_raw_pricing) -- there is no delta path
    for Turn 14 pricing.

    Idempotent, same as enqueue_all_active_company_provider_pricing_jobs. Returns the number of
    jobs enqueued.
    """
    # active=True fixed 2026-08-31 -- see enqueue_all_active_company_provider_pricing_jobs'
    # comment on the same fix; this function had the identical gap.
    turn14_kind = src_enums.BrandProviderKind.TURN_14.value
    cp_ids = list(
        src_models.CompanyProviders.objects.filter(provider__kind=turn14_kind, active=True).values_list(
            "id", flat=True
        )
    )
    enqueued = 0
    for cp_id in cp_ids:
        if not _recurring_pricing_sync_is_due(cp_id, turn14_kind):
            continue
        enqueue_company_provider_pricing_sync(cp_id, skip_raw_fetch=False)
        enqueued += 1
    logger.info(
        "{} enqueue_all_active_turn14_pricing_jobs: enqueued {} of {} active connection(s).".format(
            _LOG_PREFIX, enqueued, len(cp_ids)
        )
    )
    return enqueued


def enqueue_company_provider_pricing_sync(
    company_provider_id: int,
    skip_raw_fetch: bool = False,
    use_delta_fetch: bool = False,
) -> None:
    """
    Queue a pricing sync for this company_provider. Any existing OPEN row for the same
    connection is removed so repeated credential saves always schedule the latest snapshot.

    Pass ``skip_raw_fetch=True`` when raw pricing data was already fetched upstream (nightly
    pipeline).  Leave ``False`` (default) for on-demand new-company onboarding/reconnect so the
    full fetch + sync cycle runs.

    ``use_delta_fetch`` is currently unused by every dispatch branch in ``_fetch_raw_pricing``
    (Turn 14's own delta route was retired in favor of a flat full fetch that's cheaper than the
    delta's brand re-fetches were -- see that function's Turn 14 branch) but is left on the
    signature/model field rather than removed, since it's still a meaningful concept a future
    provider's delta endpoint could use.
    """
    src_models.IntegrationPricingSyncJob.objects.filter(
        company_provider_id=company_provider_id,
        status=src_enums.IntegrationPricingSyncJobStatus.OPEN.value,
    ).delete()
    src_models.IntegrationPricingSyncJob.objects.create(
        company_provider_id=company_provider_id,
        status=src_enums.IntegrationPricingSyncJobStatus.OPEN.value,
        status_name=src_enums.IntegrationPricingSyncJobStatus.OPEN.name,
        skip_raw_fetch=skip_raw_fetch,
        use_delta_fetch=use_delta_fetch,
    )
    logger.info(
        "{} Enqueued pricing sync job for company_provider_id={} skip_raw_fetch={}.".format(
            _LOG_PREFIX, company_provider_id, skip_raw_fetch
        )
    )


def _fetch_raw_pricing(cp: src_models.CompanyProviders, use_delta_fetch: bool = False) -> None:
    """
    Download raw pricing data from the distributor for this company-provider.
    Only called when skip_raw_fetch=False (new-company onboarding, manual re-sync, or the
    recurring cycle for kinds without a delta option). Each provider uses its own API/SFTP/CSV
    mechanism with company-specific credentials.

    use_delta_fetch is currently ignored by every branch below -- Turn 14 used to honor it (a
    delta fetch asking what changed and only re-fetching those brands) but that route was
    retired once the flat /v1/pricing fetch turned out cheaper than the delta's own brand
    re-fetches (see the Turn 14 branch just below). Left on the signature rather than removed
    for the same reason noted on enqueue_company_provider_pricing_sync.
    """
    kind = cp.provider.kind

    if kind == src_enums.BrandProviderKind.TURN_14.value:
        # Flat /v1/pricing for both paths now. The old delta route (pricing/changes -> re-fetch
        # the affected brands) existed because a full fetch meant paging all 464 brands at 200
        # rows a page -- ~4 200 requests, ~50 minutes, impossible to repeat for every customer
        # daily. The flat endpoint returns the same data 1 000 rows to a page in 776 requests, so
        # a complete refresh now costs less than the delta's own brand re-fetches did, and every
        # customer gets genuinely current pricing rather than a diff against unknown state. The
        # old delta function has been removed -- it was unreachable dead code, never called from
        # anywhere once this branch stopped conditionally falling back to it.
        turn_14_sweeps.sweep_pricing_for_company_provider(cp)

    elif kind == src_enums.BrandProviderKind.KEYSTONE.value:
        keystone_services.sync_keystone_catalog_and_company_pricing_for_company_provider(cp.id)

    elif kind == src_enums.BrandProviderKind.MEYER.value:
        meyer_services.sync_meyer_company_pricing_for_company_provider(cp.id)

    elif kind == src_enums.BrandProviderKind.ATECH.value:
        atech_services.sync_atech_company_pricing_for_company_provider(cp.id)

    elif kind == src_enums.BrandProviderKind.ROUGH_COUNTRY.value:
        rough_country_services.sync_rough_country_company_pricing_for_company_provider(cp.id)

    elif kind == src_enums.BrandProviderKind.WHEELPROS.value:
        wheelpros_services.sync_wheelpros_company_pricing_for_company_provider(cp.id)

    elif kind == src_enums.BrandProviderKind.DLG.value:
        dlg_services.sync_dlg_company_pricing_for_company_provider(cp.id)

    elif kind == src_enums.BrandProviderKind.PREMIER_PERFORMANCE.value:
        # NOT fetch_and_save_all_premier_brand_parts() -- that only maintains the SHARED
        # PremierParts/PremierBrand catalog via the primary CompanyProvider (still called from
        # ingest_all_providers' Phase 1) and never touches this company's own pricing. This was
        # the dispatch call here for a while, which meant non-primary companies' Premier pricing
        # was never actually synced despite jobs reporting success every cycle -- confirmed live
        # (two companies with zero PremierCompanyPricing rows ever, one 8 days stale). Use the
        # per-company function instead, mirroring every other provider's pattern.
        premier_services.fetch_and_save_premier_company_pricing_for_company_provider(cp.id)

    elif kind == src_enums.BrandProviderKind.VOSSEN.value:
        vossen_services.sync_vossen_company_pricing_for_company_provider(cp.id)

    elif kind == src_enums.BrandProviderKind.TIRERACK.value:
        tirerack_services.sync_tirerack_company_pricing_for_company_provider(cp.id)

    elif kind == src_enums.BrandProviderKind.QUADRATEC.value:
        quadratec_services.sync_quadratec_company_pricing_for_company_provider(cp.id)

    elif kind == src_enums.BrandProviderKind.ELITE_WHEEL.value:
        elite_wheel_services.sync_elite_wheel_company_pricing_for_company_provider(cp.id)

    elif kind == src_enums.BrandProviderKind.MOTOR_STATE_DISTRIBUTING.value:
        motorstate_services.sync_motorstate_company_pricing_for_company_provider(cp.id)

    elif kind == src_enums.BrandProviderKind.WESTERN_POWER_SPORTS.value:
        wps_services.sync_wps_company_pricing_for_company_provider(cp.id)

    elif kind == src_enums.BrandProviderKind.HELMHOUSE.value:
        helmet_house_services.sync_helmet_house_company_pricing_for_company_provider(cp.id)

    elif kind == src_enums.BrandProviderKind.THE_WHEEL_GROUP.value:
        the_wheel_group_services.sync_the_wheel_group_company_pricing_for_company_provider(cp.id)

    else:
        raise ValueError("Unsupported provider kind for raw pricing fetch: {}".format(kind))


def _sync_master_pricing(cp: src_models.CompanyProviders) -> None:
    """
    Propagate already-fetched raw pricing data into the master parts pricing layer
    (ProviderPartCompanyPricing) for this company.  Always runs regardless of skip_raw_fetch.
    """
    kind = cp.provider.kind
    company_id = cp.company_id

    if kind == src_enums.BrandProviderKind.TURN_14.value:
        master_parts.sync_provider_pricing_from_turn14_for_company(company_id)

    elif kind == src_enums.BrandProviderKind.KEYSTONE.value:
        master_parts.sync_provider_pricing_from_keystone_for_company(company_id)

    elif kind == src_enums.BrandProviderKind.MEYER.value:
        master_parts.sync_provider_pricing_from_meyer_for_company(company_id)

    elif kind == src_enums.BrandProviderKind.ATECH.value:
        master_parts.sync_provider_pricing_from_atech_for_company(company_id)

    elif kind == src_enums.BrandProviderKind.ROUGH_COUNTRY.value:
        master_parts.sync_provider_pricing_from_rough_country_for_company(company_id)

    elif kind == src_enums.BrandProviderKind.WHEELPROS.value:
        master_parts.sync_provider_pricing_from_wheelpros_for_company(company_id)

    elif kind == src_enums.BrandProviderKind.DLG.value:
        master_parts.sync_provider_pricing_from_dlg_for_company(company_id)

    elif kind == src_enums.BrandProviderKind.PREMIER_PERFORMANCE.value:
        master_parts.sync_provider_pricing_from_premier_for_company(company_id)

    elif kind == src_enums.BrandProviderKind.VOSSEN.value:
        master_parts.sync_provider_pricing_from_vossen_for_company(company_id)

    elif kind == src_enums.BrandProviderKind.TIRERACK.value:
        master_parts.sync_provider_pricing_from_tirerack_for_company(company_id)

    elif kind == src_enums.BrandProviderKind.QUADRATEC.value:
        master_parts.sync_provider_pricing_from_quadratec_for_company(company_id)

    elif kind == src_enums.BrandProviderKind.ELITE_WHEEL.value:
        master_parts.sync_provider_pricing_from_elite_wheel_for_company(company_id)

    elif kind == src_enums.BrandProviderKind.MOTOR_STATE_DISTRIBUTING.value:
        master_parts.sync_provider_pricing_from_motorstate_for_company(company_id)

    elif kind == src_enums.BrandProviderKind.WESTERN_POWER_SPORTS.value:
        master_parts.sync_provider_pricing_from_wps_for_company(company_id)

    elif kind == src_enums.BrandProviderKind.HELMHOUSE.value:
        master_parts.sync_provider_pricing_from_helmet_house_for_company(company_id)

    elif kind == src_enums.BrandProviderKind.THE_WHEEL_GROUP.value:
        master_parts.sync_provider_pricing_from_the_wheel_group_for_company(company_id)

    else:
        raise ValueError("Unsupported provider kind for master pricing sync: {}".format(kind))


def cleanup_stale_running_jobs(max_age_minutes: int = 60) -> int:
    """
    Finish IntegrationPricingSyncJob rows stuck RUNNING (worker OOM-killed or container
    restarted mid-job), then enqueue a fresh OPEN job for the same connection.

    Deliberately does NOT reopen the stale row in place: a row that was killed mid-run may have
    partial/inconsistent raw_response state, and its identity shouldn't be silently reused as if
    nothing happened. It's marked FAILED (terminal -- "finished") like any other failed run, and
    a brand new job is what actually gets retried, via the same enqueue_company_provider_pricing_sync()
    every other trigger (connect, update, nightly enqueue) already uses.
    """
    cutoff = timezone.now() - datetime.timedelta(minutes=max_age_minutes)
    stale_jobs = list(
        src_models.IntegrationPricingSyncJob.objects.filter(
            status=src_enums.IntegrationPricingSyncJobStatus.RUNNING.value,
            started_at__lt=cutoff,
        )
    )
    if not stale_jobs:
        return 0

    now = timezone.now()
    src_models.IntegrationPricingSyncJob.objects.filter(
        id__in=[j.id for j in stale_jobs]
    ).update(
        status=src_enums.IntegrationPricingSyncJobStatus.FAILED.value,
        status_name=src_enums.IntegrationPricingSyncJobStatus.FAILED.name,
        error_message=(
            "Stale RUNNING — worker was killed (OOM or container restart) before it could "
            "mark itself complete/failed. Finished as FAILED; a fresh job was enqueued for retry."
        ),
        completed_at=now,
        updated_at=now,
    )

    seen_company_providers: typing.Set[int] = set()
    requeued = 0
    for job in stale_jobs:
        if job.company_provider_id in seen_company_providers:
            continue
        seen_company_providers.add(job.company_provider_id)
        enqueue_company_provider_pricing_sync(
            job.company_provider_id,
            skip_raw_fetch=job.skip_raw_fetch,
            use_delta_fetch=job.use_delta_fetch,
        )
        requeued += 1

    logger.warning(
        "Finished {} stale RUNNING IntegrationPricingSyncJob row(s) as FAILED; "
        "enqueued {} fresh job(s) for retry.".format(len(stale_jobs), requeued)
    )
    return len(stale_jobs)


def claim_next_open_job() -> typing.Optional[src_models.IntegrationPricingSyncJob]:
    """Atomically mark one OPEN job as RUNNING. Returns None if none available."""
    with transaction.atomic():
        job = (
            src_models.IntegrationPricingSyncJob.objects.select_for_update(skip_locked=True)
            .filter(status=src_enums.IntegrationPricingSyncJobStatus.OPEN.value)
            .filter(
                django_db_models.Q(not_before__isnull=True)
                | django_db_models.Q(not_before__lte=timezone.now())
            )
            .order_by("id")
            .first()
        )
        if not job:
            return None
        now = timezone.now()
        job.status = src_enums.IntegrationPricingSyncJobStatus.RUNNING.value
        job.status_name = src_enums.IntegrationPricingSyncJobStatus.RUNNING.name
        job.started_at = now
        job.message = None
        job.error_message = None
        job.save(
            update_fields=["status", "status_name", "started_at", "message", "error_message", "updated_at"]
        )
        return job


def run_integration_pricing_sync_job(job: src_models.IntegrationPricingSyncJob) -> None:
    """
    Execute one pricing sync job.

    If ``job.skip_raw_fetch`` is True, only the master-parts pricing sync runs (raw data
    was already fetched by the nightly ingest pipeline).  If False, the full flow runs:
    raw distributor fetch first, then master-parts sync.
    """
    cp = (
        src_models.CompanyProviders.objects.select_related("company", "provider")
        .filter(id=job.company_provider_id)
        .first()
    )
    if not cp:
        job.status = src_enums.IntegrationPricingSyncJobStatus.FAILED.value
        job.status_name = src_enums.IntegrationPricingSyncJobStatus.FAILED.name
        job.error_message = "CompanyProviders row no longer exists."
        job.completed_at = timezone.now()
        job.save(
            update_fields=[
                "status",
                "status_name",
                "error_message",
                "completed_at",
                "updated_at",
            ]
        )
        return

    if not should_enqueue_pricing_sync(cp.provider.kind):
        job.status = src_enums.IntegrationPricingSyncJobStatus.COMPLETED.value
        job.status_name = src_enums.IntegrationPricingSyncJobStatus.COMPLETED.name
        job.message = "Skipped: provider kind has no pricing sync handler."
        job.completed_at = timezone.now()
        job.save(
            update_fields=["status", "status_name", "message", "completed_at", "updated_at"]
        )
        return

    # Metered so the job row records what the sync actually cost in requests and wall-clock,
    # making a run directly comparable against earlier ones. Entered manually rather than as a
    # `with` block so the existing try/except structure below stays untouched.
    meter = rate_limit_base.UsageMeter("t14:get")
    meter.__enter__()
    try:
        if not job.skip_raw_fetch:
            logger.info(
                "{} Job id={} company_provider={} provider={}: fetching raw pricing data "
                "(use_delta_fetch={}).".format(
                    _LOG_PREFIX, job.id, cp.id, cp.provider.kind, job.use_delta_fetch
                )
            )
            _fetch_raw_pricing(cp, use_delta_fetch=job.use_delta_fetch)

        logger.info(
            "{} Job id={} company_provider={} provider={}: syncing master pricing layer "
            "(skip_raw_fetch={}).".format(
                _LOG_PREFIX, job.id, cp.id, cp.provider.kind, job.skip_raw_fetch
            )
        )
        _sync_master_pricing(cp)

    except rate_limit_base.RateBudgetExhausted as e:
        # Not a failure: the distributor's hourly/daily allowance for this company's credentials
        # is spent. Turn 14 deactivates credentials that repeatedly hit their limits, so the
        # right move is to stop, keep whatever was already written, and come back in the next
        # window -- never to retry into the ceiling. Back to OPEN, deferred until then.
        meter.__exit__(None, None, None)
        retry_at = timezone.now() + datetime.timedelta(seconds=e.retry_after_seconds)
        logger.warning(
            "{} Job id={} deferred until {}: {}".format(_LOG_PREFIX, job.id, retry_at, e)
        )
        job.status = src_enums.IntegrationPricingSyncJobStatus.OPEN.value
        job.status_name = src_enums.IntegrationPricingSyncJobStatus.OPEN.name
        job.message = "Deferred: {} || {}".format(e, meter.summary("api_usage"))[:4000]
        job.not_before = retry_at
        job.started_at = None
        job.save(
            update_fields=[
                "status",
                "status_name",
                "message",
                "not_before",
                "started_at",
                "updated_at",
            ]
        )
        return

    except Exception as e:
        meter.__exit__(None, None, None)
        logger.exception("{} Job id={} failed.".format(_LOG_PREFIX, job.id))
        job.status = src_enums.IntegrationPricingSyncJobStatus.FAILED.value
        job.status_name = src_enums.IntegrationPricingSyncJobStatus.FAILED.name
        job.error_message = str(e)[:4000]
        job.completed_at = timezone.now()
        job.save(
            update_fields=[
                "status",
                "status_name",
                "error_message",
                "completed_at",
                "updated_at",
            ]
        )
        return

    meter.__exit__(None, None, None)
    job.status = src_enums.IntegrationPricingSyncJobStatus.COMPLETED.value
    job.status_name = src_enums.IntegrationPricingSyncJobStatus.COMPLETED.name
    job.message = "OK (skip_raw_fetch={}, use_delta_fetch={}) || {}".format(
        job.skip_raw_fetch, job.use_delta_fetch, meter.summary("api_usage")
    )
    job.completed_at = timezone.now()
    job.not_before = None
    job.save(
        update_fields=["status", "status_name", "message", "not_before", "completed_at", "updated_at"]
    )

    # `cp` was fetched once at the top of this function; a disconnect (credentials.feed cleared
    # to {}, initial_sync_completed reset to False) can land concurrently while this job was
    # still running its fetch/sync steps, which can legitimately succeed against data fetched
    # before the disconnect. Re-check feed credentials fresh from the DB right before flipping
    # status -- a successful job must not resurrect a connection the user just disconnected.
    cp.refresh_from_db(fields=["credentials"])
    if not credentials_helper.get_feed_credentials(cp):
        logger.info(
            "{} Job id={}: company_provider_id={} succeeded but feed credentials are now empty "
            "(disconnected while this job was running) -- not flipping to CONNECTED.".format(
                _LOG_PREFIX, job.id, cp.id
            )
        )
        return

    # Mark the company-provider as having completed its initial sync so the frontend
    # can transition from "Ingesting data…" to showing live pricing. Also flips the
    # connection status straight to CONNECTED — check_company_provider_connections only
    # covers rows still pending their first sync, so this is the only place that sets it.
    #
    # The status flip is deliberately NOT gated on initial_sync_completed. It used to be, which
    # meant a row that reached initial_sync_completed=True while its status said something else
    # (e.g. a reconnect landing between a job's completion and its status write) could never
    # recover: every later job took the `already completed` path and skipped the flip forever.
    # A successful job is proof the connection works, so re-assert CONNECTED every time it isn't
    # already set. The one-time side effects below stay gated on the actual transition.
    first_completion = not cp.initial_sync_completed
    status_needs_flip = cp.status != src_enums.CompanyProviderConnectionStatus.CONNECTED.value
    if first_completion or status_needs_flip:
        update_fields = dict(
            initial_sync_completed=True,
            status=src_enums.CompanyProviderConnectionStatus.CONNECTED.value,
            status_name=src_enums.CompanyProviderConnectionStatus.CONNECTED.name,
            status_reason=None,
            status_checked_at=timezone.now(),
        )
        # Order credentials may have already validated fine and been sitting in WAITING because
        # the feed wasn't CONNECTED yet (see CompanyProviderOrderConnectionStatus) — now that it
        # is, promote them to CONNECTED too, on both the default CompanyProviderOrderAccount row
        # (the source of truth for its own status) and this mirror. An ERROR order status is left
        # alone; that's a real credential problem, unrelated to feed sync completing. Local
        # import: avoids a circular import, since src.api.services.integrations already imports
        # this module (enqueue_company_provider_pricing_sync/should_enqueue_pricing_sync).
        if cp.order_status == src_enums.CompanyProviderOrderConnectionStatus.WAITING.value:
            from src.api.services import integrations as integrations_services

            default_account = credentials_helper.get_default_order_account(cp)
            if (
                default_account
                and default_account.order_status == src_enums.CompanyProviderOrderConnectionStatus.WAITING.value
            ):
                integrations_services._set_order_account_status(
                    default_account, src_enums.CompanyProviderOrderConnectionStatus.CONNECTED, None
                )
            update_fields.update(
                order_status=src_enums.CompanyProviderOrderConnectionStatus.CONNECTED.value,
                order_status_name=src_enums.CompanyProviderOrderConnectionStatus.CONNECTED.name,
                order_status_reason=None,
                order_status_checked_at=timezone.now(),
            )
        src_models.CompanyProviders.objects.filter(id=cp.id).update(**update_fields)
        logger.info(
            "{} Job id={}: company_provider_id={} initial_sync_completed=True, status=CONNECTED "
            "(first_completion={}, repaired_status={}).".format(
                _LOG_PREFIX, job.id, cp.id, first_completion, not first_completion and status_needs_flip
            )
        )
        # Only on the real first-sync transition — a status repair must not re-send the
        # "your data is ready" email to a company that already got it.
        if first_completion:
            notifications_services.send_first_sync_completed_email(cp)


def process_pricing_sync_jobs(limit: int = 10, workers: int = 1) -> int:
    """
    Claim and run up to ``limit`` OPEN pricing sync jobs.

    When ``workers`` > 1, jobs are processed in parallel using a ThreadPoolExecutor.
    Each worker claims its own job atomically (``select_for_update(skip_locked=True)``)
    so there are no race conditions — safe to call concurrently from multiple processes too.

    Returns the number of jobs that were started (not necessarily completed successfully).
    """
    if workers <= 1:
        processed = 0
        while processed < limit:
            job = claim_next_open_job()
            if not job:
                break
            run_integration_pricing_sync_job(job)
            connection.close()
            processed += 1
        return processed

    # Parallel path: spawn workers, each claims + runs one job at a time.
    processed_count = 0
    lock = __import__("threading").Lock()

    def _worker() -> int:
        """Claim and run jobs until none remain or limit is reached."""
        local_count = 0
        while True:
            with lock:
                nonlocal processed_count
                if processed_count >= limit:
                    break
                processed_count += 1  # Reserve a slot
            job = claim_next_open_job()
            if not job:
                with lock:
                    processed_count -= 1  # Give the slot back
                break
            try:
                run_integration_pricing_sync_job(job)
            finally:
                connection.close()
            local_count += 1
        return local_count

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=workers,
        thread_name_prefix="pricing_sync",
    ) as executor:
        futs = [executor.submit(_worker) for _ in range(workers)]
        total = sum(f.result() for f in concurrent.futures.as_completed(futs))

    return total
