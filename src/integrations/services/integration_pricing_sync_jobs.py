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
import logging
import typing

from django.db import connection, transaction
from django.utils import timezone

from src import enums as src_enums
from src import models as src_models
from src.integrations.services import keystone as keystone_services
from src.integrations.services import master_parts
from src.integrations.services import atech as atech_services
from src.integrations.services import dlg as dlg_services
from src.integrations.services import meyer as meyer_services
from src.integrations.services import premier as premier_services
from src.integrations.services import rough_country as rough_country_services
from src.integrations.services import turn_14 as turn_14_services
from src.integrations.services import wheelpros as wheelpros_services

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
    }
)


def should_enqueue_pricing_sync(provider_kind: int) -> bool:
    return provider_kind in _PRICING_SYNC_KINDS


def enqueue_all_active_company_provider_pricing_jobs() -> int:
    """
    Enqueue a pricing sync job for every active ``CompanyProviders`` row whose provider
    kind is in ``_PRICING_SYNC_KINDS``.

    Called by the nightly ``ingest_all_providers`` pipeline after Phase 2 (global catalog
    sync) completes.  All jobs use ``skip_raw_fetch=False`` so each company-provider
    downloads its own fresh per-company pricing data (FTP/SFTP/API) before syncing to
    the master pricing layer.  Phase 1 only fetches global catalog data, never per-company
    pricing, so there is nothing to skip here.

    Idempotent: any existing OPEN job for the same company-provider is removed before
    creating a fresh one (same behaviour as ``enqueue_company_provider_pricing_sync``).

    Returns the number of jobs enqueued.
    """
    qs = (
        src_models.CompanyProviders.objects.select_related("provider")
        .filter(provider__kind__in=list(_PRICING_SYNC_KINDS))
        .values_list("id", flat=True)
    )
    enqueued = 0
    for cp_id in qs:
        enqueue_company_provider_pricing_sync(cp_id, skip_raw_fetch=False)
        enqueued += 1
    logger.info(
        "{} enqueue_all_active_company_provider_pricing_jobs: enqueued {} job(s).".format(
            _LOG_PREFIX, enqueued
        )
    )
    return enqueued


def enqueue_company_provider_pricing_sync(
    company_provider_id: int,
    skip_raw_fetch: bool = False,
) -> None:
    """
    Queue a pricing sync for this company_provider. Any existing OPEN row for the same
    connection is removed so repeated credential saves always schedule the latest snapshot.

    Pass ``skip_raw_fetch=True`` when raw pricing data was already fetched upstream (nightly
    pipeline).  Leave ``False`` (default) for on-demand new-company onboarding so the full
    fetch + sync cycle runs.
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
    )
    logger.info(
        "{} Enqueued pricing sync job for company_provider_id={} skip_raw_fetch={}.".format(
            _LOG_PREFIX, company_provider_id, skip_raw_fetch
        )
    )


def _fetch_raw_pricing(cp: src_models.CompanyProviders) -> None:
    """
    Download raw pricing data from the distributor for this company-provider.
    Only called when skip_raw_fetch=False (new-company onboarding or manual re-sync).
    Each provider uses its own API/SFTP/CSV mechanism with company-specific credentials.
    """
    kind = cp.provider.kind

    if kind == src_enums.BrandProviderKind.TURN_14.value:
        turn_14_services.fetch_and_save_turn_14_brand_pricing_for_company_provider(cp.id)

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
        # Premier pricing is embedded in the parts feed — fetch the full parts catalog
        # (same call as Phase 1 ingest; only needed for on-demand/new-company runs).
        premier_services.fetch_and_save_all_premier_brand_parts()

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

    else:
        raise ValueError("Unsupported provider kind for master pricing sync: {}".format(kind))


def claim_next_open_job() -> typing.Optional[src_models.IntegrationPricingSyncJob]:
    """Atomically mark one OPEN job as RUNNING. Returns None if none available."""
    with transaction.atomic():
        job = (
            src_models.IntegrationPricingSyncJob.objects.select_for_update(skip_locked=True)
            .filter(status=src_enums.IntegrationPricingSyncJobStatus.OPEN.value)
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

    try:
        if not job.skip_raw_fetch:
            logger.info(
                "{} Job id={} company_provider={} provider={}: fetching raw pricing data.".format(
                    _LOG_PREFIX, job.id, cp.id, cp.provider.kind
                )
            )
            _fetch_raw_pricing(cp)

        logger.info(
            "{} Job id={} company_provider={} provider={}: syncing master pricing layer "
            "(skip_raw_fetch={}).".format(
                _LOG_PREFIX, job.id, cp.id, cp.provider.kind, job.skip_raw_fetch
            )
        )
        _sync_master_pricing(cp)

    except Exception as e:
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

    job.status = src_enums.IntegrationPricingSyncJobStatus.COMPLETED.value
    job.status_name = src_enums.IntegrationPricingSyncJobStatus.COMPLETED.name
    job.message = "OK (skip_raw_fetch={})".format(job.skip_raw_fetch)
    job.completed_at = timezone.now()
    job.save(
        update_fields=["status", "status_name", "message", "completed_at", "updated_at"]
    )

    # Mark the company-provider as having completed its initial sync so the frontend
    # can transition from "Ingesting data…" to showing live pricing.
    if not cp.initial_sync_completed:
        src_models.CompanyProviders.objects.filter(id=cp.id).update(initial_sync_completed=True)
        logger.info(
            "{} Job id={}: marked company_provider_id={} initial_sync_completed=True.".format(
                _LOG_PREFIX, job.id, cp.id
            )
        )


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
