"""
Enqueue and run per-company-provider distributor pricing sync (no Celery).
Jobs are stored in IntegrationPricingSyncJob and processed by a management command.
"""
import logging
import typing

from django.db import transaction
from django.utils import timezone

from src import enums as src_enums
from src import models as src_models
from src.integrations.services import keystone as keystone_services
from src.integrations.services import master_parts
from src.integrations.services import atech as atech_services
from src.integrations.services import dlg as dlg_services
from src.integrations.services import meyer as meyer_services
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
    }
)


def should_enqueue_pricing_sync(provider_kind: int) -> bool:
    return provider_kind in _PRICING_SYNC_KINDS


def enqueue_company_provider_pricing_sync(company_provider_id: int) -> None:
    """
    Queue a pricing sync for this company_provider. Any existing OPEN row for the same
    connection is removed so repeated credential saves always schedule the latest snapshot.
    """
    src_models.IntegrationPricingSyncJob.objects.filter(
        company_provider_id=company_provider_id,
        status=src_enums.IntegrationPricingSyncJobStatus.OPEN.value,
    ).delete()
    src_models.IntegrationPricingSyncJob.objects.create(
        company_provider_id=company_provider_id,
        status=src_enums.IntegrationPricingSyncJobStatus.OPEN.value,
        status_name=src_enums.IntegrationPricingSyncJobStatus.OPEN.name,
    )
    logger.info(
        "{} Enqueued pricing sync job for company_provider_id={}.".format(_LOG_PREFIX, company_provider_id)
    )


def _sync_distributor_tables_then_master_parts(cp: src_models.CompanyProviders) -> None:
    kind = cp.provider.kind
    company_id = cp.company_id

    if kind == src_enums.BrandProviderKind.TURN_14.value:
        turn_14_services.fetch_and_save_turn_14_brand_pricing_for_company_provider(cp.id)
        master_parts.sync_provider_pricing_from_turn14_for_company(company_id)
    elif kind == src_enums.BrandProviderKind.KEYSTONE.value:
        keystone_services.sync_keystone_catalog_and_company_pricing_for_company_provider(cp.id)
        master_parts.sync_provider_pricing_from_keystone_for_company(company_id)
    elif kind == src_enums.BrandProviderKind.MEYER.value:
        meyer_services.sync_meyer_company_pricing_for_company_provider(cp.id)
        master_parts.sync_provider_pricing_from_meyer_for_company(company_id)
    elif kind == src_enums.BrandProviderKind.ATECH.value:
        atech_services.sync_atech_company_pricing_for_company_provider(cp.id)
        master_parts.sync_provider_pricing_from_atech_for_company(company_id)
    elif kind == src_enums.BrandProviderKind.ROUGH_COUNTRY.value:
        rough_country_services.sync_rough_country_company_pricing_for_company_provider(cp.id)
        master_parts.sync_provider_pricing_from_rough_country_for_company(company_id)
    elif kind == src_enums.BrandProviderKind.WHEELPROS.value:
        wheelpros_services.sync_wheelpros_company_pricing_for_company_provider(cp.id)
        master_parts.sync_provider_pricing_from_wheelpros_for_company(company_id)
    elif kind == src_enums.BrandProviderKind.DLG.value:
        dlg_services.sync_dlg_company_pricing_for_company_provider(cp.id)
        master_parts.sync_provider_pricing_from_dlg_for_company(company_id)
    else:
        raise ValueError("Unsupported provider kind for pricing sync: {}".format(kind))


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
        _sync_distributor_tables_then_master_parts(cp)
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
    job.message = "OK"
    job.completed_at = timezone.now()
    job.save(
        update_fields=["status", "status_name", "message", "completed_at", "updated_at"]
    )
