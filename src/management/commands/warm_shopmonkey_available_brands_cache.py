"""
Precomputes the ShopMonkey inventory brand-picker's slow brand/provider/part_count aggregation
(see shopmonkey_sync.refresh_available_brands_cache) for every company with
FEATURE_SHOPMONKEY_INVENTORY_SYNC enabled, so a live request to GET /shopmonkey/inventory/
available-brands/ always hits a warm cache instead of running the 4.6M+-row aggregate query
itself (confirmed live to take 30s+ and 502 at Trident's real data volume).

Scheduled every 4 hours, same cadence as sync_shopmonkey_inventory -- brand/part-count data only
changes when a distributor's catalog re-syncs, not minute to minute.

    manage.py warm_shopmonkey_available_brands_cache
"""
from django.core.management.base import BaseCommand

from src import models as src_models
from src.api.services import feature_flags as feature_flags_services
from src.api.services import inventory_access as inventory_access_services
from src.audit import scheduled_tasks as audit_scheduled_tasks
from src.integrations.services import shopmonkey_sync as shopmonkey_sync_services

_TASK_NAME = "warm_shopmonkey_available_brands_cache"


class Command(BaseCommand):
    help = "Precompute and cache the ShopMonkey available-brands list for every feature-flagged company."

    def handle(self, *args, **options):
        # Same 4-hourly cron registration gap as sync_shopmonkey_inventory -- confirmed running
        # on the prod host but recording nothing in ScheduledTaskExecution until now.
        audit_scheduled_tasks.cleanup_stale_started_executions(_TASK_NAME, max_age_minutes=240)
        task_execution = audit_scheduled_tasks.start_scheduled_task_execution(_TASK_NAME)
        try:
            # Anyone who could open the page: flagged companies plus every company with a ShopMonkey connection,
            # kept to those whose plan or flag actually grants Inventory Management.
            candidate_ids = set(
                src_models.CompanyFeatureFlags.objects.filter(
                    feature=feature_flags_services.FEATURE_SHOPMONKEY_INVENTORY_SYNC, enabled=True
                ).values_list("company_id", flat=True)
            ) | set(
                src_models.CompanyShopManagementProviders.objects.filter(active=True).values_list(
                    "company_id", flat=True
                )
            )
            company_ids = sorted(c for c in candidate_ids if inventory_access_services.company_has_inventory_access(c))
            if not company_ids:
                self.stdout.write("No companies have the feature flag enabled -- nothing to warm.")
                audit_scheduled_tasks.mark_scheduled_task_skipped(
                    task_execution, message="No companies have the feature flag enabled."
                )
                return

            for company_id in company_ids:
                count = shopmonkey_sync_services.refresh_available_brands_cache(company_id)
                self.stdout.write(f"company_id={company_id}: cached {count} brands")

            message = f"Warmed {len(company_ids)} compan{'y' if len(company_ids) == 1 else 'ies'}."
            self.stdout.write(self.style.SUCCESS(f"Done. {message}"))
            audit_scheduled_tasks.mark_scheduled_task_completed(task_execution, message=message)
        except Exception as e:
            audit_scheduled_tasks.mark_scheduled_task_failed(task_execution, error_message=str(e))
            raise
