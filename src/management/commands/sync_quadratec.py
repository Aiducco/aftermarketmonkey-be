"""
Sync Quadratec feeds (catalog xlsx + wholesale/inventory csv).
Reads both feeds, upserts brands, parts, and distributor inventory; maps brands into Brands; then
propagates into the master parts / inventory layer. Per-company pricing (QuadratecCompanyPricing ->
ProviderPartCompanyPricing) is handled per company by the IntegrationPricingSyncJob queue.
"""
from django.core.management.base import BaseCommand

from src.audit import scheduled_tasks as audit_scheduled_tasks
from src.integrations.services import master_parts, quadratec


class Command(BaseCommand):
    help = (
        "Sync Quadratec feeds: read catalog + wholesale/inventory files, upsert QuadratecBrand, "
        "QuadratecPart (catalog + per-warehouse inventory); sync unmapped Quadratec brands into "
        "Brands; then propagate into master parts, provider parts, and inventory."
    )

    def handle(self, *args, **options):
        audit_scheduled_tasks.cleanup_stale_started_executions("sync_quadratec")
        self.stdout.write("Starting Quadratec feed sync...")
        execution = audit_scheduled_tasks.start_scheduled_task_execution("sync_quadratec")
        try:
            self.stdout.write("Step 1: Reading Quadratec feeds (brands, parts, inventory)...")
            quadratec.fetch_and_save_quadratec()
            self.stdout.write(self.style.SUCCESS("Quadratec feeds synced."))

            self.stdout.write("Step 2: Syncing unmapped Quadratec brands into Brands flow...")
            quadratec.sync_unmapped_quadratec_brands_to_brands()
            self.stdout.write(self.style.SUCCESS("Unmapped Quadratec brands synced."))

            self.stdout.write(
                "Step 3: Propagating Quadratec catalog into master parts, provider parts, and inventory..."
            )
            master_parts.sync_derived_from_quadratec(reindex_meilisearch=False, skip_pricing=True)
            self.stdout.write(self.style.SUCCESS("Derived master layer sync done."))

            audit_scheduled_tasks.mark_scheduled_task_completed(
                execution,
                message="Successfully completed Quadratec feed sync and derived master layer sync.",
            )
            self.stdout.write(self.style.SUCCESS("Successfully completed Quadratec feed sync."))
        except Exception as e:
            audit_scheduled_tasks.mark_scheduled_task_failed(execution, error_message=str(e))
            self.stdout.write(self.style.ERROR("Error: {}".format(str(e))))
            raise
