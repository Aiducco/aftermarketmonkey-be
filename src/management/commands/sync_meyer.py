"""
Fetch Meyer Pricing + Meyer Inventory from SFTP, upsert MeyerBrand / MeyerParts, then sync
unmapped Meyer brands into the Brands flow (mappings, brand_providers, company_brands for TICK_PERFORMANCE).

Same orchestration pattern as sync_wheelpros / sync_rough_country + ScheduledTaskExecution audit.
For MasterPart / ProviderPart / inventory / pricing sync, run sync_master_parts_from_meyer (or
sync_all_master_parts) separately.
"""
from django.core.management.base import BaseCommand

from src.audit import scheduled_tasks as audit_scheduled_tasks
from src.integrations.services import meyer


class Command(BaseCommand):
    help = (
        "Download Meyer Pricing + Inventory from SFTP, upsert MeyerBrand / MeyerParts, "
        "then sync unmapped MeyerBrand into Brands (BrandMeyerBrandMapping, BrandProviders, "
        "CompanyBrands for TICK_PERFORMANCE). Records audit as sync_meyer."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Do not fetch or write. Only log what sync_unmapped_meyer_brands_to_brands would do.",
        )
        parser.add_argument(
            "--force-download",
            action="store_true",
            help="Re-download CSVs from SFTP even if local cache exists.",
        )

    def handle(self, *args, **options):
        dry_run = options.get("dry_run", False)
        if dry_run:
            self.stdout.write(
                "Dry run: skipping SFTP fetch and Meyer ingest; preview unmapped brand sync only."
            )
            try:
                meyer.sync_unmapped_meyer_brands_to_brands(dry_run=True)
            except Exception as e:
                self.stdout.write(self.style.ERROR("Error: {}".format(str(e))))
                raise
            self.stdout.write(self.style.SUCCESS("Dry run finished (no changes applied)."))
            return

        self.stdout.write("Starting Meyer catalog fetch and brand sync...")
        execution = audit_scheduled_tasks.start_scheduled_task_execution("sync_meyer")
        try:
            self.stdout.write(
                "Step 1: Fetching Meyer Pricing + Inventory and upserting MeyerBrand / MeyerParts..."
            )
            meyer.fetch_and_save_meyer_catalog_and_inventory(
                force_download=options.get("force_download", False),
            )
            self.stdout.write(self.style.SUCCESS("Meyer catalog and inventory saved."))

            self.stdout.write("Step 2: Syncing unmapped Meyer brands into Brands flow...")
            meyer.sync_unmapped_meyer_brands_to_brands()
            self.stdout.write(self.style.SUCCESS("Unmapped Meyer brands synced."))

            audit_scheduled_tasks.mark_scheduled_task_completed(
                execution,
                message="Successfully completed Meyer catalog fetch and brand sync.",
            )
            self.stdout.write(
                self.style.SUCCESS("Successfully completed Meyer catalog fetch and brand sync.")
            )
        except Exception as e:
            audit_scheduled_tasks.mark_scheduled_task_failed(execution, error_message=str(e))
            self.stdout.write(self.style.ERROR("Error: {}".format(str(e))))
            raise
