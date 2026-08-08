"""
Sync the Elite Wheel & Tire inventory workbook (TriWeeklyUpdate<MM-DD-YYYY>.xlsx).
Downloads the newest drop, upserts brands and both part tables (wheels and tires), maps brands
into Brands, then propagates into the master parts / inventory layer. Per-company pricing
(EliteWheel*CompanyPricing -> ProviderPartCompanyPricing) is handled per company by the
IntegrationPricingSyncJob queue.
"""
from django.core.management.base import BaseCommand

from src.audit import scheduled_tasks as audit_scheduled_tasks
from src.integrations.services import elite_wheel, master_parts


class Command(BaseCommand):
    help = (
        "Sync the Elite Wheel & Tire workbook: download the newest TriWeeklyUpdate*.xlsx, upsert "
        "EliteWheelBrand, EliteWheelPartWheel and EliteWheelPartTire (catalog + per-location "
        "inventory); sync unmapped Elite brands into Brands; then propagate into master parts, "
        "provider parts, and inventory."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--local-file",
            dest="local_file",
            default=None,
            help="Parse this workbook instead of downloading (for testing against a saved drop).",
        )
        parser.add_argument(
            "--public-share",
            action="store_true",
            help=(
                "Force the public inventory share even when the connection has SFTP credentials "
                "(same effect as ELITE_WHEEL_FORCE_PUBLIC_SHARE in settings)."
            ),
        )

    def handle(self, *args, **options):
        audit_scheduled_tasks.cleanup_stale_started_executions("sync_elite_wheel")
        self.stdout.write("Starting Elite Wheel feed sync...")
        execution = audit_scheduled_tasks.start_scheduled_task_execution("sync_elite_wheel")
        try:
            self.stdout.write("Step 1: Reading the Elite workbook (brands, wheels, tires, inventory)...")
            elite_wheel.fetch_and_save_elite_wheel(
                local_file_path=options.get("local_file"),
                force_public_share=True if options.get("public_share") else None,
            )
            self.stdout.write(self.style.SUCCESS("Elite workbook synced."))

            self.stdout.write("Step 2: Syncing unmapped Elite brands into Brands flow...")
            elite_wheel.sync_unmapped_elite_wheel_brands_to_brands()
            self.stdout.write(self.style.SUCCESS("Unmapped Elite brands synced."))

            self.stdout.write(
                "Step 3: Propagating Elite catalog into master parts, provider parts, and inventory..."
            )
            master_parts.sync_derived_from_elite_wheel(reindex_meilisearch=False, skip_pricing=True)
            self.stdout.write(self.style.SUCCESS("Derived master layer sync done."))

            audit_scheduled_tasks.mark_scheduled_task_completed(
                execution,
                message="Successfully completed Elite Wheel feed sync and derived master layer sync.",
            )
            self.stdout.write(self.style.SUCCESS("Successfully completed Elite Wheel feed sync."))
        except Exception as e:
            audit_scheduled_tasks.mark_scheduled_task_failed(execution, error_message=str(e))
            self.stdout.write(self.style.ERROR("Error: {}".format(str(e))))
            raise
