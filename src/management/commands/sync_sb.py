"""
Sync the S&B Filters catalog from S&B's two shared dealer workbooks.

Downloads both Google Sheets (or reads local copies), upserts SbBrand / SbPart / SbFitment,
maps unmapped S&B brands into Brands, then propagates into the master layer. Live inventory is a
separate command -- see ``fetch_sb_inventory``.
"""
from django.core.management.base import BaseCommand

from src.audit import scheduled_tasks as audit_scheduled_tasks
from src.integrations.services import master_parts, sb


class Command(BaseCommand):
    help = (
        "Sync S&B: download the pricing master and Shopify export workbooks (or use local "
        "copies), upsert brands, parts and fitment; then sync unmapped S&B brands into Brands "
        "(mappings, brand_providers, company_brands for TICK_PERFORMANCE) and propagate into "
        "master parts, provider parts, inventory and pricing."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--force-download",
            action="store_true",
            help="Re-download both workbooks even if the cached copies are still fresh.",
        )
        parser.add_argument(
            "--pricing-master-url",
            type=str,
            default=None,
            help="Override the pricing master workbook URL (default: S&B's shared sheet).",
        )
        parser.add_argument(
            "--shopify-export-url",
            type=str,
            default=None,
            help="Override the Shopify export workbook URL (default: S&B's shared sheet).",
        )
        parser.add_argument(
            "--pricing-master-file",
            type=str,
            default=None,
            help="Path to a local pricing master .xlsx (skips downloading it).",
        )
        parser.add_argument(
            "--shopify-export-file",
            type=str,
            default=None,
            help="Path to a local Shopify export .xlsx (skips downloading it).",
        )
        parser.add_argument(
            "--skip-derived",
            action="store_true",
            help="Ingest the workbooks only; do not propagate into the master layer.",
        )

    def handle(self, *args, **options):
        audit_scheduled_tasks.cleanup_stale_started_executions("sync_sb")
        self.stdout.write("Starting S&B feed sync...")
        execution = audit_scheduled_tasks.start_scheduled_task_execution("sync_sb")
        try:
            self.stdout.write("Step 1: Reading S&B workbooks (catalog, pricing, fitment, imagery)...")
            sb.fetch_and_save_sb(
                pricing_master_url=options.get("pricing_master_url"),
                shopify_export_url=options.get("shopify_export_url"),
                pricing_master_path=options.get("pricing_master_file"),
                shopify_export_path=options.get("shopify_export_file"),
                force_download=bool(options.get("force_download")),
            )
            self.stdout.write(self.style.SUCCESS("S&B workbooks synced."))

            self.stdout.write("Step 2: Syncing unmapped S&B brands into Brands flow...")
            sb.sync_unmapped_sb_brands_to_brands()
            self.stdout.write(self.style.SUCCESS("Unmapped S&B brands synced."))

            if options.get("skip_derived"):
                self.stdout.write("Step 3: skipped (--skip-derived).")
            else:
                self.stdout.write(
                    "Step 3: Propagating S&B catalog into master parts, provider parts, "
                    "inventory, fitment and pricing..."
                )
                master_parts.sync_derived_from_sb(reindex_meilisearch=False)
                self.stdout.write(self.style.SUCCESS("Derived master layer sync done."))

            audit_scheduled_tasks.mark_scheduled_task_completed(
                execution,
                message="Successfully completed S&B feed sync and derived master layer sync.",
            )
            self.stdout.write(self.style.SUCCESS("Successfully completed S&B feed sync."))
        except Exception as e:
            audit_scheduled_tasks.mark_scheduled_task_failed(execution, error_message=str(e))
            self.stdout.write(self.style.ERROR("Error: {}".format(str(e))))
            raise
