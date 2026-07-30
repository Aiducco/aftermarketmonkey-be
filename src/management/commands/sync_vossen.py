"""
Sync Vossen feed from AfterMarket.aspx CSV.
Downloads from the connected feed_url or uses a local file; upserts the VOSSEN brand and parts.
"""
from django.core.management.base import BaseCommand

from src.audit import scheduled_tasks as audit_scheduled_tasks
from src.integrations.services import master_parts, vossen


class Command(BaseCommand):
    help = (
        "Sync Vossen feed: download CSV (or use local file), upsert VossenPart catalog; "
        "then ensure the VOSSEN brand mapping (Brands, brand_providers) and propagate into "
        "master parts, provider parts, inventory, and pricing."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--no-download",
            action="store_true",
            help="Do not download; use existing local file only (fails if file missing).",
        )
        parser.add_argument(
            "--file-url",
            type=str,
            default=None,
            help="Override feed URL (default: http://inventory.vossenwheels.com/AfterMarket.aspx).",
        )
        parser.add_argument(
            "--local-file",
            type=str,
            default=None,
            help="Path to local CSV file (if set, --no-download uses this).",
        )

    def handle(self, *args, **options):
        audit_scheduled_tasks.cleanup_stale_started_executions("sync_vossen")
        self.stdout.write("Starting Vossen feed sync...")
        execution = audit_scheduled_tasks.start_scheduled_task_execution("sync_vossen")
        try:
            self.stdout.write("Step 1: Fetching and saving Vossen feed (VossenPart catalog + stock)...")
            vossen.fetch_and_save_vossen(
                file_url=options.get("file_url"),
                local_file_path=options.get("local_file"),
                download=not options.get("no_download"),
            )
            self.stdout.write(self.style.SUCCESS("Vossen feed synced."))

            self.stdout.write("Step 2: Ensuring VOSSEN brand mapping (Brands, brand_providers)...")
            vossen.ensure_vossen_brand_and_mapping()
            self.stdout.write(self.style.SUCCESS("Vossen brand mapping ensured."))

            self.stdout.write(
                "Step 3: Propagating Vossen catalog into master parts, provider parts, and inventory..."
            )
            master_parts.sync_derived_from_vossen(reindex_meilisearch=False)
            self.stdout.write(self.style.SUCCESS("Derived master layer sync done."))

            audit_scheduled_tasks.mark_scheduled_task_completed(
                execution,
                message="Successfully completed Vossen feed sync and derived master layer sync.",
            )
            self.stdout.write(self.style.SUCCESS("Successfully completed Vossen feed sync."))
        except Exception as e:
            audit_scheduled_tasks.mark_scheduled_task_failed(execution, error_message=str(e))
            self.stdout.write(self.style.ERROR("Error: {}".format(str(e))))
            raise
