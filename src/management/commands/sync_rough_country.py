"""
Sync Rough Country feed from Excel (jobber_pc2A.xlsx).
Downloads from feeds.roughcountry.com or uses local file; upserts brands, parts, fitment, discontinued.
"""
from django.core.management.base import BaseCommand

from src.audit import scheduled_tasks as audit_scheduled_tasks
from src.integrations.services import master_parts, rough_country


class Command(BaseCommand):
    help = (
        "Sync Rough Country feed: download Excel (or use local file), "
        "upsert brand, parts (General), fitment (Vehicle Fitment), apply Discontinued; "
        "then sync unmapped Rough Country brands into Brands (mappings, brand_providers, company_brands for TICK_PERFORMANCE)."
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
            help="Override feed URL (default: https://feeds.roughcountry.com/jobber_pc2A.xlsx).",
        )
        parser.add_argument(
            "--local-file",
            type=str,
            default=None,
            help="Path to local jobber_pc2A.xlsx (if set, --no-download uses this).",
        )

    def handle(self, *args, **options):
        self.stdout.write("Starting Rough Country feed sync...")
        execution = audit_scheduled_tasks.start_scheduled_task_execution("sync_rough_country")
        try:
            self.stdout.write("Step 1: Fetching and saving Rough Country feed (brands, parts, fitment, discontinued)...")
            rough_country.fetch_and_save_rough_country(
                file_url=options.get("file_url"),
                local_file_path=options.get("local_file"),
                download=not options.get("no_download"),
            )
            self.stdout.write(self.style.SUCCESS("Rough Country feed synced."))

            self.stdout.write("Step 2: Syncing unmapped Rough Country brands into Brands flow...")
            rough_country.sync_unmapped_rough_country_brands_to_brands()
            self.stdout.write(self.style.SUCCESS("Unmapped Rough Country brands synced."))

            self.stdout.write(
                "Step 3: Propagating Rough Country catalog into master parts, provider parts, inventory, and pricing..."
            )
            master_parts.sync_derived_from_rough_country(reindex_meilisearch=False)
            self.stdout.write(self.style.SUCCESS("Derived master layer sync done."))

            audit_scheduled_tasks.mark_scheduled_task_completed(
                execution,
                message="Successfully completed Rough Country feed sync and derived master layer sync.",
            )
            self.stdout.write(self.style.SUCCESS("Successfully completed Rough Country feed sync."))
        except Exception as e:
            audit_scheduled_tasks.mark_scheduled_task_failed(execution, error_message=str(e))
            self.stdout.write(self.style.ERROR("Error: {}".format(str(e))))
            raise
