"""
Fetch and sync WheelPros parts from the SFTP CSV, then sync unmapped WheelPros brands
into the Brands flow (mappings, brand_providers, company_brands for TICK_PERFORMANCE).
"""
from django.core.management.base import BaseCommand

from src.audit import scheduled_tasks as audit_scheduled_tasks
from src.integrations.services import wheelpros


class Command(BaseCommand):
    help = (
        "Fetch and save WheelPros brands and parts from the SFTP CSV, then sync unmapped "
        "WheelPros brands into Brands (mappings, brand_providers, company_brands for TICK_PERFORMANCE)."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--no-download",
            action="store_true",
            help="Use local file only; do not connect to SFTP. Requires --local-file or existing cached file.",
        )
        parser.add_argument(
            "--local-file",
            type=str,
            default=None,
            help="Path to local CSV file. Overrides default path. With --no-download, uses this file without SFTP.",
        )
        parser.add_argument(
            "--feed-type",
            type=str,
            default="all",
            choices=["wheel", "tire", "accessories", "all"],
            help="Feed to sync: wheel, tire, accessories, or all (default).",
        )

    def handle(self, *args, **options):
        self.stdout.write("Starting WheelPros fetch and sync...")
        execution = audit_scheduled_tasks.start_scheduled_task_execution("sync_wheelpros")
        feed_type = options.get("feed_type", "all")
        feeds = ["wheel", "tire", "accessories"] if feed_type == "all" else [feed_type]
        try:
            local_file = options.get("local_file")
            for ft in feeds:
                self.stdout.write("Step 1: Fetching and saving WheelPros {} feed...".format(ft))
                wheelpros.fetch_and_save_wheelpros(
                    local_file_path=local_file if len(feeds) == 1 else None,
                    download=not options.get("no_download", False),
                    local_only=options.get("no_download", False),
                    feed_type=ft,
                )
                self.stdout.write(self.style.SUCCESS("WheelPros {} feed fetched and saved.".format(ft)))

            self.stdout.write("Step 2: Syncing unmapped WheelPros brands into Brands flow...")
            wheelpros.sync_unmapped_wheelpros_brands_to_brands()
            self.stdout.write(self.style.SUCCESS("Unmapped WheelPros brands synced."))

            audit_scheduled_tasks.mark_scheduled_task_completed(
                execution,
                message="Successfully completed WheelPros fetch and sync.",
            )
            self.stdout.write(self.style.SUCCESS("Successfully completed WheelPros fetch and sync."))
        except Exception as e:
            audit_scheduled_tasks.mark_scheduled_task_failed(execution, error_message=str(e))
            self.stdout.write(self.style.ERROR("Error: {}".format(str(e))))
            raise
