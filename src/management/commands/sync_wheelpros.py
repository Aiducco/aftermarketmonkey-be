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

    def handle(self, *args, **options):
        self.stdout.write("Starting WheelPros fetch and sync...")
        execution = audit_scheduled_tasks.start_scheduled_task_execution("sync_wheelpros")
        try:
            self.stdout.write("Step 1: Fetching and saving WheelPros brands and parts from CSV...")
            wheelpros.fetch_and_save_wheelpros(
                local_file_path=options.get("local_file"),
                download=not options.get("no_download", False),
                local_only=options.get("no_download", False),
            )
            self.stdout.write(self.style.SUCCESS("WheelPros brands and parts fetched and saved."))

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
