from django.core.management.base import BaseCommand

from src.audit import scheduled_tasks as audit_scheduled_tasks
from src.integrations.services import meyer


class Command(BaseCommand):
    help = (
        "Fetch Meyer brands from pricing CSV, then sync unmapped MeyerBrand into Brands "
        "(BrandMeyerBrandMapping, BrandProviders, CompanyBrands for TICK_PERFORMANCE)."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Do not fetch or write. Only log what the sync step would do (resolve + new brands).",
        )

    def handle(self, *args, **options):
        dry_run = options.get("dry_run", False)
        if dry_run:
            self.stdout.write("Dry run: skipping SFTP fetch and all database writes.")
            self.stdout.write("Preview: unmapped MeyerBrand -> Brands resolution...")
            try:
                meyer.sync_unmapped_meyer_brands_to_brands(dry_run=True)
            except Exception as e:
                self.stdout.write(self.style.ERROR("Error: {}".format(str(e))))
                raise
            self.stdout.write(self.style.SUCCESS("Dry run finished (no changes applied)."))
            return

        self.stdout.write("Starting Meyer brands fetch and sync...")
        execution = audit_scheduled_tasks.start_scheduled_task_execution("sync_meyer_brands")
        try:
            self.stdout.write("Step 1: Fetching Meyer brands from pricing...")
            meyer.fetch_and_save_meyer_brands()
            self.stdout.write(self.style.SUCCESS("Meyer brands fetched and saved."))

            self.stdout.write("Step 2: Syncing unmapped Meyer brands into Brands flow...")
            meyer.sync_unmapped_meyer_brands_to_brands()
            self.stdout.write(self.style.SUCCESS("Unmapped Meyer brands synced."))

            audit_scheduled_tasks.mark_scheduled_task_completed(
                execution,
                message="Successfully completed Meyer brands fetch and sync.",
            )
            self.stdout.write(self.style.SUCCESS("Successfully completed Meyer brands fetch and sync."))
        except Exception as e:
            audit_scheduled_tasks.mark_scheduled_task_failed(execution, error_message=str(e))
            self.stdout.write(self.style.ERROR("Error: {}".format(str(e))))
            raise
