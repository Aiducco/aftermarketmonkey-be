from django.core.management.base import BaseCommand

from src.audit import scheduled_tasks as audit_scheduled_tasks
from src.integrations.services import dlg


class Command(BaseCommand):
    help = (
        "Download DLG dlg_inventory.csv (primary CompanyProvider), upsert DlgBrand / DlgParts, "
        "then sync unmapped DlgBrand into Brands (exact + fuzzy on uppercase names; TOYO never maps to "
        "TOYOTA). Upserts BrandDlgBrandMapping, BrandProviders, CompanyBrands for TICK_PERFORMANCE."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Do not fetch or write. Only log unmapped DlgBrand -> Brands resolution (like Meyer).",
        )

    def handle(self, *args, **options):
        dry_run = options.get("dry_run", False)
        if dry_run:
            self.stdout.write("Dry run: skipping SFTP fetch and all database writes.")
            self.stdout.write("Preview: unmapped DlgBrand -> Brands resolution...")
            try:
                dlg.sync_unmapped_dlg_brands_to_brands(dry_run=True)
            except Exception as e:
                self.stdout.write(self.style.ERROR("Error: {}".format(str(e))))
                raise
            self.stdout.write(self.style.SUCCESS("Dry run finished (no changes applied)."))
            return

        self.stdout.write("Starting DLG inventory fetch and brand sync...")
        execution = audit_scheduled_tasks.start_scheduled_task_execution("sync_dlg_brands")
        try:
            self.stdout.write("Step 1: Fetching DLG inventory from SFTP...")
            dlg.fetch_and_save_dlg_catalog()
            self.stdout.write(self.style.SUCCESS("DLG catalog saved."))

            self.stdout.write("Step 2: Syncing unmapped DLG brands into Brands flow...")
            dlg.sync_unmapped_dlg_brands_to_brands()
            self.stdout.write(self.style.SUCCESS("Unmapped DLG brands synced."))

            audit_scheduled_tasks.mark_scheduled_task_completed(
                execution,
                message="Successfully completed DLG catalog fetch and brand sync.",
            )
            self.stdout.write(self.style.SUCCESS("Successfully completed DLG catalog fetch and brand sync."))
        except Exception as e:
            audit_scheduled_tasks.mark_scheduled_task_failed(execution, error_message=str(e))
            self.stdout.write(self.style.ERROR("Error: {}".format(str(e))))
            raise
