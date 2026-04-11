from django.core.management.base import BaseCommand

from src.audit import scheduled_tasks as audit_scheduled_tasks
from src.integrations.services import atech


class Command(BaseCommand):
    help = (
        "Sync unmapped AtechBrand rows into catalog Brands (exact + fuzzy name match, like Meyer). "
        "Upserts BrandAtechBrandMapping, BrandProviders, CompanyBrands for TICK_PERFORMANCE. "
        "Does not download the A-Tech feed; run fetch_atech_feed to refresh AtechParts first if needed."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="No database writes. Log how each unmapped AtechBrand would resolve to Brands.",
        )

    def handle(self, *args, **options):
        dry_run = options.get("dry_run", False)
        if dry_run:
            self.stdout.write("Dry run: no database writes.")
            self.stdout.write("Preview: unmapped AtechBrand -> Brands resolution...")
            try:
                atech.sync_unmapped_atech_brands_to_brands(dry_run=True)
            except Exception as e:
                self.stdout.write(self.style.ERROR("Error: {}".format(str(e))))
                raise
            self.stdout.write(self.style.SUCCESS("Dry run finished (no changes applied)."))
            return

        self.stdout.write("Starting A-Tech brand sync (unmapped -> Brands)...")
        execution = audit_scheduled_tasks.start_scheduled_task_execution("sync_atech_brands")
        try:
            atech.sync_unmapped_atech_brands_to_brands(dry_run=False)
            audit_scheduled_tasks.mark_scheduled_task_completed(
                execution,
                message="Successfully completed A-Tech unmapped brand sync.",
            )
            self.stdout.write(self.style.SUCCESS("Successfully completed A-Tech brand sync."))
        except Exception as e:
            audit_scheduled_tasks.mark_scheduled_task_failed(execution, error_message=str(e))
            self.stdout.write(self.style.ERROR("Error: {}".format(str(e))))
            raise
