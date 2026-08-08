"""
Sync The Wheel Group's US Wheel Data Mastersheet.
Downloads the newest workbook, upserts brands and parts from the ``US Data Mastersheet``
worksheet, maps brands into Brands, then propagates into the master parts layer. Per-company
pricing (TheWheelGroupCompanyPricing -> ProviderPartCompanyPricing) is handled per company by the
IntegrationPricingSyncJob queue.
"""
from django.core.management.base import BaseCommand

from src.audit import scheduled_tasks as audit_scheduled_tasks
from src.integrations.services import master_parts, the_wheel_group


class Command(BaseCommand):
    help = (
        "Sync The Wheel Group mastersheet: download the newest US Wheel Data Mastersheet.xlsx, "
        "upsert TheWheelGroupBrand and TheWheelGroupPart (catalog + MSRP/MAP); sync unmapped TWG "
        "brands into Brands; then propagate into master parts and provider parts."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--local-file",
            dest="local_file",
            default=None,
            help=(
                "Parse this workbook (or zip containing it) instead of downloading -- for testing "
                "against a saved drop."
            ),
        )
        parser.add_argument(
            "--public-share",
            action="store_true",
            help=(
                "Force TWG's public share even when the connection has relay credentials (same "
                "effect as THE_WHEEL_GROUP_FORCE_PUBLIC_SHARE in settings, which defaults on)."
            ),
        )

    def handle(self, *args, **options):
        audit_scheduled_tasks.cleanup_stale_started_executions("sync_the_wheel_group")
        self.stdout.write("Starting The Wheel Group feed sync...")
        execution = audit_scheduled_tasks.start_scheduled_task_execution("sync_the_wheel_group")
        try:
            self.stdout.write("Step 1: Reading the TWG mastersheet (brands, parts, MSRP/MAP)...")
            the_wheel_group.fetch_and_save_the_wheel_group(
                local_file_path=options.get("local_file"),
                force_public_share=True if options.get("public_share") else None,
            )
            self.stdout.write(self.style.SUCCESS("TWG mastersheet synced."))

            self.stdout.write("Step 2: Syncing unmapped TWG brands into Brands flow...")
            the_wheel_group.sync_unmapped_the_wheel_group_brands_to_brands()
            self.stdout.write(self.style.SUCCESS("Unmapped TWG brands synced."))

            self.stdout.write(
                "Step 3: Propagating TWG catalog into master parts and provider parts..."
            )
            master_parts.sync_derived_from_the_wheel_group(
                reindex_meilisearch=False, skip_pricing=True
            )
            self.stdout.write(self.style.SUCCESS("Derived master layer sync done."))

            audit_scheduled_tasks.mark_scheduled_task_completed(
                execution,
                message="Successfully completed The Wheel Group feed sync and derived master layer sync.",
            )
            self.stdout.write(self.style.SUCCESS("Successfully completed The Wheel Group feed sync."))
        except Exception as e:
            audit_scheduled_tasks.mark_scheduled_task_failed(execution, error_message=str(e))
            self.stdout.write(self.style.ERROR("Error: {}".format(str(e))))
            raise
