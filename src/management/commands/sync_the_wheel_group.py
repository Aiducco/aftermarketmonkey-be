"""
Sync The Wheel Group's US Wheel Data Mastersheet, plus the catalog connection's real relay
inventory CSV when one has landed.

Downloads the newest mastersheet workbook (product definitions, images, MSRP/MAP -- always the
public Dropbox share's source of truth), upserts brands and parts from the ``US Data Mastersheet``
worksheet, maps brands into Brands, reads the real per-warehouse stock CSV over SFTP if the
catalog connection has one, then propagates everything into the master parts layer. Per-company
pricing (TheWheelGroupCompanyPricing -> ProviderPartCompanyPricing) is handled per company by the
IntegrationPricingSyncJob queue, which also prefers that company's own relay CSV for real dealer
cost when one exists.
"""
from django.core.management.base import BaseCommand

from src.audit import scheduled_tasks as audit_scheduled_tasks
from src.integrations.services import master_parts, the_wheel_group


class Command(BaseCommand):
    help = (
        "Sync The Wheel Group mastersheet: download the newest US Wheel Data Mastersheet.xlsx, "
        "upsert TheWheelGroupBrand and TheWheelGroupPart (catalog + MSRP/MAP); sync unmapped TWG "
        "brands into Brands; read the catalog connection's relay inventory CSV if one exists; "
        "then propagate into master parts and provider parts."
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
                "Step 3: Reading the catalog connection's relay inventory CSV (real per-warehouse "
                "stock), if one has landed..."
            )
            inventory_rows = the_wheel_group.sync_the_wheel_group_relay_inventory()
            self.stdout.write(self.style.SUCCESS(
                "TWG relay inventory synced ({} rows).".format(inventory_rows)
                if inventory_rows
                else "No TWG relay inventory CSV yet (still on the public share) -- skipped."
            ))

            self.stdout.write(
                "Step 4: Propagating TWG catalog into master parts and provider parts..."
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
