"""
Fetch and sync Keystone brands from the inventory CSV, then sync unmapped KeystoneBrand
records into the Brands flow (BrandKeystoneBrandMapping, BrandProviders, CompanyBrands for TICK_PERFORMANCE).
"""
from django.core.management.base import BaseCommand

from src.audit import scheduled_tasks as audit_scheduled_tasks
from src.integrations.services import keystone


class Command(BaseCommand):
    help = (
        "Fetch and save Keystone brands from inventory, sync unmapped Keystone brands into Brands "
        "(mappings, brand_providers, company_brands for TICK_PERFORMANCE)."
    )

    def handle(self, *args, **options):
        self.stdout.write("Starting Keystone brands fetch and sync...")
        execution = audit_scheduled_tasks.start_scheduled_task_execution("sync_keystone_brands")
        try:
            self.stdout.write("Step 1: Fetching and saving Keystone brands from inventory...")
            keystone.fetch_and_save_keystone_brands()
            self.stdout.write(self.style.SUCCESS("Keystone brands fetched and saved."))

            self.stdout.write("Step 2: Syncing unmapped Keystone brands into Brands flow...")
            keystone.sync_unmapped_keystone_brands_to_brands()
            self.stdout.write(self.style.SUCCESS("Unmapped Keystone brands synced."))

            audit_scheduled_tasks.mark_scheduled_task_completed(
                execution,
                message="Successfully completed Keystone brands fetch and sync.",
            )
            self.stdout.write(
                self.style.SUCCESS("Successfully completed Keystone brands fetch and sync.")
            )
        except Exception as e:
            audit_scheduled_tasks.mark_scheduled_task_failed(
                execution, error_message=str(e)
            )
            self.stdout.write(self.style.ERROR("Error: {}".format(str(e))))
            raise
