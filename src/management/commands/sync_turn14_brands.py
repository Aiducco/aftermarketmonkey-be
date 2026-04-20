"""
Fetch and sync Turn 14 brands from the API, then sync unmapped Turn14Brand
records into the Brands flow (BrandTurn14BrandMapping, BrandProviders, CompanyBrands for TICK_PERFORMANCE).
"""
from django.core.management.base import BaseCommand

from src.audit import scheduled_tasks as audit_scheduled_tasks
from src.integrations.services import master_parts, turn_14


class Command(BaseCommand):
    help = (
        'Fetch and save Turn 14 brands from API, sync unmapped Turn14 brands into Brands '
        '(mappings, brand_providers, company_brands for TICK_PERFORMANCE), then fetch and save '
        'items, brand data, pricing, and inventory for those newly synced brands.'
    )

    def handle(self, *args, **options):
        self.stdout.write('Starting Turn 14 brands fetch and sync...')
        execution = audit_scheduled_tasks.start_scheduled_task_execution('sync_turn14_brands')
        try:
            self.stdout.write('Step 1: Fetching and saving Turn 14 brands from API...')
            turn_14.fetch_and_save_turn_14_brands()
            self.stdout.write(self.style.SUCCESS('Turn 14 brands fetched and saved.'))

            self.stdout.write('Step 2: Syncing unmapped Turn14 brands into Brands flow...')
            synced_turn14_brands = turn_14.sync_unmapped_turn_14_brands_to_brands()
            self.stdout.write(self.style.SUCCESS('Unmapped Turn14 brands synced.'))

            if synced_turn14_brands:
                n = len(synced_turn14_brands)
                self.stdout.write('Step 3: Fetching data for {} newly synced Turn14 brand(s)...'.format(n))

                self.stdout.write('  - Items...')
                turn_14.fetch_and_save_turn_14_items_for_turn14_brands(synced_turn14_brands)
                self.stdout.write(self.style.SUCCESS('  Items done.'))

                self.stdout.write('  - Brand data (media)...')
                turn_14.fetch_and_save_turn_14_brand_data_for_turn14_brands(synced_turn14_brands)
                self.stdout.write(self.style.SUCCESS('  Brand data done.'))

                self.stdout.write('  - Brand pricing...')
                turn_14.fetch_and_save_turn_14_brand_pricing_for_turn14_brands(synced_turn14_brands)
                self.stdout.write(self.style.SUCCESS('  Brand pricing done.'))

                self.stdout.write('  - Brand inventory...')
                turn_14.fetch_and_save_turn_14_brand_inventory_for_turn14_brands(synced_turn14_brands)
                self.stdout.write(self.style.SUCCESS('  Brand inventory done.'))

                self.stdout.write('Step 4: Propagating Turn14 catalog into master parts, provider parts, inventory, and pricing...')
                master_parts.sync_derived_from_turn14(reindex_meilisearch=True)
                self.stdout.write(self.style.SUCCESS('  Derived master layer sync done.'))
            else:
                self.stdout.write('Step 3: No newly synced brands; skipping items, brand data, pricing, and inventory fetch.')

            audit_scheduled_tasks.mark_scheduled_task_completed(
                execution,
                message='Successfully completed Turn 14 brands fetch and sync.',
            )
            self.stdout.write(self.style.SUCCESS('Successfully completed Turn 14 brands fetch and sync.'))
        except Exception as e:
            audit_scheduled_tasks.mark_scheduled_task_failed(execution, error_message=str(e))
            self.stdout.write(self.style.ERROR('Error: {}'.format(str(e))))
            raise
