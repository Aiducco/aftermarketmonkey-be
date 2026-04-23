from django.core.management.base import BaseCommand

from src.integrations.services import keystone, master_parts


class Command(BaseCommand):
    help = "Fetch and save all Keystone brands and parts from inventory CSV (full sync)"

    def handle(self, *args, **options):
        self.stdout.write("Starting full Keystone sync...")
        try:
            keystone.fetch_and_save_all_keystone_brands_and_parts()
            self.stdout.write(
                "Propagating Keystone catalog into master parts, provider parts, inventory, and pricing..."
            )
            master_parts.sync_derived_from_keystone(reindex_meilisearch=False)
            self.stdout.write(self.style.SUCCESS("Successfully completed full Keystone sync and derived sync."))
        except Exception as e:
            self.stdout.write(self.style.ERROR("Error: {}".format(str(e))))
            raise
