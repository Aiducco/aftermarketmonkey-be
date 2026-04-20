from django.core.management.base import BaseCommand

from src.integrations.services import keystone, master_parts


class Command(BaseCommand):
    help = "Fetch and save Keystone parts for brands with BrandKeystoneBrandMapping"

    def handle(self, *args, **options):
        self.stdout.write("Starting Keystone parts fetch...")
        try:
            keystone.fetch_and_save_all_keystone_brand_parts()
            self.stdout.write(
                "Propagating Keystone catalog into master parts, provider parts, inventory, and pricing..."
            )
            master_parts.sync_derived_from_keystone(reindex_meilisearch=True)
            self.stdout.write(self.style.SUCCESS("Successfully completed Keystone parts fetch and derived sync."))
        except Exception as e:
            self.stdout.write(self.style.ERROR("Error: {}".format(str(e))))
            raise
