from django.core.management.base import BaseCommand

from src.integrations.services import master_parts


class Command(BaseCommand):
    help = "Sync ProviderPartInventory from Turn14, Keystone, Rough Country, and WheelPros"

    def handle(self, *args, **options):
        self.stdout.write("Syncing provider inventory...")
        try:
            master_parts.sync_provider_inventory_from_turn14()
            master_parts.sync_provider_inventory_from_keystone()
            master_parts.sync_provider_inventory_from_rough_country()
            master_parts.sync_provider_inventory_from_wheelpros()
            self.stdout.write(self.style.SUCCESS("Done."))
        except Exception as e:
            self.stdout.write(self.style.ERROR("Error: {}".format(str(e))))
            raise
