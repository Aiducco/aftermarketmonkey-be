from django.core.management.base import BaseCommand

from src.integrations.services import master_parts


class Command(BaseCommand):
    help = "Sync ProviderPartCompanyPricing from Turn14, Keystone, Rough Country, and WheelPros"

    def handle(self, *args, **options):
        self.stdout.write("Syncing provider pricing...")
        try:
            master_parts.sync_provider_pricing_from_turn14()
            master_parts.sync_provider_pricing_from_keystone()
            master_parts.sync_provider_pricing_from_rough_country()
            master_parts.sync_provider_pricing_from_wheelpros()
            self.stdout.write(self.style.SUCCESS("Done."))
        except Exception as e:
            self.stdout.write(self.style.ERROR("Error: {}".format(str(e))))
            raise
