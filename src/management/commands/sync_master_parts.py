from django.core.management.base import BaseCommand

from src.integrations.services import master_parts


class Command(BaseCommand):
    help = "Sync MasterPart, ProviderPart, ProviderPartInventory, ProviderPartCompanyPricing from Turn14 and Keystone"

    def handle(self, *args, **options):
        self.stdout.write("Starting full master parts sync...")
        try:
            master_parts.sync_all_master_parts()
            self.stdout.write(self.style.SUCCESS("Successfully completed master parts sync."))
        except Exception as e:
            self.stdout.write(self.style.ERROR("Error: {}".format(str(e))))
            raise
