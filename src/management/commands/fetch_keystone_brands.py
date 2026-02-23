from django.core.management.base import BaseCommand

from src.integrations.services import keystone


class Command(BaseCommand):
    help = "Fetch and save Keystone brands from inventory CSV"

    def handle(self, *args, **options):
        self.stdout.write("Starting Keystone brands fetch...")
        try:
            keystone.fetch_and_save_keystone_brands()
            self.stdout.write(self.style.SUCCESS("Successfully completed Keystone brands fetch."))
        except Exception as e:
            self.stdout.write(self.style.ERROR("Error: {}".format(str(e))))
            raise
