from django.core.management.base import BaseCommand

from src.integrations.ecommerce.bigcommerce.services import bigcommerce


class Command(BaseCommand):
    help = 'Fetch and sync all ecommerce parts to BigCommerce destination'

    def handle(self, *args, **options):
        self.stdout.write('Starting BigCommerce parts sync...')
        try:
            bigcommerce.fetch_and_sync_all_ecommerce_parts_to_bigcommerce_destination()
            self.stdout.write(self.style.SUCCESS('Successfully completed BigCommerce parts sync.'))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Error: {str(e)}'))
            raise

