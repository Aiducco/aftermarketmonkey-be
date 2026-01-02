from django.core.management.base import BaseCommand

from src.integrations.services import turn_14


class Command(BaseCommand):
    help = 'Fetch and save Turn 14 inventory updates'

    def handle(self, *args, **options):
        self.stdout.write('Starting Turn 14 inventory updates fetch...')
        try:
            turn_14.fetch_and_save_turn_14_inventory_updates()
            self.stdout.write(self.style.SUCCESS('Successfully completed Turn 14 inventory updates fetch.'))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Error: {str(e)}'))
            raise


