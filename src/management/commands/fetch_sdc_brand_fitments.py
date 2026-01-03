from django.core.management.base import BaseCommand

from src.integrations.services import sdc


class Command(BaseCommand):
    help = 'Fetch and save all SDC brand fitments'

    def handle(self, *args, **options):
        self.stdout.write('Starting SDC brand fitments fetch...')
        try:
            sdc.fetch_and_save_all_sdc_brand_fitments()
            self.stdout.write(self.style.SUCCESS('Successfully completed SDC brand fitments fetch.'))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Error: {str(e)}'))
            raise