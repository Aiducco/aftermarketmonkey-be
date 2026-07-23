"""
Fetch Wheel Pros warehouses from GET /warehouses/v1 and save to WheelProsWarehouse.
Uses an active WheelPros CompanyProviders row with order credentials configured.
"""
from django.core.management.base import BaseCommand

from src.integrations.services import wheelpros


class Command(BaseCommand):
    help = "Fetch Wheel Pros warehouses from API and upsert into WheelProsWarehouse table."

    def handle(self, *args, **options):
        self.stdout.write("Fetching Wheel Pros warehouses...")
        try:
            wheelpros.fetch_and_save_wheelpros_warehouses()
        except Exception as e:
            self.stdout.write(self.style.ERROR("Error: {}".format(str(e))))
            raise
        self.stdout.write(self.style.SUCCESS("Done."))
