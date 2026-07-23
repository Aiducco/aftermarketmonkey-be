"""
Fetch Meyer warehouse locations from GET /Warehouses and save to MeyerLocation.
Uses primary company's Meyer order credentials.
"""
from django.core.management.base import BaseCommand

from src.integrations.services import meyer


class Command(BaseCommand):
    help = "Fetch Meyer warehouses from API and upsert into MeyerLocation table."

    def handle(self, *args, **options):
        self.stdout.write("Fetching Meyer locations...")
        try:
            meyer.fetch_and_save_meyer_locations()
        except Exception as e:
            self.stdout.write(self.style.ERROR("Error: {}".format(str(e))))
            raise
        self.stdout.write(self.style.SUCCESS("Done."))
