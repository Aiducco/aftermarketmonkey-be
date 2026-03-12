"""
Fetch Turn14 warehouse locations from GET /v1/locations and save to Turn14Location.
Uses primary company's Turn14 credentials.
"""
from django.core.management.base import BaseCommand

from src.integrations.services import turn_14


class Command(BaseCommand):
    help = "Fetch Turn14 locations from API and upsert into Turn14Location table."

    def handle(self, *args, **options):
        self.stdout.write("Fetching Turn14 locations...")
        try:
            turn_14.fetch_and_save_turn_14_locations()
        except Exception as e:
            self.stdout.write(self.style.ERROR("Error: {}".format(str(e))))
            raise
        self.stdout.write(self.style.SUCCESS("Done."))
