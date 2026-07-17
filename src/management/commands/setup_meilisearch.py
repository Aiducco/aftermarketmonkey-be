from django.core.management.base import BaseCommand

from src.search.meilisearch_client import setup_index, setup_vehicles_index


class Command(BaseCommand):
    help = "Create and configure Meilisearch 'parts' and 'vehicles' indexes (searchable/filterable attributes)"

    def handle(self, *args, **options):
        self.stdout.write("Setting up Meilisearch parts index...")
        if setup_index():
            self.stdout.write(self.style.SUCCESS("Meilisearch parts index configured successfully."))
        else:
            self.stdout.write(
                self.style.WARNING(
                    "Meilisearch parts setup skipped or failed. "
                    "Ensure MEILISEARCH_HOST is set and Meilisearch is running."
                )
            )

        self.stdout.write("Setting up Meilisearch vehicles index...")
        if setup_vehicles_index():
            self.stdout.write(self.style.SUCCESS("Meilisearch vehicles index configured successfully."))
        else:
            self.stdout.write(self.style.WARNING("Meilisearch vehicles setup skipped or failed."))
