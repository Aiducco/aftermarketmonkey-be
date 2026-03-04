from django.core.management.base import BaseCommand

from src.search.meilisearch_client import setup_index


class Command(BaseCommand):
    help = "Create and configure Meilisearch 'parts' index (searchable/filterable attributes)"

    def handle(self, *args, **options):
        self.stdout.write("Setting up Meilisearch parts index...")
        if setup_index():
            self.stdout.write(self.style.SUCCESS("Meilisearch index configured successfully."))
        else:
            self.stdout.write(
                self.style.WARNING(
                    "Meilisearch setup skipped or failed. "
                    "Ensure MEILISEARCH_HOST is set and Meilisearch is running."
                )
            )
