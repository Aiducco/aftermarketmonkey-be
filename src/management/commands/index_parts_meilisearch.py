from django.core.management.base import BaseCommand

from src import models as src_models
from src.search.meilisearch_client import add_documents_in_batches, delete_all_documents, is_configured, setup_index


class Command(BaseCommand):
    help = "Bulk index all MasterPart records into Meilisearch. Run setup_meilisearch first."

    def add_arguments(self, parser):
        parser.add_argument(
            "--batch-size",
            type=int,
            default=10000,
            help="Number of documents per batch (default: 10000)",
        )
        parser.add_argument(
            "--setup",
            action="store_true",
            help="Run index setup before indexing",
        )

    def handle(self, *args, **options):
        if not is_configured():
            self.stdout.write(
                self.style.ERROR(
                    "Meilisearch not configured. Set MEILISEARCH_HOST (and MEILISEARCH_MASTER_KEY if required)."
                )
            )
            return

        if options["setup"]:
            self.stdout.write("Running index setup...")
            setup_index()

        self.stdout.write("Deleting all documents from Meilisearch index...")
        delete_all_documents()
        self.stdout.write("Indexing MasterPart records into Meilisearch...")
        queryset = src_models.MasterPart.objects.select_related("brand").order_by("id")
        total = queryset.count()
        self.stdout.write("Total parts to index: {}".format(total))

        ok, fail = add_documents_in_batches(queryset, batch_size=options["batch_size"])

        self.stdout.write(
            self.style.SUCCESS("Indexed {} parts. Failed: {}.".format(ok, fail))
        )
