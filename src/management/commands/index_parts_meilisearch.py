from django.core.management.base import BaseCommand

from src import models as src_models
from src.search.meilisearch_client import (
    REINDEX_DEFAULT_BATCH_SIZE,
    REINDEX_DEFAULT_UPLOAD_WORKERS,
    is_configured,
    reindex_all_master_parts,
    setup_index,
)


class Command(BaseCommand):
    help = "Bulk index all MasterPart records into Meilisearch. Run setup_meilisearch first."

    def add_arguments(self, parser):
        parser.add_argument(
            "--batch-size",
            type=int,
            default=None,
            help="Documents per batch (default: MEILISEARCH_REINDEX_BATCH_SIZE or 2500). Smaller batches reduce connection resets.",
        )
        parser.add_argument(
            "--upload-workers",
            type=int,
            default=None,
            help="Parallel upload threads (default: MEILISEARCH_REINDEX_UPLOAD_WORKERS or 1). Use 1 for most stable long reindexes.",
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

        total = src_models.MasterPart.objects.count()
        self.stdout.write("Full reindex (delete + index). Total parts: {}".format(total))

        batch_size = options["batch_size"]
        if batch_size is None:
            batch_size = REINDEX_DEFAULT_BATCH_SIZE
        upload_workers = options.get("upload_workers")
        if upload_workers is None:
            upload_workers = REINDEX_DEFAULT_UPLOAD_WORKERS
        ok, fail = reindex_all_master_parts(
            batch_size=batch_size,
            max_upload_workers=upload_workers,
        )

        self.stdout.write(
            self.style.SUCCESS("Indexed {} parts. Failed: {}.".format(ok, fail))
        )
