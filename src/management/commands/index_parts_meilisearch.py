from django.core.management.base import BaseCommand

from src import models as src_models
from src.search.meilisearch_client import (
    REINDEX_DEFAULT_BATCH_SIZE,
    REINDEX_DEFAULT_UPLOAD_WORKERS,
    is_configured,
    reindex_all_master_parts,
    reindex_all_master_parts_zero_downtime,
    reindex_master_parts_with_fitment,
    setup_index,
)


class Command(BaseCommand):
    help = (
        "Bulk index all MasterPart records into Meilisearch. "
        "By default uses zero-downtime swap_indexes reindex so users never see an empty index. "
        "Pass --no-zero-downtime to use the legacy delete-then-reindex approach."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--batch-size",
            type=int,
            default=None,
            help="Documents per batch (default: MEILISEARCH_REINDEX_BATCH_SIZE, typically 20000).",
        )
        parser.add_argument(
            "--upload-workers",
            type=int,
            default=None,
            help="Parallel upload threads (default: MEILISEARCH_REINDEX_UPLOAD_WORKERS, typically 8).",
        )
        parser.add_argument(
            "--setup",
            action="store_true",
            help="Run index setup before indexing (only relevant for --no-zero-downtime).",
        )
        parser.add_argument(
            "--no-zero-downtime",
            action="store_true",
            help="Use legacy delete-then-reindex instead of zero-downtime swap_indexes.",
        )
        parser.add_argument(
            "--fitment-only",
            action="store_true",
            help=(
                "Only re-index MasterParts that have fitment data (a small fraction of the "
                "catalog), instead of the full ~2.9M-row reindex. Writes directly into the live "
                "index (no delete/staging). Use this to get fresh fitment_keys live without "
                "waiting for the next full nightly reindex."
            ),
        )

    def handle(self, *args, **options):
        if not is_configured():
            self.stdout.write(
                self.style.ERROR(
                    "Meilisearch not configured. Set MEILISEARCH_HOST (and MEILISEARCH_MASTER_KEY if required)."
                )
            )
            return

        batch_size = options["batch_size"] or REINDEX_DEFAULT_BATCH_SIZE
        upload_workers = options.get("upload_workers") or REINDEX_DEFAULT_UPLOAD_WORKERS
        no_zero_downtime = options.get("no_zero_downtime", False)

        if options.get("fitment_only"):
            self.stdout.write("Fitment-only reindex (MasterParts with fitment data, live index)...")
            ok, fail = reindex_master_parts_with_fitment(
                batch_size=batch_size,
                max_upload_workers=upload_workers,
            )
            self.stdout.write(
                self.style.SUCCESS("Indexed {} parts. Failed: {}.".format(ok, fail))
            )
            return

        total = src_models.MasterPart.objects.count()

        if no_zero_downtime:
            self.stdout.write(
                "Legacy reindex (delete + index, zero-downtime disabled). Total parts: {}".format(total)
            )
            if options["setup"]:
                self.stdout.write("Running index setup...")
                setup_index()
            ok, fail = reindex_all_master_parts(
                batch_size=batch_size,
                max_upload_workers=upload_workers,
            )
        else:
            self.stdout.write(
                "Zero-downtime reindex (swap_indexes). Total parts: {}".format(total)
            )
            ok, fail = reindex_all_master_parts_zero_downtime(
                batch_size=batch_size,
                max_upload_workers=upload_workers,
            )

        self.stdout.write(
            self.style.SUCCESS("Indexed {} parts. Failed: {}.".format(ok, fail))
        )
