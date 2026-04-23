from django.core.management.base import BaseCommand

from src.audit import scheduled_tasks as audit_scheduled_tasks
from src.integrations.services import master_parts
from src.search.meilisearch_client import is_configured, reindex_all_master_parts


class Command(BaseCommand):
    help = "Sync MasterPart, ProviderPart, ProviderPartInventory, ProviderPartCompanyPricing from Turn14, Keystone, Meyer, A-Tech, Rough Country, DLG, and WheelPros"

    def add_arguments(self, parser):
        parser.add_argument(
            "--reindex-meilisearch",
            action="store_true",
            help="Reindex all parts into Meilisearch after sync completes",
        )
        parser.add_argument(
            "--reindex-batch-size",
            type=int,
            default=None,
            help="Documents per batch for Meilisearch (default: MEILISEARCH_REINDEX_BATCH_SIZE).",
        )
        parser.add_argument(
            "--reindex-upload-workers",
            type=int,
            default=None,
            help="Parallel Meilisearch upload workers (default: MEILISEARCH_REINDEX_UPLOAD_WORKERS).",
        )

    def handle(self, *args, **options):
        self.stdout.write("Starting full master parts sync...")
        execution = audit_scheduled_tasks.start_scheduled_task_execution("sync_master_parts")
        try:
            master_parts.sync_all_master_parts()
            self.stdout.write(self.style.SUCCESS("Successfully completed master parts sync."))

            if options.get("reindex_meilisearch") and is_configured():
                self.stdout.write("Reindexing Meilisearch...")
                kw = {}
                if options.get("reindex_batch_size") is not None:
                    kw["batch_size"] = options["reindex_batch_size"]
                if options.get("reindex_upload_workers") is not None:
                    kw["max_upload_workers"] = options["reindex_upload_workers"]
                ok, fail = reindex_all_master_parts(**kw)
                self.stdout.write(
                    self.style.SUCCESS("Meilisearch: indexed {} parts, failed {}.".format(ok, fail))
                )

            audit_scheduled_tasks.mark_scheduled_task_completed(
                execution,
                message="Successfully completed master parts sync.",
            )
        except Exception as e:
            audit_scheduled_tasks.mark_scheduled_task_failed(execution, error_message=str(e))
            self.stdout.write(self.style.ERROR("Error: {}".format(str(e))))
            raise
