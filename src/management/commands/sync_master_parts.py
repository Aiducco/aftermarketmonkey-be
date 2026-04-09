from django.core.management.base import BaseCommand

from src.audit import scheduled_tasks as audit_scheduled_tasks
from src.integrations.services import master_parts
from src.search.meilisearch_client import add_documents_in_batches, delete_all_documents, is_configured, setup_index


class Command(BaseCommand):
    help = "Sync MasterPart, ProviderPart, ProviderPartInventory, ProviderPartCompanyPricing from Turn14, Keystone, Meyer, A-Tech, Rough Country, DLG, and WheelPros"

    def add_arguments(self, parser):
        parser.add_argument(
            "--reindex-meilisearch",
            action="store_true",
            help="Reindex all parts into Meilisearch after sync completes",
        )

    def handle(self, *args, **options):
        self.stdout.write("Starting full master parts sync...")
        execution = audit_scheduled_tasks.start_scheduled_task_execution("sync_master_parts")
        try:
            master_parts.sync_all_master_parts()
            self.stdout.write(self.style.SUCCESS("Successfully completed master parts sync."))

            if options.get("reindex_meilisearch") and is_configured():
                self.stdout.write("Reindexing Meilisearch...")
                setup_index()
                delete_all_documents()
                from src import models as src_models

                queryset = src_models.MasterPart.objects.select_related("brand").order_by("id")
                ok, fail = add_documents_in_batches(queryset)
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
