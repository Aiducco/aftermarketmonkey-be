from django.core.management.base import BaseCommand

from src.audit import scheduled_tasks as audit_scheduled_tasks
from src.integrations.services import master_parts, thoroughbred


class Command(BaseCommand):
    help = (
        "Fetch Thoroughbred Diesel's fully public HawkSearch catalog (no credentials of any "
        "kind -- 712 pages at MaxPerPage=72), sync unmapped brands, then propagate into "
        "MasterPart / ProviderPart / ProviderPartInventory. Per-company pricing is handled "
        "separately by the IntegrationPricingSyncJob queue. Vehicle decode (for fitment) is a "
        "separate command: fetch_thoroughbred_vehicles."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--skip-catalog",
            action="store_true",
            help="Skip the HawkSearch catalog pull; only run brand sync + derived propagation.",
        )
        parser.add_argument(
            "--skip-brands",
            action="store_true",
            help="Skip unmapped-brand sync.",
        )
        parser.add_argument(
            "--skip-derived",
            action="store_true",
            help="Skip propagating into MasterPart / ProviderPart / ProviderPartInventory.",
        )

    def handle(self, *args, **options):
        audit_scheduled_tasks.cleanup_stale_started_executions("fetch_thoroughbred_feed")
        execution = audit_scheduled_tasks.start_scheduled_task_execution("fetch_thoroughbred_feed")
        try:
            if not options["skip_catalog"]:
                self.stdout.write("Fetching Thoroughbred Diesel HawkSearch catalog...")
                upserted = thoroughbred.fetch_and_save_thoroughbred_catalog()
                self.stdout.write("Catalog rows upserted: {}".format(upserted))

            if not options["skip_brands"]:
                self.stdout.write("Syncing unmapped Thoroughbred brands...")
                thoroughbred.sync_unmapped_thoroughbred_brands_to_brands()

            if not options["skip_derived"]:
                self.stdout.write("Propagating into MasterPart / ProviderPart / ProviderPartInventory...")
                master_parts.sync_derived_from_thoroughbred(skip_pricing=True)

            audit_scheduled_tasks.mark_scheduled_task_completed(
                execution, message="Successfully completed Thoroughbred Diesel feed ingest."
            )
            self.stdout.write(self.style.SUCCESS("Done."))
        except Exception as e:
            audit_scheduled_tasks.mark_scheduled_task_failed(execution, error_message=str(e))
            self.stdout.write(self.style.ERROR("Error: {}".format(str(e))))
            raise
