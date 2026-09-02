from django.core.management.base import BaseCommand

from src.audit import scheduled_tasks as audit_scheduled_tasks
from src.integrations.services import motorstate


class Command(BaseCommand):
    help = (
        "[Superseded by fetch_motorstate_feed -- the FTP feed is the catalog/pricing "
        "source of record. This API path is kept for one-off backfills; running it "
        "overwrites feed-sourced stock and description on MotorStateProduct.] "
        "Poll Motor State ProductAvailabilityChange for stock/status changes since "
        "the last-seen high-water mark and upsert them into MotorStateAvailability. "
        "The Turn14 inventory-updates analog."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--company-provider-id",
            type=int,
            default=None,
            help="Specific CompanyProviders id; defaults to the primary active Motor State connection.",
        )

    def handle(self, *args, **options):
        audit_scheduled_tasks.cleanup_stale_started_executions("fetch_motorstate_inventory_updates")
        self.stdout.write("Starting Motor State inventory updates fetch...")
        execution = audit_scheduled_tasks.start_scheduled_task_execution("fetch_motorstate_inventory_updates")
        try:
            motorstate.fetch_and_save_motorstate_availability_updates(
                company_provider_id=options["company_provider_id"],
            )
            audit_scheduled_tasks.mark_scheduled_task_completed(
                execution, message="Successfully completed Motor State inventory updates fetch."
            )
            self.stdout.write(self.style.SUCCESS("Successfully completed Motor State inventory updates fetch."))
        except Exception as e:
            audit_scheduled_tasks.mark_scheduled_task_failed(execution, error_message=str(e))
            self.stdout.write(self.style.ERROR("Error: {}".format(str(e))))
            raise
