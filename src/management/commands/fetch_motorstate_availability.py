from django.core.management.base import BaseCommand

from src.audit import scheduled_tasks as audit_scheduled_tasks
from src.integrations.services import motorstate


class Command(BaseCommand):
    help = (
        "[Superseded by fetch_motorstate_feed -- the FTP feed is the catalog/pricing "
        "source of record. This API path is kept for one-off backfills; running it "
        "overwrites feed-sourced stock and description on MotorStateProduct.] "
        "Rebuild the Motor State part-number spine: one epoch-dated "
        "ProductAvailabilityChange pass per brand into MotorStateAvailability. "
        "Run fetch_motorstate_brands first."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--company-provider-id",
            type=int,
            default=None,
            help="Specific CompanyProviders id; defaults to the primary active Motor State connection.",
        )

    def handle(self, *args, **options):
        audit_scheduled_tasks.cleanup_stale_started_executions("fetch_motorstate_availability")
        self.stdout.write("Starting Motor State availability (spine) fetch...")
        execution = audit_scheduled_tasks.start_scheduled_task_execution("fetch_motorstate_availability")
        try:
            motorstate.fetch_and_save_motorstate_availability_full(
                company_provider_id=options["company_provider_id"],
            )
            audit_scheduled_tasks.mark_scheduled_task_completed(
                execution, message="Successfully completed Motor State availability spine fetch."
            )
            self.stdout.write(self.style.SUCCESS("Successfully completed Motor State availability fetch."))
        except Exception as e:
            audit_scheduled_tasks.mark_scheduled_task_failed(execution, error_message=str(e))
            self.stdout.write(self.style.ERROR("Error: {}".format(str(e))))
            raise
