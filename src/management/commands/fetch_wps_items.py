from django.core.management.base import BaseCommand

from src.audit import scheduled_tasks as audit_scheduled_tasks
from src.integrations.services import wps


class Command(BaseCommand):
    help = "Fetch the full WPS item catalog with inventory, images and taxonomy (GET /items?include=...)."

    def add_arguments(self, parser):
        parser.add_argument(
            "--company-provider-id",
            type=int,
            default=None,
            help="Specific CompanyProviders id; defaults to the primary active WPS connection.",
        )

    def handle(self, *args, **options):
        audit_scheduled_tasks.cleanup_stale_started_executions("fetch_wps_items")
        self.stdout.write("Starting fetch_wps_items...")
        execution = audit_scheduled_tasks.start_scheduled_task_execution("fetch_wps_items")
        try:
            wps.fetch_and_save_wps_items(company_provider_id=options["company_provider_id"])
            audit_scheduled_tasks.mark_scheduled_task_completed(
                execution, message="Successfully completed fetch_wps_items."
            )
            self.stdout.write(self.style.SUCCESS("Successfully completed fetch_wps_items."))
        except Exception as e:
            audit_scheduled_tasks.mark_scheduled_task_failed(execution, error_message=str(e))
            self.stdout.write(self.style.ERROR("Error: {}".format(str(e))))
            raise
