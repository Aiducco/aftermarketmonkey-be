from django.core.management.base import BaseCommand

from src.audit import scheduled_tasks as audit_scheduled_tasks
from src.integrations.services import helmet_house


class Command(BaseCommand):
    help = "Full Helmet House raw sync: catalog + parts, then map unmapped brands to Brands."

    def add_arguments(self, parser):
        parser.add_argument(
            "--no-download",
            action="store_true",
            help="Reuse the cached local CSV if it is younger than the client's max age.",
        )

    def handle(self, *args, **options):
        audit_scheduled_tasks.cleanup_stale_started_executions("fetch_helmet_house_full_sync")
        self.stdout.write("Starting fetch_helmet_house_full_sync...")
        execution = audit_scheduled_tasks.start_scheduled_task_execution("fetch_helmet_house_full_sync")
        try:
            helmet_house.fetch_and_save_helmet_house_full_sync(
                force_download=not options["no_download"]
            )
            audit_scheduled_tasks.mark_scheduled_task_completed(
                execution, message="Successfully completed fetch_helmet_house_full_sync."
            )
            self.stdout.write(self.style.SUCCESS("Successfully completed fetch_helmet_house_full_sync."))
        except Exception as e:
            audit_scheduled_tasks.mark_scheduled_task_failed(execution, error_message=str(e))
            self.stdout.write(self.style.ERROR("Error: {}".format(str(e))))
            raise
