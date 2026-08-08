from django.core.management.base import BaseCommand

from src.audit import scheduled_tasks as audit_scheduled_tasks
from src.integrations.services import helmet_house


class Command(BaseCommand):
    help = (
        "Download the Helmet House catalog CSV (masterv.csv) over FTP and upsert "
        "HelmetHouseBrand + HelmetHousePart (catalog + West/East stock, no pricing)."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--no-download",
            action="store_true",
            help="Reuse the cached local CSV if it is younger than the client's max age.",
        )
        parser.add_argument(
            "--local-file-path",
            type=str,
            default=None,
            help="Parse this local CSV instead of downloading (for debugging a specific drop).",
        )

    def handle(self, *args, **options):
        audit_scheduled_tasks.cleanup_stale_started_executions("fetch_helmet_house_catalog")
        self.stdout.write("Starting fetch_helmet_house_catalog...")
        execution = audit_scheduled_tasks.start_scheduled_task_execution("fetch_helmet_house_catalog")
        try:
            helmet_house.fetch_and_save_helmet_house_catalog(
                force_download=not options["no_download"],
                local_file_path=options["local_file_path"],
            )
            audit_scheduled_tasks.mark_scheduled_task_completed(
                execution, message="Successfully completed fetch_helmet_house_catalog."
            )
            self.stdout.write(self.style.SUCCESS("Successfully completed fetch_helmet_house_catalog."))
        except Exception as e:
            audit_scheduled_tasks.mark_scheduled_task_failed(execution, error_message=str(e))
            self.stdout.write(self.style.ERROR("Error: {}".format(str(e))))
            raise
