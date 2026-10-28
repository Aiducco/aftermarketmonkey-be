"""
Fetch RANDYS vehicle fitment rows onto RandysPart.fitment_rows.

One request per part (no batch endpoint exists). Convermax rate-limits on sustained volume, so
this runs at 4 workers (~11 req/s, roughly 25 minutes for the catalogue) and backs off on 429.
``--only-missing`` resumes an interrupted or rate-limited run.
Decoding into MasterPartFitment is the separate sync_master_part_fitments_from_randys command.
"""
from django.core.management.base import BaseCommand

from src.audit import scheduled_tasks as audit_scheduled_tasks
from src.integrations.services import randys


class Command(BaseCommand):
    help = "Fetch RANDYS Worldwide vehicle fitment rows from the public Convermax endpoint."

    def add_arguments(self, parser):
        parser.add_argument(
            "--only-missing",
            action="store_true",
            help="Only fetch parts with no fitment rows stored yet (resume an interrupted run).",
        )
        parser.add_argument(
            "--max-workers",
            type=int,
            default=None,
            help="Concurrent requests. Defaults to settings.RANDYS_FITMENT_MAX_WORKERS (4). Higher values trip 429s.",
        )

    def handle(self, *args, **options):
        audit_scheduled_tasks.cleanup_stale_started_executions("fetch_randys_fitment")
        execution = audit_scheduled_tasks.start_scheduled_task_execution("fetch_randys_fitment")
        try:
            updated = randys.fetch_and_save_randys_fitment(
                only_missing=options.get("only_missing", False),
                max_workers=options.get("max_workers"),
            )
            message = "RANDYS fitment fetched for {} parts.".format(updated)
            audit_scheduled_tasks.mark_scheduled_task_completed(execution, message=message)
            self.stdout.write(self.style.SUCCESS(message))
        except Exception as e:
            audit_scheduled_tasks.mark_scheduled_task_failed(execution, error_message=str(e))
            self.stdout.write(self.style.ERROR("RANDYS fitment fetch failed: {}".format(str(e))))
            raise
