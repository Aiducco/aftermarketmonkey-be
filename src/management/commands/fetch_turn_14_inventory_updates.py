"""10-minute Turn 14 inventory delta -- the fastest tier of their proposed model."""
from django.core.management.base import BaseCommand

from src.audit import scheduled_tasks as audit_scheduled_tasks
from src.integrations import rate_limit as rate_limit_base
from src.integrations.services import turn_14

_TASK_NAME = "fetch_turn_14_inventory_updates"


class Command(BaseCommand):
    help = "Fetch and save Turn 14 inventory updates (GET /v1/inventory/updates)."

    def add_arguments(self, parser):
        parser.add_argument(
            "--minutes",
            type=int,
            default=15,
            help=(
                "Look-back window. Keep it wider than the cron interval so an update landing "
                "between two runs cannot slip through the gap (default 15 for a 10-minute cron)."
            ),
        )

    def handle(self, *args, **options):
        audit_scheduled_tasks.cleanup_stale_started_executions(_TASK_NAME, max_age_minutes=30)
        execution = audit_scheduled_tasks.start_scheduled_task_execution(_TASK_NAME)
        meter = rate_limit_base.UsageMeter("t14:get")
        meter.__enter__()
        try:
            turn_14.fetch_and_save_turn_14_inventory_updates(minutes=options["minutes"])
        except Exception as e:
            meter.__exit__(None, None, None)
            audit_scheduled_tasks.mark_scheduled_task_failed(execution, error_message=str(e))
            raise
        meter.__exit__(None, None, None)
        audit_scheduled_tasks.mark_scheduled_task_completed(
            execution,
            message="minutes={} || {}".format(options["minutes"], meter.summary("api_usage")),
        )
