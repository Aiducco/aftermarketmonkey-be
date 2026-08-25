"""4-hourly Turn 14 catalog delta -- GET /v1/items/updates."""
import datetime

from django.core.management.base import BaseCommand
from django.utils import timezone

from src.audit import scheduled_tasks as audit_scheduled_tasks
from src.integrations import rate_limit as rate_limit_base
from src.integrations.services import master_parts
from src.integrations.services import turn_14

_TASK_NAME = "fetch_turn_14_items_updates"


class Command(BaseCommand):
    help = "Fetch and save Turn 14 items updates (GET /v1/items/updates), then propagate the delta."

    def add_arguments(self, parser):
        parser.add_argument(
            "--days",
            type=int,
            default=1,
            help="Look-back window in days (Turn 14's parameter is 'days', not 'day'). Default 1.",
        )

    def handle(self, *args, **options):
        audit_scheduled_tasks.cleanup_stale_started_executions(_TASK_NAME, max_age_minutes=120)
        execution = audit_scheduled_tasks.start_scheduled_task_execution(_TASK_NAME)
        meter = rate_limit_base.UsageMeter("t14:get")
        # Captured before the fetch so the propagation pass below can't miss a row updated
        # while the fetch itself was running.
        since = timezone.now() - datetime.timedelta(days=options["days"])
        meter.__enter__()
        try:
            turn_14.fetch_and_save_turn_14_items_updates(days=options["days"])
            # Scoped to this run's window, not a full ~793k-row rescan -- ingest_all_providers no
            # longer runs Turn14 at all, so nothing else propagates this delta into
            # MasterPart/ProviderPart if this doesn't. skip_inventory=True: items/updates carries
            # no stock-level data, so there is nothing here for ProviderPartInventory to sync --
            # that's fetch_turn_14_inventory_updates' job, on its own 10-minute delta.
            master_parts.sync_derived_from_turn14(skip_inventory=True, skip_pricing=True, since=since)
        except Exception as e:
            meter.__exit__(None, None, None)
            audit_scheduled_tasks.mark_scheduled_task_failed(execution, error_message=str(e))
            raise
        meter.__exit__(None, None, None)
        audit_scheduled_tasks.mark_scheduled_task_completed(
            execution,
            message="days={} || {}".format(options["days"], meter.summary("api_usage")),
        )
