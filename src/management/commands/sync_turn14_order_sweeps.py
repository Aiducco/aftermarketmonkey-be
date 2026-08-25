"""
Hourly Turn 14 tracking and invoice sweep -- the "Hourly Updates" tier of their model.

Replaces per-open-order polling with one bulk call per company per cycle. See
src.integrations.services.turn_14_order_sweeps for why.
"""
from django.core.management.base import BaseCommand

from src.audit import scheduled_tasks as audit_scheduled_tasks
from src.integrations.services import turn_14_order_sweeps

_TASK_NAME = "sync_turn14_order_sweeps"


class Command(BaseCommand):
    help = "Hourly Turn 14 tracking + invoice sweep for companies with open orders."

    def handle(self, *args, **options):
        audit_scheduled_tasks.cleanup_stale_started_executions(_TASK_NAME, max_age_minutes=30)
        execution = audit_scheduled_tasks.start_scheduled_task_execution(_TASK_NAME)
        try:
            totals = turn_14_order_sweeps.run_order_sweeps()
        except Exception as e:
            audit_scheduled_tasks.mark_scheduled_task_failed(execution, error_message=str(e))
            raise
        message = str(totals)
        audit_scheduled_tasks.mark_scheduled_task_completed(execution, message=message)
        self.stdout.write(message)
