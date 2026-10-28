"""
Decode RandysPart.fitment_rows into MasterPartFitment.

Requires fetch_randys_fitment and sync_randys (for the ProviderPart rows) to have run first.
"""
from django.core.management.base import BaseCommand

from src.audit import scheduled_tasks as audit_scheduled_tasks
from src.integrations.services import master_parts


class Command(BaseCommand):
    help = "Sync MasterPartFitment from stored RANDYS Worldwide fitment rows."

    def handle(self, *args, **options):
        task_name = "sync_master_part_fitments_from_randys"
        audit_scheduled_tasks.cleanup_stale_started_executions(task_name)
        execution = audit_scheduled_tasks.start_scheduled_task_execution(task_name)
        try:
            master_parts.sync_master_part_fitments_from_randys()
            audit_scheduled_tasks.mark_scheduled_task_completed(
                execution, message="RANDYS MasterPartFitment sync complete."
            )
            self.stdout.write(self.style.SUCCESS("RANDYS fitment sync complete."))
        except Exception as e:
            audit_scheduled_tasks.mark_scheduled_task_failed(execution, error_message=str(e))
            self.stdout.write(self.style.ERROR("RANDYS fitment sync failed: {}".format(str(e))))
            raise
