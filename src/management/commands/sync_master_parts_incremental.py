from django.core.management.base import BaseCommand

from src.audit import scheduled_tasks as audit_scheduled_tasks
from src.integrations.services import master_parts


class Command(BaseCommand):
    help = (
        "Sync ProviderPartInventory and ProviderPartCompanyPricing for all providers without "
        "re-upserting MasterPart / ProviderPart rows. Fast path: run frequently (every few hours) "
        "to keep inventory and pricing fresh between full sync_master_parts runs."
    )

    def handle(self, *args, **options):
        audit_scheduled_tasks.cleanup_stale_started_executions("sync_master_parts_incremental")
        self.stdout.write("Starting incremental master parts sync (inventory + pricing only)...")
        execution = audit_scheduled_tasks.start_scheduled_task_execution(
            "sync_master_parts_incremental"
        )
        try:
            master_parts.sync_all_master_parts_incremental()
            audit_scheduled_tasks.mark_scheduled_task_completed(
                execution,
                message="Successfully completed incremental master parts sync.",
            )
            self.stdout.write(
                self.style.SUCCESS("Successfully completed incremental master parts sync.")
            )
        except Exception as e:
            audit_scheduled_tasks.mark_scheduled_task_failed(
                execution, error_message=str(e)
            )
            self.stdout.write(self.style.ERROR("Error: {}".format(str(e))))
            raise
