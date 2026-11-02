"""
Scheduled (every 4 hours) ShopMonkey inventory sync -- pushes every enabled
ShopMonkeySyncBrand's parts (for every company with FEATURE_SHOPMONKEY_INVENTORY_SYNC on;
Trident Motorsports only today) into ShopMonkey. See
src.integrations.services.shopmonkey_sync for the actual sync logic.

    manage.py sync_shopmonkey_inventory
"""
from django.core.management.base import BaseCommand

from src.audit import scheduled_tasks as audit_scheduled_tasks
from src.integrations.services import shopmonkey_sync as shopmonkey_sync_services

_TASK_NAME = "sync_shopmonkey_inventory"


class Command(BaseCommand):
    help = "Sync every enabled ShopMonkeySyncBrand's parts into ShopMonkey."

    def handle(self, *args, **options):
        # The 4-hourly cron registration for this command is confirmed live on the prod host (not
        # in this repo), but it recorded nothing in ScheduledTaskExecution until now -- wiring
        # this in so the audit table actually reflects a cron that is demonstrably running.
        audit_scheduled_tasks.cleanup_stale_started_executions(_TASK_NAME, max_age_minutes=240)
        task_execution = audit_scheduled_tasks.start_scheduled_task_execution(_TASK_NAME)
        try:
            self.stdout.write("Starting ShopMonkey inventory sync...")
            shopmonkey_sync_services.sync_all_enabled_brands()
            self.stdout.write(self.style.SUCCESS("Done."))
            audit_scheduled_tasks.mark_scheduled_task_completed(task_execution, message="Sync run finished.")
        except Exception as e:
            audit_scheduled_tasks.mark_scheduled_task_failed(task_execution, error_message=str(e))
            raise
