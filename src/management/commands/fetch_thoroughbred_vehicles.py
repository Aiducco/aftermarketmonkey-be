from django.core.management.base import BaseCommand

from src.audit import scheduled_tasks as audit_scheduled_tasks
from src.integrations.services import thoroughbred


class Command(BaseCommand):
    help = (
        "Walk Thoroughbred Diesel's main-site ajax vehicle picker (GetYears/GetModels/GetVehicle "
        "over 8 hardcoded makes, ~1,545 requests) and upsert ThoroughbredVehicle. Kept separate "
        "from fetch_thoroughbred_feed: this hits a different, Cloudflare-fronted host with a "
        "different UA posture, at a much lower request volume -- a failure here should not block "
        "the catalog pull, and vice versa. Run this before "
        "sync_master_part_fitments_from_thoroughbred, which depends on this table."
    )

    def handle(self, *args, **options):
        audit_scheduled_tasks.cleanup_stale_started_executions("fetch_thoroughbred_vehicles")
        execution = audit_scheduled_tasks.start_scheduled_task_execution("fetch_thoroughbred_vehicles")
        try:
            self.stdout.write("Fetching Thoroughbred Diesel vehicle decode table...")
            upserted = thoroughbred.fetch_and_save_thoroughbred_vehicles()
            self.stdout.write("Vehicles upserted: {}".format(upserted))

            audit_scheduled_tasks.mark_scheduled_task_completed(
                execution, message="Successfully completed Thoroughbred Diesel vehicle fetch."
            )
            self.stdout.write(self.style.SUCCESS("Done."))
        except Exception as e:
            audit_scheduled_tasks.mark_scheduled_task_failed(execution, error_message=str(e))
            self.stdout.write(self.style.ERROR("Error: {}".format(str(e))))
            raise
