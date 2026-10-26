from django.core.management.base import BaseCommand

from src.audit import scheduled_tasks as audit_scheduled_tasks
from src.integrations.services import master_parts


class Command(BaseCommand):
    help = (
        "Decode every ThoroughbredParts.vehicleid via ThoroughbredVehicle (year/make/model -- "
        "Thoroughbred's own private vehicle-id scheme has no submodel/engine/drive_type) and "
        "upsert into MasterPartFitment, joined to MasterPart via ProviderPart.provider_external_id. "
        "Requires sync_master_parts_from_thoroughbred and fetch_thoroughbred_vehicles to have "
        "already run. Standalone pipeline, not scheduled — run manually or on its own cadence, "
        "same as every other native fitment writer."
    )

    def handle(self, *args, **options):
        audit_scheduled_tasks.cleanup_stale_started_executions(
            "sync_master_part_fitments_from_thoroughbred"
        )
        self.stdout.write("Starting Thoroughbred Diesel master part fitment sync...")
        execution = audit_scheduled_tasks.start_scheduled_task_execution(
            "sync_master_part_fitments_from_thoroughbred"
        )
        try:
            master_parts.sync_master_part_fitments_from_thoroughbred()
            audit_scheduled_tasks.mark_scheduled_task_completed(
                execution,
                message="Successfully completed Thoroughbred Diesel master part fitment sync.",
            )
            self.stdout.write(
                self.style.SUCCESS("Successfully completed Thoroughbred Diesel master part fitment sync.")
            )
        except Exception as e:
            audit_scheduled_tasks.mark_scheduled_task_failed(execution, error_message=str(e))
            self.stdout.write(self.style.ERROR("Error: {}".format(str(e))))
            raise
