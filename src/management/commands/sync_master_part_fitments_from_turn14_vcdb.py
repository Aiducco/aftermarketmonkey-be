from django.core.management.base import BaseCommand

from src.audit import scheduled_tasks as audit_scheduled_tasks
from src.integrations.services import master_parts


class Command(BaseCommand):
    help = (
        "On-demand: decode every Turn14ItemFitment.vehicle_id via VcdbVehicle "
        "(year/make/model/submodel/engine/drive_type) and upsert into MasterPartFitment, joined "
        "to MasterPart directly via (brand, part_number == Turn14Items.mfr_part_number). "
        "Parallelized across Turn14-brand partitions. Requires sync_master_parts_from_turn14 "
        "and import_vcdb_vehicles to have already run. Not scheduled — run manually."
    )

    def handle(self, *args, **options):
        audit_scheduled_tasks.cleanup_stale_started_executions("sync_master_part_fitments_from_turn14_vcdb")
        self.stdout.write("Starting Turn14 -> VCdb master part fitment sync...")
        execution = audit_scheduled_tasks.start_scheduled_task_execution("sync_master_part_fitments_from_turn14_vcdb")
        try:
            master_parts.sync_master_part_fitments_from_turn14_vcdb()
            audit_scheduled_tasks.mark_scheduled_task_completed(
                execution,
                message="Successfully completed Turn14 -> VCdb master part fitment sync.",
            )
            self.stdout.write(self.style.SUCCESS("Successfully completed Turn14 -> VCdb master part fitment sync."))
        except Exception as e:
            audit_scheduled_tasks.mark_scheduled_task_failed(execution, error_message=str(e))
            self.stdout.write(self.style.ERROR("Error: {}".format(str(e))))
            raise
