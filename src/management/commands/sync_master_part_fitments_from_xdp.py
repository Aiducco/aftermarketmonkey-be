from django.core.management.base import BaseCommand

from src.audit import scheduled_tasks as audit_scheduled_tasks
from src.integrations.services import master_parts


class Command(BaseCommand):
    help = (
        "Decode XdpPart.fitment_paths (\"2017>Ford>F-250 Super Duty>6.7 V8 DIESEL\") into "
        "MasterPartFitment, collapsing years into ranges and joining to MasterPart via "
        "ProviderPart.provider_external_id. Unlike Thoroughbred, XDP's paths carry a real engine, "
        "so MasterPartFitment.engine is populated (submodel/drive_type stay empty -- the "
        "hierarchy has no such level). Requires fetch_xdp_fitment and the XDP master-parts sync "
        "to have already run. Standalone pipeline, not scheduled."
    )

    def handle(self, *args, **options):
        audit_scheduled_tasks.cleanup_stale_started_executions(
            "sync_master_part_fitments_from_xdp"
        )
        self.stdout.write("Starting XDP master part fitment sync...")
        execution = audit_scheduled_tasks.start_scheduled_task_execution(
            "sync_master_part_fitments_from_xdp"
        )
        try:
            master_parts.sync_master_part_fitments_from_xdp()
            audit_scheduled_tasks.mark_scheduled_task_completed(
                execution,
                message="Successfully completed XDP master part fitment sync.",
            )
            self.stdout.write(
                self.style.SUCCESS("Successfully completed XDP master part fitment sync.")
            )
        except Exception as e:
            audit_scheduled_tasks.mark_scheduled_task_failed(execution, error_message=str(e))
            self.stdout.write(self.style.ERROR("Error: {}".format(str(e))))
            raise
