from django.core.management.base import BaseCommand

from src.audit import scheduled_tasks as audit_scheduled_tasks
from src.integrations.services import xdp


class Command(BaseCommand):
    help = (
        "Walk XDP's SearchSpring parts_finder_data hierarchy (year > make > model > engine) and "
        "store each product's raw fitment paths on XdpPart.fitment_paths. This is the expensive "
        "pipeline -- ~3,300 node-discovery requests plus a page per 100 product-vehicle rows "
        "against ~338,043 product-year pairs -- so it is standalone and NOT part of the hourly "
        "cycle, same as Thoroughbred's vehicle walk. Decoding into MasterPartFitment is the "
        "separate sync_master_part_fitments_from_xdp command."
    )

    def handle(self, *args, **options):
        audit_scheduled_tasks.cleanup_stale_started_executions("fetch_xdp_fitment")
        self.stdout.write("Starting XDP fitment path walk...")
        execution = audit_scheduled_tasks.start_scheduled_task_execution("fetch_xdp_fitment")
        try:
            written = xdp.fetch_and_save_xdp_fitment_paths()
            audit_scheduled_tasks.mark_scheduled_task_completed(
                execution,
                message="Successfully completed XDP fitment path walk ({} rows).".format(written),
            )
            self.stdout.write(self.style.SUCCESS("fitment_paths written on {} row(s).".format(written)))
        except Exception as e:
            audit_scheduled_tasks.mark_scheduled_task_failed(execution, error_message=str(e))
            self.stdout.write(self.style.ERROR("Error: {}".format(str(e))))
            raise
