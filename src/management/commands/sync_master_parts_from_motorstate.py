from django.core.management.base import BaseCommand

from src.audit import scheduled_tasks as audit_scheduled_tasks
from src.integrations.services import master_parts, motorstate


class Command(BaseCommand):
    help = (
        "Map unmapped Motor State brands to Brands, then propagate Motor State raw data into "
        "MasterPart / ProviderPart / ProviderPartInventory (and pricing unless --skip-pricing)."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--skip-brand-mapping",
            action="store_true",
            help="Skip the unmapped-brand resolution step (assume mappings already exist).",
        )
        parser.add_argument(
            "--skip-pricing",
            action="store_true",
            help="Only sync master parts + inventory; leave ProviderPartCompanyPricing alone.",
        )
        parser.add_argument(
            "--skip-master-parts",
            action="store_true",
            help="Only sync inventory (+ pricing); skip the MasterPart/ProviderPart upsert.",
        )

    def handle(self, *args, **options):
        audit_scheduled_tasks.cleanup_stale_started_executions("sync_master_parts_from_motorstate")
        self.stdout.write("Starting Motor State master parts sync...")
        execution = audit_scheduled_tasks.start_scheduled_task_execution("sync_master_parts_from_motorstate")
        try:
            if not options["skip_brand_mapping"]:
                self.stdout.write("Mapping unmapped Motor State brands to Brands...")
                motorstate.sync_unmapped_motorstate_brands_to_brands()

            master_parts.sync_derived_from_motorstate(
                skip_master_parts=options["skip_master_parts"],
                skip_pricing=options["skip_pricing"],
            )
            audit_scheduled_tasks.mark_scheduled_task_completed(
                execution, message="Successfully completed Motor State master parts sync."
            )
            self.stdout.write(self.style.SUCCESS("Successfully completed Motor State master parts sync."))
        except Exception as e:
            audit_scheduled_tasks.mark_scheduled_task_failed(execution, error_message=str(e))
            self.stdout.write(self.style.ERROR("Error: {}".format(str(e))))
            raise
