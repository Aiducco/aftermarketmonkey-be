"""
One-time: decode PaceCatalogPart.fitment via VcdbVehicle (year/make/model only -- see
sync_master_part_fitments_from_pace_catalog_vcdb's docstring for why submodel/engine/drive_type
can't be resolved from this data) and upsert into MasterPartFitment. Not provider-specific --
backfills fitment onto whatever MasterPart rows already exist for a (brand, part_number),
regardless of which distributor sourced them.

Requires import_pace_catalog_from_algolia and import_vcdb_vehicles to have already run. Not
scheduled -- run manually.

    python manage.py sync_master_part_fitments_from_pace_catalog_vcdb
    python manage.py sync_master_part_fitments_from_pace_catalog_vcdb --limit 50
"""
from django.core.management.base import BaseCommand

from src.audit import scheduled_tasks as audit_scheduled_tasks
from src.integrations.services import master_parts


class Command(BaseCommand):
    help = (
        "One-time: decode PaceCatalogPart.fitment via VcdbVehicle (year/make/model) and upsert "
        "into MasterPartFitment. Not scheduled -- run manually."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--limit", type=int, default=None,
            help="Process only the first N fitment-bearing PaceCatalogPart rows (for a test run).",
        )

    def handle(self, *args, **options):
        limit = options.get("limit")
        task_name = "sync_master_part_fitments_from_pace_catalog_vcdb"
        audit_scheduled_tasks.cleanup_stale_started_executions(task_name)
        self.stdout.write("Starting PaceCatalog -> VCdb master part fitment sync{}...".format(
            " (limit={})".format(limit) if limit else ""
        ))
        execution = audit_scheduled_tasks.start_scheduled_task_execution(task_name)
        try:
            processed, upserted = master_parts.sync_master_part_fitments_from_pace_catalog_vcdb(limit=limit)
            message = "Processed {} PaceCatalogPart rows, upserted {} MasterPartFitment rows.".format(
                processed, upserted
            )
            audit_scheduled_tasks.mark_scheduled_task_completed(execution, message=message)
            self.stdout.write(self.style.SUCCESS(message))
        except Exception as e:
            audit_scheduled_tasks.mark_scheduled_task_failed(execution, error_message=str(e))
            self.stdout.write(self.style.ERROR("Error: {}".format(str(e))))
            raise
