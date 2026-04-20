from django.core.management.base import BaseCommand

from src.audit import scheduled_tasks as audit_scheduled_tasks
from src.integrations.services import atech, master_parts


class Command(BaseCommand):
    help = (
        "Download the A-Tech combined catalog + pricing feed from SFTP and upsert AtechParts "
        "(requires CompanyProviders for A-Tech with sftp_user / sftp_password; "
        "AtechPrefixBrand rows must map SKU prefixes to AtechBrand)."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--force-download",
            action="store_true",
            help="Re-download feed even if local cache is fresh.",
        )

    def handle(self, *args, **options):
        self.stdout.write("Fetching A-Tech feed from SFTP...")
        execution = audit_scheduled_tasks.start_scheduled_task_execution("fetch_atech_feed")
        try:
            atech.fetch_and_save_atech_catalog(force_download=options.get("force_download", False))
            self.stdout.write(
                "Propagating A-Tech catalog into master parts, provider parts, inventory, and pricing..."
            )
            master_parts.sync_derived_from_atech(reindex_meilisearch=True)
            audit_scheduled_tasks.mark_scheduled_task_completed(
                execution,
                message="Successfully completed A-Tech ingest and derived master layer sync.",
            )
            self.stdout.write(self.style.SUCCESS("Done."))
        except Exception as e:
            audit_scheduled_tasks.mark_scheduled_task_failed(execution, error_message=str(e))
            self.stdout.write(self.style.ERROR("Error: {}".format(str(e))))
            raise
