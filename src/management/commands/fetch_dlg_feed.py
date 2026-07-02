from django.core.management.base import BaseCommand

from src.audit import scheduled_tasks as audit_scheduled_tasks
from src.integrations.services import dlg, master_parts


class Command(BaseCommand):
    help = (
        "Download DLG dlg_inventory.csv from the fixed relay host/path; SFTP auth comes from the primary "
        "DLG CompanyProviders row's credentials (sftp_user/sftp_password), falling back to "
        "DLG_RELAY_SFTP_USER / DLG_RELAY_SFTP_PASSWORD in settings if unset. Upserts DlgBrand / DlgParts, "
        "then DlgCompanyPricing per company using each company's own credentials. CompanyProviders may also "
        "set email_from and local_feed_path."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--force-download",
            action="store_true",
            help="Re-download inventory even if local cache is fresh.",
        )

    def handle(self, *args, **options):
        audit_scheduled_tasks.cleanup_stale_started_executions("fetch_dlg_feed")
        self.stdout.write("Fetching DLG inventory from SFTP...")
        execution = audit_scheduled_tasks.start_scheduled_task_execution("fetch_dlg_feed")
        try:
            dlg.fetch_and_save_dlg_catalog(force_download=options.get("force_download", False))
            self.stdout.write(
                "Propagating DLG catalog into master parts, provider parts, inventory, and pricing..."
            )
            master_parts.sync_derived_from_dlg(reindex_meilisearch=False)
            audit_scheduled_tasks.mark_scheduled_task_completed(
                execution,
                message="Successfully completed DLG ingest and derived master layer sync.",
            )
            self.stdout.write(self.style.SUCCESS("Done."))
        except Exception as e:
            audit_scheduled_tasks.mark_scheduled_task_failed(execution, error_message=str(e))
            self.stdout.write(self.style.ERROR("Error: {}".format(str(e))))
            raise
