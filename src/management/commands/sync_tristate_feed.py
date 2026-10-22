"""
Download Tri-State's relay feed files for one connection and report what arrived.

PLACEHOLDER, 2026-10-22: this does not ingest anything yet. Tri-State has not delivered a file,
so the row parser in src.integrations.services.tristate cannot be written -- this command's job
until then is to prove the relay side works and to capture the layout of the first real
delivery (see that module's "SFTP relay feed -- PLACEHOLDER" section). Same orchestration
pattern as sync_meyer, so the shape of this command will not change when the parser lands.

``--list-only`` skips the download and just lists the folder, which is what to reach for when a
rep says they delivered and feed_present() still reports nothing: it answers "empty, or named
something we aren't watching for?".
"""
from django.core.management.base import BaseCommand

from src.audit import scheduled_tasks as audit_scheduled_tasks
from src.integrations.services import tristate

_TASK_NAME = "sync_tristate_feed"


class Command(BaseCommand):
    help = (
        "Download Tri-State's pricing + inventory files from the relay and log their layout. "
        "Ingests nothing yet -- the row parser is a placeholder until Tri-State delivers a real "
        "file. Records audit as sync_tristate_feed."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--company-provider-id",
            type=int,
            default=None,
            help=(
                "Inspect this connection's relay folder instead of the primary one. Only used "
                "with --list-only; the download path always reads the primary connection, same "
                "as Meyer's catalog fetch."
            ),
        )
        parser.add_argument(
            "--list-only",
            action="store_true",
            help="List what is in the relay folder and exit. No download, no audit record.",
        )
        parser.add_argument(
            "--force-download",
            action="store_true",
            help="Re-download from the relay even if a fresh local cache exists.",
        )

    def handle(self, *args, **options):
        if options.get("list_only"):
            entries = tristate.describe_tristate_relay_folder(options.get("company_provider_id"))
            if not entries:
                self.stdout.write("Relay folder is empty (or this connection has no relay credentials yet).")
                return
            for row in entries:
                self.stdout.write("{}  {} bytes".format(row["filename"], row["size_bytes"]))
            return

        audit_scheduled_tasks.cleanup_stale_started_executions(_TASK_NAME)
        execution = audit_scheduled_tasks.start_scheduled_task_execution(_TASK_NAME)
        try:
            written = tristate.fetch_and_save_tristate_catalog_and_inventory(
                force_download=options.get("force_download", False),
            )
            message = (
                "Tri-State relay feed check complete: {} row(s) written. Zero is expected while "
                "the parser is a placeholder -- check the logs for the delivered file's "
                "layout.".format(written)
            )
            audit_scheduled_tasks.mark_scheduled_task_completed(execution, message=message)
            self.stdout.write(self.style.SUCCESS(message))
        except Exception as e:
            audit_scheduled_tasks.mark_scheduled_task_failed(execution, error_message=str(e))
            self.stdout.write(self.style.ERROR("Error: {}".format(str(e))))
            raise
