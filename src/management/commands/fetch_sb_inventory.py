"""
Pull a forwarded S&B live-inventory file off the relay and apply its quantities.

S&B publish no stock feed. A dealer emails S&B with "Live Inventory" in the subject, forwards the
reply to support@aftermarketscout.com, and support drops the attachment in that company's relay
folder; this command reads it. Everything else about the S&B catalog (pricing, fitment, imagery)
comes from ``sync_sb`` and does not depend on this.

Safe to run when nothing has been delivered yet -- that is the normal state for a new connection
and is logged rather than treated as a failure.
"""
from django.core.management.base import BaseCommand

from src.audit import scheduled_tasks as audit_scheduled_tasks
from src.integrations.services import master_parts, sb


class Command(BaseCommand):
    help = (
        "Read each active S&B connection's forwarded live-inventory file from the relay, write "
        "quantities onto sb_parts, then refresh ProviderPartInventory."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--no-force-download",
            action="store_true",
            help="Use a cached local copy if it is less than 6 hours old.",
        )
        parser.add_argument(
            "--skip-derived",
            action="store_true",
            help="Write sb_parts only; do not refresh ProviderPartInventory.",
        )

    def handle(self, *args, **options):
        audit_scheduled_tasks.cleanup_stale_started_executions("fetch_sb_inventory")
        self.stdout.write("Starting S&B live inventory fetch...")
        execution = audit_scheduled_tasks.start_scheduled_task_execution("fetch_sb_inventory")
        try:
            sb.fetch_and_save_sb_inventory(
                force_download=not options.get("no_force_download")
            )
            self.stdout.write(self.style.SUCCESS("S&B inventory fetch done."))

            if options.get("skip_derived"):
                self.stdout.write("ProviderPartInventory refresh skipped (--skip-derived).")
            else:
                master_parts.sync_provider_inventory_from_sb()
                self.stdout.write(self.style.SUCCESS("ProviderPartInventory refreshed."))

            audit_scheduled_tasks.mark_scheduled_task_completed(
                execution, message="Successfully completed S&B live inventory fetch."
            )
        except Exception as e:
            audit_scheduled_tasks.mark_scheduled_task_failed(execution, error_message=str(e))
            self.stdout.write(self.style.ERROR("Error: {}".format(str(e))))
            raise
