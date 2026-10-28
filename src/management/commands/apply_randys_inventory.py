"""
Read the newest RANDYS inventory push and propagate quantities.

Separate from ``sync_randys`` on purpose: the catalogue changes rarely and costs a full sweep,
whereas the stock file lands whenever RANDYS send it and is cheap to apply, so this is the
command to run on a short cadence.
"""
from django.core.management.base import BaseCommand

from src.audit import scheduled_tasks as audit_scheduled_tasks
from src.integrations.services import master_parts, randys


class Command(BaseCommand):
    help = "Apply the newest RANDYS Worldwide inventory file and propagate quantities."

    def add_arguments(self, parser):
        parser.add_argument(
            "--force-download",
            action="store_true",
            help="Re-download even if the cached copy of the same file is still fresh.",
        )
        parser.add_argument(
            "--local-path",
            type=str,
            default=None,
            help="Read this CSV instead of fetching over SFTP (for replaying a delivered file).",
        )
        parser.add_argument(
            "--zero-missing",
            action="store_true",
            help=(
                "Set quantity=0 on every part absent from the file. OFF by default: if RANDYS "
                "ever ship an in-stock-only file this would wipe the catalogue's quantities. "
                "Only use it once a full file has been confirmed to list every part."
            ),
        )
        parser.add_argument(
            "--skip-derived",
            action="store_true",
            help="Write RandysPart.quantity only; skip propagation into ProviderPartInventory.",
        )

    def handle(self, *args, **options):
        audit_scheduled_tasks.cleanup_stale_started_executions("apply_randys_inventory")
        execution = audit_scheduled_tasks.start_scheduled_task_execution("apply_randys_inventory")
        try:
            self.stdout.write("Step 1/2: applying the newest RANDYS inventory file.")
            updated = randys.apply_randys_inventory_file(
                force_download=options.get("force_download", False),
                local_path=options.get("local_path"),
                zero_missing=options.get("zero_missing", False),
            )

            if options.get("skip_derived", False):
                self.stdout.write("Step 2/2: skipped (--skip-derived).")
            else:
                self.stdout.write("Step 2/2: propagating into ProviderPartInventory.")
                master_parts.sync_derived_from_randys(skip_master_parts=True)

            message = "RANDYS inventory applied to {} parts.".format(updated)
            audit_scheduled_tasks.mark_scheduled_task_completed(execution, message=message)
            self.stdout.write(self.style.SUCCESS(message))
        except Exception as e:
            audit_scheduled_tasks.mark_scheduled_task_failed(execution, error_message=str(e))
            self.stdout.write(self.style.ERROR("RANDYS inventory failed: {}".format(str(e))))
            raise
