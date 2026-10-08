"""
Mirror platform signups into FreshSales: a Company becomes a sales account, each of its users a
contact. Companion to sync_instantly_replies, so the CRM holds everybody however they arrived.

See src/integrations/services/platform_crm_sync.py for what is excluded and why.

    manage.py sync_platform_signups --dry-run    # preview, writes nothing
    manage.py sync_platform_signups --limit 2    # cautious first live run
    manage.py sync_platform_signups              # what the cron runs
"""
from django.conf import settings
from django.core.management.base import BaseCommand

from common import utils as common_utils
from src.audit import scheduled_tasks as audit_scheduled_tasks
from src.integrations.services import platform_crm_sync


class Command(BaseCommand):
    help = (
        "Sync platform signups (Company -> sales account, users -> contacts) into FreshSales. "
        "Records audit as sync_platform_signups."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Report what would be created. Writes nothing to FreshSales or to our tables.",
        )
        parser.add_argument(
            "--limit",
            type=int,
            default=None,
            help="Cap how many companies are pushed this run.",
        )
        parser.add_argument(
            "--company",
            type=int,
            default=None,
            help="Restrict to one company id.",
        )

    def handle(self, *args, **options):
        missing = self._missing_settings()
        if missing:
            # Same reasoning as sync_instantly_replies: the code deploys before the keys are set on
            # the box, and a cron hitting that window every 15 minutes must stay quiet rather than
            # bury a real failure under identical tracebacks.
            execution = audit_scheduled_tasks.start_scheduled_task_execution(platform_crm_sync.TASK_NAME)
            message = "Not configured: {} unset. See docs/INSTANTLY_FRESHSALES_SYNC_PLAN.md §8.".format(
                ", ".join(missing)
            )
            audit_scheduled_tasks.mark_scheduled_task_skipped(execution, message=message)
            self.stdout.write(self.style.WARNING(message))
            return

        if not settings.FRESHSALES_SYNC_PLATFORM_SIGNUPS:
            execution = audit_scheduled_tasks.start_scheduled_task_execution(platform_crm_sync.TASK_NAME)
            audit_scheduled_tasks.mark_scheduled_task_skipped(
                execution, message="Disabled by FRESHSALES_SYNC_PLATFORM_SIGNUPS."
            )
            self.stdout.write(self.style.WARNING("Disabled by FRESHSALES_SYNC_PLATFORM_SIGNUPS."))
            return

        if options.get("dry_run"):
            self._dry_run()
            return

        audit_scheduled_tasks.cleanup_stale_started_executions(platform_crm_sync.TASK_NAME)
        execution = audit_scheduled_tasks.start_scheduled_task_execution(platform_crm_sync.TASK_NAME)
        try:
            summary = platform_crm_sync.run(
                limit=options.get("limit"),
                company_id=options.get("company"),
                max_calls=settings.FRESHSALES_MAX_CALLS_PER_RUN,
            )
        except Exception as e:
            message = common_utils.get_exception_message(exception=e)
            audit_scheduled_tasks.mark_scheduled_task_failed(execution, error_message=message)
            self.stdout.write(self.style.ERROR("Failed: {}".format(message)))
            raise

        audit_scheduled_tasks.mark_scheduled_task_completed(
            execution, message=", ".join("{}={}".format(k, v) for k, v in sorted(summary.items()))
        )
        for key in sorted(summary):
            self.stdout.write("  {:<28} {}".format(key, summary[key]))
        self.stdout.write(self.style.SUCCESS("Platform -> FreshSales sync finished."))

    def _dry_run(self):
        rows, counts = platform_crm_sync.preview()
        self.stdout.write("Dry run -- nothing was written to FreshSales or to the database.")
        self.stdout.write("")
        self.stdout.write("  {:<30} {:<34} {:<20} {:<22} {}".format("COMPANY", "USER", "NAME", "ACCOUNT", "CONTACT"))
        for row in rows:
            self.stdout.write(
                "  {:<30} {:<34} {:<20} {:<22} {}".format(
                    row["company"][:30],
                    row["email"][:34],
                    row["name"][:20],
                    row["account"][:22],
                    row["contact"],
                )
            )
        self.stdout.write("")
        for key in sorted(counts):
            self.stdout.write("  {:<34} {}".format(key, counts[key]))

    @staticmethod
    def _missing_settings():
        required = ("FRESHSALES_API_KEY", "FRESHSALES_BUNDLE_ALIAS")
        return [name for name in required if not getattr(settings, name, "")]
