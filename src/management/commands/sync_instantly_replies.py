"""
Pull replies from Instantly into FreshSales: every human reply becomes a contact, and a reply
Instantly has labelled positive also becomes a deal.

Meant for a 15-minute cron. See docs/INSTANTLY_FRESHSALES_SYNC_PLAN.md for the design, and
src/integrations/services/instantly_freshsales_sync.py for the three passes.

    manage.py sync_instantly_replies --dry-run --since 2026-09-01   # preview, writes nothing
    manage.py sync_instantly_replies --limit 3                      # cautious first live run
    manage.py sync_instantly_replies                                # what the cron runs
"""
import datetime

from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone

from common import utils as common_utils
from src.audit import scheduled_tasks as audit_scheduled_tasks
from src.integrations.clients.instantly import client as instantly_client
from src.integrations.services import instantly_freshsales_sync


class Command(BaseCommand):
    help = (
        "Sync Instantly replies into FreshSales as contacts, and Instantly's positive labels as "
        "deals. Records audit as sync_instantly_replies."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Read Instantly and report what would be created. Writes nothing -- not to "
            "FreshSales and not to our own tables.",
        )
        parser.add_argument(
            "--since",
            default=None,
            help="YYYY-MM-DD. Overrides the watermark; use for the first run or a backfill. "
            "Without it the run resumes from the newest reply already stored.",
        )
        parser.add_argument(
            "--campaign",
            default=None,
            help="Restrict to one Instantly campaign id.",
        )
        parser.add_argument(
            "--limit",
            type=int,
            default=None,
            help="Cap how many replies are pushed to FreshSales this run.",
        )
        parser.add_argument(
            "--recheck-days",
            type=int,
            default=None,
            help="Widen the interest re-check window for this run (default " "INSTANTLY_INTEREST_RECHECK_DAYS).",
        )
        parser.add_argument(
            "--max-attempts",
            type=int,
            default=5,
            help="Stop retrying a reply after this many failures (default 5).",
        )

    def handle(self, *args, **options):
        missing = self._missing_settings()
        if missing:
            # Deploys land before the environment is configured -- the code ships through CI, the
            # keys are set on the box by hand -- so this is a real state the cron will hit, every
            # 15 minutes. Exiting 0 with a SKIPPED audit row keeps that window quiet; raising here
            # would bury a genuine failure under a wall of identical tracebacks and cron mail.
            execution = audit_scheduled_tasks.start_scheduled_task_execution(instantly_freshsales_sync.TASK_NAME)
            message = "Not configured: {} unset. See docs/INSTANTLY_FRESHSALES_SYNC_PLAN.md §8.".format(
                ", ".join(missing)
            )
            audit_scheduled_tasks.mark_scheduled_task_skipped(execution, message=message)
            self.stdout.write(self.style.WARNING(message))
            return

        since = self._parse_since(options.get("since"))

        if options.get("dry_run"):
            self._dry_run(
                since=since,
                campaign_id=options.get("campaign"),
            )
            return

        audit_scheduled_tasks.cleanup_stale_started_executions(instantly_freshsales_sync.TASK_NAME)
        execution = audit_scheduled_tasks.start_scheduled_task_execution(instantly_freshsales_sync.TASK_NAME)
        try:
            summary = instantly_freshsales_sync.run(
                since=since,
                campaign_id=options.get("campaign"),
                limit=options.get("limit"),
                recheck_days=options.get("recheck_days"),
                max_attempts=options.get("max_attempts"),
            )
        except Exception as e:
            message = common_utils.get_exception_message(exception=e)
            audit_scheduled_tasks.mark_scheduled_task_failed(execution, error_message=message)
            self.stdout.write(self.style.ERROR("Failed: {}".format(message)))
            raise

        message = ", ".join("{}={}".format(k, v) for k, v in sorted(summary.items()))
        audit_scheduled_tasks.mark_scheduled_task_completed(execution, message=message)

        for key in sorted(summary):
            self.stdout.write("  {:<26} {}".format(key, summary[key]))
        if summary.get("stuck_over_max_attempts"):
            # Surfaced here as well as in the audit row: a reply that keeps failing is invisible
            # otherwise, since the pass deliberately continues past it.
            self.stdout.write(
                self.style.WARNING(
                    "{} repl(ies) have hit --max-attempts and are no longer retried. Inspect "
                    "last_error on instantly_reply.".format(summary["stuck_over_max_attempts"])
                )
            )
        self.stdout.write(self.style.SUCCESS("Instantly -> FreshSales sync finished."))

    def _dry_run(self, since, campaign_id):
        client = instantly_client.InstantlyApiClient()
        try:
            campaign_names = client.list_campaigns()
        except Exception as e:
            campaign_names = {}
            self.stdout.write(
                self.style.WARNING(
                    "Could not list campaigns: {}".format(common_utils.get_exception_message(exception=e))
                )
            )

        rows, counts = instantly_freshsales_sync.preview(
            client=client, since=since, campaign_id=campaign_id, campaign_names=campaign_names
        )

        self.stdout.write("Dry run -- nothing was written to FreshSales or to the database.")
        self.stdout.write("")
        self.stdout.write("  {:<38} {:<22} {:<13} {:<4} {}".format("ADDRESS", "NAME", "CRM STATUS", "NEW", "ACTION"))
        for row in rows:
            self.stdout.write(
                "  {:<38} {:<22} {:<13} {:<4} {}".format(
                    row["lead_email"][:38],
                    row["from_name"][:22],
                    row["status"],
                    "yes" if row["new"] else "no",
                    row["action"],
                )
            )
        self.stdout.write("")
        for key in sorted(counts):
            self.stdout.write("  {:<26} {}".format(key, counts[key]))

    @staticmethod
    def _missing_settings():
        """Which of the three required settings are unset. Checked before anything else runs."""
        from django.conf import settings

        required = ("INSTANTLY_API_KEY", "FRESHSALES_API_KEY", "FRESHSALES_BUNDLE_ALIAS")
        return [name for name in required if not getattr(settings, name, "")]

    @staticmethod
    def _parse_since(value):
        if not value:
            return None
        try:
            parsed = datetime.datetime.strptime(value, "%Y-%m-%d")
        except ValueError:
            raise CommandError("--since must be YYYY-MM-DD, got {!r}.".format(value))
        return timezone.make_aware(parsed, datetime.timezone.utc)
