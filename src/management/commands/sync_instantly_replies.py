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
            "--rewrite-notes",
            action="store_true",
            help="Rebuild every already-pushed note from stored data and stop. Use after changing "
            "what a note contains -- e.g. to backfill the Instantly thread link into older notes. "
            "Idempotent; creates nothing.",
        )
        parser.add_argument(
            "--create-deals-for",
            nargs="+",
            default=None,
            metavar="EMAIL",
            help="Open deals for these reply addresses and stop -- the catch-up path for shops "
            "whose replies were pushed as contacts while FRESHSALES_CREATE_DEALS was off. Runs "
            "the deal pass only (ingest and the contact push already happened) and ignores that "
            "setting, since naming addresses is the explicit decision the setting guards. Skips "
            "any address that is not a positive reply, or already has a deal.",
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

        if options.get("rewrite_notes"):
            from src.integrations.clients.freshsales import client as freshsales_client

            counts = instantly_freshsales_sync.rewrite_notes(
                crm=freshsales_client.FreshsalesApiClient(), limit=options.get("limit")
            )
            for key in sorted(counts):
                self.stdout.write("  {:<26} {}".format(key, counts[key]))
            self.stdout.write(self.style.SUCCESS("Notes rewritten."))
            return

        if options.get("create_deals_for"):
            self._create_deals_for(
                emails=options["create_deals_for"],
                max_attempts=options.get("max_attempts"),
            )
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

    def _create_deals_for(self, emails, max_attempts):
        """
        The deal pass, restricted to named addresses, with FRESHSALES_CREATE_DEALS ignored.

        Nothing is ingested and no contact is pushed: this exists for shops whose replies the cron
        already turned into contacts, which is every shop reachable this way. Typing the addresses
        is the deliberate per-shop decision that FRESHSALES_CREATE_DEALS otherwise guards, so the
        setting is not consulted -- leaving the cron still not opening deals on its own.
        """
        from django.conf import settings

        from src.integrations.clients.freshsales import client as freshsales_client

        # Named up front rather than inferred from the counts afterwards: a shop with three
        # replies contributes three to them, so "fewer counts than addresses" cannot tell a
        # typo'd address from a chatty one. The queue filters on is_positive / is_auto_reply /
        # deal_created_at, so an ineligible address otherwise just reports zeros and looks fine.
        eligible = {
            address.strip().lower()
            for address in instantly_freshsales_sync.deal_queue(
                emails=emails, max_attempts=max_attempts
            ).values_list("lead_email", flat=True)
        }
        missing = [e for e in emails if e.strip().lower() not in eligible]
        if missing:
            self.stdout.write(
                self.style.WARNING(
                    "No waiting positive reply for: {}. Each is either not labelled Interested, "
                    "already has a deal, or is an auto-reply -- check is_positive / "
                    "freshsales_deal_id on instantly_reply. Continuing with the rest.".format(
                        ", ".join(missing)
                    )
                )
            )
        if not eligible:
            self.stdout.write(self.style.ERROR("Nothing to do -- no address matched. Stopping."))
            return

        crm = freshsales_client.FreshsalesApiClient()
        instantly = instantly_client.InstantlyApiClient()

        execution = audit_scheduled_tasks.start_scheduled_task_execution(
            instantly_freshsales_sync.DEAL_CATCHUP_TASK_NAME
        )
        try:
            counts = instantly_freshsales_sync.create_deals(
                crm=crm,
                instantly=instantly,
                default_amount=settings.FRESHSALES_DEFAULT_DEAL_AMOUNT,
                emails=emails,
                max_calls=settings.FRESHSALES_MAX_CALLS_PER_RUN,
                max_attempts=max_attempts,
                provision_platform_accounts=settings.FRESHSALES_AUTO_PROVISION_PLATFORM_ACCOUNT,
            )
        except Exception as e:
            message = common_utils.get_exception_message(exception=e)
            audit_scheduled_tasks.mark_scheduled_task_failed(execution, error_message=message)
            self.stdout.write(self.style.ERROR("Failed: {}".format(message)))
            raise

        message = "requested={}, {}".format(
            " ".join(emails), ", ".join("{}={}".format(k, v) for k, v in sorted(counts.items()))
        )
        audit_scheduled_tasks.mark_scheduled_task_completed(execution, message=message)

        for key in sorted(counts):
            self.stdout.write("  {:<26} {}".format(key, counts[key]))
        if counts["failed"]:
            self.stdout.write(
                self.style.WARNING(
                    "{} repl(ies) failed -- read last_error on instantly_reply. A rejected "
                    "dropdown value (FRESHSALES_DEAL_SOURCE / _RESCUE_STEP / _ASSIGNED_TO must "
                    "match an existing FreshSales option exactly) looks like this.".format(counts["failed"])
                )
            )
        if counts["campaign_removal_failed"]:
            self.stdout.write(
                self.style.WARNING(
                    "{} lead(s) still sit in their Instantly campaign and will keep receiving the "
                    "cold sequence -- remove them by hand.".format(counts["campaign_removal_failed"])
                )
            )
        self.stdout.write(self.style.SUCCESS("Deal catch-up finished."))

    def _dry_run(self, since, campaign_id):
        from django.conf import settings

        if since is None:
            # Preview what a real run would do, which means starting where a real run would start.
            # Without this the dry run ignores the watermark and walks the whole account, so it
            # reports work that a real run would not attempt -- and on a busy account it would also
            # spend a page of GET /emails per 100 replies to say nothing. Pass --since to widen.
            since = instantly_freshsales_sync.watermark(
                overlap_minutes=settings.INSTANTLY_SYNC_OVERLAP_MINUTES,
                initial_days=settings.INSTANTLY_SYNC_INITIAL_DAYS,
            )
            self.stdout.write("Previewing from the watermark ({}). Pass --since to widen.".format(since))

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
