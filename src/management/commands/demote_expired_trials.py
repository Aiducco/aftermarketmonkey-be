"""
Plan expiry / downgrade cron (see src.api.services.plan_lifecycle). Scheduled via cron, hourly.

Each run, in order:
  1. sends the day-7 / day-1 trial warning emails to trials entering those windows;
  2. downgrades every comp'd trial that has ended -- one transaction per company -- to Scout:
     the distributors and members over the Scout caps are paused / suspended (never deleted) and
     the day-0 "your trial has ended" email is sent;
  3. retries day-0 emails for recent downgrades whose send failed.

Idempotent, so hourly is safe: a company is downgraded once (the second run finds it no longer
"trialing"), and every email is stamped in Company.warning_emails_sent only after the send was
accepted, so a re-run never duplicates one and a failed one is retried.

Only touches companies with manual_trial_granted_at set -- a company that converted to a real
Stripe subscription has that field cleared by billing._sync_company_subscription, so Stripe's
own webhooks (not this command) govern its plan/status from then on.

--dry-run writes nothing and sends nothing: it lists the companies it would downgrade, which
distributors would stay active / be paused, which members would be suspended, and the warning
emails that would go out. Run it before the first production run of a new deploy.
"""
from django.core.management.base import BaseCommand
from django.utils import timezone

from src import models as src_models
from src.api.services import plan_lifecycle


class Command(BaseCommand):
    help = (
        "Downgrade companies whose manually-granted trial has ended to Scout (pausing extra "
        "distributors and suspending extra members) and send the trial warning / ended emails."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Print what would be downgraded / emailed without writing or sending anything.",
        )
        parser.add_argument(
            "--company-id",
            type=int,
            default=None,
            help="Restrict the run to one company (for verification).",
        )

    def handle(self, *args, **options):
        now = timezone.now()
        dry_run = options["dry_run"]
        only_company_id = options["company_id"]

        if dry_run:
            self._dry_run(now, only_company_id)
            return

        try:
            warnings_sent = plan_lifecycle.send_trial_warning_emails(now, company_id=only_company_id)
        except Exception as e:
            # The downgrade phase below is the important one; warnings failing must not skip it.
            warnings_sent = 0
            self.stderr.write("Trial warning emails failed: {!r}".format(e))

        company_ids = plan_lifecycle.expired_trial_company_ids(now)
        if only_company_id is not None:
            company_ids = [i for i in company_ids if i == only_company_id]
        downgraded = paused = suspended = 0
        for company_id in company_ids:
            try:
                result = plan_lifecycle.downgrade_company(company_id, now=now)
            except Exception as e:
                # One bad company must not stop the rest; its own transaction already rolled back
                # and the next hourly run retries it.
                self.stderr.write("FAILED to downgrade company_id={}: {!r}".format(company_id, e))
                continue
            if result is None:
                continue  # raced (extended / upgraded since the query) -- nothing to do
            downgraded += 1
            paused += len(result["paused_ids"])
            suspended += len(result["suspended_user_ids"])
            self.stdout.write(
                "Downgraded company_id={} -- kept={} paused={} suspended_users={} source={}".format(
                    company_id,
                    result["kept_ids"],
                    result["paused_ids"],
                    result["suspended_user_ids"],
                    result["source"],
                )
            )

        try:
            retried = plan_lifecycle.send_pending_downgrade_emails(now, company_id=only_company_id)
        except Exception as e:
            retried = 0
            self.stderr.write("Trial-ended email retries failed: {!r}".format(e))
        self.stdout.write(
            self.style.SUCCESS(
                "Downgraded {} compan{}: paused {} integration(s), suspended {} member(s). "
                "Emails sent: {} warning, {} trial-ended retry.".format(
                    downgraded, "y" if downgraded == 1 else "ies", paused, suspended, warnings_sent, retried
                )
            )
        )

    def _dry_run(self, now, only_company_id):
        self.stdout.write("DRY RUN -- nothing is written or sent.")
        company_ids = plan_lifecycle.expired_trial_company_ids(now)
        if only_company_id is not None:
            company_ids = [i for i in company_ids if i == only_company_id]
        for company_id in company_ids:
            company = src_models.Company.objects.get(id=company_id)
            plan = plan_lifecycle.preview_downgrade(company_id, now=now)
            if plan is None:
                continue
            names = plan["names"]
            self.stdout.write(
                "Would downgrade company_id={} ({!r}, internal={}) -- {} trial ended {}".format(
                    company.id,
                    company.name,
                    company.is_internal,
                    company.subscription_plan,
                    company.subscription_period_end,
                )
            )
            self.stdout.write(
                "    keep ({}): {}".format(
                    plan["source"] or "nothing to choose",
                    ", ".join(names[i] for i in plan["kept_ids"]) or "-",
                )
            )
            self.stdout.write("    pause: {}".format(", ".join(names[i] for i in plan["paused_ids"]) or "-"))
            self.stdout.write("    suspend user ids: {}".format(plan["suspend_user_ids"] or "-"))
        for company_id, key in plan_lifecycle.warning_email_candidates(now, company_id=only_company_id):
            self.stdout.write("Would send {} warning email to company_id={}".format(key, company_id))
        for company_id in plan_lifecycle.pending_downgrade_email_company_ids(now, company_id=only_company_id):
            self.stdout.write("Would (re)send trial-ended email to company_id={}".format(company_id))
        self.stdout.write(
            self.style.SUCCESS(
                "Dry run: {} compan{} would be downgraded.".format(
                    len(company_ids), "y" if len(company_ids) == 1 else "ies"
                )
            )
        )
