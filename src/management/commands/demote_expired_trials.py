"""
Demotes companies whose manually-granted (comp'd) trial has expired back to Scout. Scheduled
via cron, hourly.

Only touches companies with manual_trial_granted_at set -- a company that converted to a real
Stripe subscription has that field cleared by billing._sync_company_subscription, so Stripe's
own webhooks (not this command) govern its plan/status from then on.
"""
from django.core.management.base import BaseCommand
from django.utils import timezone

from src import models as src_models


class Command(BaseCommand):
    help = "Demote companies whose manually-granted trial period has expired back to Scout."

    def handle(self, *args, **options):
        expired = src_models.Company.objects.filter(
            manual_trial_granted_at__isnull=False,
            subscription_period_end__lt=timezone.now(),
        )

        count = 0
        for company in expired:
            self.stdout.write(
                "Demoting company_id={} ({!r}) -- {} trial expired {}".format(
                    company.id, company.name, company.subscription_plan, company.subscription_period_end
                )
            )
            count += 1

        expired.update(
            subscription_plan=None,
            subscription_status=None,
            subscription_period_end=None,
            manual_trial_granted_at=None,
        )
        self.stdout.write(self.style.SUCCESS("Demoted {} compan{} to Scout.".format(
            count, "y" if count == 1 else "ies"
        )))
