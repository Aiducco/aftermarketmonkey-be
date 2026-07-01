"""
Provision relay SFTP accounts for companies that don't have one yet.

Runs in the background (via cron), not during registration, so signup never blocks on an SSH
round trip to the relay server. Safe to run frequently — idempotent per company.

Usage:
    python manage.py provision_company_sftp_accounts
"""

from django.core.management.base import BaseCommand
from django.db.models import Q

from src import models as src_models
from src.integrations.services import relay_sftp_provisioning


class Command(BaseCommand):
    help = "Provision a dedicated relay SFTP account for any company that doesn't have one yet."

    def handle(self, *args, **options):
        companies = src_models.Company.objects.filter(
            Q(relay_sftp_username__isnull=True) | Q(relay_sftp_username="")
        )
        total = companies.count()
        if not total:
            self.stdout.write("No companies pending SFTP provisioning.")
            return

        self.stdout.write("Provisioning relay SFTP accounts for {} company(ies)...".format(total))
        succeeded = 0
        failed = 0
        for company in companies:
            try:
                username = relay_sftp_provisioning.provision_company_sftp_account(company)
                if username:
                    succeeded += 1
                    self.stdout.write(self.style.SUCCESS(
                        "  company_id={} ({}): provisioned as '{}'".format(company.id, company.name, username)
                    ))
            except Exception as e:
                failed += 1
                self.stderr.write(self.style.ERROR(
                    "  company_id={} ({}): FAILED — {}".format(company.id, company.name, e)
                ))

        self.stdout.write(self.style.SUCCESS(
            "Done. Provisioned: {}, failed: {}.".format(succeeded, failed)
        ))
