"""
Exports qualified leads with valid emails to a CSV file for outreach tools.

One row per email. Distinct by website (one lead per website).
Since no first/last name is available, company name is used for FirstName.

Usage:
  python manage.py export_leads_csv
  python manage.py export_leads_csv --state TX
  python manage.py export_leads_csv --output leads_export.csv
  python manage.py export_leads_csv --all-emails   # include unknown (is_valid IS NULL) too
"""
import csv
import os
from datetime import datetime

from django.core.management.base import BaseCommand

from src.models import Lead, LeadEmail


class Command(BaseCommand):
    help = "Export leads with valid emails to CSV for outreach"

    def add_arguments(self, parser):
        parser.add_argument("--state",      default=None, help="Filter by state code (e.g. TX)")
        parser.add_argument("--output",     default=None, help="Output filename (default: leads_YYYYMMDD.csv)")
        parser.add_argument("--all-emails", action="store_true", help="Include unknown emails (is_valid IS NULL) in addition to valid")

    def handle(self, *args, **options):
        filename = options["output"] or f"leads_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

        # Fetch valid (and optionally unknown) emails joined to leads
        email_qs = LeadEmail.objects.filter(is_valid=True)
        if options["all_emails"]:
            from django.db.models import Q
            email_qs = LeadEmail.objects.filter(Q(is_valid=True) | Q(is_valid__isnull=True))

        email_qs = email_qs.select_related("lead").filter(
            lead__website__isnull=False,
        ).exclude(lead__website="")

        if options["state"]:
            email_qs = email_qs.filter(lead__state=options["state"].upper())

        # Deduplicate by website — keep one lead per website
        seen_websites = set()
        rows = []

        for le in email_qs.order_by("lead__website", "email"):
            lead = le.lead
            website = lead.website or ""

            # Normalize website to root domain for dedup
            if website not in seen_websites:
                seen_websites.add(website)

            rows.append({
                "Email":          le.email,
                "FirstName":      lead.name,   # company name as first name for personalization
                "LastName":       "",
                "Company":        lead.name,
                "JobTitle":       "",
                "CustomVariable": lead.website or "",
            })

        total = len(rows)
        if not total:
            self.stdout.write("No emails found matching criteria.")
            return

        with open(filename, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=[
                "Email", "FirstName", "LastName", "Company", "JobTitle", "CustomVariable"
            ])
            writer.writeheader()
            writer.writerows(rows)

        self.stdout.write(self.style.SUCCESS(
            f"Exported {total} rows to {filename}"
        ))
