"""
Verifies emails from qualified leads using Reoon bulk email verification API (Power mode).

Flow:
  1. Collect all unverified emails from qualified leads
  2. Submit them as a bulk task to Reoon (up to 50k per task)
  3. Poll until the task completes
  4. Save results to the lead_email table (one row per email)

Usage:
  python manage.py verify_lead_emails                  # all qualified leads with emails, not yet verified
  python manage.py verify_lead_emails --reverify       # re-verify already verified emails
  python manage.py verify_lead_emails --state TX       # filter by state
  python manage.py verify_lead_emails --limit 500      # process at most N leads

Reoon API docs: https://www.reoon.com/articles/api-documentation-of-reoon-email-verifier/
"""
import time
from datetime import datetime, timezone

import requests
from django.conf import settings
from django.core.management.base import BaseCommand

from src.models import Lead, LeadEmail, RealTruckLead, RealTruckLeadEmail

# Each source has its own lead table and its own verified-email table.
SOURCES = {
    "google": (Lead, LeadEmail),
    "realtruck": (RealTruckLead, RealTruckLeadEmail),
}

REOON_CREATE_URL = "https://emailverifier.reoon.com/api/v1/create-bulk-verification-task/"
REOON_RESULT_URL = "https://emailverifier.reoon.com/api/v1/get-result-bulk-verification-task/"
REOON_BALANCE_URL = "https://emailverifier.reoon.com/api/v1/check-account-balance/"

POLL_INTERVAL = 10    # seconds between status checks
MAX_EMAILS_PER_TASK = 50_000
BATCH_SIZE = 200



# Reoon's per-status booleans vary by plan and status, and a naive `a or b or c` chain silently
# turns an explicit False into None -- which recorded every invalid address as "unknown" and made
# the verification useless for filtering. Read booleans with an explicit None check, and fall back
# to the status string, which is always present.
DELIVERABLE_STATUSES = {"safe", "valid", "deliverable"}
UNDELIVERABLE_STATUSES = {"invalid", "disabled", "spamtrap", "disposable", "undeliverable", "bounce"}


def _first_bool(data: dict, *keys):
    """First key whose value is an actual bool; None if none are present."""
    for k in keys:
        v = data.get(k)
        if isinstance(v, bool):
            return v
    return None


def _derive_valid(data: dict, status: str | None):
    """True = safe to send, False = known bad, None = genuinely uncertain (catch_all/unknown)."""
    explicit = _first_bool(data, "is_safe_to_send", "is_valid_email", "is_valid")
    if explicit is not None:
        return explicit
    s = (status or "").lower()
    if s in DELIVERABLE_STATUSES:
        return True
    if s in UNDELIVERABLE_STATUSES:
        return False
    return None   # catch_all, role_account, unknown -- deliverability not established


class Command(BaseCommand):
    help = "Verify lead emails in bulk using Reoon API (Power mode)"

    def add_arguments(self, parser):
        parser.add_argument("--source", default="google", choices=sorted(SOURCES),
                            help="Which lead table's emails to verify")
        parser.add_argument("--state", default=None, help="Filter by state code (e.g. TX)")
        parser.add_argument("--reverify", action="store_true", help="Re-verify already verified emails")
        parser.add_argument("--limit", type=int, default=None, help="Max leads to process")

    def handle(self, *args, **options):
        api_key = getattr(settings, "REOON_API_KEY", "")
        if not api_key:
            self.stdout.write(self.style.ERROR("REOON_API_KEY is not set in .env"))
            return

        # Check balance first
        self._check_balance(api_key)

        # Collect emails to verify
        lead_model, email_model = SOURCES[options["source"]]
        self.email_model = email_model
        qs = lead_model.objects.filter(is_qualified=True).exclude(emails=[])
        if options["state"]:
            qs = qs.filter(state=options["state"].upper())
        if options["limit"]:
            qs = qs[:options["limit"]]

        leads = list(qs.only(lead_model._meta.pk.name, "name", "website", "emails"))

        # Build (lead_id, email) pairs, skip already verified unless --reverify
        already_verified = set()
        if not options["reverify"]:
            already_verified = set(
                email_model.objects.filter(
                    lead_id__in=[l.pk for l in leads],
                    verified_at__isnull=False,
                ).values_list("lead_id", "email")
            )

        # email -> lead_id mapping for saving results later
        email_to_lead: dict[str, int] = {}
        for lead in leads:
            for email in (lead.emails or []):
                if (lead.pk, email) not in already_verified:
                    email_to_lead[email] = lead.pk

        all_emails = list(email_to_lead.keys())
        total = len(all_emails)

        if not total:
            self.stdout.write("No emails to verify.")
            return

        self.stdout.write(f"Found {total} emails to verify across {len(leads)} leads.\n")

        # Split into chunks of 50k and process each as a separate task
        chunks = [all_emails[i:i + MAX_EMAILS_PER_TASK] for i in range(0, total, MAX_EMAILS_PER_TASK)]
        self.stdout.write(f"Submitting {len(chunks)} bulk task(s)...\n")

        for chunk_idx, chunk in enumerate(chunks, 1):
            self.stdout.write(f"--- Task {chunk_idx}/{len(chunks)}: {len(chunk)} emails ---")
            self._run_task(api_key, chunk, email_to_lead, chunk_idx)

        self.stdout.write(self.style.SUCCESS("\nAll tasks complete."))

    # ------------------------------------------------------------------

    def _check_balance(self, api_key):
        try:
            resp = requests.get(REOON_BALANCE_URL, params={"key": api_key}, timeout=10)
            data = resp.json()
            daily = data.get("remaining_daily_credits", "?")
            instant = data.get("remaining_instant_credits", "?")
            self.stdout.write(f"Reoon balance — daily credits: {daily}  instant credits: {instant}\n")
        except Exception as e:
            self.stdout.write(self.style.WARNING(f"Could not check balance: {e}\n"))

    def _run_task(self, api_key: str, emails: list[str], email_to_lead: dict, chunk_idx: int):
        # Step 1: Submit task
        self.stdout.write(f"  Submitting task...")
        try:
            resp = requests.post(
                REOON_CREATE_URL,
                json={"name": f"leads-{chunk_idx}", "emails": emails, "key": api_key},
                timeout=30,
            )
            data = resp.json()
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"  Failed to submit task: {e}"))
            return

        if data.get("status") != "success":
            self.stdout.write(self.style.ERROR(f"  Task creation failed: {data}"))
            return

        task_id = data["task_id"]
        submitted = data.get("count_submitted", len(emails))
        self.stdout.write(self.style.SUCCESS(f"  Task created: id={task_id}  submitted={submitted}"))

        # Step 2: Poll until complete
        self.stdout.write(f"  Polling every {POLL_INTERVAL}s...")
        while True:
            time.sleep(POLL_INTERVAL)
            try:
                resp = requests.get(
                    REOON_RESULT_URL,
                    params={"key": api_key, "task_id": task_id},
                    timeout=30,
                )
                result = resp.json()
            except Exception as e:
                self.stdout.write(self.style.WARNING(f"  Poll error: {e}, retrying..."))
                continue

            status = result.get("status")
            progress = result.get("progress_percentage", 0)
            self.stdout.write(f"  Status: {status}  Progress: {progress}%")

            if status == "completed":
                break
            elif status == "file_not_found":
                self.stdout.write(self.style.ERROR(f"  Task not found: {task_id}"))
                return

        # Step 3: Save results
        self.stdout.write("  Saving results to DB...")
        results = result.get("results", {})
        self._save_results(results, email_to_lead)

    def _save_results(self, results: dict, email_to_lead: dict):
        valid_count = 0
        invalid_count = 0
        unknown_count = 0
        batch = []
        now = datetime.now(timezone.utc)

        for email, data in results.items():
            lead_id = email_to_lead.get(email.lower()) or email_to_lead.get(email)
            if not lead_id:
                continue

            status = data.get("status")
            is_valid = _derive_valid(data, status)

            if is_valid:
                valid_count += 1
            elif is_valid is False:
                invalid_count += 1
            else:
                unknown_count += 1

            batch.append(self.email_model(
                lead_id=lead_id,
                email=email.lower(),
                status=status,
                is_valid=is_valid,
                is_disposable=_first_bool(data, "is_disposable", "is_disposable_email")
                               or (status == "disposable" or None),
                is_free_email=_first_bool(data, "is_free_email"),
                is_role_based=_first_bool(data, "is_role_based", "is_role_based_email")
                               or (status == "role_account" or None),
                mx_found=_first_bool(data, "mx_accepts_mail", "mx_found"),
                verified_at=now,
            ))

            if len(batch) >= BATCH_SIZE:
                self._flush(batch)
                batch.clear()

        if batch:
            self._flush(batch)

        self.stdout.write(self.style.SUCCESS(
            f"  Saved — Valid: {valid_count}  Invalid: {invalid_count}  Unknown: {unknown_count}"
        ))

    def _flush(self, batch: list):
        self.email_model.objects.bulk_create(
            batch,
            update_conflicts=True,
            unique_fields=["lead_id", "email"],
            update_fields=["status", "is_valid", "is_disposable", "is_free_email",
                           "is_role_based", "mx_found", "verified_at"],
        )
