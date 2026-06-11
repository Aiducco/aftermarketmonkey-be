"""
Unravels Lead.emails JSON arrays into individual LeadEmail rows,
then uses Claude Haiku to batch-validate them (50 per API call).

Stage 1 — expand:
  For every lead with a non-empty emails array, creates one LeadEmail row
  per address (skips duplicates via unique_together).

Stage 2 — AI validation:
  Sends batches of 50 emails to Claude Haiku and asks it to flag each one
  as valid (real business contact) or invalid (noreply, system, junk, etc.).
  Saves result to LeadEmail.ai_valid.

Usage:
  python manage.py expand_lead_emails                 # expand + validate all new
  python manage.py expand_lead_emails --expand-only   # only create rows, skip Claude
  python manage.py expand_lead_emails --validate-only # only run Claude on unvalidated rows
  python manage.py expand_lead_emails --revalidate    # re-run Claude even if already validated
  python manage.py expand_lead_emails --limit 500
"""
import json
import re

from django.core.management.base import BaseCommand

from src.models import Lead, LeadEmail

BATCH_SIZE = 50   # emails per Claude call


# ------------------------------------------------------------------
# Claude batch validation
# ------------------------------------------------------------------

def _validate_batch(emails: list[str], anthropic_client) -> dict[str, bool]:
    """
    Send up to BATCH_SIZE emails to Claude and get back {email: True/False}.
    True  = looks like a real business contact email
    False = system / noreply / generic junk
    """
    numbered = "\n".join(f"{i+1}. {e}" for i, e in enumerate(emails))
    prompt = (
        "For each email address below, decide if it is a real business contact email "
        "(someone would read it and reply) or a system/junk email to skip.\n\n"
        "Mark FALSE for: noreply@, no-reply@, donotreply@, admin@, webmaster@, "
        "postmaster@, abuse@, bounce@, mailer-daemon@, test@, example@, "
        "anything that is clearly auto-generated or a placeholder.\n"
        "Mark TRUE for: info@, sales@, contact@, service@, support@ when it belongs "
        "to a real named business, and any personal-looking business emails.\n\n"
        f"Emails:\n{numbered}\n\n"
        "Reply with ONLY a JSON object mapping each email to true or false. "
        'Example: {"info@shop.com": true, "noreply@shop.com": false}'
    )

    try:
        response = anthropic_client.messages.create(
            model="claude-haiku-4-5",
            max_tokens=BATCH_SIZE * 30,   # ~30 tokens per email in the result
            messages=[{"role": "user", "content": prompt}],
        )
        text = response.content[0].text.strip()
        # Extract the first {...} block
        start = text.find('{')
        if start == -1:
            return {}
        result, _ = json.JSONDecoder().raw_decode(text, start)
        if isinstance(result, dict):
            return {k.lower(): bool(v) for k, v in result.items()}
    except Exception as e:
        print(f"  [claude error] {type(e).__name__}: {e}")
    return {}


# ------------------------------------------------------------------
# Command
# ------------------------------------------------------------------

class Command(BaseCommand):
    help = "Expand Lead.emails into LeadEmail rows and AI-validate them with Claude Haiku"

    def add_arguments(self, parser):
        parser.add_argument("--expand-only",   action="store_true", help="Only create LeadEmail rows, skip validation")
        parser.add_argument("--validate-only", action="store_true", help="Only run Claude validation, skip expansion")
        parser.add_argument("--revalidate",    action="store_true", help="Re-run Claude even on already-validated rows")
        parser.add_argument("--limit",         type=int, default=None, help="Max leads to expand (stage 1 only)")

    def handle(self, *args, **options):
        def _get_key(name):
            from django.conf import settings
            val = getattr(settings, name, "")
            if not val:
                try:
                    from dotenv import dotenv_values, find_dotenv
                    path = find_dotenv(usecwd=True)
                    if path:
                        val = dotenv_values(path).get(name, "")
                except Exception:
                    pass
            return val

        # ------------------------------------------------------------------
        # Stage 1: expand
        # ------------------------------------------------------------------
        if not options["validate_only"]:
            self.stdout.write("Stage 1: expanding Lead.emails → LeadEmail rows...")

            qs = Lead.objects.exclude(emails=[]).exclude(emails__isnull=True)
            if options["limit"]:
                qs = qs[:options["limit"]]

            leads = list(qs.only("id", "emails"))

            to_insert = []
            for lead in leads:
                for email in (lead.emails or []):
                    if isinstance(email, str) and "@" in email:
                        to_insert.append(LeadEmail(lead_id=lead.id, email=email.lower()))

            # Bulk insert — ignore rows that already exist (unique_together constraint)
            created_objs = LeadEmail.objects.bulk_create(
                to_insert,
                ignore_conflicts=True,
                batch_size=500,
            )
            self.stdout.write(self.style.SUCCESS(
                f"  Inserted: {len(created_objs)}  Total candidates: {len(to_insert)}  Leads: {len(leads)}"
            ))

        if options["expand_only"]:
            return

        # ------------------------------------------------------------------
        # Stage 2: Claude batch validation
        # ------------------------------------------------------------------
        anthropic_key = _get_key("ANTHROPIC_API_KEY")
        if not anthropic_key:
            self.stdout.write(self.style.ERROR("ANTHROPIC_API_KEY not set — skipping validation"))
            return

        try:
            import anthropic
            client = anthropic.Anthropic(api_key=anthropic_key)
        except ImportError:
            self.stdout.write(self.style.ERROR("anthropic package not installed"))
            return

        self.stdout.write("\nStage 2: Claude Haiku batch validation...")

        qs = LeadEmail.objects.all()
        if not options["revalidate"]:
            qs = qs.filter(ai_valid__isnull=True)

        rows = list(qs.only("id", "email"))
        total = len(rows)

        if not total:
            self.stdout.write("  No emails to validate.")
            return

        self.stdout.write(f"  Validating {total} emails in batches of {BATCH_SIZE}...")

        valid_count   = 0
        invalid_count = 0
        error_count   = 0

        for batch_start in range(0, total, BATCH_SIZE):
            batch_rows  = rows[batch_start:batch_start + BATCH_SIZE]
            batch_emails = [r.email for r in batch_rows]

            results = _validate_batch(batch_emails, client)

            if not results:
                error_count += len(batch_rows)
                self.stdout.write(self.style.WARNING(
                    f"  [{batch_start + len(batch_rows)}/{total}] batch failed — skipping"
                ))
                continue

            updates = []
            for row in batch_rows:
                ai_valid = results.get(row.email)
                if ai_valid is None:
                    # Claude didn't return this email — default to True (don't discard)
                    ai_valid = True
                row.ai_valid = ai_valid
                updates.append(row)
                if ai_valid:
                    valid_count += 1
                else:
                    invalid_count += 1

            LeadEmail.objects.bulk_update(updates, ["ai_valid"])
            self.stdout.write(
                f"  [{min(batch_start + BATCH_SIZE, total)}/{total}] "
                f"valid={valid_count}  invalid={invalid_count}"
            )

        self.stdout.write(self.style.SUCCESS(
            f"\nDone.\n"
            f"  Valid   : {valid_count}\n"
            f"  Invalid : {invalid_count}\n"
            f"  Errors  : {error_count}\n"
            f"  Total   : {total}"
        ))
