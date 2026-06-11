"""
Enriches leads that are missing website, phone, or email by:

  1. Calling Google Places Details API using the stored place_id
     (gets website + phone directly from Google Business Profile)
  2. Once a website is found, scrapes it for email addresses

Only processes leads where at least one of website/phone is missing.

Usage:
  python manage.py enrich_missing_leads                  # all leads missing website or phone
  python manage.py enrich_missing_leads --state TX       # filter by state
  python manage.py enrich_missing_leads --limit 100      # process at most N leads
  python manage.py enrich_missing_leads --workers 5      # parallel workers (default: 5)
"""
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from django.conf import settings
from django.core.management.base import BaseCommand
from django.db.models import Q

from src.models import Lead

PLACES_DETAILS_URL = "https://maps.googleapis.com/maps/api/place/details/json"
DETAILS_FIELDS = "website,formatted_phone_number"

SCRAPE_PATHS = ["", "/contact", "/contact-us", "/about", "/about-us"]
SCRAPE_TIMEOUT = 8
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")
EMAIL_IGNORE_DOMAINS = {
    "gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "icloud.com",
    "example.com", "wix.com", "squarespace.com", "wordpress.com", "sentry.io",
}
EMAIL_IGNORE_DOMAIN_SUFFIXES = (".wixpress.com", ".sentry.io", ".cloudflare.com")
_HASH_RE = re.compile(r'^[0-9a-f]{12,}$')


def _scrape_emails(website_url: str) -> list[str]:
    """Scrape multiple pages and return all valid unique emails found."""
    base = website_url.rstrip("/")
    found = []
    seen = set()

    for path in SCRAPE_PATHS:
        try:
            resp = requests.get(base + path, timeout=SCRAPE_TIMEOUT, headers=HEADERS, allow_redirects=True)
            if resp.status_code != 200:
                continue
            for email in EMAIL_RE.findall(resp.text):
                email = email.lower()
                local, domain = email.rsplit("@", 1)
                if domain in EMAIL_IGNORE_DOMAINS:
                    continue
                if any(domain.endswith(s) for s in EMAIL_IGNORE_DOMAIN_SUFFIXES):
                    continue
                if _HASH_RE.match(local):
                    continue
                if any(email.endswith(ext) for ext in (".png", ".jpg", ".gif", ".svg", ".webp")):
                    continue
                if email not in seen:
                    seen.add(email)
                    found.append(email)
        except Exception:
            continue

    return found


def _enrich_lead(lead, api_key: str) -> dict:
    """
    Calls Places Details for website + phone, then scrapes email.
    Returns a dict of fields to update (only non-empty values).
    """
    updates = {}

    # --- Step 1: Google Places Details ---
    if lead.place_id:
        try:
            resp = requests.get(
                PLACES_DETAILS_URL,
                params={"place_id": lead.place_id, "fields": DETAILS_FIELDS, "key": api_key},
                timeout=15,
            )
            data = resp.json()
            if data.get("status") == "OK":
                result = data.get("result", {})
                if not lead.website and result.get("website"):
                    updates["website"] = result["website"]
                if not lead.phone and result.get("formatted_phone_number"):
                    updates["phone"] = result["formatted_phone_number"]
        except Exception:
            pass

        time.sleep(0.2)

    # --- Step 2: Scrape email from website ---
    website = updates.get("website") or lead.website
    if website and not lead.email and not lead.emails:
        emails = _scrape_emails(website)
        if emails:
            updates["email"] = emails[0]
            updates["emails"] = emails

    return updates


class Command(BaseCommand):
    help = "Enrich leads missing website/phone/email using Google Places API + web scraping"

    def add_arguments(self, parser):
        parser.add_argument("--state", default=None, help="Filter by state code (e.g. TX)")
        parser.add_argument("--limit", type=int, default=None, help="Max leads to process")
        parser.add_argument("--workers", type=int, default=5, help="Parallel workers (default: 5)")

    def handle(self, *args, **options):
        api_key = getattr(settings, "GOOGLE_PLACES_API_KEY", "")
        if not api_key:
            self.stdout.write(self.style.ERROR("GOOGLE_PLACES_API_KEY is not set in .env"))
            return

        # Leads missing at least website or phone
        qs = Lead.objects.filter(
            Q(website__isnull=True) | Q(website="") |
            Q(phone__isnull=True) | Q(phone="")
        ).exclude(place_id__isnull=True).exclude(place_id="")

        if options["state"]:
            qs = qs.filter(state=options["state"].upper())
        if options["limit"]:
            qs = qs[:options["limit"]]

        leads = list(qs.only("id", "name", "place_id", "website", "phone", "email", "emails", "city", "state"))
        total = len(leads)

        if not total:
            self.stdout.write("No leads to enrich.")
            return

        workers = options["workers"]
        self.stdout.write(f"Enriching {total} leads [{workers} workers]...\n")

        enriched = 0
        unchanged = 0
        BATCH_SIZE = 50
        pending: list[tuple[int, dict]] = []

        def flush(pending):
            for lead_id, updates in pending:
                Lead.objects.filter(pk=lead_id).update(**updates)

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(_enrich_lead, lead, api_key): lead for lead in leads}

            for i, future in enumerate(as_completed(futures), 1):
                lead = futures[future]
                try:
                    updates = future.result()
                except Exception as e:
                    self.stdout.write(self.style.WARNING(f"  [{i}/{total}] ERROR  {lead.name}  {e}"))
                    unchanged += 1
                    continue

                if updates:
                    enriched += 1
                    parts = []
                    if "website" in updates:
                        parts.append(f"website={updates['website']}")
                    if "phone" in updates:
                        parts.append(f"phone={updates['phone']}")
                    if "email" in updates:
                        parts.append(f"email={updates['email']}")
                    self.stdout.write(
                        self.style.SUCCESS(f"  [{i}/{total}] ✓  {lead.name} ({lead.city}, {lead.state})")
                    )
                    for p in parts:
                        self.stdout.write(f"           {p}")
                    pending.append((lead.id, updates))
                else:
                    unchanged += 1
                    self.stdout.write(f"  [{i}/{total}] —  {lead.name}  (nothing found)")

                if len(pending) >= BATCH_SIZE:
                    flush(pending)
                    self.stdout.write(f"  -- saved {len(pending)} to DB --")
                    pending.clear()

        if pending:
            flush(pending)
            self.stdout.write(f"  -- saved {len(pending)} to DB --")

        self.stdout.write(self.style.SUCCESS(
            f"\nDone.  Enriched: {enriched}  No data found: {unchanged}  Total: {total}"
        ))
