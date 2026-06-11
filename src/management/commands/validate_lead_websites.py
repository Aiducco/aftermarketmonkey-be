"""
Checks whether each lead's website is live by making a HEAD request (falls back
to GET if HEAD is not supported). Saves the result in the `website_live` field.

  True  — site responded with a 2xx or 3xx status code
  False — connection error, timeout, 4xx/5xx, or DNS failure
  None  — not checked yet (default)

Usage:
  python manage.py validate_lead_websites                  # only unchecked leads (website_live=None)
  python manage.py validate_lead_websites --recheck        # re-check all leads with a website
  python manage.py validate_lead_websites --state TX       # filter by state
  python manage.py validate_lead_websites --limit 200      # process at most N leads
"""
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from django.conf import settings
from django.core.management.base import BaseCommand

from src.models import Lead

WORKERS = 30
BATCH_SIZE = 200
TIMEOUT = 10
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; LeadBot/1.0)"}


def _build_proxies(gateway: str, username: str = "", password: str = "") -> dict:
    if username and password:
        from urllib.parse import quote
        host = f"http://{quote(username, safe='')}:{quote(password, safe='')}@{gateway.replace('http://', '')}"
    else:
        host = gateway if "://" in gateway else f"http://{gateway}"
    return {"http": host, "https": host}


def _check(lead, proxies: dict) -> tuple:
    """Returns (lead_id, is_live, status_code, error)."""
    url = lead.website
    try:
        resp = requests.head(url, timeout=TIMEOUT, headers=HEADERS, allow_redirects=True, proxies=proxies)
        if resp.status_code < 400:
            return lead.id, True, resp.status_code, None
        # Some servers reject HEAD — fall back to GET
        if resp.status_code in (405, 501):
            resp = requests.get(url, timeout=TIMEOUT, headers=HEADERS, allow_redirects=True, stream=True, proxies=proxies)
            resp.close()
            return lead.id, resp.status_code < 400, resp.status_code, None
        return lead.id, False, resp.status_code, None
    except requests.exceptions.ConnectionError as e:
        return lead.id, False, None, f"ConnectionError: {e}"
    except requests.exceptions.Timeout:
        return lead.id, False, None, "Timeout"
    except Exception as e:
        return lead.id, False, None, str(e)


def _bulk_update(results: list[tuple]) -> None:
    """Bulk update website_live for a batch of (lead_id, is_live) tuples."""
    live_ids = [lead_id for lead_id, live, *_ in results if live]
    dead_ids = [lead_id for lead_id, live, *_ in results if not live]
    if live_ids:
        Lead.objects.filter(pk__in=live_ids).update(website_live=True)
    if dead_ids:
        Lead.objects.filter(pk__in=dead_ids).update(website_live=False)


class Command(BaseCommand):
    help = "Check if lead websites are live and update the website_live flag"

    def add_arguments(self, parser):
        parser.add_argument("--state", default=None, help="Filter by state code (e.g. TX)")
        parser.add_argument("--recheck", action="store_true", help="Re-check leads even if already verified")
        parser.add_argument("--limit", type=int, default=None, help="Max number of leads to process")

    def handle(self, *args, **options):
        qs = Lead.objects.filter(website__isnull=False).exclude(website="")

        if options["state"]:
            qs = qs.filter(state=options["state"].upper())

        if not options["recheck"]:
            qs = qs.filter(website_live__isnull=True)

        if options["limit"]:
            qs = qs[:options["limit"]]

        leads = list(qs.only("id", "website"))
        total = len(leads)

        if not total:
            self.stdout.write("No leads to check.")
            return

        gateway = getattr(settings, "STORM_PROXY_GATEWAY_SCRAPE", "")
        username = getattr(settings, "STORM_PROXY_ACCOUNT_NAME", "")
        password = getattr(settings, "STORM_PROXY_PASSWORD", "")
        proxies = _build_proxies(gateway, username, password) if gateway else {}
        proxy_label = f"{username}@{gateway}" if (gateway and username) else (gateway or "no proxy")
        self.stdout.write(f"Checking {total} websites [{WORKERS} workers, batch {BATCH_SIZE}, proxy: {proxy_label}]...\n")

        live_count = 0
        dead_count = 0
        pending_batch: list[tuple] = []

        with ThreadPoolExecutor(max_workers=WORKERS) as executor:
            futures = {executor.submit(_check, lead, proxies): lead for lead in leads}

            for i, future in enumerate(as_completed(futures), 1):
                lead = futures[future]
                try:
                    lead_id, live, status_code, error = future.result()
                except Exception as e:
                    lead_id, live, status_code, error = lead.id, False, None, str(e)

                # Log
                if live:
                    live_count += 1
                    self.stdout.write(
                        self.style.SUCCESS(f"  [{i}/{total}] LIVE  ({status_code})  {lead.website}")
                    )
                else:
                    dead_count += 1
                    detail = f"status={status_code}" if status_code else error or "unknown"
                    self.stdout.write(
                        self.style.ERROR(f"  [{i}/{total}] DEAD  ({detail})  {lead.website}")
                    )

                pending_batch.append((lead_id, live, status_code, error))

                # Flush batch to DB
                if len(pending_batch) >= BATCH_SIZE:
                    _bulk_update(pending_batch)
                    self.stdout.write(f"  -- flushed {len(pending_batch)} records to DB --")
                    pending_batch.clear()

        # Final flush
        if pending_batch:
            _bulk_update(pending_batch)
            self.stdout.write(f"  -- flushed {len(pending_batch)} records to DB --")

        self.stdout.write(
            self.style.SUCCESS(
                f"\nDone. Live: {live_count}  Dead/unreachable: {dead_count}  Total: {total}"
            )
        )
