"""
Scrapes emails from lead websites and saves them to the `emails` JSON field.

Stage 1 — direct scraping:
  Tries homepage + /contact, /contact-us, /about, /about-us, /company, /team, /reach-us, /info

Stage 2 — Tavily + Claude fallback (if scraping finds nothing):
  Searches Tavily for "{name} {city} {state} email contact", then asks Claude Haiku
  to extract any business email addresses visible in the search snippets.

Usage:
  python manage.py enrich_lead_emails                   # all leads with website, no emails yet
  python manage.py enrich_lead_emails --live-only       # only website_live=TRUE leads
  python manage.py enrich_lead_emails --refetch         # re-scrape even if emails already found
  python manage.py enrich_lead_emails --state TX
  python manage.py enrich_lead_emails --limit 100
  python manage.py enrich_lead_emails --workers 20
  python manage.py enrich_lead_emails --no-tavily       # disable Tavily fallback
"""
import json
import logging
import re
import time
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse

import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logging.getLogger("httpx").setLevel(logging.WARNING)

from django.core.management.base import BaseCommand

from src.models import Lead

TAVILY_SEARCH_URL = "https://api.tavily.com/search"

EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")

EMAIL_IGNORE_DOMAINS = {
    "gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "icloud.com",
    "aol.com", "live.com", "msn.com", "protonmail.com",
    "wix.com", "squarespace.com", "wordpress.com", "weebly.com", "shopify.com",
    "webflow.io", "godaddy.com",
    "sentry.io", "sentry-next.wixpress.com",
    "example.com", "domain.com", "email.com", "test.com",
}

EMAIL_IGNORE_DOMAIN_SUFFIXES = (
    ".wixpress.com", ".sentry.io", ".cloudflare.com", ".amazonaws.com",
    ".googleusercontent.com", ".wpengine.com", ".hubspot.com",
    ".mailchimp.com", ".sendgrid.net", ".klaviyo.com",
)

EMAIL_IGNORE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp", ".pdf", ".zip"}

_HASH_RE = re.compile(r'^[0-9a-f]{12,}$')
_UUID_RE = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')

SCRAPE_PATHS = [
    "", "/contact", "/contact-us", "/contactus",
    "/about", "/about-us", "/aboutus",
    "/company", "/team", "/reach-us", "/info",
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}
TIMEOUT = 8
WORKERS = 10


# ------------------------------------------------------------------
# Email helpers
# ------------------------------------------------------------------

def _is_valid_email(email: str) -> bool:
    email = email.lower()
    if "@" not in email:
        return False
    local, domain = email.rsplit("@", 1)
    if domain in EMAIL_IGNORE_DOMAINS:
        return False
    if any(domain.endswith(s) for s in EMAIL_IGNORE_DOMAIN_SUFFIXES):
        return False
    if _HASH_RE.match(local) or _UUID_RE.match(local):
        return False
    if any(email.endswith(ext) for ext in EMAIL_IGNORE_EXTENSIONS):
        return False
    return True


def _extract_emails(text: str) -> list[str]:
    seen, result = set(), []
    for em in EMAIL_RE.findall(text):
        em = em.lower()
        if _is_valid_email(em) and em not in seen:
            seen.add(em)
            result.append(em)
    return result


# ------------------------------------------------------------------
# Stage 1: direct website scraping
# ------------------------------------------------------------------

def _scrape_website(website: str) -> list[str]:
    parsed = urlparse(website)
    base_url = f"{parsed.scheme}://{parsed.netloc}".rstrip("/")
    all_emails: list[str] = []
    seen: set[str] = set()

    for path in SCRAPE_PATHS:
        try:
            resp = requests.get(
                base_url + path, timeout=TIMEOUT, headers=HEADERS,
                allow_redirects=True, verify=False,
            )
            if resp.status_code != 200:
                continue
            for em in _extract_emails(resp.text):
                if em not in seen:
                    seen.add(em)
                    all_emails.append(em)
        except Exception:
            continue
        finally:
            time.sleep(0.05)

    return all_emails


# ------------------------------------------------------------------
# Stage 2: Tavily search + Claude Haiku extraction
# ------------------------------------------------------------------

def _tavily_search_emails(lead, tavily_key: str) -> list[dict]:
    """Search Tavily for contact info and return raw results."""
    domain = urlparse(lead.website).netloc.lstrip("www.")
    queries = [
        f'"{lead.name}" {lead.city} {lead.state} email contact',
        f'site:{domain} email contact',
    ]
    seen_urls, results = set(), []
    for query in queries:
        try:
            resp = requests.post(
                TAVILY_SEARCH_URL,
                json={
                    "api_key": tavily_key,
                    "query": query,
                    "max_results": 5,
                    "search_depth": "basic",
                },
                timeout=15,
            )
            if resp.status_code == 200:
                for r in resp.json().get("results", []):
                    url = r.get("url", "")
                    if url not in seen_urls:
                        seen_urls.add(url)
                        results.append(r)
            else:
                print(f"  [tavily error] HTTP {resp.status_code} for query: {query!r}")
        except Exception as e:
            print(f"  [tavily error] {type(e).__name__}: {e} for query: {query!r}")
    return results[:8]


def _claude_extract_emails(lead, results: list[dict], anthropic_client) -> list[str]:
    """Ask Claude Haiku to pull business emails out of Tavily snippets."""
    if not results:
        print(f"  [tavily] no results for {lead.name} ({lead.city}, {lead.state})")
        return []
    if not anthropic_client:
        return []

    snippets = []
    for r in results:
        snippets.append(
            f"URL: {r.get('url', '')}\n"
            f"Title: {r.get('title', '')}\n"
            f"Snippet: {r.get('content', '')[:300]}"
        )

    prompt = (
        f"Business: {lead.name}, {lead.city}, {lead.state}\n"
        f"Website: {lead.website}\n\n"
        f"Search results:\n\n"
        + "\n\n".join(snippets)
        + "\n\n"
        f"Extract any BUSINESS email addresses for this specific company from the snippets above.\n"
        f"Rules:\n"
        f"- Only include emails that belong to this business (match the domain or are clearly their contact)\n"
        f"- Exclude Gmail, Yahoo, Hotmail and other free email providers\n"
        f"- Exclude tracking/system emails (noreply, no-reply, etc.)\n"
        f"- If no valid business email is found, return empty array\n\n"
        f"Reply with ONLY a JSON array of email strings, e.g.: [\"info@example.com\"] or []"
    )

    # NOTE: let API errors (RateLimitError, APIError, etc.) bubble up so the
    # caller does NOT mark the lead as emails_not_found — it will be retried.
    response = anthropic_client.messages.create(
        model="claude-haiku-4-5",
        max_tokens=400,
        messages=[{"role": "user", "content": prompt}],
    )
    text = response.content[0].text.strip()
    # Use raw_decode so we stop at the exact end of the JSON array and ignore
    # any trailing prose Claude adds (e.g. "Note: [1] source").
    start = text.find('[')
    if start == -1:
        return []
    try:
        emails, _ = json.JSONDecoder().raw_decode(text, start)
        if isinstance(emails, list):
            return [e.lower() for e in emails if isinstance(e, str) and _is_valid_email(e)]
    except Exception as e:
        print(f"  [claude json error] {type(e).__name__}: {e}  raw={text!r:.120}")
    return []


# ------------------------------------------------------------------
# Per-lead processor
# ------------------------------------------------------------------

def _process_lead(lead, tavily_key: str, anthropic_client, use_tavily: bool) -> tuple[list[str], str]:
    """
    Returns (emails_found, method).
    method is one of: 'scrape', 'tavily+claude', 'none'
    """
    # Stage 1: scrape the website directly
    emails = _scrape_website(lead.website)
    if emails:
        return emails, "scrape"

    # Stage 2: Tavily + Claude fallback
    if use_tavily and tavily_key and anthropic_client:
        results = _tavily_search_emails(lead, tavily_key)
        emails = _claude_extract_emails(lead, results, anthropic_client)
        if emails:
            return emails, "tavily+claude"

    return [], "none"


# ------------------------------------------------------------------
# Command
# ------------------------------------------------------------------

class Command(BaseCommand):
    help = "Scrape emails from lead websites; falls back to Tavily+Claude if scraping finds nothing"

    def add_arguments(self, parser):
        parser.add_argument("--state",     default=None, help="Filter by state code (e.g. TX)")
        parser.add_argument("--refetch",   action="store_true", help="Re-scrape even if emails already populated")
        parser.add_argument("--limit",     type=int, default=None)
        parser.add_argument("--live-only", action="store_true", help="Only leads where website_live=TRUE")
        parser.add_argument("--workers",   type=int, default=WORKERS, help=f"Parallel workers (default: {WORKERS})")
        parser.add_argument("--no-tavily", action="store_true", help="Disable Tavily+Claude fallback")

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

        tavily_key    = _get_key("TAVILY_API_KEY")
        anthropic_key = _get_key("ANTHROPIC_API_KEY")
        use_tavily    = not options["no_tavily"]

        anthropic_client = None
        if use_tavily and anthropic_key:
            try:
                import anthropic
                anthropic_client = anthropic.Anthropic(api_key=anthropic_key)
            except ImportError:
                pass

        qs = Lead.objects.filter(website__isnull=False).exclude(website="")
        if options["state"]:
            qs = qs.filter(state=options["state"].upper())
        if options["live_only"]:
            qs = qs.filter(website_live=True)
        if not options["refetch"]:
            qs = qs.filter(emails=[], emails_not_found=False)
        if options["limit"]:
            qs = qs[:options["limit"]]

        leads = list(qs.only("id", "name", "city", "state", "website", "emails"))
        total = len(leads)

        if not total:
            self.stdout.write("No leads to process.")
            return

        workers = options["workers"]
        fallback_status = (
            "✓ Tavily + Claude Haiku" if (use_tavily and tavily_key and anthropic_client)
            else "✗ disabled (--no-tavily or missing keys)"
        )
        live_tag = " [website_live=TRUE only]" if options["live_only"] else ""
        self.stdout.write(
            f"Processing {total} leads [{workers} workers]{live_tag}\n"
            f"  Stage 1: direct website scraping\n"
            f"  Stage 2 fallback: {fallback_status}\n"
        )

        found_scrape  = 0
        found_tavily  = 0
        not_found     = 0

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(_process_lead, lead, tavily_key, anthropic_client, use_tavily): lead
                for lead in leads
            }
            for i, future in enumerate(as_completed(futures), 1):
                lead = futures[future]
                try:
                    emails, method = future.result()
                except Exception as e:
                    self.stdout.write(self.style.WARNING(f"  [{i}/{total}] ERROR {lead.website}: {e}"))
                    not_found += 1
                    continue

                if emails:
                    Lead.objects.filter(pk=lead.pk).update(emails=emails)
                    tag = f"[{method}]"
                    if method == "scrape":
                        found_scrape += 1
                        self.stdout.write(
                            self.style.SUCCESS(f"  [{i}/{total}] ✓ {tag}  {lead.website}")
                        )
                    else:
                        found_tavily += 1
                        self.stdout.write(
                            self.style.SUCCESS(f"  [{i}/{total}] ✓ {tag}  {lead.name} ({lead.city}, {lead.state})")
                        )
                    self.stdout.write(f"           {emails}")
                else:
                    not_found += 1
                    Lead.objects.filter(pk=lead.pk).update(emails_not_found=True)
                    self.stdout.write(f"  [{i}/{total}] —  {lead.website}")

        self.stdout.write(self.style.SUCCESS(
            f"\nDone.\n"
            f"  Found via scraping      : {found_scrape}\n"
            f"  Found via Tavily+Claude : {found_tavily}\n"
            f"  Not found               : {not_found}\n"
            f"  Total                   : {total}"
        ))
