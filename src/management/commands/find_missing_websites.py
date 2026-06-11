"""
For leads that have no website:
  1. Tavily Search API returns the top 5 results (URL + title + snippet)
  2. Claude Haiku looks at all results and picks the one most likely to be
     the business's own official website, or says "none" if nothing fits.
  3. If a website is found, scrapes it for phone and email.

Usage:
  python manage.py find_missing_websites                 # all leads missing website
  python manage.py find_missing_websites --state TX
  python manage.py find_missing_websites --limit 100
  python manage.py find_missing_websites --workers 5

Requires in .env:
  TAVILY_API_KEY   — sign up free at https://tavily.com
  ANTHROPIC_API_KEY — already set
"""
import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse

import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from django.core.management.base import BaseCommand
from django.db.models import Q

from src.models import Lead

TAVILY_SEARCH_URL = "https://api.tavily.com/search"
CONTACT_TIMEOUT   = 10

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# Tavily will exclude these from results server-side
EXCLUDE_DOMAINS = [
    "facebook.com", "instagram.com", "twitter.com", "x.com", "tiktok.com",
    "linkedin.com", "youtube.com", "pinterest.com", "snapchat.com",
    "google.com", "bing.com", "yahoo.com", "duckduckgo.com",
    "yelp.com", "yellowpages.com", "mapquest.com", "bbb.org", "nextdoor.com",
    "angieslist.com", "homeadvisor.com", "thumbtack.com", "manta.com",
    "chamberofcommerce.com", "whitepages.com", "superpages.com",
    "foursquare.com", "bizapedia.com", "cylex.us", "cylex.us.com",
    "findglocal.com", "loc8nearme.com", "citysearch.com", "dandb.com",
    "opencorporates.com", "hotfrog.us", "brownbook.net", "merchantcircle.com",
    "showmelocal.com", "yp.com", "dexknows.com", "fyple.com", "yellowbot.com",
    "golocal247.com", "ezlocal.com", "tuugo.us", "birdeye.com",
    "trustpilot.com", "tripadvisor.com", "wanderlog.com", "singletracks.com",
    "trailforks.com", "alltrails.com", "blm.gov", "nps.gov", "usda.gov",
    "moovitapp.com", "amazon.com", "ebay.com", "craigslist.org",
    "reddit.com", "quora.com", "medium.com", "indeed.com", "glassdoor.com",
    "carfax.com", "cars.com", "autotrader.com", "cargurus.com", "edmunds.com",
    "dealerrater.com", "wikipedia.org",
]

EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")
EMAIL_IGNORE_DOMAINS = {
    "gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "icloud.com",
    "example.com", "wix.com", "squarespace.com", "wordpress.com", "sentry.io",
}
EMAIL_IGNORE_SUFFIXES = (".wixpress.com", ".sentry.io", ".cloudflare.com")
_HASH_RE = re.compile(r'^[0-9a-f]{12,}$')
SCRAPE_PATHS = ["", "/contact", "/contact-us", "/about", "/about-us"]


# ------------------------------------------------------------------
# Tavily search
# ------------------------------------------------------------------

def _tavily_results(lead, api_key: str) -> list[dict]:
    """
    Return up to 7 Tavily results using two queries:
    - first with exact name in quotes (precise)
    - then without quotes (broader, catches low-authority sites)
    Results are deduplicated by domain.
    """
    seen_domains = set()
    combined = []

    queries = [
        f'"{lead.name}" {lead.city} {lead.state}',
        f'{lead.name} {lead.city} {lead.state} official website',
    ]

    for query in queries:
        try:
            resp = requests.post(
                TAVILY_SEARCH_URL,
                json={
                    "api_key": api_key,
                    "query": query,
                    "max_results": 5,
                    "search_depth": "advanced",
                    "exclude_domains": EXCLUDE_DOMAINS,
                },
                timeout=20,
            )
            if resp.status_code == 200:
                for r in resp.json().get("results", []):
                    from urllib.parse import urlparse as _up
                    domain = _up(r.get("url", "")).netloc.lstrip("www.")
                    if domain and domain not in seen_domains:
                        seen_domains.add(domain)
                        combined.append(r)
        except Exception:
            pass

    return combined[:8]


# ------------------------------------------------------------------
# Claude pick
# ------------------------------------------------------------------

def _claude_analyze(lead, results: list[dict], anthropic_client) -> dict:
    """
    Ask Claude Haiku to:
    1. Pick the business's own official website (if any result is one)
    2. Extract any phone/email visible in the snippets as a bonus

    Returns dict with optional keys: website, phone, email
    """
    if not results or not anthropic_client:
        return {}

    candidates = []
    for i, r in enumerate(results, 1):
        candidates.append(
            f"{i}. URL: {r.get('url','')}\n"
            f"   Title: {r.get('title','')}\n"
            f"   Snippet: {r.get('content','')[:250]}"
        )

    prompt = (
        f"Automotive business I need info for:\n"
        f"  Name:  {lead.name}\n"
        f"  City:  {lead.city}, {lead.state}\n\n"
        f"Search results:\n\n"
        + "\n\n".join(candidates)
        + "\n\n"
        f"Tasks:\n"
        f"1. WEBSITE: Is any result the business's OWN official website (not a directory, dealer page, review site, or news article)? "
        f"The domain should clearly belong to this business.\n"
        f"2. PHONE: Extract a phone number for this business if visible in any snippet.\n"
        f"3. EMAIL: Extract a business email if visible (not gmail/yahoo).\n\n"
        f"Reply with ONLY valid JSON (no markdown):\n"
        f"{{\"website\": \"https://...\" or null, \"phone\": \"...\" or null, \"email\": \"...\" or null}}"
    )

    try:
        response = anthropic_client.messages.create(
            model="claude-haiku-4-5",
            max_tokens=200,
            messages=[{"role": "user", "content": prompt}],
        )
        text = response.content[0].text.strip()
        text = re.sub(r'^```(?:json)?\s*', '', text)
        text = re.sub(r'\s*```$', '', text)
        data = json.loads(text)

        result = {}
        url = data.get("website")
        if url and isinstance(url, str) and url.startswith("http"):
            parsed = urlparse(url)
            result["website"] = f"{parsed.scheme}://{parsed.netloc}"
        if data.get("phone"):
            result["phone"] = str(data["phone"])
        if data.get("email") and "@" in str(data.get("email", "")):
            em = str(data["email"]).lower()
            # Skip generic free emails
            if not any(em.endswith(d) for d in ("@gmail.com", "@yahoo.com", "@hotmail.com", "@outlook.com")):
                result["email"] = em
        return result

    except Exception:
        pass
    return {}


# ------------------------------------------------------------------
# Contact scraping
# ------------------------------------------------------------------

def _scrape_contact(website_url: str) -> dict:
    """Scrape phone and emails from homepage + contact pages."""
    base = website_url.rstrip("/")
    emails, phone = [], None

    for path in SCRAPE_PATHS:
        try:
            resp = requests.get(
                base + path, timeout=CONTACT_TIMEOUT, headers=HEADERS,
                allow_redirects=True, verify=False,
            )
            if resp.status_code != 200:
                continue
            html = resp.text

            for em in EMAIL_RE.findall(html):
                em = em.lower()
                local, domain = em.rsplit("@", 1)
                if domain in EMAIL_IGNORE_DOMAINS:
                    continue
                if any(domain.endswith(s) for s in EMAIL_IGNORE_SUFFIXES):
                    continue
                if _HASH_RE.match(local):
                    continue
                if any(em.endswith(ext) for ext in (".png", ".jpg", ".gif", ".svg")):
                    continue
                if em not in emails:
                    emails.append(em)

            if not phone:
                m = re.search(r'(\(?\d{3}\)?[\s\-\.]\d{3}[\s\-\.]\d{4})', html)
                if m:
                    phone = m.group(1).strip()
        except Exception:
            continue

    result = {}
    if emails:
        result["email"] = emails[0]
        result["emails"] = emails
    if phone:
        result["phone"] = phone
    return result


# ------------------------------------------------------------------
# Main per-lead function
# ------------------------------------------------------------------

def _process(lead, tavily_key: str, anthropic_client) -> dict:
    # 1. Get Tavily candidates (2 queries, deduped)
    results = _tavily_results(lead, tavily_key)

    # 2. Claude picks website + extracts any visible phone/email from snippets
    claude_data = _claude_analyze(lead, results, anthropic_client)

    updates = {}

    # 3. If Claude found an official website, scrape it for more contact info
    website = claude_data.get("website")
    if website:
        updates["website"] = website
        contact = _scrape_contact(website)
        if not lead.phone and contact.get("phone"):
            updates["phone"] = contact["phone"]
        if not lead.email and contact.get("email"):
            updates["email"] = contact["email"]
        if not lead.emails and contact.get("emails"):
            updates["emails"] = contact["emails"]

    # 4. Even without a website, save phone/email Claude found in snippets
    if not lead.phone and not updates.get("phone") and claude_data.get("phone"):
        updates["phone"] = claude_data["phone"]
    if not lead.email and not updates.get("email") and claude_data.get("email"):
        updates["email"] = claude_data["email"]

    return updates


# ------------------------------------------------------------------
# Command
# ------------------------------------------------------------------

class Command(BaseCommand):
    help = "Find websites for leads using Tavily search + Claude Haiku to pick the best result"

    def add_arguments(self, parser):
        parser.add_argument("--state",   default=None, help="Two-letter state code")
        parser.add_argument("--limit",   type=int, default=None)
        parser.add_argument("--workers", type=int, default=5,
                            help="Parallel workers (default: 5)")

    def handle(self, *args, **options):
        # Load keys — fall back to dotenv_values if shell env has them pre-set empty
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

        if not tavily_key:
            self.stdout.write(self.style.ERROR(
                "TAVILY_API_KEY not set. Sign up free at https://tavily.com"
            ))
            return

        anthropic_client = None
        if anthropic_key:
            try:
                import anthropic
                anthropic_client = anthropic.Anthropic(api_key=anthropic_key)
            except ImportError:
                self.stdout.write(self.style.WARNING("anthropic not installed — no LLM filtering"))
        if not anthropic_client:
            self.stdout.write(self.style.WARNING(
                "ANTHROPIC_API_KEY not set — will use raw Tavily results without LLM validation"
            ))

        qs = Lead.objects.filter(Q(website__isnull=True) | Q(website="")).filter(website_not_found=False)
        if options["state"]:
            qs = qs.filter(state=options["state"].upper())
        if options["limit"]:
            qs = qs[:options["limit"]]

        leads = list(qs.only("id", "name", "city", "state", "phone", "email", "emails"))
        total = len(leads)

        if not total:
            self.stdout.write("No leads without a website.")
            return

        workers = options["workers"]
        self.stdout.write(
            f"Finding websites for {total} leads [{workers} workers]\n"
            f"  Tavily search  : ✓\n"
            f"  Claude Haiku   : {'✓ picking best result' if anthropic_client else '✗ disabled'}\n"
        )

        found = 0
        not_found = 0
        BATCH_SIZE = 50
        pending: list[tuple] = []

        def flush(batch):
            for lead_id, upd in batch:
                Lead.objects.filter(pk=lead_id).update(**upd)

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(_process, lead, tavily_key, anthropic_client): lead
                for lead in leads
            }

            for i, future in enumerate(as_completed(futures), 1):
                lead = futures[future]
                try:
                    updates = future.result()
                except Exception as e:
                    self.stdout.write(self.style.WARNING(
                        f"  [{i}/{total}] ERROR  {lead.name}: {e}"
                    ))
                    not_found += 1
                    continue

                if updates.get("website"):
                    found += 1
                    parts = [f"website={updates['website']}"]
                    if updates.get("phone"):
                        parts.append(f"phone={updates['phone']}")
                    if updates.get("email"):
                        parts.append(f"email={updates['email']}")
                    self.stdout.write(
                        self.style.SUCCESS(f"  [{i}/{total}] ✓  {lead.name} ({lead.city}, {lead.state})")
                    )
                    for p in parts:
                        self.stdout.write(f"           {p}")
                    pending.append((lead.id, updates))
                elif updates:
                    # No website but found phone/email in snippets
                    not_found += 1
                    parts = []
                    if updates.get("phone"):
                        parts.append(f"phone={updates['phone']}")
                    if updates.get("email"):
                        parts.append(f"email={updates['email']}")
                    self.stdout.write(
                        self.style.WARNING(f"  [{i}/{total}] ~  {lead.name} ({lead.city}, {lead.state})  [no website, but got contact]")
                    )
                    for p in parts:
                        self.stdout.write(f"           {p}")
                    pending.append((lead.id, updates))
                else:
                    not_found += 1
                    self.stdout.write(f"  [{i}/{total}] —  {lead.name} ({lead.city}, {lead.state})")
                    pending.append((lead.id, {"website_not_found": True}))

                if len(pending) >= BATCH_SIZE:
                    flush(pending)
                    self.stdout.write(f"  -- saved {len(pending)} to DB --")
                    pending.clear()

        if pending:
            flush(pending)
            self.stdout.write(f"  -- saved {len(pending)} to DB --")

        self.stdout.write(self.style.SUCCESS(
            f"\nDone.  Found: {found}  Not found: {not_found}  Total: {total}"
        ))
