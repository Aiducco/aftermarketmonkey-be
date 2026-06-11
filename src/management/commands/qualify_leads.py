"""
Uses Claude Haiku (cheapest model) to qualify leads based on their website content.

For each lead with a live website:
  1. Scrapes the homepage text
  2. Sends it to Claude Haiku with a strict qualification prompt
  3. Parses the JSON response and saves results to the DB

Fields updated:
  is_qualified, business_typology, confidence_score, brands_mentioned,
  ai_reasoning, ai_qualified_at

Usage:
  python manage.py qualify_leads                        # all unqualified leads with live website
  python manage.py qualify_leads --state TX             # filter by state
  python manage.py qualify_leads --requalify            # re-run even if already qualified
  python manage.py qualify_leads --limit 100            # process at most N leads (for testing)
  python manage.py qualify_leads --workers 5            # parallel workers (default 5)

Cost estimate (Claude 3 Haiku):
  ~$0.25 / 1M input tokens — roughly $5–8 for 10,000 leads
"""
import json
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from html.parser import HTMLParser
from urllib.parse import urlparse

import anthropic
import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

try:
    from curl_cffi import requests as cffi_requests
    HAS_CURL_CFFI = True
except ImportError:
    HAS_CURL_CFFI = False

from django.conf import settings
from django.core.management.base import BaseCommand

from src.models import Lead

MODEL = "claude-haiku-4-5"
MAX_WEBSITE_CHARS = 6000   # ~1500 tokens — enough context, keeps cost low
SCRAPE_TIMEOUT = 10
BATCH_SIZE = 50
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

SYSTEM_PROMPT = """You are an expert B2B lead qualifier for an automotive aftermarket e-commerce SaaS company.
Your objective is to analyze the scraped text from a local business website and determine if they are a highly qualified prospect for an automotive parts aggregator platform.

The platform helps automotive shop parts managers save time by aggregating live inventory and pricing from major wholesale distributors (like Turn14, Keystone, Meyer Distributing, Rough Country, and Driven Lighting Group) into a single dashboard.

QUALIFICATION CRITERIA:
A "Qualified" lead must be a business that actively purchases and installs high-margin aftermarket automotive parts.
Look for businesses in these categories:
- Off-Road & 4x4 Outfitters (lift kits, off-road wheels, Jeep/truck accessories)
- Automotive Restyling & 12-Volt Specialists (custom lighting, audio, vehicle wraps)
- Performance & Speed Shops (dyno tuning, engine building, sport-compact/muscle upgrades)
- RV, Commercial Trailer & Fleet Service (heavy duty hitches, fleet outfitting)

An "Unqualified" lead is a business that does NOT frequently order aftermarket parts from major distributors. Reject the following:
- Standard oil change and lube franchises (e.g., Jiffy Lube)
- Standard car washes or auto detailing-only shops (unless they explicitly mention restyling/lighting)
- Standard tire repair shops (unless they mention custom wheels/suspension lifts)
- Used or New Car Dealerships (unless they explicitly mention an in-house custom modification shop)
- Non-automotive businesses
- Podiatrists, medical offices, or any non-automotive business

INSTRUCTIONS:
Analyze the provided website content. Be conservative — only mark a business as qualified if there is clear evidence in the text that they install aftermarket accessories or performance parts.

You must respond ONLY with a valid JSON object. Do not include markdown formatting, code blocks, or any conversational text.

JSON SCHEMA:
{
  "is_qualified": boolean,
  "business_typology": "Off-Road" | "Restyling" | "Performance" | "Commercial/RV" | "General Repair" | "Unqualified",
  "confidence_score": integer between 0 and 100,
  "brands_mentioned": array of brand name strings,
  "reasoning": "A 1-2 sentence explanation of why this lead was qualified or disqualified based on the text."
}"""


# ------------------------------------------------------------------
# HTML text extractor (no external deps)
# ------------------------------------------------------------------
class _TextExtractor(HTMLParser):
    SKIP_TAGS = {"script", "style", "noscript", "head", "meta", "link"}

    def __init__(self):
        super().__init__()
        self._skip = False
        self._skip_tag = None
        self.chunks = []

    def handle_starttag(self, tag, attrs):
        if tag.lower() in self.SKIP_TAGS:
            self._skip = True
            self._skip_tag = tag.lower()

    def handle_endtag(self, tag):
        if tag.lower() == self._skip_tag:
            self._skip = False
            self._skip_tag = None

    def handle_data(self, data):
        if not self._skip:
            text = data.strip()
            if text:
                self.chunks.append(text)


def _extract_text(html: str) -> str:
    parser = _TextExtractor()
    try:
        parser.feed(html)
    except Exception:
        pass
    text = " ".join(parser.chunks)
    # Collapse whitespace
    text = re.sub(r"\s+", " ", text).strip()
    return text[:MAX_WEBSITE_CHARS]


# Pages to try scraping per site (in order, stop once we have enough text)
SCRAPE_PATHS = ["", "/about", "/about-us", "/services", "/products", "/what-we-do"]

# ------------------------------------------------------------------
# Scrape helpers — three-tier fallback per page
#   1. curl_cffi Chrome impersonation  (beats TLS fingerprinting + Cloudflare)
#   2. plain requests                  (simpler sites)
# ------------------------------------------------------------------
def _fetch(url: str) -> str | None:
    # Tier 1: curl_cffi with Chrome TLS fingerprint
    if HAS_CURL_CFFI:
        try:
            resp = cffi_requests.get(
                url, timeout=SCRAPE_TIMEOUT, headers=HEADERS,
                allow_redirects=True, impersonate="chrome124", verify=False,
            )
            if resp.status_code == 200:
                return _extract_text(resp.text) or None
        except Exception:
            pass

    # Tier 2: plain requests
    try:
        resp = requests.get(
            url, timeout=SCRAPE_TIMEOUT, headers=HEADERS,
            allow_redirects=True, verify=False,
        )
        if resp.status_code == 200:
            return _extract_text(resp.text) or None
    except Exception:
        pass

    return None


def _scrape(url: str, **_kwargs) -> str | None:
    """Scrape multiple pages per site, stop once we have enough text."""
    parsed = urlparse(url)
    base = f"{parsed.scheme}://{parsed.netloc}"
    all_text = []

    for path in SCRAPE_PATHS:
        text = _fetch(base + path)
        if text:
            all_text.append(text)
        if len(" ".join(all_text)) >= MAX_WEBSITE_CHARS:
            break

    combined = " ".join(all_text).strip()
    return combined[:MAX_WEBSITE_CHARS] if combined else None


# ------------------------------------------------------------------
# Call Claude
# ------------------------------------------------------------------
def _qualify(client: anthropic.Anthropic, url: str, text: str) -> tuple[dict | None, str | None]:
    """Returns (result_dict, error_message)."""
    user_prompt = (
        f'Please analyze the following website content and output the JSON qualification object.\n\n'
        f'Website URL: {url}\n'
        f'Website Text:\n"""\n{text}\n"""'
    )
    try:
        message = client.messages.create(
            model=MODEL,
            max_tokens=512,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_prompt}],
        )
        raw = message.content[0].text.strip()
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw)
        return json.loads(raw), None
    except json.JSONDecodeError as e:
        return None, f"JSON parse error: {e}"
    except Exception as e:
        return None, f"LLM error: {e}"


# ------------------------------------------------------------------
# Worker
# ------------------------------------------------------------------
def _process_lead(lead, client, **_kwargs) -> tuple[dict | None, str | None]:
    """Scrape + qualify one lead. Returns (update_dict, error_message)."""
    text = _scrape(lead.website)
    if not text:
        return None, "scrape failed (site unreachable or empty)"

    result, error = _qualify(client, lead.website, text)
    if not result:
        return None, error

    return {
        "id": lead.id,
        "is_qualified": result.get("is_qualified"),
        "business_typology": result.get("business_typology"),
        "confidence_score": result.get("confidence_score"),
        "brands_mentioned": result.get("brands_mentioned") or [],
        "ai_reasoning": result.get("reasoning"),
        "ai_qualified_at": datetime.now(timezone.utc),
    }, None


# ------------------------------------------------------------------
# Bulk save
# ------------------------------------------------------------------
def _bulk_save(batch: list[dict]):
    for item in batch:
        Lead.objects.filter(pk=item["id"]).update(
            is_qualified=item.get("is_qualified"),
            business_typology=item.get("business_typology"),
            confidence_score=item.get("confidence_score"),
            brands_mentioned=item.get("brands_mentioned") or [],
            ai_reasoning=item.get("ai_reasoning"),
            ai_skip_reason=item.get("ai_skip_reason"),
            ai_qualified_at=item["ai_qualified_at"],
        )


# ------------------------------------------------------------------
# Command
# ------------------------------------------------------------------
class Command(BaseCommand):
    help = "Qualify leads using Claude Haiku based on website content"

    def add_arguments(self, parser):
        parser.add_argument("--state", default=None, help="Filter by state code (e.g. TX)")
        parser.add_argument("--requalify", action="store_true", help="Re-qualify already processed leads")
        parser.add_argument("--limit", type=int, default=None, help="Max leads to process")
        parser.add_argument("--workers", type=int, default=2, help="Parallel workers (default: 2, keep low to avoid rate limits)")

    def handle(self, *args, **options):
        api_key = getattr(settings, "ANTHROPIC_API_KEY", "")
        if not api_key:
            self.stdout.write(self.style.ERROR("ANTHROPIC_API_KEY is not set in .env"))
            return

        client = anthropic.Anthropic(api_key=api_key)
        self.stdout.write(
            f"  curl_cffi Chrome impersonation: {'✓ enabled' if HAS_CURL_CFFI else '✗ not installed (pip install curl_cffi)'}\n"
        )

        qs = Lead.objects.filter(website__isnull=False).exclude(website="").filter(website_live=True)

        if options["state"]:
            qs = qs.filter(state=options["state"].upper())

        if not options["requalify"]:
            # Only leads not yet qualified (is_qualified IS NULL) and not previously skipped by AI
            qs = qs.filter(is_qualified__isnull=True, ai_skip_reason__isnull=True)

        if options["limit"]:
            qs = qs[:options["limit"]]

        leads = list(qs.only("id", "website"))
        total = len(leads)

        if not total:
            self.stdout.write("No leads to qualify.")
            return

        workers = options["workers"]
        self.stdout.write(
            f"Qualifying {total} leads with {workers} workers "
            f"[model: {MODEL}]...\n"
        )

        qualified = 0
        disqualified = 0
        failed = 0
        pending: list[dict] = []

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(_process_lead, lead, client): lead
                for lead in leads
            }

            for i, future in enumerate(as_completed(futures), 1):
                lead = futures[future]
                try:
                    result, error = future.result()
                except Exception as e:
                    self.stdout.write(self.style.WARNING(f"  [{i}/{total}] ERROR  {lead.website}  {e}"))
                    failed += 1
                    continue

                if result is None:
                    self.stdout.write(self.style.WARNING(f"  [{i}/{total}] SKIP   {lead.website}  ({error})"))
                    failed += 1
                    # Save the skip to DB so we don't retry endlessly
                    pending.append({
                        "id": lead.id,
                        "ai_skip_reason": (error or "unknown")[:255],
                        "ai_qualified_at": datetime.now(timezone.utc),
                    })
                    if len(pending) >= BATCH_SIZE:
                        _bulk_save(pending)
                        pending.clear()
                    continue

                label = self.style.SUCCESS("✓ QUALIFIED  ") if result["is_qualified"] else self.style.ERROR("✗ DISQUALIFIED")
                self.stdout.write(
                    f"  [{i}/{total}] {label}  "
                    f"[{result['business_typology']}]  "
                    f"score={result['confidence_score']}  "
                    f"{lead.website}"
                )
                self.stdout.write(f"           {result['ai_reasoning']}")

                if result["is_qualified"]:
                    qualified += 1
                else:
                    disqualified += 1

                pending.append(result)

                if len(pending) >= BATCH_SIZE:
                    _bulk_save(pending)
                    self.stdout.write(f"  -- saved {len(pending)} to DB --")
                    pending.clear()

        if pending:
            _bulk_save(pending)
            self.stdout.write(f"  -- saved {len(pending)} to DB --")

        self.stdout.write(self.style.SUCCESS(
            f"\nDone.  Qualified: {qualified}  Disqualified: {disqualified}  "
            f"Failed/skipped: {failed}  Total: {total}"
        ))
