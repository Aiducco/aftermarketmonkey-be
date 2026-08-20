"""
Qualifies leads from their website content using Azure OpenAI (GPT-4.1).

For each lead with a live website:
  1. Scrapes the homepage text
  2. Sends it to the Azure GPT-4.1 deployment with a strict qualification prompt
  3. Parses the JSON response and saves results to the DB

Fields updated:
  is_qualified, business_typology, confidence_score, brands_mentioned,
  ai_reasoning, ai_qualified_at

Only leads with website_live=True are considered, so we never pay for a dead site.

Works on both lead tables — pick with --source:
  google     Lead          (Google Maps leads, the default)
  realtruck  RealTruckLead (RealTruck dealer-locator leads)

Usage:
  python manage.py qualify_leads                        # all unqualified leads with live website
  python manage.py qualify_leads --source realtruck     # qualify RealTruck dealers instead
  python manage.py qualify_leads --state TX             # filter by state
  python manage.py qualify_leads --requalify            # re-run even if already qualified
  python manage.py qualify_leads --limit 100            # process at most N leads (for testing)
  python manage.py qualify_leads --workers 5            # parallel workers (default 2)
  python manage.py qualify_leads --model <deployment>   # override the Azure deployment

Cost (measured on real RealTruck dealer sites, 2026-08):
  ~1,655 input + ~106 output tokens per lead. On Azure gpt-4.1 ($2.00/1M in, $8.00/1M out)
  that is ~$0.0042 per lead — about $4.20 per 1,000 leads.
"""
import json
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from html.parser import HTMLParser
from urllib.parse import urlparse

import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

try:
    from curl_cffi import requests as cffi_requests
    HAS_CURL_CFFI = True
except ImportError:
    HAS_CURL_CFFI = False

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.db import models as django_models
from django.db.models import F
from django.db.models.functions import Mod

from src.integrations.llm import azure_llm
from src.models import Lead, LeerLead, RealTruckLead

# Azure OpenAI deployment name; overridden by AZURE_OPENAI_DEPLOYMENT in .env
MODEL = azure_llm.deployment()

SOURCES = {
    "google": Lead,
    "realtruck": RealTruckLead,
    "leer": LeerLead,
}
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
- Off-Road & 4x4 Outfitters — suspension lifts, leveling kits, coilovers, long-travel and
  bypass shocks; bumpers, bull bars, rock sliders, skid plates, armour and cages; winches,
  recovery gear, traction boards, on-board air and snorkels; lockers, regearing, axles and
  differentials; mud-terrain and all-terrain tyres, beadlock and off-road wheels; light bars and
  auxiliary lighting; fender flares, side steps, running boards; roof racks, bed racks, roof-top
  tents and overlanding builds; Jeep, Bronco and truck specifics (tops, tube doors, half doors);
  4x4 conversions and trail-prep work
- Truck Accessories & Upfitting — tonneau covers, truck caps and toppers, spray-on and drop-in
  bedliners, tool boxes, bed slides, towing and hitch work, mud flaps, step bars
- Automotive Restyling & 12-Volt Specialists — custom lighting, car audio, remote start, alarms,
  window tint, paint protection film, vinyl wraps, custom paint, interior and upholstery
- Performance & Speed Shops — dyno tuning, ECU tuning, engine building, forced induction,
  exhaust upgrades, brakes and suspension for performance, sport-compact and muscle work
- RV, Commercial Trailer & Fleet Service — heavy-duty hitches, awnings, solar, fleet outfitting,
  van and work-truck upfitting, shelving, ladder racks, partitions
- Powersports & UTV/ATV — side-by-sides, quads, snowmobiles and their accessories: winches,
  cab enclosures, roofs, windshields, wheels and tyres, light bars, cargo racks, audio

IMPORTANT — what counts as qualifying installation:
The test is whether they fit DISCRETIONARY AFTERMARKET UPGRADES — parts a customer chooses to
improve or restyle a vehicle: lift kits, tonneau covers, truck caps, bedliners, custom wheels,
light bars, winches, hitches, audio, tint, wraps, performance parts, upfitting.

Installing parts as part of ordinary repair or maintenance does NOT qualify: brakes, clutches,
exhausts, batteries, alternators, plain tyre replacement, oil changes, engine or transmission
repair, bodywork and collision. Every garage installs parts — that alone is not evidence.

MIXED BUSINESSES QUALIFY. Most real prospects also do ordinary repair, tyres or servicing —
that does not count against them. If the text names even ONE aftermarket upgrade product or
service anywhere (for example "suspension repair and lift kits", "vehicle accessories &
customizations", "truck caps", "bedliners", "custom wheels", "window tinting", "accessories"),
the business QUALIFIES — regardless of how much general repair sits alongside it. A tyre shop
listing lift kits, or a garage listing vehicle accessories, is a real prospect: they are buying
those parts from a distributor, which is exactly the customer we want.

Stocking or selling aftermarket accessories counts as much as fitting them — either way they
purchase from distributors.

Do not disqualify a company merely for being a retailer, a manufacturer, or for selling direct to
consumers; many of the best prospects are exactly that.

What does NOT qualify is a business where NO aftermarket upgrade product or service is named at
all — only repair, maintenance, tyres, oil changes, bodywork or diagnostics. Judge on what the
text actually shows: do NOT infer from the business name, and do not qualify on a generic "we
install parts" with no aftermarket product named. If you find yourself writing "while not a
specialty shop" or "implies they install", the answer is unqualified.

Positive evidence of a real installation business:
- A store locator, a "Locations" page, or several branches. Physical locations that fit truck
  accessories are shops, whatever the company calls itself.
- Services named anywhere: installation, fitting, spray-on bedliner, window tinting, hitch
  fitting, wheel/tire mounting, custom builds, service department, labor rates.
- Brand names of aftermarket lines they carry.

Only treat "retailer" as disqualifying when the business is purely mail-order or e-commerce with
no physical premises where vehicles are worked on.

Note: you may be shown navigation menus, cookie banners and product-category lists rather than
prose. Absence of the word "install" in such text is NOT evidence that a business does not
install — judge on what the business evidently is, and lower your confidence instead of rejecting.

An "Unqualified" lead is a business that does NOT frequently order aftermarket parts from major distributors. Reject the following:
- Standard oil change and lube franchises (e.g., Jiffy Lube)
- Standard car washes or auto detailing-only shops (unless they explicitly mention restyling/lighting)
- Standard tire repair shops (unless they mention custom wheels/suspension lifts)
- Used or New Car Dealerships (unless they explicitly mention an in-house custom modification shop)
- OEM, European or import parts specialists (VW, Audi, BMW, Mercedes, Volvo, Subaru, Japanese
  or Euro spares). Replacement parts sold for repair are NOT discretionary aftermarket upgrades,
  even when the shop fits them. This is the single most common false positive — a "VW parts and
  service" or "foreign auto repair" business is UNQUALIFIED unless it separately names accessory,
  off-road, restyling or performance work.
- General repair garages, transmission shops, collision/body shops and diagnostics-only businesses
- Non-automotive businesses
- Podiatrists, medical offices, or any non-automotive business

INSTRUCTIONS:
Analyze the provided website content. Be conservative — only mark a business as qualified if there is clear evidence in the text that they install aftermarket accessories or performance parts.

You must respond ONLY with a valid JSON object. Do not include markdown formatting, code blocks, or any conversational text.

JSON SCHEMA:
{
  "is_qualified": boolean,
  "business_typology": "Off-Road" | "Restyling" | "Performance" | "Commercial/RV" | "Powersports" | "General Repair" | "Unqualified",
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


# Cookie/consent banners are the first thing on many sites and can consume a large share of the
# character budget before any real content is reached. Leonard USA's page opened with ~400 chars
# of privacy dialog followed by a product mega-menu -- the model never saw a sentence about the
# business, and disqualified 83 dealers on that basis.
BOILERPLATE_RE = re.compile(
    r"(manage your privacy|this site uses cookies[^.]*\.|reject advertising cookies|"
    r"save preferences|accept all cookies|cookie preferences|we use cookies[^.]*\.|"
    r"opens in new tab|skip to (?:main )?content)",
    re.IGNORECASE,
)


def _strip_boilerplate(text: str) -> str:
    return re.sub(r"\s+", " ", BOILERPLATE_RE.sub(" ", text)).strip()


def _extract_text(html: str) -> str:
    parser = _TextExtractor()
    try:
        parser.feed(html)
    except Exception:
        pass
    text = " ".join(parser.chunks)
    # Collapse whitespace
    text = re.sub(r"\s+", " ", text).strip()
    return _strip_boilerplate(text)[:MAX_WEBSITE_CHARS]


# Pages to try scraping per site (in order, stop once we have enough text)
SCRAPE_PATHS = ["", "/about", "/about-us", "/services", "/service", "/installation",
                "/locations", "/store-locator", "/stores", "/products", "/what-we-do"]

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
def _qualify(client, url: str, text: str, model: str = None) -> tuple[dict | None, str | None]:
    """Returns (result_dict, error_message)."""
    user_prompt = (
        f'Please analyze the following website content and output the JSON qualification object.\n\n'
        f'Website URL: {url}\n'
        f'Website Text:\n"""\n{text}\n"""'
    )
    return azure_llm.complete_json(client, SYSTEM_PROMPT, user_prompt, max_tokens=1024, model=model)



# ------------------------------------------------------------------
# Packed qualification
#
# The system prompt is ~550 tokens and was previously re-sent for every single lead. Packing N
# businesses into one request amortises it: measured 31% cheaper at N=5 with identical
# qualified/typology verdicts on a 5-lead sample. Returns diminish past ~8 because the scraped
# text then dominates the request.
#
# Caveat measured on that sample: packed confidence scores land ~5 points lower than single-call
# ones. The boolean verdict and typology were unchanged, but a `confidence >= 80` filter will
# behave slightly differently.
# ------------------------------------------------------------------
PACK_SYSTEM_PROMPT = SYSTEM_PROMPT.replace(
    "You must respond ONLY with a valid JSON object.",
    "You will be given SEVERAL businesses, each with an id. Judge each one INDEPENDENTLY -- do not "
    "let one business influence another, and return one entry for every id you were given. "
    "You must respond ONLY with a valid JSON object.",
).replace(
    "JSON SCHEMA:\n{",
    'JSON SCHEMA -- an object with a "results" array holding one entry per business, each entry\n'
    'carrying the id it was given:\n{\n  "results": [{\n    "id": "<the id you were given>",',
)


def _qualify_pack(client, items: list, model=None) -> dict:
    """
    items: [(pk, website, text), ...]. Returns {pk: result_dict} for whatever came back.

    Any pack that fails to parse, or comes back with entries missing, is handled by the caller
    falling back to one call per lead -- a malformed pack must not silently lose N leads.
    """
    blocks = "\n\n".join(
        f"--- BUSINESS id={pk} url={site} ---\n{text}" for pk, site, text in items
    )
    parsed, err = azure_llm.complete_json(
        client, PACK_SYSTEM_PROMPT,
        f"Analyze each business below and return one result per id.\n\n{blocks}",
        max_tokens=min(1024 * len(items), 8192), model=model,
    )
    if not parsed:
        return {}
    out = {}
    for entry in parsed.get("results") or []:
        rid = str(entry.get("id", ""))
        for pk, _site, _t in items:
            if str(pk) == rid:
                out[pk] = entry
                break
    return out

# ------------------------------------------------------------------
# Worker
# ------------------------------------------------------------------
def _process_lead(lead, client, model: str = MODEL, **_kwargs) -> tuple[dict | None, str | None]:
    """Scrape + qualify one lead. Returns (update_dict, error_message)."""
    # RealTruck stores plenty of bare hostnames; _scrape needs a scheme to build the base URL.
    website = (lead.website or "").strip()
    if "://" not in website:
        website = f"http://{website}"

    text = _scrape(website)
    if not text:
        return None, "scrape failed (site unreachable or empty)"

    result, error = _qualify(client, website, text, model)
    if not result:
        return None, error

    return {
        "id": lead.pk,
        "is_qualified": result.get("is_qualified"),
        "business_typology": result.get("business_typology"),
        "confidence_score": result.get("confidence_score"),
        "brands_mentioned": result.get("brands_mentioned") or [],
        "ai_reasoning": result.get("reasoning"),
        "ai_qualified_at": datetime.now(timezone.utc),
    }, None



def _process_pack(leads, client, model=None, pack_size: int = 5) -> list:
    """
    Scrape a group of leads, qualify them in one request, and return
    [(lead, update_dict_or_None, error_or_None), ...] in the caller's expected shape.
    """
    scraped, out = [], []
    for lead in leads:
        website = (lead.website or "").strip()
        if "://" not in website:
            website = f"http://{website}"
        text = _scrape(website)
        if text:
            scraped.append((lead, website, text))
        else:
            out.append((lead, None, "scrape failed (site unreachable or empty)"))

    if not scraped:
        return out

    results = {}
    if pack_size > 1 and len(scraped) > 1:
        results = _qualify_pack(
            client, [(l.pk, site, text) for l, site, text in scraped], model)

    for lead, site, text in scraped:
        r = results.get(lead.pk)
        if r is None:
            # Pack failed or skipped this one -- fall back to a single call so a malformed
            # pack response never silently loses leads.
            r, err = _qualify(client, site, text, model)
            if not r:
                out.append((lead, None, err))
                continue
        out.append((lead, {
            "id": lead.pk,
            "is_qualified": r.get("is_qualified"),
            "business_typology": r.get("business_typology"),
            "confidence_score": r.get("confidence_score"),
            "brands_mentioned": r.get("brands_mentioned") or [],
            "ai_reasoning": r.get("reasoning"),
            "ai_qualified_at": datetime.now(timezone.utc),
        }, None))
    return out

# ------------------------------------------------------------------
# Bulk save
# ------------------------------------------------------------------
def _bulk_save(model, batch: list[dict]):
    for item in batch:
        model.objects.filter(pk=item["id"]).update(
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
        parser.add_argument("--source", default="google", choices=sorted(SOURCES), help="Which lead table to qualify")
        parser.add_argument("--model", default=MODEL, help=f"Anthropic model to use (default: {MODEL})")
        parser.add_argument("--state", default=None, help="Filter by state code (e.g. TX)")
        parser.add_argument("--requalify", action="store_true", help="Re-qualify already processed leads")
        parser.add_argument("--limit", type=int, default=None, help="Max leads to process")
        parser.add_argument("--pack", type=int, default=5,
                            help="Businesses per LLM request (1 disables packing). 5 measured ~31%% cheaper.")
        parser.add_argument("--shard", default=None,
                            help="i/N — process only shard i of N (e.g. 0/4). Lets several\n"
                                 "processes split the backlog with no overlap.")
        parser.add_argument("--workers", type=int, default=2, help="Parallel workers (default: 2, keep low to avoid rate limits)")

    def handle(self, *args, **options):
        if not getattr(settings, "AZURE_OPENAI_API_KEY", ""):
            self.stdout.write(self.style.ERROR("AZURE_OPENAI_API_KEY is not set in .env"))
            return

        # Bulk runs will hit 429s; the SDK backs off rather than losing the lead.
        client = azure_llm.client(max_retries=8)
        self.stdout.write(
            f"  curl_cffi Chrome impersonation: {'✓ enabled' if HAS_CURL_CFFI else '✗ not installed (pip install curl_cffi)'}\n"
        )

        model = SOURCES[options["source"]]
        llm_model = options["model"]

        # website_live=True only — never pay for a scrape+LLM call on a site we know is dead.
        qs = model.objects.filter(website__isnull=False).exclude(website="").filter(website_live=True)

        if options["state"]:
            qs = qs.filter(state=options["state"].upper())

        if not options["requalify"]:
            # Only leads not yet qualified (is_qualified IS NULL) and not previously skipped by AI
            qs = qs.filter(is_qualified__isnull=True, ai_skip_reason__isnull=True)

        if options["shard"]:
            i, n = (int(x) for x in options["shard"].split("/"))
            if not 0 <= i < n:
                raise CommandError(f"--shard {options['shard']}: i must be in 0..{n - 1}")
            pk = model._meta.pk
            if not isinstance(pk, (django_models.AutoField, django_models.IntegerField,
                                   django_models.BigAutoField)):
                raise CommandError(f"--shard needs an integer primary key; {model.__name__}.{pk.name} is {type(pk).__name__}")
            # MOD on the pk spreads work evenly and is stable across restarts, so a shard that
            # dies can be resumed without redoing another shard's rows.
            qs = qs.annotate(_shard=Mod(F(pk.name), n)).filter(_shard=i)

        if options["limit"]:
            qs = qs[:options["limit"]]

        leads = list(qs.only(model._meta.pk.name, "website"))
        total = len(leads)

        if not total:
            self.stdout.write("No leads to qualify.")
            return

        workers = options["workers"]
        self.stdout.write(
            f"Qualifying {total} {options['source']} leads with {workers} workers "
            f"[model: {llm_model}]...\n"
        )

        qualified = 0
        disqualified = 0
        failed = 0
        pending: list[dict] = []

        pack_size = max(1, options["pack"])
        packs = [leads[j:j + pack_size] for j in range(0, len(leads), pack_size)]

        i = 0
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [executor.submit(_process_pack, pk_group, client, llm_model, pack_size)
                       for pk_group in packs]

            for future in as_completed(futures):
                try:
                    pack_results = future.result()
                except Exception as e:
                    self.stdout.write(self.style.WARNING(f"  pack ERROR  {e}"))
                    failed += pack_size
                    continue

                for lead, result, error in pack_results:
                    i += 1

                    if result is None:
                        self.stdout.write(self.style.WARNING(f"  [{i}/{total}] SKIP   {lead.website}  ({error})"))
                        failed += 1
                        # Save the skip to DB so we don't retry endlessly
                        pending.append({
                            "id": lead.pk,
                            "ai_skip_reason": (error or "unknown")[:255],
                            "ai_qualified_at": datetime.now(timezone.utc),
                        })
                        if len(pending) >= BATCH_SIZE:
                            _bulk_save(model, pending)
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
                        _bulk_save(model, pending)
                        self.stdout.write(f"  -- saved {len(pending)} to DB --")
                        pending.clear()

        if pending:
            _bulk_save(model, pending)
            self.stdout.write(f"  -- saved {len(pending)} to DB --")

        self.stdout.write(self.style.SUCCESS(
            f"\nDone.  Qualified: {qualified}  Disqualified: {disqualified}  "
            f"Failed/skipped: {failed}  Total: {total}"
        ))
