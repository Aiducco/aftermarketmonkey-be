"""
Standalone re-scrape + qualify for RealTruck leads that the first pass could not read.

Why this exists: ~1,100 dealer sites answer a HEAD request (so website_live=True) but refuse the
scraper -- Cloudflare and friends. Most of that refusal is geographic and TLS-fingerprint based,
so this is meant to run ON THE US SERVER, where the egress IP is a US datacenter address rather
than a European one.

No Django -- talks to Postgres directly, same as the other scripts/*_standalone.py, so it does
not depend on the server checkout having the RealTruckLead model.

Scrape tiers, tried in order until one returns usable text:
  1. curl_cffi with rotating Chrome/Safari/Edge TLS fingerprints (beats JA3 fingerprinting)
  2. primp (Rust impersonation, different fingerprint surface than curl_cffi)
  3. plain requests (for the handful of sites that dislike the above)

Usage (on the server):
  python3 scripts/rescrape_realtruck_standalone.py --scrape-only --limit 50   # measure, costs $0
  python3 scripts/rescrape_realtruck_standalone.py            # full run (Azure creds from .env)
  ... --limit 200 --workers 12
"""
import argparse
import json
import os
import random
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from html.parser import HTMLParser
from urllib.parse import urlparse

import psycopg2
import psycopg2.extras

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.integrations.llm import azure_llm  # noqa: E402  (pure module, no Django)

try:
    from curl_cffi import requests as cffi_requests
except ImportError:
    cffi_requests = None
try:
    import primp
except ImportError:
    primp = None
import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

MAX_WEBSITE_CHARS = 6000
TIMEOUT = 15
SCRAPE_PATHS = ["", "/about", "/about-us", "/services", "/service", "/installation",
                "/locations", "/store-locator", "/stores", "/products", "/what-we-do"]

# Rotating fingerprints -- a site that blocks one Chrome build often lets another through.
IMPERSONATE = ["chrome131", "chrome124", "chrome120", "safari17_0", "edge101"]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Sec-Ch-Ua": '"Chromium";v="131", "Not_A Brand";v="24"',
    "Sec-Ch-Ua-Platform": '"Windows"',
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
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


class _TextExtractor(HTMLParser):
    SKIP_TAGS = {"script", "style", "noscript", "head", "meta", "link"}

    def __init__(self):
        super().__init__()
        self._skip_tag = None
        self.chunks = []

    def handle_starttag(self, tag, attrs):
        if tag.lower() in self.SKIP_TAGS:
            self._skip_tag = tag.lower()

    def handle_endtag(self, tag):
        if tag.lower() == self._skip_tag:
            self._skip_tag = None

    def handle_data(self, data):
        if not self._skip_tag and data.strip():
            self.chunks.append(data.strip())


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


def extract_text(html: str) -> str:
    p = _TextExtractor()
    try:
        p.feed(html)
    except Exception:
        pass
    return _strip_boilerplate(" ".join(p.chunks))[:MAX_WEBSITE_CHARS]


def _looks_blocked(text: str) -> bool:
    """A Cloudflare interstitial parses fine but is not the site -- treat it as a failure."""
    t = text[:600].lower()
    return any(m in t for m in (
        "just a moment", "checking your browser", "enable javascript and cookies",
        "attention required", "access denied", "request blocked", "ddos protection",
        "verifying you are human", "captcha",
    ))


def fetch(url: str) -> tuple[str | None, str | None]:
    """Returns (text, tier_that_worked)."""
    # Tier 1 -- curl_cffi, rotating TLS fingerprints
    if cffi_requests is not None:
        for imp in random.sample(IMPERSONATE, k=min(3, len(IMPERSONATE))):
            try:
                r = cffi_requests.get(url, timeout=TIMEOUT, headers=HEADERS,
                                      allow_redirects=True, impersonate=imp, verify=False)
                if r.status_code == 200:
                    t = extract_text(r.text)
                    if t and not _looks_blocked(t):
                        return t, f"curl_cffi:{imp}"
            except Exception:
                continue

    # Tier 2 -- primp, a different fingerprint surface entirely
    if primp is not None:
        try:
            c = primp.Client(impersonate="chrome_131", timeout=TIMEOUT, verify=False)
            r = c.get(url)
            if r.status_code == 200:
                t = extract_text(r.text)
                if t and not _looks_blocked(t):
                    return t, "primp"
        except Exception:
            pass

    # Tier 3 -- plain requests
    try:
        r = requests.get(url, timeout=TIMEOUT, headers=HEADERS, allow_redirects=True, verify=False)
        if r.status_code == 200:
            t = extract_text(r.text)
            if t and not _looks_blocked(t):
                return t, "requests"
    except Exception:
        pass

    return None, None


def scrape(website: str) -> tuple[str | None, str | None]:
    website = (website or "").strip()
    if "://" not in website:
        website = "https://" + website
    p = urlparse(website)
    hosts = [f"https://{p.netloc}", f"http://{p.netloc}"]

    parts, tier = [], None
    for base in hosts:
        # Probe the root first. If every tier fails on it the host is unreachable, and walking the
        # remaining paths just multiplies the timeout cost -- 11 paths x 5 tiers x 15s is ~20
        # minutes per dead site, which stalls the whole pool.
        root_text, root_tier = fetch(base)
        if not root_text:
            continue
        parts.append(root_text)
        tier = root_tier
        for path in SCRAPE_PATHS[1:]:
            if len(" ".join(parts)) >= MAX_WEBSITE_CHARS:
                break
            text, _ = fetch(base + path)
            if text:
                parts.append(text)
        break  # this scheme works; no need to retry the other
    combined = " ".join(parts).strip()[:MAX_WEBSITE_CHARS]
    return (combined or None), tier


def qualify(client, url: str, text: str, model: str):
    prompt = (f'Please analyze the following website content and output the JSON qualification object.\n\n'
              f'Website URL: {url}\nWebsite Text:\n"""\n{text}\n"""')
    return azure_llm.complete_json(client, SYSTEM_PROMPT, prompt, max_tokens=1024, model=model)


def db_conn():
    """Read credentials from the repo's .env rather than hardcoding them."""
    env = {}
    for line in open(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env")):
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip().strip('"').strip("'")
    return psycopg2.connect(
        host=env.get("DATABASE_HOST", "127.0.0.1"), port=env.get("DATABASE_PORT", "5432"),
        dbname=env["DATABASE_NAME"], user=env["DATABASE_USER"], password=env["DATABASE_PASSWORD"],
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--scrape-only", action="store_true", help="Measure scrape success; no LLM calls, no writes")
    ap.add_argument("--select", default="blocked", choices=["blocked", "pending", "both"],
                    help="blocked = never scraped; pending = awaiting a verdict; both")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--workers", type=int, default=10)
    ap.add_argument("--model", default=None, help="Azure deployment (default: AZURE_OPENAI_DEPLOYMENT)")
    args = ap.parse_args()

    client = None
    if not args.scrape_only:
        client = azure_llm.client(max_retries=8)

    conn = db_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    # Two populations need the server's US IP, not just the originally-blocked ones:
    #   blocked  - never scraped successfully (ai_skip_reason set)
    #   pending  - cleared for requalification and awaiting a fresh verdict
    # Requalifying from a European IP silently converts "pending" into "blocked", losing the
    # verdict entirely, which is why both are handled here rather than locally.
    where = {
        "blocked": "ai_skip_reason like 'scrape failed%%'",
        "pending": "is_qualified is null and ai_skip_reason is null",
        "both": "(ai_skip_reason like 'scrape failed%%' or (is_qualified is null and ai_skip_reason is null))",
    }[args.select]
    cur.execute("""
        select id, name, website from realtruck_leads
        where {where}
          and website is not null and website <> ''
          and website_live is true
        order by id {limit}
    """.format(where=where, limit=f"limit {int(args.limit)}" if args.limit else ""))
    leads = cur.fetchall()
    cur.close()

    total = len(leads)
    print(f"Processing {total} leads [select={args.select}] "
          f"[{args.workers} workers, {'scrape only' if args.scrape_only else (args.model or azure_llm.deployment())}]\n")

    ok = failed = qualified = disqualified = llm_err = 0
    tiers: dict[str, int] = {}

    def work(lead):
        text, tier = scrape(lead["website"])
        if not text:
            return lead, None, None, None
        if args.scrape_only:
            return lead, text, tier, None
        result, err = qualify(client, lead["website"], text, args.model)
        return lead, text, tier, (result or err)

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {ex.submit(work, l): l for l in leads}
        for i, fut in enumerate(as_completed(futures), 1):
            lead, text, tier, outcome = fut.result()
            if not text:
                failed += 1
                print(f"  [{i}/{total}] still blocked   {lead['website'][:60]}")
                continue

            ok += 1
            tiers[tier] = tiers.get(tier, 0) + 1
            if args.scrape_only:
                print(f"  [{i}/{total}] SCRAPED ({tier}, {len(text)}c)  {lead['website'][:55]}")
                continue

            if not isinstance(outcome, dict):
                llm_err += 1
                print(f"  [{i}/{total}] LLM error  {lead['website'][:50]}  {outcome}")
                continue

            q = bool(outcome.get("is_qualified"))
            qualified += q
            disqualified += (not q)
            with conn.cursor() as c:
                c.execute("""
                    update realtruck_leads set is_qualified=%s, business_typology=%s,
                      confidence_score=%s, brands_mentioned=%s, ai_reasoning=%s,
                      ai_skip_reason=null, ai_qualified_at=%s where id=%s
                """, (q, outcome.get("business_typology"), outcome.get("confidence_score"),
                      json.dumps(outcome.get("brands_mentioned") or []), outcome.get("reasoning"),
                      datetime.now(timezone.utc), lead["id"]))
            conn.commit()
            print(f"  [{i}/{total}] {'QUALIFIED   ' if q else 'DISQUALIFIED'} "
                  f"[{outcome.get('business_typology')}] via {tier}  {lead['website'][:45]}")

    print(f"\nScraped OK: {ok}/{total} ({100*ok/total if total else 0:.0f}%)   still blocked: {failed}")
    if tiers:
        print("  by tier: " + ", ".join(f"{k}={v}" for k, v in sorted(tiers.items(), key=lambda x: -x[1])))
    if not args.scrape_only:
        print(f"Qualified: {qualified}   Disqualified: {disqualified}   LLM errors: {llm_err}")
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
