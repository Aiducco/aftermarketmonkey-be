"""
Qualification at scale via the Anthropic Batch API -- 50% cheaper than live calls, which at the
88k-lead backlog is roughly $110 rather than $221.

Batch is asynchronous, so the work splits into three phases you run in order:

  scrape    Fetch each lead's site and write {id, website, text} to a JSONL file. This is the slow
            part (network-bound, not LLM-bound) and the part worth sharding across processes.
  submit    Turn those JSONL files into Batch API jobs and record the batch ids in a state file.
  collect   Poll the batches, parse the verdicts, write them to the DB.

The phases are separate on purpose: scraping takes hours and batches complete within one, so you
do not want a crash in one to lose the other. A finished scrape file can be re-submitted, and a
submitted batch can be collected repeatedly -- collect is idempotent.

No Django, same as the other scripts/*_standalone.py, so it runs on the server regardless of what
that checkout's models look like.

Usage (on the server):
  # phase 1 -- shard across as many processes as you want
  python3 scripts/qualify_leads_batch.py scrape --shard 0/4 --workers 40 --out /tmp/sc_0.jsonl
  # phase 2 -- once the scrape files exist
  python3 scripts/qualify_leads_batch.py submit --in '/tmp/sc_*.jsonl' --state /tmp/batches.json
  # phase 3 -- safe to re-run; skips batches already collected
  python3 scripts/qualify_leads_batch.py collect --state /tmp/batches.json

Requires: psycopg2-binary, requests, anthropic, curl_cffi (primp optional)
"""
import argparse
import glob
import json
import os
import random
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from html.parser import HTMLParser
from urllib.parse import urlparse

import psycopg2
import psycopg2.extras
import requests
import urllib3

try:
    from curl_cffi import requests as cffi_requests
except ImportError:
    cffi_requests = None
try:
    import primp
except ImportError:
    primp = None

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

MODEL = "claude-haiku-4-5"
MAX_WEBSITE_CHARS = 6000
TIMEOUT = 15
MAX_TOKENS = 1024

# Keep well under the API's 100k-request / 256MB per-batch ceiling. At ~2KB of scraped text per
# request, 20k requests is roughly 40MB -- comfortable, and it fails in smaller pieces.
BATCH_SIZE = 20_000

SCRAPE_PATHS = ["", "/about", "/about-us", "/services", "/service", "/installation",
                "/locations", "/store-locator", "/stores", "/products", "/what-we-do"]
IMPERSONATE = ["chrome131", "chrome124", "chrome120", "safari17_0", "edge101"]

TABLES = {"lead": "id", "realtruck_leads": "id"}

HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
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

BOILERPLATE_RE = re.compile(
    r"(manage your privacy|this site uses cookies[^.]*\.|reject advertising cookies|"
    r"save preferences|accept all cookies|cookie preferences|we use cookies[^.]*\.|"
    r"opens in new tab|skip to (?:main )?content)", re.IGNORECASE)


class _Enough(Exception):
    """Raised to abort parsing once we have more text than we will ever use."""


class _TextExtractor(HTMLParser):
    SKIP_TAGS = {"script", "style", "noscript", "head", "meta", "link"}

    def __init__(self):
        super().__init__()
        self._skip_tag = None
        self.chunks = []
        self._len = 0

    def handle_starttag(self, tag, attrs):
        if tag.lower() in self.SKIP_TAGS:
            self._skip_tag = tag.lower()

    def handle_endtag(self, tag):
        if tag.lower() == self._skip_tag:
            self._skip_tag = None

    def handle_data(self, data):
        if not self._skip_tag and data.strip():
            self.chunks.append(data.strip())
            self._len += len(data)
            if self._len > MAX_WEBSITE_CHARS * 3:
                raise _Enough()


MAX_HTML_BYTES = 250000


def extract_text(html: str) -> str:
    # Parse only the head of the document -- see MAX_HTML_BYTES rationale above.
    html = html[:MAX_HTML_BYTES]
    p = _TextExtractor()
    try:
        p.feed(html)
    except Exception:
        pass
    text = BOILERPLATE_RE.sub(" ", " ".join(p.chunks))
    return re.sub(r"\s+", " ", text).strip()[:MAX_WEBSITE_CHARS]


def _looks_blocked(text: str) -> bool:
    t = text[:600].lower()
    return any(m in t for m in (
        "just a moment", "checking your browser", "enable javascript and cookies",
        "attention required", "access denied", "request blocked", "verifying you are human"))


def fetch(url: str):
    if cffi_requests is not None:
        for imp in random.sample(IMPERSONATE, k=3):
            try:
                r = cffi_requests.get(url, timeout=TIMEOUT, headers=HEADERS,
                                      allow_redirects=True, impersonate=imp, verify=False)
                if r.status_code == 200:
                    t = extract_text(r.text)
                    if t and not _looks_blocked(t):
                        return t
            except Exception:
                continue
    if primp is not None:
        try:
            r = primp.Client(impersonate="chrome_131", timeout=TIMEOUT, verify=False).get(url)
            if r.status_code == 200:
                t = extract_text(r.text)
                if t and not _looks_blocked(t):
                    return t
        except Exception:
            pass
    try:
        r = requests.get(url, timeout=TIMEOUT, headers=HEADERS, allow_redirects=True, verify=False)
        if r.status_code == 200:
            t = extract_text(r.text)
            if t and not _looks_blocked(t):
                return t
    except Exception:
        pass
    return None


def scrape(website: str):
    website = (website or "").strip()
    if "://" not in website:
        website = "https://" + website
    p = urlparse(website)
    for base in (f"https://{p.netloc}", f"http://{p.netloc}"):
        # Probe the root first: if it is unreachable, walking the other paths only multiplies
        # timeouts on a site that is not going to answer.
        root = fetch(base)
        if not root:
            continue
        parts = [root]
        for path in SCRAPE_PATHS[1:]:
            if len(" ".join(parts)) >= MAX_WEBSITE_CHARS:
                break
            t = fetch(base + path)
            if t:
                parts.append(t)
        return " ".join(parts).strip()[:MAX_WEBSITE_CHARS] or None
    return None


def db_conn():
    env = {}
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    for line in open(os.path.join(root, ".env")):
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip().strip('"').strip("'")
    return psycopg2.connect(
        host=env.get("DATABASE_HOST", "127.0.0.1"), port=env.get("DATABASE_PORT", "5432"),
        dbname=env["DATABASE_NAME"], user=env["DATABASE_USER"], password=env["DATABASE_PASSWORD"])


# ------------------------------------------------------------------ phase 1
def cmd_scrape(args) -> int:
    pk = TABLES[args.table]
    shard_sql = ""
    if args.shard:
        i, n = (int(x) for x in args.shard.split("/"))
        shard_sql = f" and mod({pk}, {n}) = {i}"

    conn = db_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(f"""
        select {pk} as id, website from {args.table}
        where website_live is true and is_qualified is null and ai_skip_reason is null
          and website is not null and website <> ''{shard_sql}
        order by {pk} {f'limit {int(args.limit)}' if args.limit else ''}
    """)
    leads = cur.fetchall()
    cur.close()

    # Resume support: a scrape of tens of thousands will get interrupted at some point.
    done = set()
    if os.path.exists(args.out):
        with open(args.out) as fh:
            for line in fh:
                try:
                    done.add(json.loads(line)["id"])
                except Exception:
                    pass
        leads = [l for l in leads if l["id"] not in done]
        print(f"resuming: {len(done)} already scraped in {args.out}")

    total = len(leads)
    print(f"scraping {total} leads [shard={args.shard or 'all'}, {args.workers} workers] -> {args.out}")
    ok = fail = 0
    t0 = time.time()
    with open(args.out, "a") as out, ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {ex.submit(scrape, l["website"]): l for l in leads}
        for i, fut in enumerate(as_completed(futures), 1):
            lead = futures[fut]
            try:
                text = fut.result()
            except Exception:
                text = None
            if text:
                ok += 1
                out.write(json.dumps({"id": lead["id"], "website": lead["website"], "text": text}) + "\n")
            else:
                fail += 1
                # text=null marks "tried, unreachable" so a resume skips it instead of paying the
                # full timeout chain again. submit() filters these out.
                out.write(json.dumps({"id": lead["id"], "website": lead["website"], "text": None}) + "\n")
            out.flush()
            if i % 250 == 0:
                rate = i / max(time.time() - t0, 1) * 60
                print(f"  [{i}/{total}] ok={ok} blocked={fail}  {rate:.0f}/min  "
                      f"eta {(total - i) / max(rate, 1):.0f}min", flush=True)
    print(f"\nscrape done: {ok} usable, {fail} unreachable -> {args.out}")
    conn.close()
    return 0


# ------------------------------------------------------------------ phase 2
def cmd_submit(args) -> int:
    import anthropic
    from anthropic.types.message_create_params import MessageCreateParamsNonStreaming
    from anthropic.types.messages.batch_create_params import Request

    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    rows = []
    for path in sorted(glob.glob(args.inp)):
        with open(path) as fh:
            for line in fh:
                try:
                    rows.append(json.loads(line))
                except Exception:
                    pass
    # One id may appear twice if a shard was re-run; last write wins.
    rows = [r for r in rows if r.get("text")]          # drop unreachable markers
    rows = list({r["id"]: r for r in rows}.values())
    print(f"{len(rows)} scraped leads to submit, {BATCH_SIZE} per batch")

    state = {"table": args.table, "batches": []}
    if os.path.exists(args.state):
        state = json.load(open(args.state))
        already = {b["id"] for b in state["batches"]}
        print(f"state file already holds {len(already)} batches; appending")

    for start in range(0, len(rows), BATCH_SIZE):
        chunk = rows[start:start + BATCH_SIZE]
        reqs = [
            Request(
                custom_id=str(r["id"]),
                params=MessageCreateParamsNonStreaming(
                    model=args.model, max_tokens=MAX_TOKENS, system=SYSTEM_PROMPT,
                    messages=[{"role": "user", "content":
                               'Please analyze the following website content and output the JSON '
                               'qualification object.\n\nWebsite URL: ' + str(r["website"]) +
                               '\nWebsite Text:\n"""\n' + r["text"] + '\n"""'}],
                ),
            ) for r in chunk
        ]
        batch = client.messages.batches.create(requests=reqs)
        state["batches"].append({"id": batch.id, "n": len(reqs), "collected": False})
        print(f"  submitted {batch.id}  ({len(reqs)} requests)")
        json.dump(state, open(args.state, "w"), indent=2)

    print(f"\n{len(state['batches'])} batches recorded in {args.state}")
    return 0


# ------------------------------------------------------------------ phase 3
def cmd_collect(args) -> int:
    import anthropic
    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    state = json.load(open(args.state))
    table = state.get("table", "lead")
    pk = TABLES[table]
    conn = db_conn()

    qualified = disqualified = errored = 0
    for entry in state["batches"]:
        if entry.get("collected"):
            print(f"  {entry['id']}: already collected, skipping")
            continue
        while True:
            b = client.messages.batches.retrieve(entry["id"])
            if b.processing_status == "ended":
                break
            print(f"  {entry['id']}: {b.processing_status} "
                  f"(done {b.request_counts.succeeded}/{entry['n']})", flush=True)
            time.sleep(30)

        now = datetime.now(timezone.utc)
        for res in client.messages.batches.results(entry["id"]):
            if res.result.type != "succeeded":
                errored += 1
                continue
            raw = next((b.text for b in res.result.message.content if b.type == "text"), "").strip()
            raw = re.sub(r"^```(?:json)?\s*", "", raw)
            raw = re.sub(r"\s*```$", "", raw)
            try:
                d = json.loads(raw)
            except Exception:
                errored += 1
                continue
            q = bool(d.get("is_qualified"))
            qualified += q
            disqualified += (not q)
            with conn.cursor() as c:
                c.execute(f"""update {table} set is_qualified=%s, business_typology=%s,
                              confidence_score=%s, brands_mentioned=%s, ai_reasoning=%s,
                              ai_qualified_at=%s where {pk} = %s""",
                          (q, d.get("business_typology"), d.get("confidence_score"),
                           json.dumps(d.get("brands_mentioned") or []), d.get("reasoning"),
                           now, res.custom_id))
        conn.commit()
        entry["collected"] = True
        json.dump(state, open(args.state, "w"), indent=2)
        print(f"  {entry['id']}: collected")

    print(f"\nqualified {qualified} | disqualified {disqualified} | errors {errored}")
    conn.close()
    return 0



# ------------------------------------------------------------------ live (no Batch API)
# Azure Batch needs a GlobalBatch-SKU deployment; production-gpt-4.1 is GlobalStandard, so the
# submit/collect phases above cannot be used with it. This phase does the same job with live
# calls, reading the scrape files so no site is fetched twice, and packing N businesses per
# request (~31% cheaper than one call each, since the system prompt is sent once).
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


def cmd_live(args) -> int:
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from src.integrations.llm import azure_llm

    pk = TABLES[args.table]
    conn = db_conn()

    # Only leads still awaiting a verdict -- makes the phase resumable and safe to re-run.
    with conn.cursor() as c:
        c.execute(f"select {pk} from {args.table} where is_qualified is null and ai_skip_reason is null")
        todo = {str(r[0]) for r in c.fetchall()}

    rows = []
    for path in sorted(glob.glob(args.inp)):
        with open(path) as fh:
            for line in fh:
                try:
                    r = json.loads(line)
                except Exception:
                    continue
                if r.get("text") and str(r["id"]) in todo:
                    rows.append(r)
    rows = list({r["id"]: r for r in rows}.values())
    if args.limit:
        rows = rows[:args.limit]

    client = azure_llm.client(max_retries=8)
    packs = [rows[i:i + args.pack] for i in range(0, len(rows), args.pack)]
    print(f"qualifying {len(rows)} leads in {len(packs)} packs of {args.pack} "
          f"[{args.workers} workers, {azure_llm.deployment()}]", flush=True)

    def run_pack(pack):
        blocks = "\n\n".join(
            f"--- BUSINESS id={r['id']} url={r['website']} ---\n{r['text']}" for r in pack)
        parsed, err = azure_llm.complete_json(
            client, PACK_SYSTEM_PROMPT,
            f"Analyze each business below and return one result per id.\n\n{blocks}",
            max_tokens=min(1024 * len(pack), 8192))
        got = {}
        if parsed:
            for e in parsed.get("results") or []:
                got[str(e.get("id"))] = e
        # Anything the pack missed gets its own call rather than being silently dropped.
        for r in pack:
            if str(r["id"]) not in got:
                one, _ = azure_llm.complete_json(
                    client, SYSTEM_PROMPT,
                    f'Please analyze the following website content and output the JSON '
                    f'qualification object.\n\nWebsite URL: {r["website"]}\n'
                    f'Website Text:\n"""\n{r["text"]}\n"""', max_tokens=1024)
                if one:
                    got[str(r["id"])] = one
        return pack, got

    q = d = miss = 0
    done = 0
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        for fut in as_completed([ex.submit(run_pack, p) for p in packs]):
            try:
                pack, got = fut.result()
            except Exception as e:
                print(f"  pack failed: {type(e).__name__}: {e}", flush=True)
                continue
            now = datetime.now(timezone.utc)
            with conn.cursor() as c:
                for r in pack:
                    e = got.get(str(r["id"]))
                    done += 1
                    if not e:
                        miss += 1
                        continue
                    val = bool(e.get("is_qualified"))
                    q += val
                    d += (not val)
                    c.execute(f"""update {args.table} set is_qualified=%s, business_typology=%s,
                                  confidence_score=%s, brands_mentioned=%s, ai_reasoning=%s,
                                  ai_qualified_at=%s where {pk} = %s""",
                              (val, e.get("business_typology"), e.get("confidence_score"),
                               json.dumps(e.get("brands_mentioned") or []), e.get("reasoning"),
                               now, r["id"]))
            conn.commit()
            if done % 250 < args.pack:
                print(f"  [{done}/{len(rows)}] qualified={q} disqualified={d} no-result={miss}", flush=True)

    print(f"\nDone. qualified {q} | disqualified {d} | no result {miss} | total {done}")
    conn.close()
    return 0



# ------------------------------------------------------------------ prioritize
# Outreach ordering for already-qualified leads. Standalone rather than a Django command because
# the server checkout predates the RealTruckLead model, and copying models.py onto a box running
# production is not worth the risk.
PRIORITY_SYSTEM = """You assess how established and professional an automotive shop is, from the text of its website.

You are NOT deciding whether they are a good prospect -- that is already settled. You are judging
how ADVANCED the business appears, so a sales team knows who to approach first.

Report only what the text actually shows. If something is not evident, mark it false.

website_quality is 0-100 for how substantial the operation looks: 0-30 a bare page or placeholder,
40-60 a normal small shop site, 70-85 a well-built site with rich content, 90-100 a serious
e-commerce or multi-location operation.

Respond ONLY with JSON:
{"results":[{"id":"<id>","website_quality":0-100,"has_ecommerce":bool,"has_online_booking":bool,
"has_financing":bool,"has_team_page":bool,"mentions_multiple_bays_or_locations":bool,
"named_brand_partnerships":int,"years_established":int|null,"serves_fleet_or_commercial":bool,
"summary":"one sentence"}]}"""


def _domain(url):
    u = (url or "").strip().lower()
    u = re.sub(r"^https?://", "", u).split("/")[0].split(":")[0]
    return re.sub(r"^www\.", "", u)


def _composite(row, llm, loc):
    wq = int(llm.get("website_quality") or 0)
    feats = sum(bool(llm.get(k)) for k in ("has_ecommerce", "has_online_booking", "has_financing",
                "has_team_page", "mentions_multiple_bays_or_locations", "serves_fleet_or_commercial"))
    web = wq * 0.45 + min(feats, 6) * 2.5
    hard = min(loc - 1, 5) * 3
    hard += 8 if row["is_preferred"] else 0
    hard += 4 if row["is_double_warranty"] else 0
    hard += 4 if row["is_next_gen"] else 0
    hard += min((row["brand_count"] or 0) / 25.0, 1) * 5
    hard += min(int(llm.get("named_brand_partnerships") or 0) / 10.0, 1) * 4
    score = max(0, min(100, round(web + hard)))
    sig = {"website_quality": wq, "feature_hits": feats, "location_count": loc,
           "is_preferred": bool(row["is_preferred"]), "brand_count": row["brand_count"],
           "named_brand_partnerships": llm.get("named_brand_partnerships"),
           "years_established": llm.get("years_established"),
           "has_ecommerce": bool(llm.get("has_ecommerce")),
           "has_online_booking": bool(llm.get("has_online_booking")),
           "has_financing": bool(llm.get("has_financing")),
           "has_team_page": bool(llm.get("has_team_page")),
           "multi_bay_or_location": bool(llm.get("mentions_multiple_bays_or_locations")),
           "fleet_commercial": bool(llm.get("serves_fleet_or_commercial"))}
    return score, sig


def cmd_prioritize(args) -> int:
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from src.integrations.llm import azure_llm
    from collections import Counter

    conn = db_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""select id, name, website, is_preferred, is_double_warranty, is_next_gen, brand_count
                   from realtruck_leads
                   where is_qualified is true and website is not null and website <> ''
                     %s order by id %s""" % (
        "" if args.rescore else "and outreach_priority is null",
        f"limit {int(args.limit)}" if args.limit else ""))
    rows = cur.fetchall()

    cur.execute("select website from realtruck_leads where is_qualified is true and website <> ''")
    loc_counts = Counter(_domain(r["website"]) for r in cur.fetchall())
    cur.close()

    client = azure_llm.client(max_retries=6, timeout=120)
    packs = [rows[i:i + args.pack] for i in range(0, len(rows), args.pack)]
    print(f"scoring {len(rows)} leads in {len(packs)} packs [{args.workers} workers]", flush=True)

    def run(pack):
        scraped = []
        for r in pack:
            site = r["website"] if "://" in r["website"] else "https://" + r["website"]
            t = scrape(site)
            if t:
                scraped.append((r, site, t))
        if not scraped:
            return []
        blocks = "\n\n".join(f"--- BUSINESS id={r['id']} url={s} ---\n{t}" for r, s, t in scraped)
        parsed, _ = azure_llm.complete_json(
            client, PRIORITY_SYSTEM,
            f"Assess each business below and return one result per id.\n\n{blocks}",
            max_tokens=min(900 * len(scraped), 8192))
        got = {}
        if parsed:
            for e in parsed.get("results") or []:
                got[str(e.get("id"))] = e
        out = []
        for r, s, t in scraped:
            llm = got.get(str(r["id"]))
            if llm:
                out.append((r, llm))
        return out

    tiers, done = Counter(), 0
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        for fut in as_completed([ex.submit(run, p) for p in packs]):
            try:
                res = fut.result()
            except Exception as e:
                print(f"  pack error: {type(e).__name__}: {e}", flush=True)
                continue
            now = datetime.now(timezone.utc)
            with conn.cursor() as c:
                for r, llm in res:
                    loc = loc_counts.get(_domain(r["website"]), 1)
                    score, sig = _composite(r, llm, loc)
                    tier = "A" if score >= 70 else ("B" if score >= 45 else "C")
                    tiers[tier] += 1; done += 1
                    c.execute("""update realtruck_leads set outreach_priority=%s, priority_tier=%s,
                                 website_quality=%s, location_count=%s, priority_signals=%s,
                                 priority_reasoning=%s, prioritized_at=%s where id=%s""",
                              (score, tier, sig["website_quality"], loc, json.dumps(sig),
                               llm.get("summary"), now, r["id"]))
            conn.commit()
            if done % 100 < args.pack:
                print(f"  [{done}/{len(rows)}] A={tiers['A']} B={tiers['B']} C={tiers['C']}", flush=True)

    print(f"\nDone. A: {tiers['A']}  B: {tiers['B']}  C: {tiers['C']}  scored: {done}")
    conn.close()
    return 0



# ------------------------------------------------------------------ salvage
# Second-chance extraction for sites the normal scrape wrote off. Measured on a 30-site sample:
# 60% are recoverable, and the largest group (13/30) returned perfectly good body text -- they
# failed only because MAX_HTML_BYTES truncated the document before the body, on pages carrying
# hundreds of KB of inline script. Stripping script/style/svg BEFORE truncating fixes those.
# A further 5/30 have thin bodies but usable <meta>, JSON-LD or <title> content.
_SCRIPTish = re.compile(r"<(script|style|svg|noscript)\b.*?</\1>", re.I | re.S)
_META_RE = re.compile(r'<meta[^>]+(?:name|property)="(?:description|og:description|og:title|keywords)"[^>]+content="([^"]{15,400})"', re.I)
_LD_RE = re.compile(r'<script[^>]+application/ld\+json[^>]*>(.*?)</script>', re.I | re.S)
_TITLE_RE = re.compile(r"<title[^>]*>(.*?)</title>", re.I | re.S)


def salvage_text(html: str) -> str | None:
    """Extraction that tries much harder than the fast path, for pages that already failed once."""
    if not html:
        return None
    # Strip the heavy tags first so the byte budget is spent on real markup, not bundled JS.
    lean = _SCRIPTish.sub(" ", html)
    body = extract_text(lean)
    parts = [body] if body else []

    # Metadata is often the only prose on a JS-rendered page, and enough to classify a business.
    meta = " ".join(m.strip() for m in _META_RE.findall(html)[:8])
    title = " ".join(t.strip() for t in _TITLE_RE.findall(html)[:1])
    ld_bits = []
    for blob in _LD_RE.findall(html)[:4]:
        try:
            data = json.loads(blob.strip())
        except Exception:
            continue
        def walk(o):
            if isinstance(o, dict):
                for k, v in o.items():
                    if k in ("name", "description", "makesOffer", "knowsAbout", "slogan",
                             "hasOfferCatalog", "serviceType", "@type") and isinstance(v, str):
                        ld_bits.append(v)
                    else:
                        walk(v)
            elif isinstance(o, list):
                for v in o:
                    walk(v)
        walk(data)
    extra = " ".join(x for x in ([title, meta] + ld_bits) if x)
    if extra:
        parts.append(re.sub(r"\s+", " ", extra))

    out = " ".join(parts).strip()[:MAX_WEBSITE_CHARS]
    return out if len(out) >= 120 else None


def salvage_fetch(url: str) -> str | None:
    """Plain requests first -- the sample showed it succeeds where the impersonating tiers did not."""
    for verify in (True, False):
        try:
            r = requests.get(url, timeout=TIMEOUT, headers=HEADERS,
                             allow_redirects=True, verify=verify)
            if r.status_code == 200:
                t = salvage_text(r.text)
                if t:
                    return t
        except Exception:
            continue
    if cffi_requests is not None:
        for imp in random.sample(IMPERSONATE, k=2):
            try:
                r = cffi_requests.get(url, timeout=TIMEOUT, headers=HEADERS,
                                      allow_redirects=True, impersonate=imp, verify=False)
                if r.status_code == 200:
                    t = salvage_text(r.text)
                    if t:
                        return t
            except Exception:
                continue
    return None


def cmd_salvage(args) -> int:
    pk = TABLES[args.table]
    conn = db_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(f"""select {pk} as id, website from {args.table}
                    where website_live is true and is_qualified is null
                      and website is not null and website <> ''
                    order by {pk} {f'limit {int(args.limit)}' if args.limit else ''}""")
    leads = cur.fetchall()
    cur.close()

    done = set()
    if os.path.exists(args.out):
        for line in open(args.out):
            try:
                done.add(json.loads(line)["id"])
            except Exception:
                pass
        leads = [l for l in leads if l["id"] not in done]

    print(f"salvaging {len(leads)} previously-unreadable leads [{args.workers} workers] -> {args.out}",
          flush=True)
    ok = fail = 0
    with open(args.out, "a") as out, ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {ex.submit(salvage_fetch,
                             l["website"] if "://" in l["website"] else "https://" + l["website"]): l
                   for l in leads}
        for i, fut in enumerate(as_completed(futures), 1):
            lead = futures[fut]
            try:
                text = fut.result()
            except Exception:
                text = None
            if text:
                ok += 1
                out.write(json.dumps({"id": lead["id"], "website": lead["website"], "text": text}) + "\n")
            else:
                fail += 1
            out.flush()
            if i % 250 == 0:
                print(f"  [{i}/{len(leads)}] recovered={ok} still-dead={fail} "
                      f"({100*ok/max(i,1):.0f}%)", flush=True)
    print(f"\nsalvage done: {ok} recovered, {fail} still unreadable -> {args.out}")
    conn.close()
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)

    s1 = sub.add_parser("scrape")
    s1.add_argument("--table", default="lead", choices=sorted(TABLES))
    s1.add_argument("--shard", default=None, help="i/N")
    s1.add_argument("--workers", type=int, default=30)
    s1.add_argument("--limit", type=int, default=None)
    s1.add_argument("--out", default="/tmp/scrape.jsonl")
    s1.set_defaults(fn=cmd_scrape)

    s2 = sub.add_parser("submit")
    s2.add_argument("--table", default="lead", choices=sorted(TABLES))
    s2.add_argument("--in", dest="inp", default="/tmp/scrape*.jsonl")
    s2.add_argument("--state", default="/tmp/batches.json")
    s2.add_argument("--model", default=MODEL)
    s2.set_defaults(fn=cmd_submit)

    s4 = sub.add_parser("live", help="qualify from scrape files with live calls (no Batch API)")
    s4.add_argument("--table", default="lead", choices=sorted(TABLES))
    s4.add_argument("--in", dest="inp", default="/tmp/sc_*.jsonl")
    s4.add_argument("--limit", type=int, default=None)
    s4.add_argument("--pack", type=int, default=5)
    s4.add_argument("--workers", type=int, default=6)
    s4.set_defaults(fn=cmd_live)

    s5 = sub.add_parser("prioritize", help="score qualified realtruck leads for outreach order")
    s5.add_argument("--limit", type=int, default=None)
    s5.add_argument("--pack", type=int, default=5)
    s5.add_argument("--workers", type=int, default=5)
    s5.add_argument("--rescore", action="store_true")
    s5.set_defaults(fn=cmd_prioritize)

    s6 = sub.add_parser("salvage", help="second-chance scrape for leads the normal pass could not read")
    s6.add_argument("--table", default="lead", choices=sorted(TABLES))
    s6.add_argument("--limit", type=int, default=None)
    s6.add_argument("--workers", type=int, default=20)
    s6.add_argument("--out", default="/tmp/salvage.jsonl")
    s6.set_defaults(fn=cmd_salvage)

    s3 = sub.add_parser("collect")
    s3.add_argument("--state", default="/tmp/batches.json")
    s3.set_defaults(fn=cmd_collect)

    args = ap.parse_args()
    return args.fn(args)


if __name__ == "__main__":
    raise SystemExit(main())
