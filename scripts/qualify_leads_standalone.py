"""
Standalone script — no Django required.
Qualifies leads by scraping their websites via Storm proxy and asking Claude Haiku.

Run on the server:
  python3 qualify_leads_standalone.py
  python3 qualify_leads_standalone.py --state TX
  python3 qualify_leads_standalone.py --limit 100
  python3 qualify_leads_standalone.py --workers 15
  python3 qualify_leads_standalone.py --requalify     # re-run even if already qualified/skipped

Requirements:
  pip3 install psycopg2-binary requests anthropic curl_cffi
"""
import argparse
import json
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from html.parser import HTMLParser
from urllib.parse import urlparse

import anthropic
import psycopg2
import psycopg2.extras
import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

try:
    from curl_cffi import requests as cffi_requests
    HAS_CURL_CFFI = True
except ImportError:
    HAS_CURL_CFFI = False
    print("WARNING: curl_cffi not installed — falling back to plain requests (more blocks expected)")

# ------------------------------------------------------------------
# Config
# ------------------------------------------------------------------
DB_HOST     = "5.161.121.143"
DB_PORT     = 5432
DB_NAME     = "aftermarketmonkey-db"
DB_USER     = "aftermarketmonkey_admin"
DB_PASSWORD = "%UUqiucpRIdg1XWx"

ANTHROPIC_API_KEY = ""   # set here or export ANTHROPIC_API_KEY=... before running

WORKERS    = 10
BATCH_SIZE = 50
SCRAPE_TIMEOUT  = 15
CLAUDE_MODEL    = "claude-haiku-4-5"
MAX_WEBSITE_CHARS = 6000

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

SCRAPE_PATHS = ["", "/about", "/about-us", "/services", "/products", "/what-we-do"]

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
# HTML text extractor
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
    return re.sub(r"\s+", " ", text).strip()[:MAX_WEBSITE_CHARS]


# ------------------------------------------------------------------
# Scraping — three-tier fallback
#   1. curl_cffi Chrome impersonation + proxy  (beats TLS fingerprinting + Cloudflare)
#   2. plain requests + proxy                  (simpler sites)
#   3. curl_cffi Chrome impersonation direct   (if proxy fails)
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


def _scrape(url: str) -> str | None:
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
# Claude qualification
# ------------------------------------------------------------------
def _qualify(client: anthropic.Anthropic, url: str, text: str) -> tuple[dict | None, str | None]:
    user_prompt = (
        f"Please analyze the following website content and output the JSON qualification object.\n\n"
        f"Website URL: {url}\n"
        f'Website Text:\n"""\n{text}\n"""'
    )
    try:
        message = client.messages.create(
            model=CLAUDE_MODEL,
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
# Per-lead worker
# ------------------------------------------------------------------
def _process(row: dict, client: anthropic.Anthropic) -> tuple[dict | None, str | None]:
    """Returns (update_dict, error_message)."""
    text = _scrape(row["website"])
    if not text:
        return None, "scrape failed"

    result, error = _qualify(client, row["website"], text)
    if not result:
        return None, error

    return {
        "id": row["id"],
        "is_qualified":      result.get("is_qualified"),
        "business_typology": result.get("business_typology"),
        "confidence_score":  result.get("confidence_score"),
        "brands_mentioned":  json.dumps(result.get("brands_mentioned") or []),
        "ai_reasoning":      result.get("reasoning"),
        "ai_qualified_at":   datetime.now(timezone.utc),
        "ai_skip_reason":    None,
    }, None


# ------------------------------------------------------------------
# Bulk DB save
# ------------------------------------------------------------------
def bulk_save(conn, batch: list[tuple]):
    """
    batch items: (id, is_qualified, business_typology, confidence_score,
                  brands_mentioned, ai_reasoning, ai_qualified_at, ai_skip_reason)
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(
            cur,
            """
            UPDATE lead SET
                is_qualified      = %(is_qualified)s,
                business_typology = %(business_typology)s,
                confidence_score  = %(confidence_score)s,
                brands_mentioned  = %(brands_mentioned)s,
                ai_reasoning      = %(ai_reasoning)s,
                ai_qualified_at   = %(ai_qualified_at)s,
                ai_skip_reason    = %(ai_skip_reason)s
            WHERE id = %(id)s
            """,
            batch,
            page_size=100,
        )
    conn.commit()


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------
def main():
    import os

    parser = argparse.ArgumentParser(description="Qualify leads via website scraping + Claude Haiku")
    parser.add_argument("--state",     default=None, help="Filter by state code (e.g. TX)")
    parser.add_argument("--limit",     type=int, default=None, help="Max leads to process")
    parser.add_argument("--workers",   type=int, default=WORKERS, help=f"Parallel workers (default: {WORKERS})")
    parser.add_argument("--requalify", action="store_true", help="Re-run even if already qualified or skipped")
    args = parser.parse_args()

    api_key = ANTHROPIC_API_KEY or os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        print("ERROR: ANTHROPIC_API_KEY not set")
        return

    client = anthropic.Anthropic(api_key=api_key)

    print(f"Connecting to {DB_HOST}/{DB_NAME}...")
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD,
        connect_timeout=10,
    )

    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        query = """
            SELECT id, website
            FROM lead
            WHERE website IS NOT NULL
              AND website <> ''
              AND website_live = TRUE
        """
        params = []

        if not args.requalify:
            query += " AND is_qualified IS NULL AND ai_skip_reason IS NULL"

        if args.state:
            query += " AND state = %s"
            params.append(args.state.upper())

        if args.limit:
            query += " LIMIT %s"
            params.append(args.limit)

        cur.execute(query, params)
        leads = [dict(r) for r in cur.fetchall()]

    total = len(leads)
    print(f"Found {total} leads to qualify")
    print(f"curl_cffi: {'✓ enabled' if HAS_CURL_CFFI else '✗ not installed'}")
    print(f"Workers  : {args.workers}  Batch: {BATCH_SIZE}  Model: {CLAUDE_MODEL}")
    if args.state:
        print(f"State    : {args.state.upper()}")
    if args.requalify:
        print(f"Mode     : requalify (overwriting existing results)")
    print()

    qualified   = 0
    disqualified = 0
    failed      = 0
    pending     = []

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {executor.submit(_process, row, client): row for row in leads}

        for i, future in enumerate(as_completed(futures), 1):
            row = futures[future]
            try:
                result, error = future.result()
            except Exception as e:
                result, error = None, str(e)

            if result is None:
                failed += 1
                print(f"  [{i}/{total}] SKIP  {row['website']}  ({error})")
                pending.append({
                    "id": row["id"],
                    "is_qualified":      None,
                    "business_typology": None,
                    "confidence_score":  None,
                    "brands_mentioned":  json.dumps([]),
                    "ai_reasoning":      None,
                    "ai_qualified_at":   datetime.now(timezone.utc),
                    "ai_skip_reason":    (error or "unknown")[:255],
                })
            else:
                if result["is_qualified"]:
                    qualified += 1
                    tag = "✓ QUALIFIED    "
                else:
                    disqualified += 1
                    tag = "✗ DISQUALIFIED "
                print(
                    f"  [{i}/{total}] {tag} "
                    f"[{result['business_typology']}] "
                    f"score={result['confidence_score']}  "
                    f"{row['website']}"
                )
                print(f"           {result['ai_reasoning']}")
                pending.append(result)

            if len(pending) >= BATCH_SIZE:
                bulk_save(conn, pending)
                print(f"  -- saved {len(pending)} to DB --")
                pending.clear()

    if pending:
        bulk_save(conn, pending)
        print(f"  -- saved {len(pending)} to DB --")

    conn.close()
    print(f"\nDone.")
    print(f"  Qualified    : {qualified}")
    print(f"  Disqualified : {disqualified}")
    print(f"  Failed/skip  : {failed}")
    print(f"  Total        : {total}")


if __name__ == "__main__":
    main()
