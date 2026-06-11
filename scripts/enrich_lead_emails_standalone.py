"""
Standalone script — no Django required.
Scrapes emails from lead websites and saves them to the `emails` JSON field.

Stage 1 — direct scraping:
  Tries homepage + /contact, /contact-us, /about, /about-us, /company, /team, /reach-us, /info

Stage 2 — Tavily + Claude fallback (if scraping finds nothing):
  Searches Tavily for "{name} {city} {state} email contact", then asks Claude Haiku
  to extract any business email addresses visible in the search snippets.

Run on the server:
  python3 enrich_lead_emails_standalone.py
  python3 enrich_lead_emails_standalone.py --live-only
  python3 enrich_lead_emails_standalone.py --state TX
  python3 enrich_lead_emails_standalone.py --limit 100
  python3 enrich_lead_emails_standalone.py --workers 5
  python3 enrich_lead_emails_standalone.py --refetch
  python3 enrich_lead_emails_standalone.py --no-tavily

Requirements:
  pip3 install psycopg2-binary requests anthropic curl_cffi
"""
import argparse
import json
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse

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
    print("WARNING: curl_cffi not installed — falling back to plain requests")

# ------------------------------------------------------------------
# Config
# ------------------------------------------------------------------
DB_HOST     = "5.161.121.143"
DB_PORT     = 5432
DB_NAME     = "aftermarketmonkey-db"
DB_USER     = "aftermarketmonkey_admin"
DB_PASSWORD = "%UUqiucpRIdg1XWx"

ANTHROPIC_API_KEY = ""   # or set via env: export ANTHROPIC_API_KEY=...
TAVILY_API_KEY    = ""   # or set via env: export TAVILY_API_KEY=...

WORKERS   = 5
BATCH_SIZE = 50
TIMEOUT   = 10

TAVILY_SEARCH_URL = "https://api.tavily.com/search"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

SCRAPE_PATHS = [
    "", "/contact", "/contact-us", "/contactus",
    "/about", "/about-us", "/aboutus",
    "/company", "/team", "/reach-us", "/info",
]

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
# Stage 1: direct website scraping (curl_cffi → plain requests fallback)
# ------------------------------------------------------------------

def _fetch(url: str) -> str | None:
    # Tier 1: curl_cffi Chrome TLS impersonation
    if HAS_CURL_CFFI:
        try:
            resp = cffi_requests.get(
                url, timeout=TIMEOUT, headers=HEADERS,
                allow_redirects=True, impersonate="chrome124", verify=False,
            )
            if resp.status_code == 200:
                return resp.text
        except Exception:
            pass

    # Tier 2: plain requests
    try:
        resp = requests.get(
            url, timeout=TIMEOUT, headers=HEADERS,
            allow_redirects=True, verify=False,
        )
        if resp.status_code == 200:
            return resp.text
    except Exception:
        pass

    return None


def _scrape_website(website: str) -> list[str]:
    parsed = urlparse(website)
    base_url = f"{parsed.scheme}://{parsed.netloc}".rstrip("/")
    all_emails: list[str] = []
    seen: set[str] = set()

    for path in SCRAPE_PATHS:
        html = _fetch(base_url + path)
        if html:
            for em in _extract_emails(html):
                if em not in seen:
                    seen.add(em)
                    all_emails.append(em)
        time.sleep(0.05)

    return all_emails


# ------------------------------------------------------------------
# Stage 2: Tavily search + Claude Haiku extraction
# ------------------------------------------------------------------

def _tavily_search_emails(row: dict, tavily_key: str) -> list[dict]:
    domain = urlparse(row["website"]).netloc.lstrip("www.")
    queries = [
        f'"{row["name"]}" {row["city"]} {row["state"]} email contact',
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
            print(f"  [tavily error] {type(e).__name__}: {e}")
    return results[:8]


def _claude_extract_emails(row: dict, results: list[dict], anthropic_client) -> list[str]:
    if not results:
        print(f"  [tavily] no results for {row['name']} ({row['city']}, {row['state']})")
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
        f"Business: {row['name']}, {row['city']}, {row['state']}\n"
        f"Website: {row['website']}\n\n"
        f"Search results:\n\n"
        + "\n\n".join(snippets)
        + "\n\n"
        "Extract any BUSINESS email addresses for this specific company from the snippets above.\n"
        "Rules:\n"
        "- Only include emails that belong to this business (match the domain or are clearly their contact)\n"
        "- Exclude Gmail, Yahoo, Hotmail and other free email providers\n"
        "- Exclude tracking/system emails (noreply, no-reply, etc.)\n"
        "- If no valid business email is found, return empty array\n\n"
        'Reply with ONLY a JSON array of email strings, e.g.: ["info@example.com"] or []'
    )

    # Let API errors bubble up — caller will NOT mark emails_not_found so lead gets retried
    response = anthropic_client.messages.create(
        model="claude-haiku-4-5",
        max_tokens=400,
        messages=[{"role": "user", "content": prompt}],
    )
    text = response.content[0].text.strip()
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

def _process(row: dict, tavily_key: str, anthropic_client, use_tavily: bool) -> tuple[list[str], str]:
    # Stage 1: scrape
    emails = _scrape_website(row["website"])
    if emails:
        return emails, "scrape"

    # Stage 2: Tavily + Claude
    if use_tavily and tavily_key and anthropic_client:
        results = _tavily_search_emails(row, tavily_key)
        emails = _claude_extract_emails(row, results, anthropic_client)
        if emails:
            return emails, "tavily+claude"

    return [], "none"


# ------------------------------------------------------------------
# DB helpers
# ------------------------------------------------------------------

def bulk_save_emails(conn, batch: list[tuple]):
    """batch: list of (lead_id, emails_json)"""
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(
            cur,
            "UPDATE lead SET emails = %s WHERE id = %s",
            batch,
            page_size=100,
        )
    conn.commit()


def bulk_mark_not_found(conn, ids: list[int]):
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE lead SET emails_not_found = TRUE WHERE id = ANY(%s)",
            (ids,)
        )
    conn.commit()


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------

def main():
    import os

    parser = argparse.ArgumentParser(description="Enrich lead emails via scraping + Tavily + Claude Haiku")
    parser.add_argument("--state",     default=None, help="Filter by state code (e.g. TX)")
    parser.add_argument("--live-only", action="store_true", help="Only leads where website_live=TRUE")
    parser.add_argument("--refetch",   action="store_true", help="Re-scrape even if emails already found")
    parser.add_argument("--no-tavily", action="store_true", help="Disable Tavily+Claude fallback")
    parser.add_argument("--limit",     type=int, default=None)
    parser.add_argument("--workers",   type=int, default=WORKERS, help=f"Parallel workers (default: {WORKERS})")
    args = parser.parse_args()

    tavily_key    = TAVILY_API_KEY    or os.environ.get("TAVILY_API_KEY", "")
    anthropic_key = ANTHROPIC_API_KEY or os.environ.get("ANTHROPIC_API_KEY", "")
    use_tavily    = not args.no_tavily

    anthropic_client = None
    if use_tavily and anthropic_key:
        try:
            import anthropic
            anthropic_client = anthropic.Anthropic(api_key=anthropic_key)
        except ImportError:
            print("WARNING: anthropic not installed — Tavily fallback disabled")

    print(f"Connecting to {DB_HOST}/{DB_NAME}...")
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD,
        connect_timeout=10,
    )

    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        query = """
            SELECT id, name, city, state, website
            FROM lead
            WHERE website IS NOT NULL AND website <> ''
        """
        params = []

        if args.live_only:
            query += " AND website_live = TRUE"

        if not args.refetch:
            query += " AND emails = '[]' AND emails_not_found = FALSE"

        if args.state:
            query += " AND state = %s"
            params.append(args.state.upper())

        if args.limit:
            query += " LIMIT %s"
            params.append(args.limit)

        cur.execute(query, params)
        leads = [dict(r) for r in cur.fetchall()]

    total = len(leads)
    if not total:
        print("No leads to process.")
        return

    fallback = "✓ Tavily + Claude Haiku" if (use_tavily and tavily_key and anthropic_client) else "✗ disabled"
    live_tag = " [website_live=TRUE only]" if args.live_only else ""
    print(f"Processing {total} leads [{args.workers} workers]{live_tag}")
    print(f"  curl_cffi      : {'✓ enabled' if HAS_CURL_CFFI else '✗ not installed'}")
    print(f"  Stage 1        : direct website scraping")
    print(f"  Stage 2 fallback: {fallback}")
    print()

    found_scrape  = 0
    found_tavily  = 0
    not_found     = 0
    pending_found     = []   # (lead_id, emails_json)
    pending_not_found = []   # lead_id

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(_process, row, tavily_key, anthropic_client, use_tavily): row
            for row in leads
        }

        for i, future in enumerate(as_completed(futures), 1):
            row = futures[future]
            try:
                emails, method = future.result()
            except Exception as e:
                print(f"  [{i}/{total}] ERROR  {row['website']}  ({e})")
                not_found += 1
                continue

            if emails:
                if method == "scrape":
                    found_scrape += 1
                    print(f"  [{i}/{total}] ✓ [scrape]        {row['website']}")
                else:
                    found_tavily += 1
                    print(f"  [{i}/{total}] ✓ [tavily+claude]  {row['name']} ({row['city']}, {row['state']})")
                print(f"           {emails}")
                pending_found.append((json.dumps(emails), row["id"]))
            else:
                not_found += 1
                print(f"  [{i}/{total}] —  {row['website']}")
                pending_not_found.append(row["id"])

            # Flush every BATCH_SIZE
            if len(pending_found) + len(pending_not_found) >= BATCH_SIZE:
                if pending_found:
                    bulk_save_emails(conn, pending_found)
                if pending_not_found:
                    bulk_mark_not_found(conn, pending_not_found)
                print(f"  -- saved {len(pending_found)} found, {len(pending_not_found)} not-found to DB --")
                pending_found.clear()
                pending_not_found.clear()

    # Final flush
    if pending_found:
        bulk_save_emails(conn, pending_found)
    if pending_not_found:
        bulk_mark_not_found(conn, pending_not_found)
    if pending_found or pending_not_found:
        print(f"  -- saved {len(pending_found)} found, {len(pending_not_found)} not-found to DB --")

    conn.close()
    print(f"\nDone.")
    print(f"  Found via scraping      : {found_scrape}")
    print(f"  Found via Tavily+Claude : {found_tavily}")
    print(f"  Not found               : {not_found}")
    print(f"  Total                   : {total}")


if __name__ == "__main__":
    main()
