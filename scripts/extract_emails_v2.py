"""
Email extraction, second generation.

The first version found ~49% of qualified leads. Measuring where it lost the rest:

  fetch quality      it used the plain fetch path, which the salvage work showed was failing on
                     sites that answer a normal request perfectly well (huge inline scripts
                     truncating the document, no verify=False retry, no fingerprint rotation)
  obfuscation        small-shop sites routinely hide addresses: Cloudflare's data-cfemail, HTML
                     entities, "info [at] shop [dot] com", JS string concatenation. A plain
                     regex over raw HTML sees none of these.
  page coverage      11 fixed paths, and no attempt to follow a "Contact" link living at a
                     non-standard URL -- common on custom-built sites.

This version fixes all three, and always sweeps every candidate page rather than stopping at the
first hit, so a business with sales@/service@/parts@ yields all three.

Usage:
  python3 scripts/extract_emails_v2.py --table realtruck_leads --qualified-only
  python3 scripts/extract_emails_v2.py --table lead --qualified-only --workers 20
  python3 scripts/extract_emails_v2.py --table realtruck_leads --retry-not-found   # redo misses
"""
import argparse
import html as htmllib
import json
import os
import random
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin, urlparse

import psycopg2
import psycopg2.extras
import requests
import urllib3

try:
    from curl_cffi import requests as cffi_requests
except ImportError:
    cffi_requests = None

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

TIMEOUT = 15
MAX_PAGES = 14          # ceiling per site, including any discovered contact links
TABLES = {"lead": "id", "realtruck_leads": "id", "leer_leads": "location_id"}
IMPERSONATE = ["chrome131", "chrome124", "chrome120", "safari17_0", "edge101"]

HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# Widened from 11 to 20. /careers and /staff are unglamorous but reliably carry a real address.
SCRAPE_PATHS = [
    "", "/contact", "/contact-us", "/contactus", "/contact.php", "/contact.html",
    "/about", "/about-us", "/aboutus", "/company", "/team", "/our-team", "/staff",
    "/reach-us", "/info", "/get-in-touch", "/support", "/careers", "/locations",
]

EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")

# "info [at] shop [dot] com", "info (at) shop . com", "info AT shop DOT com"
OBFUSCATED_RE = re.compile(
    r"([a-zA-Z0-9._%+\-]+)\s*(?:\[at\]|\(at\)|\{at\}|\s+at\s+|&#64;|%40)\s*"
    r"([a-zA-Z0-9.\-]+)\s*(?:\[dot\]|\(dot\)|\{dot\}|\s+dot\s+|\.)\s*([a-zA-Z]{2,})",
    re.IGNORECASE)

# Cloudflare "email protection": <a class="__cf_email__" data-cfemail="a1b2c3...">
CFEMAIL_RE = re.compile(r'data-cfemail="([0-9a-fA-F]+)"')

MAILTO_RE = re.compile(r'mailto:([^"\'?>\s]+)', re.IGNORECASE)

# Links whose text or href suggests a contact page, for sites that do not use standard paths.
CONTACT_LINK_RE = re.compile(
    r'<a[^>]+href="([^"]+)"[^>]*>(?:(?!</a>).){0,120}?'
    r'(contact|get in touch|reach us|email us|about us|our team|staff|locations)',
    re.IGNORECASE | re.DOTALL)

IGNORE_DOMAINS = {
    "gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "icloud.com", "aol.com",
    "live.com", "msn.com", "protonmail.com", "wix.com", "squarespace.com", "wordpress.com",
    "weebly.com", "shopify.com", "webflow.io", "godaddy.com", "sentry.io", "example.com",
    "domain.com", "email.com", "test.com", "yourdomain.com", "sentry-next.wixpress.com",
}
IGNORE_SUFFIXES = (".wixpress.com", ".sentry.io", ".cloudflare.com", ".amazonaws.com",
                   ".googleusercontent.com", ".wpengine.com", ".hubspot.com", ".mailchimp.com",
                   ".sendgrid.net", ".klaviyo.com", ".squarespace.com", ".shopify.com")
IGNORE_EXT = {".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp", ".pdf", ".zip", ".css", ".js"}
_HASHY = re.compile(r"^[0-9a-f]{12,}$")
_UUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")

_SCRIPTish = re.compile(r"<(script|style)\b.*?</\1>", re.I | re.S)


def decode_cfemail(hexstr: str) -> str | None:
    """Cloudflare XORs the address with the first byte; trivially reversible."""
    try:
        data = bytes.fromhex(hexstr)
        key = data[0]
        return "".join(chr(b ^ key) for b in data[1:])
    except Exception:
        return None


def valid_email(e: str) -> bool:
    e = e.lower().strip(".,;:'\"()<>")
    if "@" not in e or e.count("@") != 1:
        return False
    local, _, domain = e.partition("@")
    if not local or not domain or "." not in domain:
        return False
    if any(e.endswith(x) for x in IGNORE_EXT):
        return False
    if domain in IGNORE_DOMAINS or any(domain.endswith(s) for s in IGNORE_SUFFIXES):
        return False
    if _HASHY.match(local) or _UUID.match(local):
        return False
    if len(local) > 64 or len(e) > 254:
        return False
    return True


def emails_from_html(raw: str) -> set:
    """Every address recoverable from one page, through all the obfuscation routes."""
    found = set()
    if not raw:
        return found

    # 1. Cloudflare-protected addresses, before anything else touches the markup.
    for hx in CFEMAIL_RE.findall(raw):
        dec = decode_cfemail(hx)
        if dec and valid_email(dec):
            found.add(dec.lower())

    # 2. mailto: hrefs -- present even when the address is never rendered as text.
    for m in MAILTO_RE.findall(raw):
        m = htmllib.unescape(m).split("?")[0].strip()
        if valid_email(m):
            found.add(m.lower())

    # Decode entities and drop script/style so JS-embedded noise does not pollute the regex.
    text = htmllib.unescape(_SCRIPTish.sub(" ", raw))

    # 3. Plain addresses.
    for e in EMAIL_RE.findall(text):
        if valid_email(e):
            found.add(e.lower())

    # 4. "info [at] shop [dot] com" and friends.
    for local, dom, tld in OBFUSCATED_RE.findall(text):
        cand = f"{local}@{dom}.{tld}".lower()
        if valid_email(cand):
            found.add(cand)

    return found


def fetch(url: str) -> str | None:
    """Plain requests first (empirically the most reliable), then impersonation, then no-verify."""
    attempts = [lambda u: requests.get(u, timeout=TIMEOUT, headers=HEADERS,
                                       allow_redirects=True, verify=True)]
    if cffi_requests is not None:
        for imp in random.sample(IMPERSONATE, k=2):
            attempts.append(lambda u, i=imp: cffi_requests.get(
                u, timeout=TIMEOUT, headers=HEADERS, allow_redirects=True,
                impersonate=i, verify=False))
    attempts.append(lambda u: requests.get(u, timeout=TIMEOUT, headers=HEADERS,
                                           allow_redirects=True, verify=False))
    for get in attempts:
        try:
            r = get(url)
            if r.status_code == 200 and r.text:
                return r.text
        except Exception:
            continue
    return None


def harvest(website: str) -> tuple[list, bool]:
    """
    Returns (emails, reachable). Sweeps every candidate page -- never stops at the first hit,
    so a shop publishing sales@, service@ and parts@ yields all three.
    """
    site = (website or "").strip()
    if "://" not in site:
        site = "https://" + site
    p = urlparse(site)
    base = f"{p.scheme}://{p.netloc}"

    found, reachable, seen_urls = set(), False, set()

    root_html = fetch(base)
    if root_html:
        reachable = True
        found |= emails_from_html(root_html)

    # Follow contact-ish links the site actually publishes, for non-standard URLs.
    discovered = []
    if root_html:
        for href, _label in CONTACT_LINK_RE.findall(root_html)[:8]:
            u = urljoin(base, htmllib.unescape(href))
            if urlparse(u).netloc == p.netloc and u not in seen_urls:
                seen_urls.add(u)
                discovered.append(u)

    candidates = [base + path for path in SCRAPE_PATHS[1:]] + discovered
    for url in candidates[:MAX_PAGES]:
        if url in seen_urls and url not in discovered:
            continue
        seen_urls.add(url)
        h = fetch(url)
        if h:
            reachable = True
            found |= emails_from_html(h)

    # Prefer addresses on the business's own domain, then role accounts.
    own = p.netloc.lower().replace("www.", "")
    def rank(e):
        dom = e.split("@")[1]
        return (0 if own and (dom in own or own in dom) else 1,
                0 if e.split("@")[0] in ("info", "sales", "service", "parts", "contact") else 1, e)
    return sorted(found, key=rank), reachable


def db_conn():
    env = {}
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    for line in open(os.path.join(root, ".env")):
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip().strip('"').strip("'")
    return psycopg2.connect(host=env.get("DATABASE_HOST", "127.0.0.1"),
                            port=env.get("DATABASE_PORT", "5432"), dbname=env["DATABASE_NAME"],
                            user=env["DATABASE_USER"], password=env["DATABASE_PASSWORD"])


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--table", default="realtruck_leads", choices=sorted(TABLES))
    ap.add_argument("--qualified-only", action="store_true", default=True)
    ap.add_argument("--retry-not-found", action="store_true",
                    help="also redo leads previously marked emails_not_found")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--workers", type=int, default=15)
    args = ap.parse_args()

    pk = TABLES[args.table]
    where = ["website is not null", "website <> ''", "website_live is true"]
    if args.qualified_only:
        where.append("is_qualified is true")
    where.append("emails = '[]'::jsonb" if args.retry_not_found
                 else "emails = '[]'::jsonb and emails_not_found is false")

    conn = db_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(f"select {pk} as id, website from {args.table} where {' and '.join(where)} "
                f"order by {pk} {f'limit {int(args.limit)}' if args.limit else ''}")
    leads = cur.fetchall()
    cur.close()

    print(f"harvesting {len(leads)} leads from {args.table} [{args.workers} workers]", flush=True)
    found = none = unreachable = 0
    total_addrs = 0

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {ex.submit(harvest, l["website"]): l for l in leads}
        for i, fut in enumerate(as_completed(futures), 1):
            lead = futures[fut]
            try:
                emails, reachable = fut.result()
            except Exception:
                emails, reachable = [], False

            with conn.cursor() as c:
                if emails:
                    found += 1
                    total_addrs += len(emails)
                    c.execute(f"update {args.table} set emails=%s, emails_not_found=false "
                              f"where {pk}=%s", (json.dumps(emails), lead["id"]))
                elif reachable:
                    none += 1
                    c.execute(f"update {args.table} set emails_not_found=true where {pk}=%s",
                              (lead["id"],))
                else:
                    # Never mark a site we could not load as "no emails" -- it is retryable.
                    unreachable += 1
            conn.commit()

            if i % 200 == 0:
                print(f"  [{i}/{len(leads)}] found={found} none={none} unreachable={unreachable} "
                      f"({100*found/max(i,1):.0f}% hit, {total_addrs/max(found,1):.1f} addrs each)",
                      flush=True)

    print(f"\nDone. leads with emails: {found} | none found: {none} | unreachable: {unreachable}")
    print(f"total addresses: {total_addrs} ({total_addrs/max(found,1):.1f} per lead)")
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
