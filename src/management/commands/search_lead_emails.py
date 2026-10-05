"""
Finds email addresses for leads whose own website never gave one up, by searching the web.

``enrich_lead_emails`` crawls the shop's site. When that comes back empty the address usually is
not on the site at all -- it sits behind a contact form, or in an image, or only on somebody
else's page: the shop's Facebook "About" box, a Yelp or BBB listing, a chamber-of-commerce roster,
a supplier's dealer page. This command goes after those pages instead.

Per business:
  1. One Tavily search for ``"{name}" {city} {state} contact email``.
  2. Every address the results mention is harvested by regex from both the snippet and the full
     page text, along with the words around it.
  3. Claude Haiku decides which of those addresses actually belong to THIS business -- the step
     regex cannot do, since a directory page carries the directory's own support address, the
     neighbouring shop's address, and a stock photographer's, all in the same HTML.

Searches run one per DOMAIN, not one per lead. Chains appear in the table once per location and
share a head office address, so a 15-branch dealer is one search whose result lands on all 15
rows -- the same "same domain means same operation" rule the send-list export already uses.

Tavily is metered on a monthly allowance, so a search is only ever paid for once: ``email_search_at``
records the attempt whether or not it found anything, and a re-run skips those rows unless you
pass --refetch. Check the balance the run prints before starting a big one.

Found addresses land in ``emails``; they are candidates, not confirmed mailboxes. Run
``verify_lead_emails --source <source>`` afterwards to put them through Reoon -- the send-list
export only trusts verified rows.

Usage:
  python manage.py search_lead_emails --source realtruck --qualified-only --limit 25 --dry-run
  python manage.py search_lead_emails --source realtruck --qualified-only
  python manage.py search_lead_emails --source realtruck --qualified-only --mx-only
  python manage.py search_lead_emails --source realtruck --allow-free-email
  python manage.py search_lead_emails --source realtruck --max-searches 400 --out found.csv
"""
import csv
import json
import logging
import re
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import quote_plus, unquote, urlparse

import requests
from django.core.management.base import BaseCommand, CommandError
from django.db.models import Q
from django.utils import timezone

from src.models import Lead, LeadEmail, RealTruckLead, RealTruckLeadEmail
from src.management.commands.enrich_lead_emails import (
    EMAIL_RE,
    EMAIL_IGNORE_DOMAINS,
    _is_valid_email,
)

logging.getLogger("httpx").setLevel(logging.WARNING)

SOURCES = {
    "google": Lead,
    "realtruck": RealTruckLead,
}

# The verdicts the send-list export is willing to mail. A lead holding only addresses outside
# this set cannot actually be contacted, whatever its `emails` field says.
SENDABLE_STATUSES = ["safe", "role_account", "catch_all"]

EMAIL_MODELS = {
    "google": LeadEmail,
    "realtruck": RealTruckLeadEmail,
}

TAVILY_SEARCH_URL = "https://api.tavily.com/search"
TAVILY_USAGE_URL = "https://api.tavily.com/usage"
SCRAPE_DO_URL = "https://api.scrape.do/"
SCRAPE_DO_INFO_URL = "https://api.scrape.do/info"
OLLAMA_CHAT_URL = "https://ollama.com/api/chat"
OLLAMA_MODEL = "gpt-oss:120b-cloud"

WORKERS = 8
MAX_RESULTS = 6
CONTEXT_CHARS = 140          # text kept either side of a candidate address
MAX_RAW_PER_RESULT = 60_000  # page text beyond this is boilerplate; cap it so one huge page
                             # cannot dominate the regex pass

# Platforms many unrelated shops all link to -- sharing one is not sharing a business, so they
# must not merge two leads into a single search.
GENERIC_DOMAINS = {
    "facebook.com", "m.facebook.com", "instagram.com", "linkedin.com", "twitter.com", "x.com",
    "yelp.com", "google.com", "sites.google.com", "wixsite.com", "business.site",
    "godaddysites.com", "squarespace.com", "weebly.com", "wordpress.com", "linktr.ee",
}

FREE_PROVIDERS = {
    "gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "icloud.com", "aol.com",
    "live.com", "msn.com", "protonmail.com", "sbcglobal.net", "comcast.net", "att.net",
    "bellsouth.net", "verizon.net", "me.com", "ymail.com", "cox.net", "charter.net",
    "earthlink.net", "roadrunner.com", "windstream.net", "frontier.com",
}

# Addresses that belong to the page, not to the business on it.
NOISE_LOCALS = {
    "noreply", "no-reply", "donotreply", "do-not-reply", "postmaster", "abuse", "webmaster",
    "privacy", "legal", "support", "help", "hostmaster", "mailer-daemon",
}

# "info (at) shop (dot) com" and friends -- written that way precisely to defeat the regex.
_DEOBFUSCATE = [
    (re.compile(r"\s*[\(\[\{]\s*at\s*[\)\]\}]\s*", re.I), "@"),
    (re.compile(r"\s+at\s+(?=[a-z0-9.\-]+\s*(?:[\(\[\{]\s*dot\s*[\)\]\}]|\.)\s*[a-z]{2,})", re.I), "@"),
    (re.compile(r"\s*[\(\[\{]\s*dot\s*[\)\]\}]\s*", re.I), "."),
    (re.compile(r"\s+dot\s+(?=[a-z]{2,6}\b)", re.I), "."),
]


def _norm(text: str) -> str:
    return re.sub(r"[^a-z0-9]", "", (text or "").lower())


def _related(a: str, b: str, minlen: int = 5) -> bool:
    """Whether two names share enough to be the same operation."""
    a, b = _norm(a), _norm(b)
    if not a or not b:
        return False
    if a in b or b in a:
        return True
    return any(a[i:i + minlen] in b for i in range(len(a) - minlen + 1))


def _brand_tokens() -> set:
    """Manufacturer names from the parts catalogue, used to spot a brand's own contact address."""
    from src.models import Brands
    return {t for (n,) in Brands.objects.values_list("name")
            if len(t := _norm(n)) >= 5}


def _learned_vendor_domains(min_businesses: int = 3) -> set:
    """
    Domains already proven to be suppliers by how they behave in the data.

    The parts catalogue only names manufacturers we stock, so it misses turnoverball.com,
    bakindustries.com, n-fab.com and every web agency. Those give themselves away another way:
    one address turning up under many businesses whose names have nothing to do with it. A real
    shop's address appears under that shop; a dealer group's under its own stores, whose names
    match the group. Only a supplier is scattered across unrelated names.
    """
    from src.models import Lead, LeadEmail, RealTruckLead, RealTruckLeadEmail

    holders = defaultdict(set)
    for LM, EM in ((Lead, LeadEmail), (RealTruckLead, RealTruckLeadEmail)):
        meta = {i: (n, domain_of(w)) for i, n, w in LM.objects.values_list("id", "name", "website")}
        for lead_id, email in EM.objects.values_list("lead_id", "email"):
            if lead_id not in meta or not email:
                continue
            name, site = meta[lead_id]
            domain = email.split("@")[-1].lower()
            if domain in FREE_PROVIDERS:
                continue
            core = domain.rsplit(".", 1)[0]
            if _related(core, site) or _related(core, name):
                continue          # the shop's own, or its group's -- not a supplier
            holders[domain].add(_norm(name))
    return {d for d, names in holders.items() if len(names) >= min_businesses}


def _is_vendor_address(email: str, site_domain: str, business_name: str, brand_tokens: set,
                       vendor_domains: frozenset = frozenset()) -> bool:
    """
    True when an address belongs to a supplier, franchisor's brand, or the shop's web agency
    rather than to the shop.

    A dealer's site says "DECKED dealer", so a search for that dealer surfaces DECKED's contact
    page and the address on it looks, to the model, like a contact for this business. It is not:
    mailing it reaches the manufacturer, and the same address then appears under a dozen
    different shop names.

    The test is relatedness to the shop's own domain OR its name, which is what separates this
    from a legitimate corporate address -- a dealer group's stores really are reachable at the
    group's domain (Moran Automotive at moranautomotive.com), and a franchise location really is
    reachable through the franchisor. Those stay; decked.com under "Cape Fear Customs" does not.
    """
    domain = email.split("@")[-1].lower()
    if domain in FREE_PROVIDERS:
        return False
    core = domain.rsplit(".", 1)[0]
    if _related(core, site_domain) or _related(core, business_name):
        return False
    if domain in vendor_domains:
        return True
    return _norm(core) in brand_tokens or any(
        _norm(core).startswith(b) and len(b) >= 6 for b in brand_tokens
    )


def domain_of(url: str) -> str:
    """Registrable-ish host for a URL: no scheme, no www, no path."""
    u = re.sub(r"^https?://", "", (url or "").strip().lower())
    return re.sub(r"^www\.", "", u.split("/")[0].split("?")[0].split(":")[0])


def _deobfuscate(text: str) -> str:
    for pattern, repl in _DEOBFUSCATE:
        text = pattern.sub(repl, text)
    return text


def _acceptable(email: str, allow_free: bool) -> bool:
    """
    Whether an address is worth handing to Claude.

    ``_is_valid_email`` throws out free providers, which is right for a site scrape -- an address
    found on a shop's own site should be on the shop's own domain. It is wrong here: a
    single-location shop with no mail on its domain very often runs the business out of a Gmail
    account, and that Gmail is the only way to reach them. --allow-free-email lets those through.
    """
    email = email.lower().strip(".,;:)]}\"'<>")
    if "@" not in email or email.count("@") != 1:
        return False
    local, domain = email.rsplit("@", 1)
    if not local or local in NOISE_LOCALS or local.startswith(("noreply", "no-reply")):
        return False
    if allow_free and domain in EMAIL_IGNORE_DOMAINS:
        # Only the free mailbox providers are forgiven; the builder/CDN/analytics domains in the
        # same set are never a business address.
        free = {"gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "icloud.com",
                "aol.com", "live.com", "msn.com", "protonmail.com"}
        return domain in free and bool(re.match(r"^[a-z0-9._%+\-]+$", local))
    return _is_valid_email(email)


# ------------------------------------------------------------------
# Tavily
# ------------------------------------------------------------------

class KeyPool:
    """
    Round-robin over several Tavily keys, retiring each as it runs dry.

    Tavily meters per KEY at 1,000 searches a month, and a full pass over the Google leads needs
    close to 8,000. One key cannot do it however long you wait, so the work is spread across
    however many keys are configured. A key that answers 432/429 is out of credit rather than
    broken: it is dropped from the rotation and the same search is retried on the next key, so a
    business is never lost to whichever key happened to be handed it.
    """

    def __init__(self, keys: list[str]):
        self._keys = list(dict.fromkeys(k.strip() for k in keys if k.strip()))
        self._i = 0
        self._lock = threading.Lock()

    def __len__(self):
        with self._lock:
            return len(self._keys)

    def next(self) -> str | None:
        with self._lock:
            if not self._keys:
                return None
            key = self._keys[self._i % len(self._keys)]
            self._i += 1
            return key

    def retire(self, key: str) -> int:
        """Drop an exhausted key. Returns how many remain."""
        with self._lock:
            if key in self._keys:
                self._keys.remove(key)
                self._i = 0
            return len(self._keys)


def _tavily_usage(api_key: str) -> dict | None:
    try:
        r = requests.get(TAVILY_USAGE_URL, headers={"Authorization": f"Bearer {api_key}"}, timeout=15)
        if r.status_code == 200:
            return r.json().get("account")
    except Exception:
        pass
    return None


def _tavily_search(query: str, pool: "KeyPool") -> tuple[list[dict], str | None]:
    """
    One search, retried across the key pool when a key turns out to be exhausted.

    Bearer auth rather than the legacy ``api_key`` body field, because ``include_raw_content`` is
    silently ignored on the legacy form -- and raw content is the whole point: snippets are ~300
    characters and an address on a Facebook About page sits well past that. It is also what makes
    this engine beat scraping Google directly, 64% against 21% on the same businesses.
    """
    attempts = max(len(pool), 1)
    last_error = "no Tavily keys left"

    for _ in range(attempts):
        key = pool.next()
        if key is None:
            return [], "all Tavily keys exhausted"
        try:
            resp = requests.post(
                TAVILY_SEARCH_URL,
                headers={"Authorization": f"Bearer {key}"},
                json={
                    "query": query,
                    "max_results": MAX_RESULTS,
                    "search_depth": "basic",
                    "include_raw_content": True,
                },
                timeout=45,
            )
        except Exception as e:
            last_error = f"{type(e).__name__}: {e}"
            continue

        if resp.status_code == 200:
            return resp.json().get("results", []), None

        # 432 is Tavily's "exceeds your plan's set usage limit" -- the code that actually fires
        # when a key runs dry, and the one this originally missed. A key left in rotation after
        # it is spent keeps drawing its share of the work and failing all of it, so every
        # out-of-credit code has to retire the key, not just the obvious 402.
        if resp.status_code in (401, 402, 429, 432):
            left = pool.retire(key)
            last_error = f"HTTP {resp.status_code} (key retired, {left} left)"
            continue

        return [], f"HTTP {resp.status_code} {resp.text[:160]}"

    return [], last_error


# ------------------------------------------------------------------
# Google, via scrape.do
# ------------------------------------------------------------------

def _scrape_do_balance(token: str) -> dict | None:
    try:
        r = requests.get(SCRAPE_DO_INFO_URL, params={"token": token}, timeout=20)
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return None


def _fetch_via_scrape_do(url: str, token: str, timeout: int = 60) -> tuple[str | None, str | None]:
    """(html, error) for one URL pulled through the scrape.do proxy."""
    try:
        r = requests.get(SCRAPE_DO_URL, params={"token": token, "url": url}, timeout=timeout)
    except Exception as e:
        return None, f"{type(e).__name__}: {e}"
    if r.status_code != 200:
        return None, f"HTTP {r.status_code} {r.text[:120]}"
    return r.text, None


def _parse_serp(html: str) -> list[dict]:
    """
    Result blocks out of a Google SERP, in the same shape Tavily returns.

    Google serves two layouts to a scraper. The JS one links results with ordinary hrefs; the
    no-JS fallback wraps every result in ``/goto?url=<opaque blob>`` and prints the real URL as
    breadcrumb TEXT beside the title. Anchoring on <h3> catches both, since the heading is the
    one element common to them -- and the real URL is then recovered from the breadcrumb when the
    href is unusable.

    Class names are deliberately not used: Google's are obfuscated and rotate, so a parser built
    on them breaks silently within weeks and looks like "this business has no web presence".
    """
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "lxml")
    results, seen = [], set()
    skip_hosts = {"webcache.googleusercontent.com", "policies.google.com",
                  "support.google.com", "accounts.google.com"}

    for heading in soup.select("h3, h2"):
        title = heading.get_text(" ", strip=True)
        if not title:
            continue

        block, node = None, heading
        for _ in range(5):
            node = node.find_parent(["div", "li"]) if node else None
            if node is None:
                break
            if len(node.get_text(" ", strip=True)) > len(title) + 60:
                block = node
                break
        block_text = block.get_text(" ", strip=True) if block else title

        url = ""
        anchor = heading.find_parent("a")
        href = anchor.get("href", "") if anchor else ""
        if href.startswith("http"):
            url = href
        elif href.startswith("/url?q="):
            url = unquote(href[7:].split("&")[0])
        if not url:
            # The no-JS layout: recover the URL from the breadcrumb Google prints as text.
            match = re.search(r"https?://[^\s›]+", block_text)
            if match:
                url = match.group(0)

        host = domain_of(url)
        if not host or host in seen or host in skip_hosts or "google." in host:
            continue
        seen.add(host)
        results.append({"url": url, "title": title, "content": block_text[:600], "raw_content": ""})

    return results


def _google_search(query: str, token: str, fetch_pages: int) -> tuple[list[dict], str | None]:
    """
    One Google search through scrape.do. Returns (results, error).

    The whole SERP body is kept as raw_content on the first result: Google frequently prints the
    address straight into a snippet or a knowledge panel, and that text is already paid for by
    the one request. Fetching the result PAGES is what costs extra, hence --fetch-pages.
    """
    url = ("https://www.google.com/search?q=" + quote_plus(query) + "&num=10&hl=en&gl=us")
    html, error = _fetch_via_scrape_do(url, token)
    if error:
        return [], error

    from bs4 import BeautifulSoup
    serp_text = BeautifulSoup(html, "lxml").get_text(" ", strip=True)[:MAX_RAW_PER_RESULT]

    results = _parse_serp(html)
    if results:
        results[0]["raw_content"] = serp_text
    else:
        # A layout this parser does not know is not the same as "nothing found" -- Google prints
        # the snippets into the page text either way, and an address in a snippet is the single
        # most common hit. Hand the text over rather than throwing the request away.
        results = [{"url": url, "title": "Google results", "content": serp_text[:600],
                    "raw_content": serp_text}]

    for r in results[:fetch_pages]:
        page, err = _fetch_via_scrape_do(r["url"], token, timeout=45)
        if page:
            from bs4 import BeautifulSoup as BS
            r["raw_content"] = BS(page, "lxml").get_text(" ", strip=True)[:MAX_RAW_PER_RESULT]

    return results[:MAX_RESULTS], None


def _harvest(results: list[dict], allow_free: bool) -> tuple[list[dict], list[str]]:
    """
    (candidates, snippets) from a result set.

    Each candidate is {email, url, context} -- the words around the address are what let Claude
    tell "email us at sales@shop.com" from "photo credit: sales@stockphotos.com".
    """
    seen: set[str] = set()
    candidates: list[dict] = []
    snippets: list[str] = []

    for r in results:
        url = r.get("url", "")
        snippet = (r.get("content") or "")[:400]
        if snippet:
            snippets.append(f"URL: {url}\nTitle: {r.get('title', '')}\nSnippet: {snippet}")

        blob = _deobfuscate(snippet + "\n" + (r.get("raw_content") or "")[:MAX_RAW_PER_RESULT])
        for match in EMAIL_RE.finditer(blob):
            email = match.group(0).lower().strip(".,;:)]}\"'<>")
            if email in seen or not _acceptable(email, allow_free):
                continue
            seen.add(email)
            start = max(0, match.start() - CONTEXT_CHARS)
            context = " ".join(blob[start:match.end() + CONTEXT_CHARS].split())
            candidates.append({"email": email, "url": url, "context": context})

    return candidates, snippets


# ------------------------------------------------------------------
# Claude adjudication
# ------------------------------------------------------------------

def _claude_pick(name, city, state, website, candidates, snippets, client, allow_free,
                 carried_brands=(), brand_tokens=frozenset(), vendor_domains=frozenset()):
    """Which harvested addresses belong to this business. Returns a list of emails."""
    if not client:
        return []

    lines = [f"{i}. {c['email']}\n   found on: {c['url']}\n   context: {c['context'][:400]}"
             for i, c in enumerate(candidates, 1)]

    # Naming the shop's own stocked brands is the strongest signal available: those are exactly
    # the manufacturer pages a search for a dealer surfaces, and exactly the addresses that were
    # being misattributed to the shop.
    brand_rule = ""
    if carried_brands:
        brand_rule = (f"- This shop STOCKS these brands: {', '.join(list(carried_brands)[:12])}. "
                      f"An address at any of their domains belongs to the MANUFACTURER, not to "
                      f"this shop — reject it\n")

    free_rule = (
        "- A Gmail/Yahoo/Outlook address IS acceptable if the page presents it as this business's "
        "contact address (small shops often run on one)"
        if allow_free else
        "- Exclude Gmail, Yahoo, Hotmail, Outlook and other free email providers"
    )

    prompt = (
        f"Business: {name}, {city}, {state}\n"
        f"Their website: {website}\n\n"
        f"Candidate email addresses found on web pages about this business:\n\n"
        + ("\n".join(lines) if lines else "(none)")
        + "\n\nOther search snippets (an address may be written out in words here, e.g. "
          "'info at shop dot com'):\n\n"
        + "\n\n".join(snippets[:6])
        + "\n\n"
        f"Return the email addresses that belong to THIS business.\n"
        f"Rules:\n"
        f"- The address must be this business's own contact address, not the directory site's "
        f"(Yelp, BBB, Facebook, a chamber of commerce), not a neighbouring business's, and not a "
        f"web designer's or photographer's credit\n"
        + brand_rule +
        f"- Reject any address at a manufacturer, supplier, or web-agency domain. This shop SELLS "
        f"those brands; a page listing them is a product page, not a contact for this shop\n"
        f"- An address on this business's own domain is almost always correct\n"
        f"{free_rule}\n"
        f"- Exclude noreply/no-reply/postmaster and other system addresses\n"
        f"- If none of the candidates clearly belong to this business, return an empty array\n\n"
        f'Reply with ONLY a JSON array of email strings, e.g. ["info@example.com"] or []'
    )

    # Let API errors bubble: the caller must not stamp email_search_at on a lead whose search
    # never actually got a verdict, or the retry is lost.
    text = client(prompt)
    start = text.find("[")
    if start == -1:
        return []
    try:
        picked, _ = json.JSONDecoder().raw_decode(text, start)
    except Exception:
        return []
    if not isinstance(picked, list):
        return []

    out, seen = [], set()
    for e in picked:
        if not isinstance(e, str):
            continue
        e = e.lower().strip()
        if e in seen or not _acceptable(e, allow_free):
            continue
        # Belt and braces: the prompt asks the model to reject supplier addresses, but the
        # catalogue check does not depend on it having complied.
        if _is_vendor_address(e, domain_of(website), name, brand_tokens, vendor_domains):
            continue
        seen.add(e)
        out.append(e)
    return out


def _haiku_backend(api_key: str):
    import anthropic
    client = anthropic.Anthropic(api_key=api_key)

    def call(prompt: str) -> str:
        response = client.messages.create(
            model="claude-haiku-4-5",
            max_tokens=400,
            messages=[{"role": "user", "content": prompt}],
        )
        return response.content[0].text.strip()

    return call


def _ollama_backend(api_key: str):
    """
    gpt-oss:120b on Ollama Cloud.

    Two things differ from the Anthropic call. The reply carries the model's reasoning in a
    separate ``thinking`` field, so the answer must be read from ``message.content`` alone --
    reading the whole message would feed paragraphs of deliberation into the JSON parser. And
    the model is chatty about format, so the temperature is pinned low; the caller's raw_decode
    already tolerates trailing prose either way.
    """
    def call(prompt: str) -> str:
        # The free tier starts refusing at ~8 in flight, so 429s are routine rather than
        # exceptional and must be waited out. Raising past the last attempt is deliberate: the
        # caller treats an exception as retryable and leaves email_search_at unstamped, so a
        # throttled business is searched again later instead of being recorded as "no address".
        for attempt in range(5):
            resp = requests.post(
                OLLAMA_CHAT_URL,
                headers={"Authorization": f"Bearer {api_key}"},
                json={
                    "model": OLLAMA_MODEL,
                    "messages": [{"role": "user", "content": prompt}],
                    "stream": False,
                    "options": {"temperature": 0, "num_predict": 400},
                },
                timeout=120,
            )
            if resp.status_code == 429:
                time.sleep(2 ** attempt)
                continue
            resp.raise_for_status()
            return (resp.json().get("message", {}).get("content") or "").strip()
        resp.raise_for_status()
        return ""

    return call


def _process_domain(group: dict, cfg: dict, client, allow_free: bool) -> dict:
    """One search + adjudication for one business. Returns a result dict for the caller to write."""
    query = f'"{group["name"]}" {group["city"] or ""} {group["state"] or ""} contact email'.strip()
    if cfg["engine"] == "google":
        results, error = _google_search(query, cfg["scrape_do_token"], cfg["fetch_pages"])
    else:
        results, error = _tavily_search(query, cfg["pool"])
    if error:
        return {**group, "emails": [], "status": "search_error", "detail": error}
    if not results:
        return {**group, "emails": [], "status": "no_results", "detail": ""}

    candidates, snippets = _harvest(results, allow_free)
    if not candidates and not snippets:
        return {**group, "emails": [], "status": "no_candidates", "detail": ""}

    emails = _claude_pick(group["name"], group["city"], group["state"], group["website"],
                          candidates, snippets, client, allow_free,
                          group.get("brands", ()), cfg.get("brand_tokens", frozenset()),
                          cfg.get("vendor_domains", frozenset()))
    return {
        **group,
        "emails": emails,
        "status": "found" if emails else "rejected",
        "detail": f"{len(candidates)} candidates",
    }


# ------------------------------------------------------------------
# MX
# ------------------------------------------------------------------

def _mx_map(domains, workers=40) -> dict:
    """{domain: accepts mail}. A domain with no MX cannot receive anything, so searching for an
    address there is a credit spent on a business that could never be mailed."""
    try:
        import dns.resolver
    except ImportError:
        return {}
    resolver = dns.resolver.Resolver()
    resolver.timeout, resolver.lifetime = 3, 4

    def probe(d):
        try:
            return d, bool(resolver.resolve(d, "MX"))
        except Exception:
            return d, False

    with ThreadPoolExecutor(max_workers=workers) as ex:
        return dict(ex.map(probe, domains))


# ------------------------------------------------------------------
# Command
# ------------------------------------------------------------------

class Command(BaseCommand):
    help = "Find lead emails by web search + LLM extraction, for leads whose own site had none"

    def add_arguments(self, parser):
        parser.add_argument("--source", default="google", choices=sorted(SOURCES),
                            help="Which lead table to search for")
        parser.add_argument("--llm", default="haiku", choices=["haiku", "ollama"],
                            help="Which model adjudicates candidates: haiku = claude-haiku-4-5 "
                                 "(paid, concurrent); ollama = gpt-oss:120b-cloud (free tier, "
                                 "1 concurrent request)")
        parser.add_argument("--tavily-keys", default=None,
                            help="Comma-separated Tavily keys, overriding TAVILY_API_KEYS")
        parser.add_argument("--engine", default="google", choices=["google", "tavily"],
                            help="google = Google SERP via scrape.do (1.2M req/mo); "
                                 "tavily = Tavily search API (1k/mo)")
        parser.add_argument("--fetch-pages", type=int, default=2,
                            help="google engine: also pull the top N result PAGES for their full "
                                 "text. Each costs one more scrape.do request. 0 = SERP only")
        parser.add_argument("--qualified-only", action="store_true",
                            help="Only leads the AI marked qualified (is_qualified=True)")
        parser.add_argument("--sendable-statuses", default=",".join(SENDABLE_STATUSES),
                            help="Which Reoon verdicts count as 'already reachable' for "
                                 "--unreachable. Drop catch_all to hunt for a confirmable address "
                                 "on leads whose only one is accept-all")
        parser.add_argument("--unreachable", action="store_true",
                            help="Target leads with no VERIFIED SENDABLE address, rather than only "
                                 "those with no address at all — picks up leads whose only address "
                                 "failed verification")
        parser.add_argument("--state", default=None, help="Filter by state code (e.g. TX)")
        parser.add_argument("--limit", type=int, default=None, help="Max businesses to search")
        parser.add_argument("--max-searches", type=int, default=None,
                            help="Hard cap on Tavily searches — match your credit balance")
        parser.add_argument("--workers", type=int, default=WORKERS)
        parser.add_argument("--mx-only", action="store_true",
                            help="Skip domains with no MX record — they cannot receive mail at all")
        parser.add_argument("--allow-free-email", action="store_true",
                            help="Accept Gmail/Yahoo/Outlook addresses (often a small shop's only one)")
        parser.add_argument("--refetch", action="store_true",
                            help="Search again even if email_search_at is already set")
        parser.add_argument("--dry-run", action="store_true",
                            help="Search and report, write nothing")
        parser.add_argument("--out", default=None, help="Write found addresses to this CSV")

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

        engine = options["engine"]
        scrape_do_token = _get_key("SCRAPE_DO_TOKEN")

        # TAVILY_API_KEYS (comma-separated) pools several 1,000/month accounts; TAVILY_API_KEY
        # remains the single-key form, and both are accepted so nothing that already sets one
        # variable has to change.
        keys = [k for k in (_get_key("TAVILY_API_KEYS") or "").split(",") if k.strip()]
        if options["tavily_keys"]:
            keys = options["tavily_keys"].split(",")
        if not keys:
            keys = [_get_key("TAVILY_API_KEY")]
        pool = KeyPool(keys)

        if engine == "tavily" and not len(pool):
            raise CommandError("No Tavily key set (TAVILY_API_KEY or TAVILY_API_KEYS)")
        if engine == "google" and not scrape_do_token:
            raise CommandError("SCRAPE_DO_TOKEN not set — needed to reach Google without being blocked")

        if options["llm"] == "ollama":
            ollama_key = _get_key("OLLAMA_API_KEY") or _get_key("LLAMA_API_KEY")
            if not ollama_key:
                raise CommandError("OLLAMA_API_KEY not set — needed for --llm ollama")
            client = _ollama_backend(ollama_key)
        else:
            anthropic_key = _get_key("ANTHROPIC_API_KEY")
            if not anthropic_key:
                raise CommandError("ANTHROPIC_API_KEY not set — nothing would adjudicate candidates")
            client = _haiku_backend(anthropic_key)

        model = SOURCES[options["source"]]
        email_model = EMAIL_MODELS[options["source"]]
        brand_tokens = _brand_tokens()
        vendor_domains = frozenset(_learned_vendor_domains())
        allow_free = options["allow_free_email"]
        dry_run = options["dry_run"]

        qs = (model.objects
              .filter(website__isnull=False)
              .exclude(website=""))
        if options["unreachable"]:
            # "Has an address" is not the same as "can be contacted". A lead whose only address
            # came back invalid is as unreachable as one with no address, yet the default filter
            # skips it forever -- the dead address blocks the search that would find a live one.
            # Selecting on the absence of a SENDABLE verdict puts those leads back in scope.
            # catch_all is sendable but unconfirmable -- the domain accepts every recipient, so
            # the verdict says the server is permissive, not that the mailbox exists. Dropping it
            # from this list re-opens leads whose only address is accept-all, to look for one
            # Reoon can actually confirm. They keep the catch_all either way; the merge below
            # only ever adds.
            statuses = [x.strip() for x in options["sendable_statuses"].split(",") if x.strip()]
            # A supplier's address is not a way to reach this shop, however well it verifies, so a
            # lead holding only one of those is unreachable and belongs back in the queue.
            meta = {i: (n, domain_of(w)) for i, n, w in model.objects.values_list("id", "name", "website")}
            sendable = set()
            for lead_id, email in email_model.objects.filter(
                    verified_at__isnull=False, status__in=statuses).values_list("lead_id", "email"):
                name, site = meta.get(lead_id, ("", ""))
                if not _is_vendor_address(email or "", site, name, brand_tokens, vendor_domains):
                    sendable.add(lead_id)
            qs = qs.exclude(pk__in=sendable)
        else:
            qs = qs.filter(Q(emails=[]) | Q(emails__isnull=True))
        if options["qualified_only"]:
            qs = qs.filter(is_qualified=True)
        if options["state"]:
            qs = qs.filter(state=options["state"].upper())
        if not options["refetch"]:
            qs = qs.filter(email_search_at__isnull=True)

        brand_field = "all_brands" if hasattr(model, "all_brands") else None
        cols = [model._meta.pk.name, "name", "city", "state", "website"] + ([brand_field] if brand_field else [])
        rows = [tuple(r) + ((None,) if not brand_field else ()) for r in qs.values_list(*cols)]
        if not rows:
            self.stdout.write("No leads to search.")
            return

        # One search per business, not per location.
        by_domain = defaultdict(list)
        for pk, name, city, state, website, all_brands in rows:
            dom = domain_of(website)
            # A shop whose only "website" is its Facebook page gets keyed by its own pk, so it is
            # searched on its own rather than merged with every other Facebook-only shop.
            key = f"pk:{pk}" if (not dom or dom in GENERIC_DOMAINS) else dom
            by_domain[key].append(
                {"pk": pk, "name": name, "city": city, "state": state, "website": website,
                 "brands": [b.strip() for b in (all_brands or "").split(";") if b.strip()]}
            )

        groups = []
        for key, leads in by_domain.items():
            first = leads[0]
            groups.append({
                "domain": key,
                "name": first["name"],
                "city": first["city"],
                "state": first["state"],
                "website": first["website"],
                "brands": first.get("brands") or [],
                "pks": [l["pk"] for l in leads],
            })
        groups.sort(key=lambda g: g["domain"])

        skipped_no_mx = 0
        if options["mx_only"]:
            real = [g["domain"] for g in groups if not g["domain"].startswith("pk:")]
            mx = _mx_map(real)
            if not mx:
                self.stdout.write(self.style.WARNING(
                    "  --mx-only: dnspython not installed, MX filter skipped (pip install dnspython)"
                ))
            else:
                before = len(groups)
                groups = [g for g in groups if mx.get(g["domain"], True)]
                skipped_no_mx = before - len(groups)

        if options["limit"]:
            groups = groups[:options["limit"]]
        if options["max_searches"] and len(groups) > options["max_searches"]:
            self.stdout.write(self.style.WARNING(
                f"  capping at --max-searches {options['max_searches']} "
                f"({len(groups) - options['max_searches']} businesses left unsearched)"
            ))
            groups = groups[:options["max_searches"]]

        total_leads = sum(len(g["pks"]) for g in groups)
        cfg = {"engine": engine, "pool": pool,
               "scrape_do_token": scrape_do_token, "fetch_pages": options["fetch_pages"],
               "brand_tokens": brand_tokens, "vendor_domains": vendor_domains}

        balance = ""
        if engine == "tavily":
            # /usage is itself rate-limited, so a burst of lookups gets 429s. A failed lookup is
            # not a zero balance -- reporting it as one would print an alarming and wrong "short
            # by 7,828" warning -- so unreadable keys are counted separately and never summed.
            total, read, unread = 0, 0, 0
            for k in list(pool._keys):
                usage = _tavily_usage(k)
                if usage and usage.get("plan_limit") is not None:
                    total += usage["plan_limit"] - usage["plan_usage"]
                    read += 1
                else:
                    unread += 1
                time.sleep(0.6)
            if read:
                balance = (f"  Tavily         : {len(pool)} key(s), ~{total:,} credits across "
                           f"{read} readable; this run needs {len(groups):,}\n")
                if unread:
                    balance += f"                   ({unread} key(s) rate-limited, balance unknown)\n"
                elif total < len(groups):
                    balance += self.style.WARNING(
                        f"  short by ~{len(groups) - total:,} — the tail fails as retryable; "
                        f"re-run with more keys or --engine google\n")
            else:
                balance = (f"  Tavily         : {len(pool)} key(s); balance unreadable "
                           f"(usage endpoint rate-limited). Run needs {len(groups):,}\n")
        else:
            info = _scrape_do_balance(scrape_do_token)
            if info:
                per = 1 + options["fetch_pages"]
                need = len(groups) * per
                balance = (f"  scrape.do      : {info.get('RemainingMonthlyRequest'):,} requests left; "
                           f"this run needs ~{need:,} ({per} per business)\n")

        self.stdout.write(
            f"Searching {len(groups)} businesses covering {total_leads} leads "
            f"[{options['workers']} workers]\n"
            f"  Source         : {options['source']}\n"
            f"  Engine         : {engine}"
            + (f" (SERP + top {options['fetch_pages']} pages)\n" if engine == "google" else "\n")
            +
            f"  Free providers : {'accepted' if allow_free else 'rejected'}\n"
            f"  Adjudicator    : {OLLAMA_MODEL if options['llm'] == 'ollama' else 'claude-haiku-4-5'}\n"
            + (f"  Skipped (no MX): {skipped_no_mx}\n" if skipped_no_mx else "")
            + balance
            + (self.style.WARNING("  DRY RUN — nothing will be written\n") if dry_run else "")
        )

        found = rejected = no_candidates = no_results = errors = 0
        leads_filled = 0
        csv_rows = []
        searched_at = timezone.now()

        with ThreadPoolExecutor(max_workers=options["workers"]) as executor:
            futures = {
                executor.submit(_process_domain, g, cfg, client, allow_free): g
                for g in groups
            }
            for i, future in enumerate(as_completed(futures), 1):
                group = futures[future]
                try:
                    res = future.result()
                except Exception as e:
                    errors += 1
                    self.stdout.write(self.style.WARNING(
                        f"  [{i}/{len(groups)}] ERROR {group['name']}: {type(e).__name__}: {e}"
                    ))
                    continue   # no email_search_at stamp — leave it retryable

                emails, status = res["emails"], res["status"]
                if status == "search_error":
                    errors += 1
                    self.stdout.write(self.style.WARNING(
                        f"  [{i}/{len(groups)}] SEARCH FAILED {group['name']}: {res['detail']}"
                    ))
                    continue   # a Tavily outage or a 402 must not burn the retry

                if not dry_run:
                    updates = {"email_search_at": searched_at}
                    if emails:
                        # emails_not_found records the site scrape's verdict ("tried, nothing
                        # there"). That verdict is now wrong: the address exists, it just was not
                        # on their own site. Leaving it set makes every downstream filter treat a
                        # reachable shop as unreachable.
                        updates["emails_not_found"] = False
                    if emails and options["unreachable"]:
                        # These leads already carry addresses -- dead ones. Overwriting would drop
                        # them from the lead while their verdicts stay in the email table, so the
                        # two disagree. Merge instead, newest first: verify_lead_emails reads
                        # index 0 under --one-per-lead, and the fresh address is the one to spend
                        # a credit on.
                        for pk in group["pks"]:
                            existing = model.objects.filter(pk=pk).values_list("emails", flat=True)[0] or []
                            merged = emails + [e for e in existing if e not in emails]
                            model.objects.filter(pk=pk).update(emails=merged, **updates)
                    else:
                        if emails:
                            updates["emails"] = emails
                        model.objects.filter(pk__in=group["pks"]).update(**updates)

                if emails:
                    found += 1
                    leads_filled += len(group["pks"])
                    tag = f" (+{len(group['pks']) - 1} more locations)" if len(group["pks"]) > 1 else ""
                    self.stdout.write(self.style.SUCCESS(
                        f"  [{i}/{len(groups)}] ✓ {group['name']} ({group['city']}, {group['state']}){tag}\n"
                        f"           {emails}"
                    ))
                    for e in emails:
                        csv_rows.append({
                            "domain": group["domain"], "business_name": group["name"],
                            "city": group["city"], "state": group["state"],
                            "website": group["website"], "email": e,
                            "locations": len(group["pks"]), "lead_ids": ";".join(map(str, group["pks"])),
                        })
                else:
                    if status == "no_results":
                        no_results += 1
                    elif status == "no_candidates":
                        no_candidates += 1
                    else:
                        rejected += 1
                    self.stdout.write(f"  [{i}/{len(groups)}] —  {group['name']} [{status}]")

        if options["out"] and csv_rows:
            with open(options["out"], "w", newline="") as fh:
                writer = csv.DictWriter(fh, fieldnames=list(csv_rows[0]))
                writer.writeheader()
                writer.writerows(sorted(csv_rows, key=lambda r: r["business_name"]))
            self.stdout.write(f"\nWrote {len(csv_rows)} addresses to {options['out']}")

        searched = len(groups) - errors
        hit_rate = (found / searched * 100) if searched else 0
        self.stdout.write(self.style.SUCCESS(
            f"\nDone.\n"
            f"  Businesses searched     : {searched}\n"
            f"  Emails found            : {found}  ({hit_rate:.0f}% hit rate)\n"
            f"  Leads filled            : {leads_filled}\n"
            f"  Candidates all rejected : {rejected}\n"
            f"  No address on any page  : {no_candidates}\n"
            f"  No search results       : {no_results}\n"
            f"  Errors (retryable)      : {errors}\n"
            + ("" if dry_run else
               f"\nNext: python manage.py verify_lead_emails --source {options['source']}")
        ))
