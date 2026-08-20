"""
Checks whether each lead's website is live by making a HEAD request (falls back
to GET if HEAD is not supported). Saves the result in the `website_live` field.

  True  — site responded with a 2xx or 3xx status code
  False — connection error, timeout, 4xx/5xx, or DNS failure
  None  — not checked yet (default)

Works on both lead tables — pick with --source:
  google     Lead        (Google Maps leads, the default)
  realtruck  RealTruckLead (RealTruck dealer-locator leads)

Usage:
  python manage.py validate_lead_websites                          # only unchecked Google leads
  python manage.py validate_lead_websites --source realtruck       # only unchecked RealTruck leads
  python manage.py validate_lead_websites --recheck                # re-check all leads with a website
  python manage.py validate_lead_websites --recheck-dead           # re-check only the ones marked dead
  python manage.py validate_lead_websites --state TX               # filter by state
  python manage.py validate_lead_websites --limit 200              # process at most N leads
"""
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from django.conf import settings
from django.core.management.base import BaseCommand
from django.utils import timezone

from src.models import Lead, RealTruckLead

try:
    from curl_cffi import requests as cffi_requests
    HAS_CURL_CFFI = True
except ImportError:
    HAS_CURL_CFFI = False

WORKERS = 30
BATCH_SIZE = 200
TIMEOUT = 10
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; LeadBot/1.0)"}

# A real browser's headers, used for the retry below.
BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# Status codes that mean "the server answered, it just refused us" — bot protection, a login
# wall, rate limiting. The site is up: a first pass over the 4.4k RealTruck dealers wrote off 727
# sites on a bare 403, including 4wheelparts.com. Cloudflare/Akamai in front of a real business
# does not budge for impersonation from a datacenter IP, so a blocked response counts as live.
BLOCKED_CODES = {401, 403, 406, 409, 429, 503}
# Those, plus "this server doesn't do HEAD", all earn a browser-headed GET retry first.
RETRY_CODES = BLOCKED_CODES | {405, 501}

SOURCES = {
    "google": Lead,
    "realtruck": RealTruckLead,
}


def _build_proxies(gateway: str, username: str = "", password: str = "") -> dict:
    if username and password:
        from urllib.parse import quote
        host = f"http://{quote(username, safe='')}:{quote(password, safe='')}@{gateway.replace('http://', '')}"
    else:
        host = gateway if "://" in gateway else f"http://{gateway}"
    return {"http": host, "https": host}


def _normalize(url: str) -> str:
    """RealTruck stores bare hostnames ("medfordchryslercenter.com") — requests needs a scheme."""
    url = (url or "").strip()
    if url and "://" not in url:
        url = f"http://{url}"
    return url


def _browser_get(url: str, proxies: dict) -> int | None:
    """GET the page as a browser. Returns the status code, or None if the request itself failed."""
    if HAS_CURL_CFFI:
        try:
            resp = cffi_requests.get(
                url,
                timeout=TIMEOUT,
                headers=BROWSER_HEADERS,
                allow_redirects=True,
                impersonate="chrome",
                proxies=proxies or None,
                verify=False,
            )
            return resp.status_code
        except Exception:
            pass  # fall through to plain requests

    try:
        resp = requests.get(
            url, timeout=TIMEOUT, headers=BROWSER_HEADERS, allow_redirects=True, stream=True, proxies=proxies
        )
        resp.close()
        return resp.status_code
    except Exception:
        return None


def _is_tls_error(error: str) -> bool:
    """A bad/expired/mismatched certificate means the host is up — the handshake is what failed."""
    return any(marker in error for marker in ("SSLError", "CertificateError", "SSLCertVerification"))


def _attempt(url: str, proxies: dict) -> tuple:
    """One URL, one verdict: (is_live, status_code, error). is_live is None = inconclusive."""
    try:
        resp = requests.head(url, timeout=TIMEOUT, headers=HEADERS, allow_redirects=True, proxies=proxies)
        status = resp.status_code
    except requests.exceptions.ProxyError as e:
        return None, None, f"ProxyError: {e}"
    except requests.exceptions.Timeout:
        return False, None, "Timeout"
    except requests.exceptions.ConnectionError as e:
        error = f"ConnectionError: {e}"
        if "Cannot connect to proxy" in error or "ProxyError" in error:
            return None, None, f"ProxyError: {e}"
        # Cert problems: retry unverified — plenty of small shops run an expired certificate.
        if _is_tls_error(error):
            status = _browser_get(url, proxies)
            if status is not None:
                return status < 400 or status in BLOCKED_CODES, status, "bad certificate"
        return False, None, error
    except Exception as e:
        return False, None, str(e)

    if status < 400:
        return True, status, None

    # Rejected HEAD, or flagged us as a bot — retry as a browser before ruling on it
    if status in RETRY_CODES:
        retried = _browser_get(url, proxies)
        if retried is not None:
            status = retried
        if status < 400:
            return True, status, None
        if status in BLOCKED_CODES:
            return True, status, "blocked, but responding"

    return False, status, None


def _check(lead_id, url: str, proxies: dict) -> tuple:
    """
    Returns (lead_id, is_live, status_code, error).

    ``is_live`` is None when the check itself failed rather than the site — currently only when
    the proxy refuses us. Those rows are left untouched instead of being written as dead: a
    proxy outage would otherwise flip the whole table to False in one run.
    """
    live, status, error = _attempt(url, proxies)
    if live is not None and not live:
        # RealTruck stores plenty of bare hostnames, and _normalize has to guess a scheme.
        # Before writing one off, try the other scheme — https-only hosts are the common case.
        alt = f"https://{url[7:]}" if url.startswith("http://") else f"http://{url[8:]}"
        alt_live, alt_status, alt_error = _attempt(alt, proxies)
        if alt_live:
            return lead_id, True, alt_status, alt_error
    return lead_id, live, status, error


def _bulk_update(model, results: list[tuple], stamp_checked_at: bool) -> None:
    """Bulk update website_live for a batch of (lead_id, is_live) tuples. is_live=None is skipped."""
    live_ids = [lead_id for lead_id, live, *_ in results if live is True]
    dead_ids = [lead_id for lead_id, live, *_ in results if live is False]
    extra = {"website_checked_at": timezone.now()} if stamp_checked_at else {}
    if live_ids:
        model.objects.filter(pk__in=live_ids).update(website_live=True, **extra)
    if dead_ids:
        model.objects.filter(pk__in=dead_ids).update(website_live=False, **extra)


class Command(BaseCommand):
    help = "Check if lead websites are live and update the website_live flag"

    def add_arguments(self, parser):
        parser.add_argument("--source", default="google", choices=sorted(SOURCES), help="Which lead table to check")
        parser.add_argument("--state", default=None, help="Filter by state code (e.g. TX)")
        parser.add_argument("--recheck", action="store_true", help="Re-check leads even if already verified")
        parser.add_argument("--recheck-dead", action="store_true", help="Re-check only leads currently marked dead")
        parser.add_argument("--limit", type=int, default=None, help="Max number of leads to process")
        parser.add_argument("--no-proxy", action="store_true", help="Check directly instead of via the Storm proxy")

    def handle(self, *args, **options):
        model = SOURCES[options["source"]]
        stamp_checked_at = any(f.name == "website_checked_at" for f in model._meta.get_fields())

        qs = model.objects.filter(website__isnull=False).exclude(website="")

        if options["state"]:
            qs = qs.filter(state=options["state"].upper())

        if options["recheck_dead"]:
            qs = qs.filter(website_live=False)
        elif not options["recheck"]:
            qs = qs.filter(website_live__isnull=True)

        if options["limit"]:
            qs = qs[:options["limit"]]

        leads = [(pk, _normalize(website)) for pk, website in qs.values_list("pk", "website")]
        leads = [(pk, url) for pk, url in leads if url]
        total = len(leads)

        if not total:
            self.stdout.write("No leads to check.")
            return

        gateway = "" if options["no_proxy"] else getattr(settings, "STORM_PROXY_GATEWAY_SCRAPE", "")
        username = getattr(settings, "STORM_PROXY_ACCOUNT_NAME", "")
        password = getattr(settings, "STORM_PROXY_PASSWORD", "")
        proxies = _build_proxies(gateway, username, password) if gateway else {}
        proxy_label = f"{username}@{gateway}" if (gateway and username) else (gateway or "no proxy")
        self.stdout.write(
            f"Checking {total} {options['source']} websites "
            f"[{WORKERS} workers, batch {BATCH_SIZE}, proxy: {proxy_label}]...\n"
        )

        live_count = 0
        dead_count = 0
        skipped_count = 0
        pending_batch: list[tuple] = []

        with ThreadPoolExecutor(max_workers=WORKERS) as executor:
            futures = {executor.submit(_check, pk, url, proxies): (pk, url) for pk, url in leads}

            for i, future in enumerate(as_completed(futures), 1):
                pk, url = futures[future]
                try:
                    lead_id, live, status_code, error = future.result()
                except Exception as e:
                    lead_id, live, status_code, error = pk, False, None, str(e)

                # Log
                if live is None:
                    skipped_count += 1
                    self.stdout.write(
                        self.style.WARNING(f"  [{i}/{total}] SKIP  (proxy unreachable)  {url}")
                    )
                elif live:
                    live_count += 1
                    note = f"{status_code}, {error}" if error else status_code
                    self.stdout.write(
                        self.style.SUCCESS(f"  [{i}/{total}] LIVE  ({note})  {url}")
                    )
                else:
                    dead_count += 1
                    detail = f"status={status_code}" if status_code else error or "unknown"
                    self.stdout.write(
                        self.style.ERROR(f"  [{i}/{total}] DEAD  ({detail})  {url}")
                    )

                pending_batch.append((lead_id, live, status_code, error))

                # Flush batch to DB
                if len(pending_batch) >= BATCH_SIZE:
                    _bulk_update(model, pending_batch, stamp_checked_at)
                    self.stdout.write(f"  -- flushed {len(pending_batch)} records to DB --")
                    pending_batch.clear()

        # Final flush
        if pending_batch:
            _bulk_update(model, pending_batch, stamp_checked_at)
            self.stdout.write(f"  -- flushed {len(pending_batch)} records to DB --")

        self.stdout.write(
            self.style.SUCCESS(
                f"\nDone. Live: {live_count}  Dead/unreachable: {dead_count}  "
                f"Skipped (proxy): {skipped_count}  Total: {total}"
            )
        )
        if skipped_count:
            self.stdout.write(
                self.style.WARNING(
                    f"{skipped_count} leads were left unchecked because the proxy refused the "
                    f"connection — re-run once the proxy is reachable, or pass --no-proxy."
                )
            )
