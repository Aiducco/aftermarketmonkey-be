"""
HTTP client for summitracing.com, which sits behind Imperva Advanced Bot Protection.

Three facts about that protection shape every decision in here, all measured against the live
site rather than assumed:

1. **A real browser TLS fingerprint is mandatory.** ``requests``/``urllib`` are answered with the
   "Pardon Our Interruption" interstitial no matter what headers they send, because Imperva
   fingerprints the TLS/HTTP2 handshake (JA3/JA4). ``curl_cffi`` with ``impersonate=`` presents
   Chrome's handshake and is let through. This is why the module does not use ``requests`` like
   the rest of the codebase.

2. **A ``reese84`` session cookie is mandatory and must be minted by a browser.** The cookie is
   issued by a JS sensor that fingerprints the browser and POSTs the result back; there is no
   way to mint one from Python. Supply it via ``--cookie-file`` / ``SUMMIT_RACING_COOKIE`` /
   ``summit_racing_cookie.local.txt``, the same convention
   ``backfill_atech_summit_image_urls`` already uses.

3. **A token must be used from the IP it was minted on.** A single replay from another IP may
   succeed, which is misleading; sustained use does not. Measured 2026-10-23: 12 workers
   sharing one token across 12 rotating exits were revoked after exactly 13 requests -- one per
   worker -- while 4 workers on a single IP ran 61 requests clean. Replay from a different exit
   is answered with the throttle stub, not an error.

Concurrency therefore needs **one token per sticky exit IP**, not threads on one IP and not a
rotating gateway. A rotating gateway fails silently here: ``context.request`` calls inside a
single browser context were observed leaving from 62.233.39.79 and then 185.201.139.248,
because it rotates per connection, so the token and the exit drift apart mid-session. Point
``STORM_PROXY_GATEWAY_SCRAPE`` at comma-separated **sticky** gateways and give each worker its
own; ``token.mint_token`` then mints that worker's token through that same gateway.

Imperva also throttles per IP cumulatively: enough traffic from one address earns a site-wide
~900-byte stub on every route, in a real browser too, which takes hours to lift. Pace
conservatively -- this is a backfill, not a race.
"""

from __future__ import annotations

import itertools
import logging
import threading
import time
import typing
import urllib.parse

from django.conf import settings

from src.integrations.clients.summit_racing import exceptions

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[summit-racing]"

_BASE_URL = "https://www.summitracing.com"
_APPLICATIONS_PATH = "/parts/{sku}/applications"

# Imperva's throttle stub is a ~930-byte NOINDEX page. A genuine applications page is ~150KB and
# even a part with zero applications renders the full chrome, so anything this small is a block,
# never content. Deliberately generous: the cost of mistaking a stub for a page is silently
# recording "no fitment" for a part that has some.
_MIN_PAGE_BYTES = 3_000

# Only the interstitial's own title identifies a challenge. Do NOT add "_Incapsula_Resource"
# here: ordinary pages load Incapsula's sensor script from that path, so matching on it flags
# every healthy page as blocked (caught in the first full sweep, which stopped after 13
# requests on a 188KB page carrying 25 application rows).
_CHALLENGE_MARKERS = ("Pardon Our Interruption",)

# Chrome build to impersonate. Pinned rather than "chrome" so an upgrade of curl_cffi cannot
# silently change the handshake we present -- that is the one variable Imperva scores us on.
_IMPERSONATE = "chrome131"

_DEFAULT_HEADERS = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "accept-language": "en-US,en;q=0.9",
    "cache-control": "no-cache",
    "pragma": "no-cache",
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "none",
    "sec-fetch-user": "?1",
    "upgrade-insecure-requests": "1",
}


def proxy_url(gateway: str) -> str:
    """Build a proxy URL from the StormProxies settings.

    ``STORM_PROXY_ACCOUNT_NAME`` is an email address, so the ``@`` has to be percent-encoded or
    it splits the authority and the proxy silently resolves the wrong host -- the same trap
    documented in ``src/management/commands/_pace_search.py``.
    """
    host = gateway.replace("http://", "").replace("https://", "").strip("/")
    username = getattr(settings, "STORM_PROXY_ACCOUNT_NAME", "") or ""
    password = getattr(settings, "STORM_PROXY_PASSWORD", "") or ""
    if not (username and password):
        return "http://{}".format(host)
    return "http://{}:{}@{}".format(
        urllib.parse.quote(username, safe=""),
        urllib.parse.quote(password, safe=""),
        host,
    )


def configured_gateways() -> typing.List[str]:
    """Gateways to spread workers across, newest-config-wins, falling back to direct."""
    raw = getattr(settings, "STORM_PROXY_GATEWAY_SCRAPE", "") or getattr(settings, "STORM_PROXY_GATEWAY", "") or ""
    return [g.strip() for g in raw.split(",") if g.strip()]


def classify_response(status_code: int, body: str) -> None:
    """Raise the specific failure this body represents, or return for a usable page.

    Imperva answers blocks with HTTP 200, so status alone tells us nothing and the body is the
    only signal available.
    """
    if any(marker in body for marker in _CHALLENGE_MARKERS):
        raise exceptions.SummitRacingChallenged(
            "Imperva interstitial (status={}, {} bytes) -- the reese84 cookie is stale; "
            "mint a fresh one in a browser.".format(status_code, len(body))
        )
    if len(body) < _MIN_PAGE_BYTES:
        raise exceptions.SummitRacingThrottled(
            "Throttle stub ({} bytes, status={}) -- this exit IP is rate-limited on this "
            "route.".format(len(body), status_code)
        )


class SummitRacingClient:
    """One pacing, rotating HTTP session. Not thread-safe: give each worker its own.

    ``pace_seconds`` is measured from the end of the previous request rather than slept blindly,
    so time already spent waiting on a slow response counts toward the gap (a residential proxy
    hop costs seconds on its own, and double-paying that would halve throughput for nothing).
    """

    def __init__(
        self,
        cookie_header: str = "",
        *,
        token_provider: typing.Optional[typing.Callable[[str], typing.Tuple[str, str]]] = None,
        gateways: typing.Optional[typing.Sequence[str]] = None,
        pace_seconds: float = 1.5,
        timeout: float = 60.0,
        max_attempts: int = 4,
        backoff_seconds: float = 20.0,
        max_remints: int = 3,
    ) -> None:
        if not cookie_header.strip() and token_provider is None:
            raise exceptions.SummitRacingError(
                "Pass a summitracing.com cookie header, or a token_provider that mints one."
            )
        # A minted token must be replayed from the IP *and* under the user agent it was minted
        # with, so the provider hands back both and they are applied together.
        self._token_provider = token_provider
        self._user_agent: typing.Optional[str] = None
        self._max_remints = max_remints
        self._remints = 0
        self._cookies = _parse_cookie_header(cookie_header) if cookie_header.strip() else {}
        # An empty gateway string means "go direct"; keeping it in the ring lets a caller run
        # without proxies by configuring nothing.
        self._gateways = list(gateways) if gateways else [""]
        self._gateway_cycle = itertools.cycle(self._gateways)
        self._gateway = next(self._gateway_cycle)
        self._pace_seconds = pace_seconds
        self._timeout = timeout
        self._max_attempts = max_attempts
        self._backoff_seconds = backoff_seconds
        self._last_request_at = 0.0
        self._session = None  # type: ignore[var-annotated]
        self.request_count = 0
        self.throttle_count = 0

    def _remint(self) -> bool:
        """Mint a fresh token on the current gateway. False when that is not possible.

        The token is minted on *this* client's gateway, never a shared one: a token replayed
        from a different exit IP is answered with the throttle stub, so one token per worker per
        sticky exit is the only arrangement that holds.
        """
        if self._token_provider is None or self._remints >= self._max_remints:
            return False
        self._remints += 1
        cookie_header, user_agent = self._token_provider(self._gateway)
        self._cookies = _parse_cookie_header(cookie_header)
        self._user_agent = user_agent
        self._session = None  # rebuild with the new cookies and UA
        return True

    def _ensure_session(self):
        if self._session is None and not self._cookies and not self._remint():
            raise exceptions.SummitRacingChallenged("No cookie available and no token provider.")
        if self._session is None:
            # Imported lazily so that importing this module (e.g. by Django's command autodiscovery)
            # does not hard-require curl_cffi in environments that never scrape.
            from curl_cffi import requests as curl_requests

            self._session = curl_requests.Session(
                impersonate=_IMPERSONATE,
                timeout=self._timeout,
                verify=False,  # noqa: S501 -- Imperva terminates TLS; cert pinning buys nothing here
            )
            self._session.headers.update(_DEFAULT_HEADERS)
            if self._user_agent:
                self._session.headers["user-agent"] = self._user_agent
            for name, value in self._cookies.items():
                self._session.cookies.set(name, value, domain=".summitracing.com")
        return self._session

    def rotate(self) -> None:
        """Move to the next gateway and drop the session.

        StormProxies' gateways are back-connect rotating, so a *new connection* is what buys a
        new exit IP -- reusing the pooled session would keep us on the IP that just got
        throttled. Dropping the session forces that new connection.
        """
        self._gateway = next(self._gateway_cycle)
        self._session = None

    def _wait_turn(self) -> None:
        if self._pace_seconds <= 0:
            return
        elapsed = time.monotonic() - self._last_request_at
        if elapsed < self._pace_seconds:
            time.sleep(self._pace_seconds - elapsed)

    def get_applications_page(self, sku: str, page: int = 1) -> str:
        """Fetch one applications page, retrying throttles on fresh exit IPs.

        Raises ``SummitRacingChallenged`` immediately without retrying: a stale cookie fails
        identically on every exit IP, so retrying only burns the budget and delays the operator
        learning they need a new token.
        """
        path = _APPLICATIONS_PATH.format(sku=urllib.parse.quote(sku.lower(), safe=""))
        url = "{}{}".format(_BASE_URL, path)
        if page > 1:
            url = "{}?page={}".format(url, page)

        last_error: typing.Optional[Exception] = None
        for attempt in range(1, self._max_attempts + 1):
            self._wait_turn()
            session = self._ensure_session()
            proxies = {"http": proxy_url(self._gateway), "https": proxy_url(self._gateway)} if self._gateway else None
            try:
                response = session.get(url, proxies=proxies)
                body = response.text
                self.request_count += 1
                self._last_request_at = time.monotonic()
                classify_response(response.status_code, body)
                return body
            except exceptions.SummitRacingChallenged as e:
                # Recoverable only when we can mint our own replacement: a hand-supplied cookie
                # fails identically on every attempt, so retrying it just delays the operator
                # learning they need a new one.
                last_error = e
                self._last_request_at = time.monotonic()
                if not self._remint():
                    raise
                logger.info(
                    "%s re-minted token after a challenge (attempt %s/%s)",
                    _LOG_PREFIX,
                    attempt,
                    self._max_attempts,
                )
            except exceptions.SummitRacingThrottled as e:
                self.throttle_count += 1
                last_error = e
                self._last_request_at = time.monotonic()
                # Linear-in-attempt backoff on a *different* exit IP. The throttle is per-IP, so
                # rotating is the actual fix; the sleep only keeps us from cycling the whole ring
                # into the same wall within a second.
                self.rotate()
                if attempt < self._max_attempts:
                    time.sleep(self._backoff_seconds * attempt)
            except Exception as e:  # noqa: BLE001 -- network errors: rotate and retry like a throttle
                last_error = e
                self._last_request_at = time.monotonic()
                self.rotate()
                if attempt < self._max_attempts:
                    time.sleep(min(self._backoff_seconds, 5.0) * attempt)

        raise exceptions.SummitRacingThrottled(
            "{} gave up on {} after {} attempts: {}".format(_LOG_PREFIX, sku, self._max_attempts, last_error)
        )


def _parse_cookie_header(cookie_header: str) -> typing.Dict[str, str]:
    """Split a raw browser ``Cookie:`` header into a name->value mapping.

    Values are kept verbatim (no unquoting): ``reese84`` is base64 with ``=`` padding and
    ``search_s`` is percent-encoded, and both must reach Imperva byte-identical to what the
    browser sent.
    """
    out: typing.Dict[str, str] = {}
    for part in cookie_header.strip().split(";"):
        part = part.strip()
        if not part or "=" not in part:
            continue
        name, value = part.split("=", 1)
        out[name.strip()] = value.strip()
    return out
