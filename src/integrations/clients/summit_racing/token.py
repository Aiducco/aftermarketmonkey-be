"""
Mint ``reese84`` session tokens with a headless browser.

Imperva issues ``reese84`` from a JS sensor that fingerprints the browser and POSTs the result
back, so the token cannot be produced from Python -- but it *can* be produced by a real browser
engine, and headless Chromium is not rejected: all four of headless-shell, headed Chromium,
headless Chrome and headed Chrome were issued a token on 2026-10-23.

**A token must be used from the IP it was minted on.** Measured the hard way: a sweep of 12
workers sharing one token across 12 rotating exits was revoked after exactly 13 requests (one
per worker), while 4 workers on a single IP ran 61 requests clean. A token minted on exit A and
replayed from exit B is answered with the throttle stub.

That makes a *rotating* proxy gateway unusable here, and the failure is silent rather than
loud -- ``context.request`` calls inside a single Playwright context were observed leaving from
62.233.39.79 and then 185.201.139.248, because the gateway rotates per connection. Parallelism
therefore needs **sticky** exits: one session that holds its IP for the life of its token, per
worker. Configure those gateways in ``STORM_PROXY_GATEWAY_SCRAPE`` (comma-separated) and give
each worker its own.
"""

from __future__ import annotations

import logging
import typing
import urllib.parse

from django.conf import settings

from src.integrations.clients.summit_racing import exceptions

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[summit-racing]"

# Any Summit page issues the token; the home page is the cheapest and least likely to be
# rate-limited on its own.
_MINT_URL = "https://www.summitracing.com/"

# The sensor POSTs its fingerprint after load, and the cookie only exists once that round trip
# finishes. Measured at ~6-8s; polling below means a fast mint is not forced to wait the max.
_SENSOR_POLL_MS = 500
_SENSOR_TIMEOUT_MS = 20_000


class MintedToken(typing.NamedTuple):
    """A cookie header and the user agent it was minted with.

    The UA travels with the cookie because Imperva scores the pair: replaying a token under a
    different user agent is rejected the same way a different IP is.
    """

    cookie_header: str
    user_agent: str
    gateway: str


def mint_token(
    *,
    gateway: str = "",
    headless: bool = True,
    timeout_ms: int = 120_000,
) -> MintedToken:
    """Drive a headless browser until Imperva issues a ``reese84``, through ``gateway`` if given.

    Raises ``SummitRacingChallenged`` when no token appears, which is the same failure the
    caller already handles for a stale cookie.
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as e:  # pragma: no cover -- environment problem, not logic
        raise exceptions.SummitRacingError(
            "Minting a Summit token needs Playwright: pip install playwright && playwright install chromium"
        ) from e

    proxy = _proxy_config(gateway)
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=headless)
        try:
            context = browser.new_context(
                viewport={"width": 1440, "height": 900},
                locale="en-US",
                **({"proxy": proxy} if proxy else {}),
            )
            page = context.new_page()
            page.goto(_MINT_URL, wait_until="domcontentloaded", timeout=timeout_ms)

            waited = 0
            cookies: typing.Dict[str, str] = {}
            while waited < _SENSOR_TIMEOUT_MS:
                cookies = {c["name"]: c["value"] for c in context.cookies()}
                if cookies.get("reese84"):
                    break
                page.wait_for_timeout(_SENSOR_POLL_MS)
                waited += _SENSOR_POLL_MS

            if not cookies.get("reese84"):
                raise exceptions.SummitRacingChallenged(
                    "No reese84 issued after {}ms via gateway {!r}".format(_SENSOR_TIMEOUT_MS, gateway or "direct")
                )

            user_agent = page.evaluate("navigator.userAgent")
            header = "; ".join("{}={}".format(name, value) for name, value in cookies.items())
            logger.info("%s minted reese84 via %s (%s cookies)", _LOG_PREFIX, gateway or "direct", len(cookies))
            return MintedToken(cookie_header=header, user_agent=user_agent, gateway=gateway)
        finally:
            browser.close()


def _proxy_config(gateway: str) -> typing.Optional[typing.Dict[str, str]]:
    if not gateway:
        return None
    host = gateway.replace("http://", "").replace("https://", "").strip("/")
    username = getattr(settings, "STORM_PROXY_ACCOUNT_NAME", "") or ""
    password = getattr(settings, "STORM_PROXY_PASSWORD", "") or ""
    config = {"server": "http://{}".format(host)}
    if username and password:
        # Playwright takes credentials as fields, so unlike a proxy URL the "@" in the account
        # email needs no percent-encoding here -- encoding it would send the wrong username.
        config["username"] = username
        config["password"] = password
    return config


def proxy_url_for_gateway(gateway: str) -> str:
    """URL form of the same gateway, for HTTP clients that take a proxy URL."""
    host = gateway.replace("http://", "").replace("https://", "").strip("/")
    username = getattr(settings, "STORM_PROXY_ACCOUNT_NAME", "") or ""
    password = getattr(settings, "STORM_PROXY_PASSWORD", "") or ""
    if not (username and password):
        return "http://{}".format(host)
    return "http://{}:{}@{}".format(urllib.parse.quote(username, safe=""), urllib.parse.quote(password, safe=""), host)
