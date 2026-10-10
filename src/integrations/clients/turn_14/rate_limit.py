"""
Turn 14's Acceptable Usage Policy, expressed as buckets, plus the process-wide token cache.

Limits (https://www.turn14.com/api_settings.php):

    Per IP                    10 token requests / minute
    Per credential set         5 GET / second
                               2 quote / second
                           5 000 GET / hour
                          30 000 GET / day

Two things about that table drive the whole design.

**The hourly limit, not the per-second one, is what governs a long sweep.** 5 000/hour is 83
requests/minute sustained -- well under the 5/second burst rate. Anything that pages through
the catalog should be budgeted against 5 000/hour.

**Both Turn 14 clients spend the same budget.** ``client.py`` (catalog/pricing) and
``order_client.py`` (quote/order/invoice) authenticate with the same client_id, so a company's
nightly pricing sync and its hourly order sweep draw down one shared 5 000/hour allowance.
They therefore share the buckets here rather than each keeping their own.

Token issuance is the exception: it is metered per *IP*, so every credential set on this server
shares one 10/minute bucket -- hence ``identity="ip"`` rather than a client_id hash.
"""
import logging
import typing

import requests
from django.core.cache import cache

from src.integrations import rate_limit

logger = logging.getLogger(__name__)
_LOG_PREFIX = "[TURN14-RATE-LIMIT]"

GET_PER_SECOND = 5
GET_PER_HOUR = 5000

# Not a Turn 14 limit -- a governor we impose on ourselves, derived from the hourly one.
#
# Our hour bucket is a *fixed* window: it resets on the hour. Nothing in it prevents spending
# the whole 5 000 in the last two minutes of one hour and another 5 000 in the first two of the
# next -- 10 000 requests inside four minutes, which any rolling-window limiter on their side
# would (correctly) reject. 20% above the hourly average leaves room to catch up after a stall
# while keeping the burst bounded. Deliberately a soft bucket, so it paces rather than aborts.
GET_PER_MINUTE = 100
GET_PER_DAY = 30000
QUOTE_PER_SECOND = 2
TOKEN_PER_MINUTE_PER_IP = 10

# Refresh a token this long before it actually expires, so a request never races the boundary.
TOKEN_EXPIRATION_BUFFER_SECONDS = 60

_TOKEN_CACHE_KEY_PREFIX = "turn14_token"


def get_buckets(client_id: str) -> typing.List[rate_limit.Bucket]:
    """Buckets every GET on either Turn 14 client must pass, ordered day -> hour -> second."""
    identity = rate_limit.identity_for(client_id)
    return [
        rate_limit.Bucket("t14:get:day", identity, GET_PER_DAY, 86400),
        rate_limit.Bucket("t14:get:hour", identity, GET_PER_HOUR, 3600),
        rate_limit.Bucket("t14:get:minute", identity, GET_PER_MINUTE, 60),
        rate_limit.Bucket("t14:get:second", identity, GET_PER_SECOND, 1),
    ]


def quote_buckets(client_id: str) -> typing.List[rate_limit.Bucket]:
    """
    Quote requests are metered separately at 2/s, but still count as requests against the
    hour and day allowances, so those are included here too.
    """
    identity = rate_limit.identity_for(client_id)
    return [
        rate_limit.Bucket("t14:get:day", identity, GET_PER_DAY, 86400),
        rate_limit.Bucket("t14:get:hour", identity, GET_PER_HOUR, 3600),
        rate_limit.Bucket("t14:get:minute", identity, GET_PER_MINUTE, 60),
        rate_limit.Bucket("t14:quote:second", identity, QUOTE_PER_SECOND, 1),
    ]


def token_buckets() -> typing.List[rate_limit.Bucket]:
    """Metered per IP, so deliberately not scoped to a client_id."""
    return [rate_limit.Bucket("t14:token:minute", "ip", TOKEN_PER_MINUTE_PER_IP, 60)]


def hourly_bucket(client_id: str) -> rate_limit.Bucket:
    """The bucket worth reporting on -- the one that actually governs sweep throughput."""
    return rate_limit.Bucket(
        "t14:get:hour", rate_limit.identity_for(client_id), GET_PER_HOUR, 3600
    )


def parse_retry_after_seconds(response: requests.Response) -> typing.Optional[float]:
    """
    ``Retry-After`` in seconds when a 429 response carries a usable one, else ``None``.

    Shared by both Turn 14 clients so a 429's real, distributor-reported cooldown -- when it
    sends one -- is what gets passed to :func:`src.integrations.rate_limit.mark_exhausted`,
    rather than each client guessing its own default. ``None`` (no usable header) is common in
    practice for Turn 14 -- confirmed live 2026-08-27 -- and is a real, meaningful case
    ``mark_exhausted`` handles deliberately (backoff, not a flat guess); it is not a placeholder
    for "assume it's fine."
    """
    raw = (response.headers or {}).get("Retry-After")
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return None
    return value if value > 0 else None


def _token_cache_key(client_id: str) -> str:
    return "{}:{}".format(_TOKEN_CACHE_KEY_PREFIX, client_id)


def get_cached_token(client_id: str) -> typing.Optional[str]:
    """
    A live token for ``client_id``, or None.

    Cached per client_id in the shared Django cache (Redis) rather than a process-local dict --
    confirmed live 2026-10-10 that an in-memory cache here was the root cause of Turn 14 flagging
    our token-request volume (Dan Ziegler, ~50/hour against an expected <300/day): every Turn 14
    cron (check_company_provider_connections, inventory/items deltas, pricing jobs, ...) runs as
    its own fresh ``docker exec`` process, so a cache that doesn't outlive one process meant a
    brand new token on every single invocation, independent of whether the previous one was
    still valid. A shared cache fixes that for every call site at once, the same way several
    call sites already share this cache by client_id to avoid 464 token requests in one sweep
    (per-brand client construction) -- this is that same problem one layer up, across processes
    instead of across one process's loop iterations.

    Cache is an optimization, never a dependency: per this codebase's standing rule (a cache
    outage once silently broke /address/suggest), any read/write failure here just means "mint a
    new token," not a propagated error.
    """
    try:
        return cache.get(_token_cache_key(client_id))
    except Exception as e:
        logger.warning("{} Cache read failed, minting a new token: {}.".format(_LOG_PREFIX, e))
        return None


def store_token(client_id: str, token: str, expires_in: typing.Optional[float]) -> None:
    """Cache ``token``. Falls back to the OAuth2-conventional hour when expires_in is absent."""
    ttl = float(expires_in) if expires_in else 3600.0
    # Still refresh this long before real expiry (TOKEN_EXPIRATION_BUFFER_SECONDS) so a request
    # already in flight never races the cache's own expiry boundary.
    cache_ttl = max(ttl - TOKEN_EXPIRATION_BUFFER_SECONDS, 1.0)
    try:
        cache.set(_token_cache_key(client_id), token, timeout=cache_ttl)
    except Exception as e:
        # Token is still returned to this caller for immediate use (see client.py) -- only the
        # reuse-across-requests benefit is lost, not this request.
        logger.warning("{} Cache write failed, token won't be reused: {}.".format(_LOG_PREFIX, e))


def clear_token(client_id: str) -> None:
    """Drop a cached token — called after a 401 so the retry fetches a fresh one."""
    try:
        cache.delete(_token_cache_key(client_id))
    except Exception as e:
        logger.warning("{} Cache delete failed: {}.".format(_LOG_PREFIX, e))
