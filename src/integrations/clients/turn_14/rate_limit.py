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
import threading
import time
import typing

from src.integrations import rate_limit

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

_token_cache: typing.Dict[str, typing.Tuple[str, float]] = {}
_token_cache_lock = threading.Lock()


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


def get_cached_token(client_id: str) -> typing.Optional[str]:
    """
    A live token for ``client_id``, or None.

    Cached per client_id at module level rather than per client instance: several call sites
    construct a fresh ``Turn14ApiClient`` inside a per-brand loop, and with a per-instance
    cache that meant one token request per brand -- 464 per sweep, from a single IP, against a
    10/minute ceiling. Keyed by credential, so two companies never share a token.
    """
    with _token_cache_lock:
        entry = _token_cache.get(client_id)
        if not entry:
            return None
        token, expires_at = entry
        if time.time() >= (expires_at - TOKEN_EXPIRATION_BUFFER_SECONDS):
            return None
        return token


def store_token(client_id: str, token: str, expires_in: typing.Optional[float]) -> None:
    """Cache ``token``. Falls back to the OAuth2-conventional hour when expires_in is absent."""
    ttl = float(expires_in) if expires_in else 3600.0
    with _token_cache_lock:
        _token_cache[client_id] = (token, time.time() + ttl)


def clear_token(client_id: str) -> None:
    """Drop a cached token — called after a 401 so the retry fetches a fresh one."""
    with _token_cache_lock:
        _token_cache.pop(client_id, None)
