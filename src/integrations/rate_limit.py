"""
Cross-process fixed-window rate limiting for outbound distributor API calls.

Why this exists
---------------
Distributors enforce their limits per set of credentials, not per process. Turn 14's
Acceptable Usage Policy is 5 GET/s, 5 000 GET/hour and 30 000 GET/day per client_id, plus
10 token requests/minute *per IP*. Our workers are several independent OS processes
(``ingest_all_providers``, the inventory-delta cron, ``process_pricing_sync_jobs``), so the
``ratelimit`` package's decorators -- which keep their counter on a decorator object created
once when the class body executes -- were wrong twice over: the counter was shared by every
credential set inside one process (so eleven companies queued behind one budget instead of
getting eleven), and not shared at all *between* processes (so the true request rate was
whatever the counters said, times the number of running workers).

The counter therefore lives in Postgres (``src.models.ApiRateBucket``), the one thing every
worker already shares.

Hard vs soft buckets
--------------------
Turn 14 does not merely throttle: credentials are **deactivated** if the hourly limit is
reached 10 times in 30 days, or the daily limit exceeded twice in 30 days, and re-enabling
them requires a support conversation. Sleeping until an hour-long window rolls over is
therefore the wrong response twice over -- it pins a worker for up to an hour *and* keeps us
pressed against the ceiling we are being counted against.

So buckets come in two kinds:

``SOFT``  sub-minute windows (per-second burst, per-minute token issuance). Blocking is
          cheap and correct -- sleep until the window rolls over and retry.
``HARD``  hour and day windows. Never slept on: :class:`RateBudgetExhausted` is raised so the
          caller can checkpoint its cursor and requeue (see
          ``integration_pricing_sync_jobs.run_integration_pricing_sync_job``).

:class:`RateBudgetExhausted` deliberately does **not** subclass any provider's API exception.
The Turn 14 fetch loops all wrap their per-brand call in ``except Turn14APIException: continue``,
and continuing to the next brand is precisely the wrong thing to do when the budget is gone --
it would burn the remainder of the quota on 464 more doomed requests. Letting it propagate to
the job runner is the point.

Backend note
------------
Postgres was chosen over Redis only because Redis is not deployed yet. Everything
backend-specific is confined to :func:`_consume`; swapping in a Redis/Lua implementation later
means replacing that one function.
"""
import hashlib
import logging
import threading
import time
import typing

from django.db import connection
from django.utils import timezone

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[API-RATE-LIMIT]"

# Sub-minute windows are slept on; hour/day windows raise. See module docstring.
SOFT_WINDOW_MAX_SECONDS = 60

# A soft bucket should clear as soon as its window rolls over. The cap only guards against a
# clock jump or a pathologically contended window turning into an unbounded wait.
_SOFT_MAX_ATTEMPTS = 15

# Expired rows are dead weight; sweep them occasionally rather than on every call.
_PURGE_EVERY_N_ACQUIRES = 500

_purge_counter = 0
_purge_lock = threading.Lock()

# Requests issued per meter key, for reporting a sweep's cost into ScheduledTaskExecution.
# In-process only: this measures what *this* worker spent, which is what a sweep's audit row
# should say. The authoritative cross-process count is the ApiRateBucket rows themselves.
_counters: typing.Dict[str, int] = {}
_counters_lock = threading.Lock()


class RateBudgetExhausted(Exception):
    """
    A hard (hour/day) budget is spent. Callers should checkpoint and retry in a later window --
    never busy-wait, and never fall through to the next item as if this were a transport error.
    """

    def __init__(self, scope: str, limit: int, period_seconds: int, retry_after_seconds: float) -> None:
        Exception.__init__(
            self,
            "Rate budget exhausted for {} ({} requests per {}s). Retry in {:.0f}s.".format(
                scope, limit, period_seconds, retry_after_seconds
            ),
        )
        self.scope = scope
        self.limit = limit
        self.period_seconds = period_seconds
        self.retry_after_seconds = retry_after_seconds


# RateBudgetExhausted's own docstring already says the right response: "checkpoint and retry in
# a later window." A hard bucket's cooldown is bounded by its own window (an hour bucket can
# never report more than ~3600s), so for a long-running command that has already done real,
# valuable work (a multi-step sweep, a multi-thousand-page walk), waiting out that cooldown and
# resuming is strictly better than abandoning the whole run and starting over on the next cron
# tick. Bounded retries so a genuinely stuck budget (the daily cap, or an upstream outage)
# surfaces as a real failure rather than an unbounded sleep loop inside a cron-triggered process.
DEFAULT_MAX_RATE_LIMIT_RETRIES = 5


def retry_on_rate_budget(
    step_name: str,
    fn: typing.Callable[[], typing.Any],
    log_fn: typing.Callable[[str], None],
    max_retries: int = DEFAULT_MAX_RATE_LIMIT_RETRIES,
) -> typing.Any:
    """
    Call ``fn()``, and on RateBudgetExhausted sleep the reported cooldown and retry, up to
    ``max_retries`` times. Re-raises on the final attempt so the caller's own top-level handling
    (mark the scheduled task failed, etc.) still applies to a genuinely unrecoverable exhaustion.
    """
    for attempt in range(1, max_retries + 1):
        try:
            return fn()
        except RateBudgetExhausted as e:
            if attempt >= max_retries:
                raise
            wait_s = e.retry_after_seconds + 5
            log_fn(
                "{}: rate budget exhausted (attempt {}/{}), waiting {:.0f}s then retrying: {}".format(
                    step_name, attempt, max_retries, wait_s, e
                )
            )
            time.sleep(wait_s)


class Bucket(typing.NamedTuple):
    """
    One fixed-window limit. ``scope`` is the human-readable name used in keys, logs and
    exceptions (e.g. ``"t14:get:hour"``); ``identity`` scopes the counter to whatever the
    distributor actually meters -- a client_id for per-credential limits, or a constant such
    as ``"ip"`` for limits metered per source address.
    """
    scope: str
    identity: str
    limit: int
    period_seconds: int

    @property
    def is_soft(self) -> bool:
        return self.period_seconds <= SOFT_WINDOW_MAX_SECONDS


def identity_for(value: str) -> str:
    """
    Short, stable, non-reversible stand-in for a credential. Bucket keys land in a table any
    engineer can read and in log lines, so a client_id must never appear there verbatim.
    """
    return hashlib.sha256((value or "").encode("utf-8")).hexdigest()[:16]


def _bucket_key(bucket: Bucket, window_index: int) -> str:
    return "{}:{}:{}".format(bucket.scope, bucket.identity, window_index)


def _window(bucket: Bucket, now_epoch: float) -> typing.Tuple[str, float]:
    """The current window's key for ``bucket``, and the seconds left in it."""
    index = int(now_epoch // bucket.period_seconds)
    remaining_seconds = bucket.period_seconds - (now_epoch % bucket.period_seconds)
    return _bucket_key(bucket, index), remaining_seconds


def _consume(buckets: typing.Sequence[Bucket], now_epoch: float) -> typing.Set[str]:
    """
    Atomically take one slot from each of ``buckets``, independently, in a single statement.

    Returns the scopes that got a slot; anything missing from that set was full. Doing all
    buckets in one round trip matters: this runs before every outbound API call, and on a
    776-page sweep three separate round trips is three times the latency added to every
    request for no benefit.

    ``WHERE b.count < EXCLUDED.limit_value`` makes the update a no-op once a window is full,
    and an update that does not happen returns no row -- so the check and the increment cannot
    be split by a concurrent worker. Per-bucket independence is deliberate: the caller decides
    what a partial result means (see :func:`acquire`).
    """
    if not buckets:
        return set()

    rows = []
    params: typing.List[typing.Any] = []
    for bucket in buckets:
        key, remaining_seconds = _window(bucket, now_epoch)
        rows.append("(%s, %s::int, %s::timestamptz)")
        params.extend([key, bucket.limit, timezone.now() + timezone.timedelta(seconds=remaining_seconds)])

    sql = """
        INSERT INTO api_rate_buckets AS b
            (bucket_key, count, limit_value, expires_at, created_at, updated_at)
        SELECT v.k, 1, v.lim, v.exp, NOW(), NOW()
        FROM (VALUES {}) AS v(k, lim, exp)
        ON CONFLICT (bucket_key) DO UPDATE
            SET count = b.count + 1,
                updated_at = NOW()
            WHERE b.count < EXCLUDED.limit_value
        RETURNING bucket_key
    """.format(", ".join(rows))

    with connection.cursor() as cursor:
        cursor.execute(sql, params)
        granted_keys = {r[0] for r in cursor.fetchall()}

    return {b.scope for b in buckets if _window(b, now_epoch)[0] in granted_keys}


def mark_exhausted(bucket: Bucket) -> None:
    """
    Slam ``bucket`` shut for the remainder of its current window.

    Called when the distributor returns 429 even though our own accounting said there was
    budget left -- which happens whenever something other than this codebase is spending the
    same credentials (Turn 14 customers may hand the same client_id to third-party integrators),
    or when their rolling window disagrees with our fixed one. Without this, every concurrent
    worker would have to discover the 429 independently and each would burn another request
    doing so.
    """
    key, remaining_seconds = _window(bucket, time.time())
    expires_at = timezone.now() + timezone.timedelta(seconds=remaining_seconds)
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO api_rate_buckets AS b
                (bucket_key, count, limit_value, expires_at, created_at, updated_at)
            VALUES (%s, %s, %s, %s, NOW(), NOW())
            ON CONFLICT (bucket_key) DO UPDATE
                SET count = GREATEST(b.count, EXCLUDED.count),
                    updated_at = NOW()
            """,
            [key, bucket.limit, bucket.limit, expires_at],
        )
    logger.warning(
        "{} Marked {} exhausted for the next {:.0f}s after an upstream 429.".format(
            _LOG_PREFIX, bucket.scope, remaining_seconds
        )
    )


def purge_expired() -> int:
    """Delete windows that have rolled over. Safe to call from anywhere; cheap (indexed)."""
    with connection.cursor() as cursor:
        cursor.execute("DELETE FROM api_rate_buckets WHERE expires_at < NOW()")
        return cursor.rowcount or 0


def _maybe_purge() -> None:
    global _purge_counter
    with _purge_lock:
        _purge_counter += 1
        due = _purge_counter % _PURGE_EVERY_N_ACQUIRES == 0
    if not due:
        return
    try:
        purge_expired()
    except Exception as e:  # noqa: BLE001 - housekeeping must never break a real request
        logger.warning("{} Failed to purge expired rate buckets: {}".format(_LOG_PREFIX, e))


def acquire(buckets: typing.Sequence[Bucket], meter_key: typing.Optional[str] = None) -> None:
    """
    Take one slot from every bucket, or raise.

    ``meter_key`` groups this call for :func:`usage_snapshot` -- one tick per call, not per
    bucket, so the number reported is "requests issued" rather than "buckets touched".

    Hard buckets are consumed first and in the order given, so a request blocked by the daily
    budget never burns a slot from the per-second one. Soft buckets are consumed last, after
    the hard ones have already been paid for, so the sleep-and-retry loop can repeat without
    re-charging any hour or day counter.

    Raises :class:`RateBudgetExhausted` if a hard bucket is full, or if a soft bucket is still
    full after ``_SOFT_MAX_ATTEMPTS`` window rollovers (which would mean a stuck clock, not
    ordinary contention).
    """
    hard = [b for b in buckets if not b.is_soft]
    soft = [b for b in buckets if b.is_soft]

    # One round trip for the common case: everything has budget and the call proceeds.
    now = time.time()
    granted = _consume(buckets, now)

    exhausted_hard = [b for b in hard if b.scope not in granted]
    if exhausted_hard:
        bucket = exhausted_hard[0]
        retry_after = _window(bucket, now)[1]
        logger.warning(
            "{} Hard budget exhausted (scope={}, limit={}/{}s, retry_after={:.0f}s).".format(
                _LOG_PREFIX, bucket.scope, bucket.limit, bucket.period_seconds, retry_after
            )
        )
        raise RateBudgetExhausted(
            scope=bucket.scope,
            limit=bucket.limit,
            period_seconds=bucket.period_seconds,
            retry_after_seconds=retry_after,
        )

    # Hard budget is paid for. Any soft bucket that missed is just burst pacing: wait out its
    # window and retry *only* that bucket, so the hour/day counters are never charged twice for
    # one request.
    for bucket in [b for b in soft if b.scope not in granted]:
        for _ in range(_SOFT_MAX_ATTEMPTS):
            now = time.time()
            # Small margin so we do not land exactly on the boundary and race the next
            # window's first caller.
            time.sleep(_window(bucket, now)[1] + 0.01)
            if bucket.scope in _consume([bucket], time.time()):
                break
        else:
            raise RateBudgetExhausted(
                scope=bucket.scope,
                limit=bucket.limit,
                period_seconds=bucket.period_seconds,
                retry_after_seconds=bucket.period_seconds,
            )

    if meter_key:
        with _counters_lock:
            _counters[meter_key] = _counters.get(meter_key, 0) + 1

    _maybe_purge()


def remaining(bucket: Bucket) -> int:
    """
    Slots left in ``bucket``'s current window. Read-only -- for logging a sweep's remaining
    budget into ScheduledTaskExecution, never as a check before acquiring (that would be a
    read-then-act race; :func:`acquire` is the atomic one).
    """
    key = _window(bucket, time.time())[0]
    with connection.cursor() as cursor:
        cursor.execute("SELECT count FROM api_rate_buckets WHERE bucket_key = %s", [key])
        row = cursor.fetchone()
    used = row[0] if row else 0
    return max(0, bucket.limit - used)


def usage_snapshot() -> typing.Dict[str, int]:
    """Requests issued by this process so far, per meter key."""
    with _counters_lock:
        return dict(_counters)


def reset_usage() -> None:
    with _counters_lock:
        _counters.clear()


class UsageMeter(object):
    """
    Measures what a sweep cost, for the audit trail.

    Usage:
        with UsageMeter("t14:get", hourly=turn14_rate_limit.hourly_bucket(client_id)) as meter:
            ...
        audit.mark_scheduled_task_completed(execution, message=meter.summary("Turn14 pricing"))

    Reports requests issued and wall-clock, so before/after runs of the same task are directly
    comparable in ScheduledTaskExecution -- which is the only record we have of how long these
    sweeps used to take.
    """

    def __init__(self, meter_key: str, hourly: typing.Optional[Bucket] = None) -> None:
        self.meter_key = meter_key
        self.hourly = hourly
        self.started_at = 0.0
        self.elapsed_seconds = 0.0
        self._start_count = 0
        self.requests = 0

    def __enter__(self) -> "UsageMeter":
        self.started_at = time.time()
        self._start_count = usage_snapshot().get(self.meter_key, 0)
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        self.elapsed_seconds = time.time() - self.started_at
        self.requests = usage_snapshot().get(self.meter_key, 0) - self._start_count
        return False

    def summary(self, label: str) -> str:
        parts = [
            label,
            "requests={}".format(self.requests),
            "elapsed={:.1f}s".format(self.elapsed_seconds),
        ]
        if self.elapsed_seconds > 0:
            parts.append("rate={:.1f}/min".format(self.requests / self.elapsed_seconds * 60))
        if self.hourly is not None:
            try:
                parts.append("hourly_budget_left={}".format(remaining(self.hourly)))
            except Exception:  # noqa: BLE001 - reporting must never break the task
                pass
        return " | ".join(parts)
