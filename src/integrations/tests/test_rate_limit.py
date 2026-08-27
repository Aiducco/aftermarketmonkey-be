"""
Tests for ``src.integrations.rate_limit``'s upstream-429 cooldown.

Needs a real database: the mechanism is a raw-SQL lock against ``api_rate_buckets``, not
in-memory state. What matters here is the incident this replaced (2026-08-27): a single Turn 14
429 must never lock out unrelated callers for anywhere near an hour again.
"""
from django.db import connection
from django.test import TestCase

from src.integrations import rate_limit


def _bucket(scope="test:rate-limit-cooldown"):
    return rate_limit.Bucket(scope, "test-identity", 100, 3600)


class MarkExhaustedCooldownTests(TestCase):
    def test_distributor_retry_after_is_honored_exactly(self):
        applied = rate_limit.mark_exhausted(_bucket(), retry_after_seconds=45)
        self.assertEqual(applied, 45.0)

        with self.assertRaises(rate_limit.RateBudgetExhausted) as ctx:
            rate_limit.acquire([_bucket()])
        self.assertAlmostEqual(ctx.exception.retry_after_seconds, 45.0, delta=1.0)

    def test_a_huge_retry_after_is_capped(self):
        applied = rate_limit.mark_exhausted(_bucket(), retry_after_seconds=999_999)
        self.assertEqual(applied, rate_limit.MAX_COOLDOWN_SECONDS)

    def test_no_signal_uses_the_short_base_not_the_old_hour_long_lockout(self):
        applied = rate_limit.mark_exhausted(_bucket(), retry_after_seconds=None)
        self.assertEqual(applied, rate_limit._NO_SIGNAL_BASE_COOLDOWN_SECONDS)
        self.assertLess(applied, 60, "a single unexplained 429 must cost seconds, not most of an hour")

    def test_no_signal_backoff_doubles_on_a_back_to_back_repeat(self):
        first = rate_limit.mark_exhausted(_bucket(), retry_after_seconds=None)
        # Simulate the first cooldown having just ended, well inside the streak-memory window.
        with connection.cursor() as cursor:
            cursor.execute(
                "UPDATE api_rate_buckets SET expires_at = NOW() - INTERVAL '1 second' WHERE bucket_key = %s",
                [rate_limit._cooldown_key(_bucket())],
            )
        second = rate_limit.mark_exhausted(_bucket(), retry_after_seconds=None)
        self.assertEqual(second, first * 2)

    def test_acquire_proceeds_once_a_cooldown_has_actually_expired(self):
        rate_limit.mark_exhausted(_bucket(), retry_after_seconds=30)
        with connection.cursor() as cursor:
            cursor.execute(
                "UPDATE api_rate_buckets SET expires_at = NOW() - INTERVAL '1 second' WHERE bucket_key = %s",
                [rate_limit._cooldown_key(_bucket())],
            )
        rate_limit.acquire([_bucket()])  # must not raise

    def test_a_bucket_with_no_cooldown_is_unaffected(self):
        rate_limit.acquire([_bucket(scope="test:rate-limit-cooldown-untouched")])  # must not raise
