"""
Tests for the credential-rejection backoff in check_company_provider_connections.

What this pins down is the thing that went wrong in production: the cron re-validates every
connection whose initial sync hasn't completed, every ~5 minutes, by actually authenticating
against the distributor. Credentials the distributor rejects outright never start working on
their own, so ATD's Frontline Off-Road connection (company 61) spent four days in 2026-10
sending ~1,150 rejected logins to ATD -- the kind of traffic that gets an account locked or a
source IP throttled for every dealer behind it.

The distinction that matters, and the easiest one to regress: only `invalid_credentials` backs
off. A timeout or a 5xx says nothing about the stored credentials and can clear on its own, so
it keeps the normal cadence -- but it must not RESET an existing backoff either, or one blip
mid-deferral hands a permanently-bad credential set a fresh round of 5-minute retries.

No DB: the command's decision logic is exercised against a stub row, so these run as
SimpleTestCase rather than needing a test database.
"""
import datetime
import typing

from django.test import SimpleTestCase
from django.utils import timezone

from src import enums as src_enums
from src.api.services import integrations as integrations_services
from src.management.commands import check_company_provider_connections as cmd_module


class _StubCompanyProvider(object):
    """Just enough of CompanyProviders for Command._check_validated / _save_status."""

    def __init__(
        self,
        consecutive_failures: int = 0,
        next_check_at: typing.Optional[datetime.datetime] = None,
    ) -> None:
        self.id = 182
        self.company = None
        self.provider = None
        self.credentials = {"feed": {"username": "frontlineoutfitters"}}
        self.status = None
        self.status_name = None
        self.status_reason = None
        self.status_checked_at = None
        self.status_consecutive_failures = consecutive_failures
        self.status_next_check_at = next_check_at
        self.saved_fields: typing.List[str] = []

    def save(self, update_fields: typing.Optional[typing.List[str]] = None) -> None:
        self.saved_fields = list(update_fields or [])


_KIND = src_enums.BrandProviderKind.ATD.value

_REJECTED = (
    "Unable to authenticate User's Credentials. Please contact Online Customer Care if you "
    "feel that this User Account should have access to ATDConnect"
)


class InvalidCredentialsRetryDelayTests(SimpleTestCase):
    def test_first_failures_are_not_deferred(self):
        """Someone who just mistyped a password is usually still on the page -- the 5-minute
        cadence is what makes the status flip feel immediate once they fix it."""
        for failures in range(1, cmd_module._INVALID_CREDENTIALS_GRACE_CHECKS + 1):
            self.assertIsNone(cmd_module._invalid_credentials_retry_delay(failures))

    def test_schedule_escalates_then_caps(self):
        self.assertEqual(
            cmd_module._invalid_credentials_retry_delay(3), datetime.timedelta(minutes=15)
        )
        self.assertEqual(cmd_module._invalid_credentials_retry_delay(4), datetime.timedelta(hours=1))
        self.assertEqual(cmd_module._invalid_credentials_retry_delay(5), datetime.timedelta(hours=6))
        self.assertEqual(
            cmd_module._invalid_credentials_retry_delay(6), cmd_module._INVALID_CREDENTIALS_MAX_BACKOFF
        )

    def test_delay_never_decreases(self):
        delays = [
            cmd_module._invalid_credentials_retry_delay(n) or datetime.timedelta(0)
            for n in range(1, 40)
        ]
        self.assertEqual(delays, sorted(delays))

    def test_long_running_failure_caps_at_a_day(self):
        self.assertEqual(
            cmd_module._invalid_credentials_retry_delay(5000),
            cmd_module._INVALID_CREDENTIALS_MAX_BACKOFF,
        )


class CheckValidatedBackoffTests(SimpleTestCase):
    def setUp(self):
        self.command = cmd_module.Command()

    def _run(self, cp, result):
        """Run _check_validated with the kind's validator stubbed to return ``result``."""
        original = integrations_services._CONNECTION_VALIDATORS.get(_KIND)
        integrations_services._CONNECTION_VALIDATORS[_KIND] = lambda _creds: result
        try:
            self.command._check_validated(cp, _KIND)
        finally:
            if original is not None:
                integrations_services._CONNECTION_VALIDATORS[_KIND] = original

    def test_rejection_increments_and_eventually_defers(self):
        cp = _StubCompanyProvider(consecutive_failures=2)
        before = timezone.now()

        self._run(cp, (_REJECTED, integrations_services.CONNECTION_ERROR_INVALID_CREDENTIALS))

        self.assertEqual(cp.status, src_enums.CompanyProviderConnectionStatus.FAILING.value)
        self.assertEqual(cp.status_reason, _REJECTED)
        self.assertEqual(cp.status_consecutive_failures, 3)
        self.assertIsNotNone(cp.status_next_check_at)
        self.assertGreaterEqual(
            cp.status_next_check_at, before + datetime.timedelta(minutes=15)
        )
        # The new columns have to be in update_fields or the deferral is silently dropped and
        # the cron keeps hammering exactly as before.
        self.assertIn("status_consecutive_failures", cp.saved_fields)
        self.assertIn("status_next_check_at", cp.saved_fields)

    def test_early_rejection_stays_on_the_normal_cadence(self):
        cp = _StubCompanyProvider(consecutive_failures=0)

        self._run(cp, (_REJECTED, integrations_services.CONNECTION_ERROR_INVALID_CREDENTIALS))

        self.assertEqual(cp.status_consecutive_failures, 1)
        self.assertIsNone(cp.status_next_check_at)

    def test_success_clears_the_backoff(self):
        cp = _StubCompanyProvider(
            consecutive_failures=9, next_check_at=timezone.now() + datetime.timedelta(hours=24)
        )

        self._run(cp, (None, None))

        self.assertEqual(cp.status, src_enums.CompanyProviderConnectionStatus.INGESTING.value)
        self.assertIsNone(cp.status_reason)
        self.assertEqual(cp.status_consecutive_failures, 0)
        self.assertIsNone(cp.status_next_check_at)

    def test_transient_failure_preserves_an_existing_backoff(self):
        """A timeout mid-deferral must not reset the counter -- otherwise a distributor having
        a bad afternoon buys every rejected credential set a fresh round of 5-minute retries."""
        deferred_until = timezone.now() + datetime.timedelta(hours=6)
        cp = _StubCompanyProvider(consecutive_failures=5, next_check_at=deferred_until)

        self._run(
            cp, ("Timed out calling ATD", integrations_services.CONNECTION_ERROR_CONNECTION_FAILED)
        )

        self.assertEqual(cp.status, src_enums.CompanyProviderConnectionStatus.FAILING.value)
        self.assertEqual(cp.status_reason, "Timed out calling ATD")
        self.assertEqual(cp.status_consecutive_failures, 5)
        self.assertEqual(cp.status_next_check_at, deferred_until)

    def test_transient_failure_alone_never_defers(self):
        cp = _StubCompanyProvider(consecutive_failures=0)

        self._run(
            cp, ("Timed out calling ATD", integrations_services.CONNECTION_ERROR_CONNECTION_FAILED)
        )

        self.assertEqual(cp.status_consecutive_failures, 0)
        self.assertIsNone(cp.status_next_check_at)

    def test_relay_feed_check_leaves_rows_undeferred(self):
        """_check_relay_feed goes through the same _save_status, whose backoff arguments default
        to "clear it" -- a relay row must never pick up a deferral it can't clear."""
        cp = _StubCompanyProvider(consecutive_failures=4, next_check_at=timezone.now())

        self.command._save_status(cp, src_enums.CompanyProviderConnectionStatus.WAITING, "no file yet")

        self.assertEqual(cp.status_consecutive_failures, 0)
        self.assertIsNone(cp.status_next_check_at)


class ConnectionStatusFieldsResetTests(SimpleTestCase):
    """Re-saving feed credentials is the one event that can turn a rejected connection into a
    working one, so it has to put the row back on the fast cadence immediately."""

    def test_feed_status_fields_clear_the_backoff(self):
        fields = integrations_services._connection_status_fields(
            src_enums.CompanyProviderConnectionStatus.INGESTING, None
        )
        self.assertEqual(fields["status_consecutive_failures"], 0)
        self.assertIsNone(fields["status_next_check_at"])

    def test_cleared_feed_also_clears_the_backoff(self):
        fields = integrations_services._connection_status_fields(None, None)
        self.assertEqual(fields["status_consecutive_failures"], 0)
        self.assertIsNone(fields["status_next_check_at"])
