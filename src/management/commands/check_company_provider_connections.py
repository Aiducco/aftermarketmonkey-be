"""
Periodic connectivity check for CompanyProviders connections. Scheduled via cron (see
command_runner.sh), every few minutes.

Feed status — only rows whose initial pricing sync hasn't completed yet AND that actually have
feed credentials configured (credentials.feed non-empty) -- a row with feed credentials cleared
(e.g. disconnect_provider(namespace="feed"), which clears them without deleting the row when
Ordering is still configured) has nothing to check and is left untouched, not re-validated or
resurrected from a stale relay file:
  - Kinds with a live credential validator (Turn 14, Keystone, Wheel Pros, Premier, Rough
    Country — same registry used at connect/update time): re-run that check against the
    stored credentials.
      passes -> INGESTING (connectivity fine, sync just hasn't finished yet)
      fails  -> FAILING, reason = the validator's message
  - Relay-provisioned kinds with an ingest client (Meyer, A-Tech, The Wheel Group): via
    integrations_services._relay_feed_connection_status — same helper connect_provider/
    update_connection use to set the initial status, so there's one source of truth for
    what "file arrived" means. (The Wheel Group is checked against the public mastersheet it
    is actually read from today rather than the relay folder — see that helper.)
  - Everything else (no ingest client built yet — CTP, Crown, DIX, and the rest of the
    catalog-only distributors): left untouched, nothing to check against.

  Rows where initial_sync_completed is already True are out of scope for this part — those
  are set to CONNECTED once, directly, in integration_pricing_sync_jobs when the first sync
  completes, and aren't re-checked here.

Order status — every active CompanyProviderOrderAccount (not just the connection's default —
each account is its own source of truth for its own order_status/... fields, see that model's
docstring) belonging to an active row whose provider kind has a registered order validator (see
integrations_services._ORDER_CONNECTION_VALIDATORS), regardless of initial_sync_completed: order
credentials can go stale on their own (a rotated API key, a revoked security key) independently
of feed sync state, so they're re-checked on a schedule of their own rather than only while the
feed is still mid-sync -- but at most once per _ORDER_RECHECK_INTERVAL (2 hours), not every time
this command runs. A revoked credential still surfaces immediately through the normal path (the
next real order attempt fails loudly); this check only catches it a little sooner, in the
background, and doesn't need 5-minute resolution to do that usefully. Confirmed live 2026-10-10
that rechecking every cycle regardless of staleness was most of Turn 14's own API call volume.
  passes and feed status is CONNECTED -> CONNECTED
  passes but feed status isn't CONNECTED yet -> WAITING
  fails -> ERROR, reason = the validator's message
CompanyProviders.order_status/... is refreshed afterward to mirror whichever account is
currently the connection's default (see integrations_services._refresh_default_order_status) —
that stays a read-only summary for consumers that only know the single-value legacy shape.
"""
import datetime
import logging
import typing

from django.core.management.base import BaseCommand
from django.utils import timezone

from src import enums as src_enums
from src import models as src_models
from src.api.services import integrations as integrations_services
from src.integrations import credentials as credentials_helper

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[CHECK-COMPANY-PROVIDER-CONNECTIONS]"

# Order credentials go stale on their own (a rotated API key, a revoked security key) rarely and
# slowly -- and a real revocation surfaces immediately anyway the next time someone actually
# tries to place an order (submit_order's own live call fails loudly then). This cron's job is
# only to catch it a little sooner than that in the background, which doesn't need 5-minute
# resolution: re-checking every cycle regardless of how recently an account was already confirmed
# good was Turn 14's own token-request volume complaint (Dan Ziegler, 2026-10-10) -- each
# recheck here constructs a fresh client and authenticates, independent of the token-cache fix in
# src/integrations/clients/turn_14/rate_limit.py (that fix stops a recheck from costing a *new*
# token; this stops the recheck itself from happening needlessly often). Applies to every
# provider's order accounts, not just Turn14 -- no reason the other three registered validators
# (Keystone, Meyer, Premier, WheelPros) need 5-minute resolution either.
_ORDER_RECHECK_INTERVAL = datetime.timedelta(hours=2)


class Command(BaseCommand):
    help = (
        "Check connectivity for CompanyProviders whose initial sync hasn't completed yet, and "
        "re-check order credentials for any active account not checked within the last "
        "_ORDER_RECHECK_INTERVAL; updates status/status_name/status_reason/status_checked_at "
        "and their order_ counterparts."
    )

    def handle(self, *args: typing.Any, **options: typing.Any) -> None:
        checked = 0
        skipped = 0
        order_checked = 0
        try:
            queryset = (
                src_models.CompanyProviders.objects.filter(active=True)
                .select_related("company", "provider")
            )

            for cp in queryset:
                kind = cp.provider.kind if cp.provider else None

                if not cp.initial_sync_completed:
                    # A row can reach here with no feed credentials at all -- disconnect_provider
                    # (namespace="feed") clears credentials.feed to {} and initial_sync_completed
                    # to False without deleting the row when Ordering is still configured. There's
                    # nothing to check in that case: a validated kind would just get called with
                    # blank credentials (some validators raise cleanly, but not a guarantee), and a
                    # relay-feed kind's file-presence check is credential-blind entirely, so a file
                    # dropped before the disconnect would make it look connected/ingesting again.
                    if not credentials_helper.get_feed_credentials(cp):
                        skipped += 1
                        continue
                    try:
                        if kind in integrations_services._CONNECTION_VALIDATORS:
                            self._check_validated(cp, kind)
                            checked += 1
                        elif kind in integrations_services._RELAY_FEED_CHECK_KINDS:
                            self._check_relay_feed(cp, kind)
                            checked += 1
                        else:
                            skipped += 1
                    except Exception as e:  # noqa: BLE001 — one row's failure shouldn't kill the run
                        logger.error(
                            "{} Error checking company_provider_id={}: {}".format(_LOG_PREFIX, cp.id, e)
                        )

                if kind in integrations_services._ORDER_CONNECTION_VALIDATORS:
                    now = timezone.now()
                    accounts = [
                        a for a in cp.order_accounts.filter(active=True)
                        if a.credentials and (
                            a.order_status_checked_at is None
                            or now - a.order_status_checked_at >= _ORDER_RECHECK_INTERVAL
                        )
                    ]
                    if accounts:
                        try:
                            self._check_order_validated(cp, kind, accounts)
                            order_checked += len(accounts)
                        except Exception as e:  # noqa: BLE001
                            logger.error(
                                "{} Error checking order connection for company_provider_id={}: {}".format(
                                    _LOG_PREFIX, cp.id, e
                                )
                            )

            message = "Checked {} connection(s), {} order connection(s), skipped {} (no check available).".format(
                checked, order_checked, skipped
            )
            self.stdout.write(self.style.SUCCESS(message))
        except Exception as e:
            self.stdout.write(self.style.ERROR("Error: {}".format(str(e))))
            raise

    def _save_status(
        self,
        cp: src_models.CompanyProviders,
        status: "src_enums.CompanyProviderConnectionStatus",
        reason: typing.Optional[str],
    ) -> None:
        cp.status = status.value
        cp.status_name = status.name
        cp.status_reason = reason
        cp.status_checked_at = timezone.now()
        cp.save(
            update_fields=["status", "status_name", "status_reason", "status_checked_at", "updated_at"]
        )

    def _check_validated(self, cp: src_models.CompanyProviders, kind: int) -> None:
        validator = integrations_services._CONNECTION_VALIDATORS[kind]
        message, _code = validator(credentials_helper.get_feed_credentials(cp))
        if message:
            self._save_status(cp, src_enums.CompanyProviderConnectionStatus.FAILING, message)
        else:
            self._save_status(cp, src_enums.CompanyProviderConnectionStatus.INGESTING, None)

    def _check_relay_feed(self, cp: src_models.CompanyProviders, kind: int) -> None:
        status, reason = integrations_services._relay_feed_connection_status(cp.company, kind)
        if status is not None:
            self._save_status(cp, status, reason)

    def _check_order_validated(
        self,
        cp: src_models.CompanyProviders,
        kind: int,
        accounts: typing.List[src_models.CompanyProviderOrderAccount],
    ) -> None:
        """Validates and stores status on EVERY given account individually — each one is its own
        source of truth (see CompanyProviderOrderAccount's docstring) — then mirrors whichever
        account is currently the connection's default onto CompanyProviders.order_status/..."""
        validator = integrations_services._ORDER_CONNECTION_VALIDATORS[kind]
        feed_status_enum = integrations_services._order_gating_feed_status_enum(cp)
        any_updated = False
        for account in accounts:
            message, _code = validator(account.credentials)
            status, reason = integrations_services._resolve_order_status(
                order_validated=(message is None),
                order_val_error=message,
                feed_status_enum=feed_status_enum,
            )
            if status is not None:
                integrations_services._set_order_account_status(account, status, reason)
                any_updated = True
        if any_updated:
            integrations_services._refresh_default_order_status(cp)
