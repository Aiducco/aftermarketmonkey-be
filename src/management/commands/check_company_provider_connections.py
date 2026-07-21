"""
Periodic connectivity check for CompanyProviders connections. Scheduled via cron (see
command_runner.sh), every few minutes.

Feed status — only rows whose initial pricing sync hasn't completed yet:
  - Kinds with a live credential validator (Turn 14, Keystone, Wheel Pros, Premier, Rough
    Country — same registry used at connect/update time): re-run that check against the
    stored credentials.
      passes -> INGESTING (connectivity fine, sync just hasn't finished yet)
      fails  -> FAILING, reason = the validator's message
  - Relay-provisioned kinds we have a known expected filename for (Meyer, A-Tech): via
    integrations_services._relay_feed_connection_status — same helper connect_provider/
    update_connection use to set the initial status, so there's one source of truth for
    what "file arrived" means.
  - Everything else (no ingest client built yet — CTP, Crown, DIX, Wheel Group, and the rest
    of the catalog-only distributors): left untouched, nothing to check against.

  Rows where initial_sync_completed is already True are out of scope for this part — those
  are set to CONNECTED once, directly, in integration_pricing_sync_jobs when the first sync
  completes, and aren't re-checked here.

Order status — every active row with order credentials configured and a registered order
validator (see integrations_services._ORDER_CONNECTION_VALIDATORS), regardless of
initial_sync_completed: order credentials can go stale on their own (a rotated API key, a
revoked security key) independently of feed sync state, so they're re-checked continuously
rather than only while the feed is still mid-sync.
  passes and feed status is CONNECTED -> CONNECTED
  passes but feed status isn't CONNECTED yet -> WAITING
  fails -> ERROR, reason = the validator's message
"""
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


class Command(BaseCommand):
    help = (
        "Check connectivity for CompanyProviders whose initial sync hasn't completed yet, and "
        "re-check order credentials for any active row that has them configured; updates "
        "status/status_name/status_reason/status_checked_at and their order_ counterparts."
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
                    order_creds = credentials_helper.get_order_credentials(cp)
                    if order_creds:
                        try:
                            self._check_order_validated(cp, kind)
                            order_checked += 1
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

    def _save_order_status(
        self,
        cp: src_models.CompanyProviders,
        status: "src_enums.CompanyProviderOrderConnectionStatus",
        reason: typing.Optional[str],
    ) -> None:
        cp.order_status = status.value
        cp.order_status_name = status.name
        cp.order_status_reason = reason
        cp.order_status_checked_at = timezone.now()
        cp.save(
            update_fields=[
                "order_status", "order_status_name", "order_status_reason",
                "order_status_checked_at", "updated_at",
            ]
        )

    def _check_order_validated(self, cp: src_models.CompanyProviders, kind: int) -> None:
        validator = integrations_services._ORDER_CONNECTION_VALIDATORS[kind]
        message, _code = validator(credentials_helper.get_order_credentials(cp))
        status, reason = integrations_services._resolve_order_status(
            order_validated=(message is None),
            order_val_error=message,
            feed_status_enum=(
                src_enums.CompanyProviderConnectionStatus.CONNECTED
                if cp.status == src_enums.CompanyProviderConnectionStatus.CONNECTED.value
                else None
            ),
        )
        if status is not None:
            self._save_order_status(cp, status, reason)
