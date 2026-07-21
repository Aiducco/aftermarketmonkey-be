"""
Periodic connectivity check for CompanyProviders connections whose initial pricing sync
hasn't completed yet. Scheduled via cron (see command_runner.sh), every few minutes.

For each such row:
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

Rows where initial_sync_completed is already True are out of scope entirely — those are set
to CONNECTED once, directly, in integration_pricing_sync_jobs when the first sync completes,
and aren't re-checked here.
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
        "Check connectivity for CompanyProviders whose initial sync hasn't completed yet; "
        "updates status/status_name/status_reason/status_checked_at."
    )

    def handle(self, *args: typing.Any, **options: typing.Any) -> None:
        checked = 0
        skipped = 0
        try:
            queryset = (
                src_models.CompanyProviders.objects.filter(
                    initial_sync_completed=False,
                    active=True,
                )
                .select_related("company", "provider")
            )

            for cp in queryset:
                kind = cp.provider.kind if cp.provider else None
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

            message = "Checked {} connection(s), skipped {} (no check available).".format(
                checked, skipped
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
