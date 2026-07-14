"""
Periodic connectivity check for CompanyProviders connections whose initial pricing sync
hasn't completed yet. Scheduled via cron (see command_runner.sh), every few minutes.

For each such row:
  - Kinds with a live credential validator (Turn 14, Keystone, Wheel Pros, Premier, Rough
    Country — same registry used at connect/update time): re-run that check against the
    stored credentials.
      passes -> INGESTING (connectivity fine, sync just hasn't finished yet)
      fails  -> FAILING, reason = the validator's message
  - Relay-provisioned kinds we have a known expected filename for (Meyer, A-Tech): SFTP into
    our own relay with the company's relay credentials and stat() the expected file(s).
      relay login fails -> FAILING (our own relay account broken — should not normally happen)
      file(s) present    -> INGESTING (data has landed, waiting on our sync job to process it)
      file(s) missing    -> WAITING (waiting on the distributor's rep to send it)
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
from src.audit import scheduled_tasks as audit_scheduled_tasks
from src.integrations.clients.atech import client as atech_client
from src.integrations.clients.atech import exceptions as atech_exceptions
from src.integrations.clients.meyer import client as meyer_client
from src.integrations.clients.meyer import exceptions as meyer_exceptions

logger = logging.getLogger(__name__)

_TASK_NAME = "check_company_provider_connections"
_LOG_PREFIX = "[CHECK-COMPANY-PROVIDER-CONNECTIONS]"

# Relay-provisioned kinds we have a known expected filename for — see module docstring.
_RELAY_FEED_KINDS = {
    src_enums.BrandProviderKind.MEYER.value,
    src_enums.BrandProviderKind.ATECH.value,
}


class Command(BaseCommand):
    help = (
        "Check connectivity for CompanyProviders whose initial sync hasn't completed yet; "
        "updates status/status_name/status_reason/status_checked_at."
    )

    def handle(self, *args: typing.Any, **options: typing.Any) -> None:
        audit_scheduled_tasks.cleanup_stale_started_executions([_TASK_NAME])
        execution = audit_scheduled_tasks.start_scheduled_task_execution(_TASK_NAME)

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
                    elif kind in _RELAY_FEED_KINDS:
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
            audit_scheduled_tasks.mark_scheduled_task_completed(execution, message=message)
            self.stdout.write(self.style.SUCCESS(message))
        except Exception as e:
            audit_scheduled_tasks.mark_scheduled_task_failed(execution, error_message=str(e))
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
        message, _code = validator(cp.credentials or {})
        if message:
            self._save_status(cp, src_enums.CompanyProviderConnectionStatus.FAILING, message)
        else:
            self._save_status(cp, src_enums.CompanyProviderConnectionStatus.INGESTING, None)

    def _check_relay_feed(self, cp: src_models.CompanyProviders, kind: int) -> None:
        company = cp.company
        if not company or not company.relay_sftp_username or not company.relay_sftp_password:
            self._save_status(
                cp,
                src_enums.CompanyProviderConnectionStatus.WAITING,
                "Your relay SFTP account is still being created.",
            )
            return

        creds = {"sftp_user": company.relay_sftp_username, "sftp_password": company.relay_sftp_password}
        try:
            if kind == src_enums.BrandProviderKind.MEYER.value:
                client = meyer_client.MeyerSFTPClient(credentials=creds)
            else:
                client = atech_client.AtechSFTPClient(credentials=creds)
            present = client.feed_present()
        except (meyer_exceptions.MeyerException, atech_exceptions.AtechException, ValueError) as e:
            self._save_status(
                cp,
                src_enums.CompanyProviderConnectionStatus.FAILING,
                "Could not reach our relay to check for your file: {}".format(e),
            )
            return

        if present:
            self._save_status(
                cp,
                src_enums.CompanyProviderConnectionStatus.INGESTING,
                "File received — waiting for it to be processed.",
            )
        else:
            self._save_status(
                cp,
                src_enums.CompanyProviderConnectionStatus.WAITING,
                "Waiting for your first file to arrive on our relay.",
            )
