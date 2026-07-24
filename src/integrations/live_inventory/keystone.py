"""
KeystoneLiveInventoryProvider - on-demand single-item inventory refresh via Keystone's
Electronic Order Web Service CheckInventory operation (SOAP), keyed by VCPN (the same value
stored as ProviderPart.provider_external_id - see src.integrations.services.master_parts's
Keystone ingest). Distinct from the nightly catalog sync, which reads KeystoneParts (populated
from Keystone's FTP inventory feed, not this SOAP API) via
src.integrations.services.master_parts.sync_provider_inventory_from_keystone.

CheckInventory requires ORDER credentials (security_key + account_number), not the FTP feed
credentials used for the nightly sync - it's part of the same Electronic Order Web Service as
shipping quotes/order placement, so a company needs Keystone ordering configured for this to
work, independent of whether their catalog feed is connected.

Writes the raw KeystoneParts row's warehouse qty columns first (best-effort - if no matching
row exists yet, e.g. before the item's first catalog sync, this step is skipped and only
ProviderPartInventory is updated), then builds warehouse_availability the same way the nightly
sync does (see master_parts._keystone_warehouse_availability) to upsert ProviderPartInventory.
"""
import logging
import typing

from django.utils import timezone

from src import enums as src_enums
from src import models as src_models
from src.integrations import credentials as credentials_helper
from src.integrations.clients.keystone import exceptions as keystone_client_exceptions
from src.integrations.clients.keystone.order_client import KeystoneOrderApiClient
from src.integrations.live_inventory import base
from src.integrations.live_inventory import exceptions as live_inventory_exceptions
from src.integrations.services import master_parts as master_parts_services

logger = logging.getLogger(__name__)
_LOG_PREFIX = "[KEYSTONE-LIVE-INVENTORY]"

# CheckInventory's raw string uses full uppercase warehouse names (confirmed against Keystone's
# own SDK doc sample: "EAST,0,MIDWEST,0,CALIFORNIA,0,SOUTHEAST,0,TEXAS,0,GREAT LAKES,0,PACIFIC
# NORTHWEST,0") - note "PACIFIC NORTHWEST" here vs. the "Pacific NW" display name used elsewhere
# (master_parts.KEYSTONE_WAREHOUSE_DISPLAY_NAMES), hence this separate map to our field names.
_CHECK_INVENTORY_WAREHOUSE_FIELD_MAP = {
    "EAST": "east_qty",
    "MIDWEST": "midwest_qty",
    "CALIFORNIA": "california_qty",
    "SOUTHEAST": "southeast_qty",
    "TEXAS": "texas_qty",
    "GREAT LAKES": "great_lakes_qty",
    "PACIFIC NORTHWEST": "pacific_nw_qty",
    "FLORIDA": "florida_qty",
}


def _parse_check_inventory_result(raw: str) -> typing.Dict[str, int]:
    """
    Parses CheckInventory's raw string into {warehouse_qty_field: qty}. Raises
    LiveInventoryNotFoundError for Keystone's documented plain-text business errors ("Invalid
    part number[.:] ...", "Part is blocked."), or LiveInventoryTransportError if the string
    doesn't match either shape (unexpected API change).
    """
    text = (raw or "").strip()
    lower = text.lower()
    if lower.startswith("invalid part number") or "part is blocked" in lower:
        raise live_inventory_exceptions.LiveInventoryNotFoundError("Keystone: {}".format(text))

    tokens = [t.strip() for t in text.split(",")] if text else []
    if not tokens or len(tokens) % 2 != 0:
        raise live_inventory_exceptions.LiveInventoryTransportError(
            "Unexpected CheckInventory response from Keystone: {!r}".format(text)
        )

    result: typing.Dict[str, int] = {}
    for i in range(0, len(tokens), 2):
        field = _CHECK_INVENTORY_WAREHOUSE_FIELD_MAP.get(tokens[i].upper())
        if not field:
            continue
        try:
            result[field] = int(tokens[i + 1])
        except ValueError:
            continue
    return result


class KeystoneLiveInventoryProvider(base.LiveInventoryProvider):
    provider_kind = src_enums.BrandProviderKind.KEYSTONE.value

    def __init__(self, company_provider: src_models.CompanyProviders) -> None:
        base.LiveInventoryProvider.__init__(self, company_provider)
        credentials = credentials_helper.get_order_credentials(company_provider)
        self._client = KeystoneOrderApiClient(credentials=credentials)

    def refresh(self, provider_part: src_models.ProviderPart) -> src_models.ProviderPartInventory:
        vcpn = provider_part.provider_external_id

        try:
            raw_result = self._client.check_inventory(vcpn)
        except keystone_client_exceptions.KeystoneException as e:
            logger.error(
                "{} Keystone API error refreshing inventory for provider_part_id={} vcpn={}: {}".format(
                    _LOG_PREFIX, provider_part.id, vcpn, str(e)
                )
            )
            raise live_inventory_exceptions.LiveInventoryTransportError(str(e)) from e

        qty_by_field = _parse_check_inventory_result(raw_result)
        now = timezone.now()
        self._save_raw_inventory(vcpn, qty_by_field, now)
        return self._sync_provider_part_inventory(provider_part, qty_by_field, now)

    def _save_raw_inventory(self, vcpn: str, qty_by_field: typing.Dict[str, int], now: typing.Any) -> None:
        keystone_part = src_models.KeystoneParts.objects.filter(vcpn=vcpn).first()
        if keystone_part is None:
            # Nothing to mirror into yet (item hasn't gone through a catalog sync as this
            # vcpn) - ProviderPartInventory still gets updated below, just without a raw-table
            # side effect.
            logger.info(
                "{} No KeystoneParts row for vcpn={} - skipping raw-table update.".format(_LOG_PREFIX, vcpn)
            )
            return

        update_fields = ["updated_at"]
        for field, qty in qty_by_field.items():
            setattr(keystone_part, field, qty)
            update_fields.append(field)
        keystone_part.total_qty = sum(qty_by_field.values())
        update_fields.append("total_qty")
        keystone_part.save(update_fields=update_fields)

    def _sync_provider_part_inventory(
        self,
        provider_part: src_models.ProviderPart,
        qty_by_field: typing.Dict[str, int],
        now: typing.Any,
    ) -> src_models.ProviderPartInventory:
        row = {field: qty_by_field.get(field) for field in master_parts_services.KEYSTONE_WAREHOUSE_QTY_FIELDS}
        warehouse_availability = master_parts_services._keystone_warehouse_availability(row)
        inv_obj, _ = src_models.ProviderPartInventory.objects.update_or_create(
            provider_part=provider_part,
            defaults={
                "warehouse_total_qty": sum(qty_by_field.values()),
                "manufacturer_inventory": None,
                "manufacturer_esd": None,
                "warehouse_availability": warehouse_availability,
                "last_synced_at": now,
            },
        )
        return inv_obj
