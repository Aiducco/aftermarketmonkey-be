"""
MeyerLiveInventoryProvider - on-demand single-item inventory refresh via Meyer's Order API
GET /ItemInformation, keyed by Meyer's ItemNumber (the same value stored as
ProviderPart.provider_external_id / MeyerParts.meyer_part - see
src.integrations.services.master_parts's Meyer ingest). Distinct from the nightly catalog sync,
which reads MeyerParts (populated from Meyer's SFTP inventory/pricing files, not this REST API)
via src.integrations.services.master_parts.sync_provider_inventory_from_meyer.

ItemInformation requires ORDER credentials (api_key + customer_number), not the SFTP feed
credentials used for the nightly sync - it's the same Order API used for shipping quotes/order
placement (already relied on for live pricing/Prop 65 by MeyerOrderAdapter._get_item_info), so
a company needs Meyer ordering configured for this to work, independent of whether their
catalog feed is connected.

ItemInformation only returns a single total QtyAvailable - no per-warehouse breakdown, no
manufacturer/LTL/stocking-flag data (those come only from Meyer's Inventory SFTP file). So a
refresh only ever updates MeyerParts.available_qty; the other warehouse_availability fields are
carried over unchanged from whatever the last catalog sync wrote, keeping this refresh narrow
rather than pretending to update columns for which the Order API has no data.
"""
import logging
import typing

from django.conf import settings
from django.utils import timezone

from src import enums as src_enums
from src import models as src_models
from src.integrations import credentials as credentials_helper
from src.integrations.clients.meyer import exceptions as meyer_client_exceptions
from src.integrations.clients.meyer.order_client import MeyerOrderApiClient
from src.integrations.live_inventory import base
from src.integrations.live_inventory import exceptions as live_inventory_exceptions
from src.integrations.services import master_parts as master_parts_services

logger = logging.getLogger(__name__)
_LOG_PREFIX = "[MEYER-LIVE-INVENTORY]"


def _safe_int(value: typing.Any) -> typing.Optional[int]:
    if value is None:
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


class MeyerLiveInventoryProvider(base.LiveInventoryProvider):
    provider_kind = src_enums.BrandProviderKind.MEYER.value

    def __init__(self, company_provider: src_models.CompanyProviders) -> None:
        base.LiveInventoryProvider.__init__(self, company_provider)
        environment = getattr(settings, "MEYER_ORDER_ENVIRONMENT", "testing")
        self._client = MeyerOrderApiClient(
            credentials=credentials_helper.get_order_credentials(company_provider),
            environment=environment,
        )

    def refresh(self, provider_part: src_models.ProviderPart) -> src_models.ProviderPartInventory:
        item_number = (provider_part.provider_external_id or "").strip()

        try:
            items = self._client.get_item_information([item_number], self._client.customer_number)
        except meyer_client_exceptions.MeyerOrderValidationError as e:
            raise live_inventory_exceptions.LiveInventoryNotFoundError("Meyer: {}".format(str(e))) from e
        except meyer_client_exceptions.MeyerException as e:
            logger.error(
                "{} Meyer API error refreshing inventory for provider_part_id={} item_number={}: {}".format(
                    _LOG_PREFIX, provider_part.id, item_number, str(e)
                )
            )
            raise live_inventory_exceptions.LiveInventoryTransportError(str(e)) from e

        match = next(
            (item for item in items if (item.get("ItemNumber") or "").strip() == item_number), None
        )
        if match is None:
            raise live_inventory_exceptions.LiveInventoryNotFoundError(
                "Meyer has no item information for {}.".format(item_number)
            )

        qty_available = _safe_int(match.get("QtyAvailable"))
        now = timezone.now()
        row = self._refresh_raw_inventory(item_number, qty_available, now)
        return self._sync_provider_part_inventory(provider_part, row, now)

    def _refresh_raw_inventory(
        self, item_number: str, qty_available: typing.Optional[int], now: typing.Any
    ) -> typing.Dict:
        """Updates MeyerParts.available_qty (best-effort - skipped if no matching row exists
        yet) and returns a row dict merging the fresh qty with whatever else the last catalog
        sync wrote, ready for master_parts._meyer_warehouse_availability."""
        existing = src_models.MeyerParts.objects.filter(meyer_part=item_number).first()
        if existing is None:
            logger.info(
                "{} No MeyerParts row for meyer_part={} - skipping raw-table update.".format(
                    _LOG_PREFIX, item_number
                )
            )
            return {
                "available_qty": qty_available,
                "mfg_qty_available": None,
                "inventory_ltl": None,
                "is_stocking": False,
                "is_special_order": False,
            }

        existing.available_qty = qty_available
        existing.save(update_fields=["available_qty", "updated_at"])
        return {
            "available_qty": qty_available,
            "mfg_qty_available": existing.mfg_qty_available,
            "inventory_ltl": existing.inventory_ltl,
            "is_stocking": existing.is_stocking,
            "is_special_order": existing.is_special_order,
        }

    def _sync_provider_part_inventory(
        self,
        provider_part: src_models.ProviderPart,
        row: typing.Dict,
        now: typing.Any,
    ) -> src_models.ProviderPartInventory:
        warehouse_availability = master_parts_services._meyer_warehouse_availability(row)
        inv_obj, _ = src_models.ProviderPartInventory.objects.update_or_create(
            provider_part=provider_part,
            defaults={
                "warehouse_total_qty": row.get("available_qty") or 0,
                "manufacturer_inventory": row.get("mfg_qty_available"),
                "manufacturer_esd": None,
                "warehouse_availability": warehouse_availability,
                "last_synced_at": now,
            },
        )
        return inv_obj
