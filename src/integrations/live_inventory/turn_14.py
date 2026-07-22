"""
Turn14LiveInventoryProvider - on-demand single-item inventory refresh via Turn14's
GET /v1/inventory/item/{item_id}, distinct from the brand-wide paginated pull used by the
scheduled catalog sync (src.integrations.services.turn_14.fetch_and_save_all_turn_14_brand_inventory).

Writes the raw Turn14BrandInventory row first (so the table stays consistent for whatever the
next scheduled bulk sync does), then reuses the same parsing/mapping helpers the bulk sync
uses (src.integrations.services.master_parts) to upsert ProviderPartInventory.
"""
import logging
import typing

from django.utils import timezone

from src import enums as src_enums
from src import models as src_models
from src.integrations import credentials as credentials_helper
from src.integrations.clients.turn_14 import client as turn_14_client
from src.integrations.clients.turn_14 import exceptions as turn14_client_exceptions
from src.integrations.live_inventory import base
from src.integrations.live_inventory import exceptions as live_inventory_exceptions
from src.integrations.services import master_parts as master_parts_services

logger = logging.getLogger(__name__)
_LOG_PREFIX = "[TURN14-LIVE-INVENTORY]"


class Turn14LiveInventoryProvider(base.LiveInventoryProvider):
    provider_kind = src_enums.BrandProviderKind.TURN_14.value

    def __init__(self, company_provider: src_models.CompanyProviders) -> None:
        base.LiveInventoryProvider.__init__(self, company_provider)
        credentials = credentials_helper.get_feed_credentials(company_provider)
        self._client = turn_14_client.Turn14ApiClient(credentials=credentials)

    def refresh(self, provider_part: src_models.ProviderPart) -> src_models.ProviderPartInventory:
        external_id = provider_part.provider_external_id
        try:
            raw_item = self._client.get_inventory_item(external_id)
        except turn14_client_exceptions.Turn14APIException as e:
            logger.error(
                "{} Turn14 API error refreshing inventory for provider_part_id={} external_id={}: {}".format(
                    _LOG_PREFIX, provider_part.id, external_id, str(e)
                )
            )
            raise live_inventory_exceptions.LiveInventoryTransportError(str(e)) from e

        if raw_item is None:
            raise live_inventory_exceptions.LiveInventoryNotFoundError(
                "Turn14 has no inventory record for item {}.".format(external_id)
            )

        now = timezone.now()
        raw_row = self._save_raw_inventory(external_id, raw_item, now)
        return self._sync_provider_part_inventory(provider_part, raw_row, now)

    def _save_raw_inventory(
        self, external_id: str, raw_item: typing.Dict, now: typing.Any
    ) -> src_models.Turn14BrandInventory:
        attributes = raw_item.get("attributes", {})
        inventory = attributes.get("inventory", {})
        manufacturer = attributes.get("manufacturer", {})

        total_inventory = 0
        if isinstance(inventory, dict):
            for qty in inventory.values():
                if isinstance(qty, (int, float)):
                    total_inventory += int(qty)
        if isinstance(manufacturer, dict) and isinstance(manufacturer.get("stock"), (int, float)):
            total_inventory += int(manufacturer["stock"])

        # Preserve the existing brand FK on refresh - a single-item lookup has no brand context
        # of its own, and this table's brand scoping is only used by the bulk sync's partitioning.
        existing = src_models.Turn14BrandInventory.objects.filter(external_id=external_id).first()
        raw_row, _ = src_models.Turn14BrandInventory.objects.update_or_create(
            external_id=external_id,
            defaults={
                "brand": existing.brand if existing else None,
                "type": raw_item.get("type"),
                "inventory": inventory,
                "manufacturer": manufacturer,
                "eta": attributes.get("eta"),
                "relationships": raw_item.get("relationships"),
                "total_inventory": total_inventory if total_inventory > 0 else None,
                "updated_at": now,
            },
        )
        return raw_row

    def _sync_provider_part_inventory(
        self,
        provider_part: src_models.ProviderPart,
        raw_row: src_models.Turn14BrandInventory,
        now: typing.Any,
    ) -> src_models.ProviderPartInventory:
        mfr_qty, mfr_esd, wh_total = master_parts_services._parse_turn14_inventory(
            {"inventory": raw_row.inventory, "manufacturer": raw_row.manufacturer}
        )
        location_map = master_parts_services._get_turn14_location_map()
        warehouse_availability = master_parts_services._map_turn14_inventory_to_location_names(
            raw_row.inventory, location_map
        )
        inv_obj, _ = src_models.ProviderPartInventory.objects.update_or_create(
            provider_part=provider_part,
            defaults={
                "warehouse_total_qty": wh_total,
                "manufacturer_inventory": mfr_qty,
                "manufacturer_esd": mfr_esd,
                "warehouse_availability": warehouse_availability,
                "last_synced_at": now,
            },
        )
        return inv_obj
