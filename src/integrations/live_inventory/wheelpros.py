"""
WheelProsLiveInventoryProvider - on-demand single-item inventory refresh via Wheel Pros' Orders
API POST /inventory/v1/search (the same Inventory Search endpoint WheelProsOrderAdapter.
get_shipping_quote already uses for cart-time availability - see
src.integrations.clients.wheelpros.order_client.WheelProsOrderApiClient.search_inventory).
Keyed by Wheel Pros' real part number/SKU - NOT ProviderPart.provider_external_id directly,
which for Wheel Pros is a composite "{wp_brand_id}_{part_number}" key (see
master_parts._wheelpros_provider_external_id) needed only for our own DB uniqueness, since the
same part_number can recur across different Wheel Pros brands. Resolves the real item number the
same way WheelProsOrderAdapter.get_shipping_quote/submit_order already do, by reusing
orders.wheelpros._wheelpros_item_number.

POST /inventory/v1/search requires ORDER credentials (Product Data Portal username/password),
not the SFTP feed credentials used for the nightly sync - same Order API used for shipping
quotes/order placement (already relied on by WheelProsOrderAdapter). It's marked "Internal Use
Only" in Wheel Pros' own docs - some dealer accounts may not be granted access to it even though
their Orders API access works fine (see that adapter's own module docstring, point 1); that
surfaces here as a plain LiveInventoryTransportError, same as any other transport failure, since
this module's contract has no dedicated "not authorized for this API" exception type.

Only country_codes=["US"] is queried - every warehouse this codebase tracks (see
master_parts._WHEELPROS_WAREHOUSE_RAW) is a US location; a Canada/AU/GB/BE-only account would
need this widened, but none is modeled today.
"""
import logging
import typing

from django.conf import settings
from django.utils import timezone

from src import enums as src_enums
from src import models as src_models
from src.integrations import credentials as credentials_helper
from src.integrations.clients.wheelpros import exceptions as wheelpros_client_exceptions
from src.integrations.clients.wheelpros.order_client import WheelProsOrderApiClient
from src.integrations.live_inventory import base
from src.integrations.live_inventory import exceptions as live_inventory_exceptions
from src.integrations.orders.wheelpros import _wheelpros_item_number
from src.integrations.services import master_parts as master_parts_services

logger = logging.getLogger(__name__)
_LOG_PREFIX = "[WHEELPROS-LIVE-INVENTORY]"


def _parse_brand_id(provider_external_id: str) -> typing.Optional[int]:
    """The brand id prefix of Wheel Pros' composite provider_external_id is always purely
    numeric (see orders.wheelpros._wheelpros_item_number / master_parts.
    _wheelpros_provider_external_id) - used to key back into WheelProsPart, which is unique on
    (brand, part_number)."""
    brand_id_str, _, _ = (provider_external_id or "").partition("_")
    try:
        return int(brand_id_str)
    except (TypeError, ValueError):
        return None


class WheelProsLiveInventoryProvider(base.LiveInventoryProvider):
    provider_kind = src_enums.BrandProviderKind.WHEELPROS.value

    def __init__(self, company_provider: src_models.CompanyProviders) -> None:
        base.LiveInventoryProvider.__init__(self, company_provider)
        environment = getattr(settings, "WHEELPROS_ORDER_ENVIRONMENT", "production")
        self._client = WheelProsOrderApiClient(
            credentials=credentials_helper.get_order_credentials(company_provider),
            environment=environment,
        )

    def refresh(self, provider_part: src_models.ProviderPart) -> src_models.ProviderPartInventory:
        item_number = _wheelpros_item_number(provider_part)

        try:
            response = self._client.search_inventory(skus=[item_number], country_codes=["US"])
        except wheelpros_client_exceptions.WheelProsOrderPermissionError as e:
            raise live_inventory_exceptions.LiveInventoryTransportError(
                "Wheel Pros denied access to the Inventory API for this account ({}).".format(e)
            ) from e
        except wheelpros_client_exceptions.WheelProsException as e:
            logger.error(
                "{} Wheel Pros API error refreshing inventory for provider_part_id={} item_number={}: {}".format(
                    _LOG_PREFIX, provider_part.id, item_number, str(e)
                )
            )
            raise live_inventory_exceptions.LiveInventoryTransportError(str(e)) from e

        skus = response.get("skus") or []
        match = next((s for s in skus if (s.get("sku") or "").strip() == item_number), None)
        if match is None:
            raise live_inventory_exceptions.LiveInventoryNotFoundError(
                "Wheel Pros has no inventory record for item {}.".format(item_number)
            )

        wh_avail: typing.Dict[str, int] = {}
        total_qoh = 0
        for w in match.get("warehouses") or []:
            code = str(w.get("warehouseId") or "").strip()
            try:
                qty = int(w.get("atp") or 0)
            except (TypeError, ValueError):
                qty = 0
            total_qoh += qty
            if code:
                wh_avail[code] = qty

        now = timezone.now()
        row = self._refresh_raw_inventory(provider_part.provider_external_id, item_number, wh_avail, total_qoh)
        return self._sync_provider_part_inventory(provider_part, row, now)

    def _refresh_raw_inventory(
        self,
        provider_external_id: str,
        item_number: str,
        wh_avail: typing.Dict[str, int],
        total_qoh: int,
    ) -> typing.Dict:
        """Updates WheelProsPart.total_qoh/warehouse_availability (best-effort - skipped if no
        matching row exists yet) and returns a row dict in the same raw numeric-warehouse-code
        shape sync_provider_inventory_from_wheelpros already reads off WheelProsPart, ready for
        master_parts._map_wheelpros_warehouse_availability."""
        brand_id = _parse_brand_id(provider_external_id)
        wp_part = None
        if brand_id is not None:
            wp_part = src_models.WheelProsPart.objects.filter(brand_id=brand_id, part_number=item_number).first()

        if wp_part is None:
            logger.info(
                "{} No WheelProsPart row for brand_id={} part_number={} - skipping raw-table "
                "update.".format(_LOG_PREFIX, brand_id, item_number)
            )
            return {"total_qoh": total_qoh, "warehouse_availability": wh_avail or None}

        wp_part.total_qoh = total_qoh
        wp_part.warehouse_availability = wh_avail or None
        wp_part.save(update_fields=["total_qoh", "warehouse_availability", "updated_at"])
        return {"total_qoh": total_qoh, "warehouse_availability": wh_avail or None}

    def _sync_provider_part_inventory(
        self,
        provider_part: src_models.ProviderPart,
        row: typing.Dict,
        now: typing.Any,
    ) -> src_models.ProviderPartInventory:
        warehouse_availability = master_parts_services._map_wheelpros_warehouse_availability(
            row.get("warehouse_availability")
        )
        inv_obj, _ = src_models.ProviderPartInventory.objects.update_or_create(
            provider_part=provider_part,
            defaults={
                "warehouse_total_qty": row.get("total_qoh") or 0,
                # Wheel Pros' Inventory Search response has no manufacturer/dropship figure of
                # its own (atp/backOrderQty/inTransitQty/poQty/poAvQty are all warehouse-stock
                # concepts) - left unset here rather than guessed, same as manufacturer_esd.
                "manufacturer_inventory": None,
                "manufacturer_esd": None,
                "warehouse_availability": warehouse_availability,
                "last_synced_at": now,
            },
        )
        return inv_obj
