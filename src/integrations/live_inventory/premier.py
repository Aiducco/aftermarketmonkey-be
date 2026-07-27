"""
PremierLiveInventoryProvider - on-demand single-item inventory refresh via Premier (APG
Wholesale)'s Order API GET /inventory. Keyed by Premier's own itemNumber - NOT
ProviderPart.provider_external_id directly, which for Premier is a composite
"{premier_brand_id}_{premier_part_number}" key (see master_parts._premier_provider_external_id)
needed only for our own DB uniqueness, since the same premier_part_number can recur across
different Premier brands. Resolves the real item number the same way
PremierOrderAdapter.get_shipping_quote already does, by reusing orders.premier._premier_item_number.

GET /inventory requires ORDER credentials (api_key), not the FTP feed credentials used for the
nightly sync - same Order API used for shipping quotes/order placement.

Only refreshes the three warehouses this codebase already models (NV, KY, WA - see
src.constants.PREMIER_WAREHOUSE_QTY_FIELD_TO_LOCATION_LABEL) plus the USA total
(usa_item_availability, summed across every "-US" warehouse code in the live response, not just
the three tracked ones, to match what the nightly sync's own total represents). mfg_qty
(dropship/manufacturer stock) has no confirmed live-API equivalent, so it's preserved from
whatever the last catalog sync wrote rather than guessed at.
"""
import logging
import typing

from django.conf import settings
from django.utils import timezone

from src import enums as src_enums
from src import models as src_models
from src.integrations import credentials as credentials_helper
from src.integrations.clients.premier import exceptions as premier_client_exceptions
from src.integrations.clients.premier.order_client import PremierOrderApiClient
from src.integrations.live_inventory import base
from src.integrations.live_inventory import exceptions as live_inventory_exceptions
from src.integrations.orders.premier import _premier_item_number
from src.integrations.services import master_parts as master_parts_services

logger = logging.getLogger(__name__)
_LOG_PREFIX = "[PREMIER-LIVE-INVENTORY]"

# GET /inventory's warehouseCode -> our PremierParts qty field (only the US warehouses this
# codebase separately tracks - see src.constants.PREMIER_WAREHOUSE_QTY_FIELD_TO_LOCATION_LABEL).
_WAREHOUSE_CODE_FIELD_MAP = {
    "NV-1-US": "nv_qty",
    "KY-1-US": "ky_qty",
    "WA-1-US": "wa_qty",
}


def _parse_brand_id(provider_external_id: str) -> typing.Optional[int]:
    """The brand id prefix of Premier's composite provider_external_id is always purely
    numeric (see orders.premier._premier_item_number) - used to key back into PremierParts,
    which is unique on (premier_part_number, brand)."""
    brand_id_str, _, _ = (provider_external_id or "").partition("_")
    try:
        return int(brand_id_str)
    except (TypeError, ValueError):
        return None


class PremierLiveInventoryProvider(base.LiveInventoryProvider):
    provider_kind = src_enums.BrandProviderKind.PREMIER_PERFORMANCE.value

    def __init__(self, company_provider: src_models.CompanyProviders) -> None:
        base.LiveInventoryProvider.__init__(self, company_provider)
        environment = getattr(settings, "PREMIER_ORDER_ENVIRONMENT", "production")
        self._client = PremierOrderApiClient(
            credentials=credentials_helper.get_order_credentials(company_provider),
            environment=environment,
        )

    def refresh(self, provider_part: src_models.ProviderPart) -> src_models.ProviderPartInventory:
        item_number = _premier_item_number(provider_part)

        try:
            results = self._client.get_inventory([item_number])
        except premier_client_exceptions.PremierException as e:
            logger.error(
                "{} Premier API error refreshing inventory for provider_part_id={} item_number={}: {}".format(
                    _LOG_PREFIX, provider_part.id, item_number, str(e)
                )
            )
            raise live_inventory_exceptions.LiveInventoryTransportError(str(e)) from e

        match = next(
            (r for r in results if (r.get("itemNumber") or "").strip() == item_number), None
        )
        if match is None:
            raise live_inventory_exceptions.LiveInventoryNotFoundError(
                "Premier has no inventory record for item {}.".format(item_number)
            )

        qty_by_field: typing.Dict[str, int] = {}
        us_total = 0
        for w in match.get("inventory") or []:
            code = (w.get("warehouseCode") or "").upper()
            try:
                qty = int(w.get("quantityAvailable") or 0)
            except (TypeError, ValueError):
                qty = 0
            if code.endswith("-US"):
                us_total += qty
            field = _WAREHOUSE_CODE_FIELD_MAP.get(code)
            if field:
                qty_by_field[field] = qty

        now = timezone.now()
        row = self._refresh_raw_inventory(
            provider_part.provider_external_id, item_number, qty_by_field, us_total
        )
        return self._sync_provider_part_inventory(provider_part, row, now)

    def _refresh_raw_inventory(
        self,
        provider_external_id: str,
        item_number: str,
        qty_by_field: typing.Dict[str, int],
        us_total: int,
    ) -> typing.Dict:
        brand_id = _parse_brand_id(provider_external_id)
        premier_part = None
        if brand_id is not None:
            premier_part = src_models.PremierParts.objects.filter(
                brand_id=brand_id, premier_part_number=item_number
            ).first()

        if premier_part is None:
            logger.info(
                "{} No PremierParts row for brand_id={} premier_part_number={} - skipping "
                "raw-table update.".format(_LOG_PREFIX, brand_id, item_number)
            )
            row = {"nv_qty": None, "ky_qty": None, "wa_qty": None, "mfg_qty": None}
            row.update(qty_by_field)
            row["usa_item_availability"] = us_total
            return row

        row = {
            "nv_qty": premier_part.nv_qty,
            "ky_qty": premier_part.ky_qty,
            "wa_qty": premier_part.wa_qty,
            "mfg_qty": premier_part.mfg_qty,
        }
        row.update(qty_by_field)
        row["usa_item_availability"] = us_total

        update_fields = ["updated_at", "usa_item_availability"]
        for field, qty in qty_by_field.items():
            setattr(premier_part, field, qty)
            update_fields.append(field)
        premier_part.usa_item_availability = us_total
        premier_part.save(update_fields=update_fields)
        return row

    def _sync_provider_part_inventory(
        self,
        provider_part: src_models.ProviderPart,
        row: typing.Dict,
        now: typing.Any,
    ) -> src_models.ProviderPartInventory:
        warehouse_availability = master_parts_services._premier_warehouse_availability(row)
        inv_obj, _ = src_models.ProviderPartInventory.objects.update_or_create(
            provider_part=provider_part,
            defaults={
                "warehouse_total_qty": row.get("usa_item_availability") or 0,
                "manufacturer_inventory": row.get("mfg_qty"),
                "manufacturer_esd": None,
                "warehouse_availability": warehouse_availability,
                "last_synced_at": now,
            },
        )
        return inv_obj
