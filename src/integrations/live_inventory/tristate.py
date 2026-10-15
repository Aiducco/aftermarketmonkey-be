"""
TriStateLiveInventoryProvider -- on-demand single-item inventory refresh via Tri-State's real
get_inventory_and_pricing endpoint, which IS itself a single/few-item live lookup (not a bulk
feed) -- so "live refresh" here is just calling the same client used for the scheduled sync,
one item at a time. Uses FEED credentials (app_id/api_key), unlike Keystone/Meyer which need
separate order credentials for live inventory -- Tri-State's real API has no such split.
"""
import logging

from django.utils import timezone

from src import enums as src_enums
from src import models as src_models
from src.integrations import credentials as credentials_helper
from src.integrations.clients.tristate import client as tristate_client
from src.integrations.clients.tristate import exceptions as tristate_client_exceptions
from src.integrations.live_inventory import base
from src.integrations.live_inventory import exceptions as live_inventory_exceptions

logger = logging.getLogger(__name__)
_LOG_PREFIX = "[TRISTATE-LIVE-INVENTORY]"


class TriStateLiveInventoryProvider(base.LiveInventoryProvider):
    provider_kind = src_enums.BrandProviderKind.TRI_STATE.value

    def __init__(self, company_provider: src_models.CompanyProviders) -> None:
        base.LiveInventoryProvider.__init__(self, company_provider)
        credentials = credentials_helper.get_feed_credentials(company_provider)
        self._client = tristate_client.TriStateApiClient(credentials=credentials)

    def refresh(self, provider_part: src_models.ProviderPart) -> src_models.ProviderPartInventory:
        # provider_external_id is "{tse_brand_id}_{part_number}" (see
        # master_parts._ingest_tse_parts_for_mapped_brands) -- resolve the real TSEParts row
        # through it rather than master_part_id, since that's what carries warehouse_code.
        try:
            tse_brand_id_str, part_number = provider_part.provider_external_id.split("_", 1)
            tse_brand_id = int(tse_brand_id_str)
        except (ValueError, AttributeError) as e:
            raise live_inventory_exceptions.LiveInventoryTransportError(
                "Unexpected TSE provider_external_id {!r}: {}".format(
                    provider_part.provider_external_id, e
                )
            ) from e

        tse_brand = src_models.TSEBrand.objects.filter(id=tse_brand_id).first()
        if not tse_brand or not tse_brand.warehouse_code:
            raise live_inventory_exceptions.LiveInventoryNotFoundError(
                "No confirmed warehouse_code for TSE brand_id={}.".format(tse_brand_id)
            )

        try:
            results = self._client.get_inventory_and_pricing([(tse_brand.warehouse_code, part_number)])
        except tristate_client_exceptions.TriStateException as e:
            logger.error(
                "{} TSE API error refreshing inventory for provider_part_id={}: {}".format(
                    _LOG_PREFIX, provider_part.id, e
                )
            )
            raise live_inventory_exceptions.LiveInventoryTransportError(str(e)) from e

        if not results:
            raise live_inventory_exceptions.LiveInventoryNotFoundError(
                "TSE returned no result for {}|{}.".format(tse_brand.warehouse_code, part_number)
            )

        item = results[0]
        now = timezone.now()
        quantity = item.get("inventory") or 0

        tse_part = src_models.TSEParts.objects.filter(brand=tse_brand, part_number=part_number).first()
        if tse_part:
            src_models.TSECompanyPricing.objects.update_or_create(
                part=tse_part,
                company_id=self.company_provider.company_id,
                defaults={
                    "cost": item.get("account_price"),
                    "list_price": item.get("list_price"),
                    "quantity_available": quantity,
                    "orderable": bool(item.get("orderable", 1)),
                },
            )

        inv_obj, _ = src_models.ProviderPartInventory.objects.update_or_create(
            provider_part=provider_part,
            defaults={
                "warehouse_total_qty": quantity,
                "manufacturer_inventory": None,
                "manufacturer_esd": None,
                "warehouse_availability": None,
                "last_synced_at": now,
            },
        )
        return inv_obj
