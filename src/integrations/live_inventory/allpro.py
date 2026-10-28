"""
AllProLiveInventoryProvider -- on-demand single-part stock refresh against AllPro's Fulfillment
Master stock check, for the part-detail refresh action. The FTP feed stays the bulk source.

Credentials come from the ORDER namespace, not the feed's: the FTP catalog login is rejected by
this API ("Invalid username/password", measured), and Fulfillment Master issues a separate
per-dealer account. That is the same split NTW has, and for the same reason -- the live stock
check is part of the ordering service rather than the feed.

AllPro's inventory is distributor-wide rather than per-dealer (one shared AllProParts row, see
master_parts.sync_provider_inventory_from_allpro), so unlike ATD there is no per-company key to
merge into: a refresh updates the shared row for everyone.

One call, with a blank branch, which asks across all three warehouses. The per-warehouse
breakdown the feed carries (600 Virginia / 800 Florida / 900 Alabama) is PRESERVED unless the
response names the branch it answered for -- blanking Virginia/Florida/Alabama because a
whole-network total came back would delete real numbers and call it a refresh.
"""
import logging
import typing

from django.utils import timezone

from src import enums as src_enums
from src import models as src_models
from src.integrations import credentials as credentials_helper
from src.integrations.clients.allpro import exceptions as allpro_exceptions
from src.integrations.clients.allpro import order_client as allpro_client
from src.integrations.live_inventory import base
from src.integrations.live_inventory import exceptions as live_inventory_exceptions

logger = logging.getLogger(__name__)
_LOG_PREFIX = "[ALLPRO-LIVE-INVENTORY]"

# branch code -> (AllProParts column, display name used by the feed's own availability dict)
_BRANCH_COLUMNS = {
    "600": ("warehouse_va_qty", "Virginia"),
    "800": ("warehouse_fl_qty", "Florida"),
    "900": ("warehouse_al_qty", "Alabama"),
}


def _int_or_zero(value: typing.Any) -> int:
    try:
        return int(float(str(value).strip()))
    except (TypeError, ValueError):
        return 0


class AllProLiveInventoryProvider(base.LiveInventoryProvider):
    provider_kind = src_enums.BrandProviderKind.ALLPRO_DISTRIBUTING.value

    def __init__(self, company_provider: src_models.CompanyProviders) -> None:
        base.LiveInventoryProvider.__init__(self, company_provider)
        # ValueError when the FM account is not configured -> registry returns None -> the
        # product page shows feed values with the refresh action disabled.
        self._client = allpro_client.AllProFulfillmentClient(
            credentials=credentials_helper.get_order_credentials(company_provider)
        )

    # -- part resolution -----------------------------------------------------

    def _allpro_part_for(self, provider_part: src_models.ProviderPart) -> src_models.AllProParts:
        """``provider_external_id`` is "<allpro_brand_id>_<part_number>" (master_parts builds it);
        the API wants the line code, which only the catalog row carries."""
        external_id = (provider_part.provider_external_id or "").strip()
        brand_id, _, part_number = external_id.partition("_")
        if not brand_id.isdigit() or not part_number:
            raise live_inventory_exceptions.LiveInventoryNotFoundError(
                "AllPro provider_external_id {!r} is not in the expected "
                "<brand_id>_<part_number> form.".format(external_id)
            )
        part = src_models.AllProParts.objects.filter(
            brand_id=int(brand_id), part_number=part_number
        ).first()
        if not part:
            raise live_inventory_exceptions.LiveInventoryNotFoundError(
                "No AllProParts row for brand {} part {}.".format(brand_id, part_number)
            )
        return part

    # -- refresh -------------------------------------------------------------

    def refresh(self, provider_part: src_models.ProviderPart) -> src_models.ProviderPartInventory:
        part = self._allpro_part_for(provider_part)

        try:
            rows = self._client.stock_check(
                [{"linecode": part.line_code, "partno": part.part_number, "qtyreq": 1}],
                branch=allpro_client.BRANCH_ALL,
            )
        except allpro_exceptions.AllProUpstreamNotProvisionedError as e:
            # AllPro's own upstream mapping, not the dealer's credentials. Surfaced as transport
            # so the page degrades to feed values, but logged distinctly because the fix is a
            # phone call to AllPro rather than anything in our configuration.
            logger.error("{} AllPro has not provisioned this account upstream: {}".format(
                _LOG_PREFIX, str(e)))
            raise live_inventory_exceptions.LiveInventoryTransportError(str(e)) from e
        except allpro_exceptions.AllProException as e:
            logger.error("{} stock check failed for provider_part_id={} part={}: {}".format(
                _LOG_PREFIX, provider_part.id, part.part_number, str(e)))
            raise live_inventory_exceptions.LiveInventoryTransportError(str(e)) from e

        match = None
        for row in rows or []:
            if str(row.get("partno") or "").strip().upper() == part.part_number.strip().upper():
                match = row
                break
        if match is None:
            # A stock check echoes every requested part, so nothing coming back for ours means
            # AllPro does not carry it -- a real not-found rather than a transport problem.
            raise live_inventory_exceptions.LiveInventoryNotFoundError(
                "AllPro returned no stock line for {} {}.".format(part.line_code, part.part_number)
            )
        errcode = (match.get("errcode") or "").strip().lower()
        if errcode and errcode != "success":
            raise live_inventory_exceptions.LiveInventoryNotFoundError(
                "AllPro reported {!r} for {} {}.".format(
                    match.get("errcode"), part.line_code, part.part_number)
            )

        return self._write(provider_part, part, match)

    # -- persistence ---------------------------------------------------------

    def _write(
        self,
        provider_part: src_models.ProviderPart,
        part: src_models.AllProParts,
        row: typing.Dict,
    ) -> src_models.ProviderPartInventory:
        now = timezone.now()
        total = _int_or_zero(row.get("qtyavail"))
        branch = str(row.get("branch") or "").strip()

        # Raw table first, so the next bulk sync reads back what the dealer was just shown.
        update_fields = ["total_qty", "updated_at"]
        part.total_qty = total
        column_name = None
        if branch in _BRANCH_COLUMNS:
            column_name, _ = _BRANCH_COLUMNS[branch]
            setattr(part, column_name, total)
            update_fields.append(column_name)
        part.save(update_fields=update_fields)

        warehouse_availability: typing.Dict[str, int] = {}
        for code, (column, label) in _BRANCH_COLUMNS.items():
            value = getattr(part, column, None)
            if value is not None:
                warehouse_availability[label] = int(value)

        inventory, _ = src_models.ProviderPartInventory.objects.update_or_create(
            provider_part=provider_part,
            defaults={
                "warehouse_total_qty": total,
                "manufacturer_inventory": None,
                "manufacturer_esd": None,
                "warehouse_availability": warehouse_availability or None,
                "last_synced_at": now,
            },
        )
        if column_name is None:
            logger.info(
                "{} part {} {}: AllPro answered across all branches (total={}); the per-warehouse "
                "split is left at the feed's last values.".format(
                    _LOG_PREFIX, part.line_code, part.part_number, total)
            )
        return inventory
