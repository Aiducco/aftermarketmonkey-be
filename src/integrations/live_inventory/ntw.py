"""
NtwLiveInventoryProvider -- on-demand single-item stock refresh against TBC's v2 tire inventory
API, for the part-detail "refresh" action. The feed (four files a day) stays the bulk source;
this is for the moment a buyer is looking at one tire and wants to know now.

Two things make this provider unlike every other one in this package.

**It looks a part up directly**, via ``searchType: "ByProductId"`` with the part's brand and
number. An earlier version asked for the whole tire SIZE and filtered, because the non-existent
``ByPartNumber`` made direct lookup look impossible; that detour also meant a part whose sidewall
string does not parse -- commercial and farm sizes, ~4% of the feed -- could not be refreshed at
all. Direct lookup has no such hole.

**Its credentials come from two places, and neither is ours.** Each dealer creates their own
application in TBC's developer portal and subscribes it to the InventoryService plan, so the
gateway key/secret live on that company's ORDER account -- the same namespace Keystone uses for
the same reason, its stock check being part of the order service. ``customer_id`` is the
dealer's NTW account number and lives on that same order account -- not with the feed login,
which needs no account number to read a file. Every other live-inventory
provider reads exactly one namespace, so this split is deliberate rather than an oversight.

Missing either one raises ValueError, which the registry turns into None, which the product page
reads as "live check not available for this connection" -- a disabled affordance rather than a
failing click.
"""
import logging
import typing

from django.utils import timezone

from src import constants
from src import enums as src_enums
from src import models as src_models
from src.integrations import credentials as credentials_helper
from src.integrations.clients.ntw import api_client as ntw_api
from src.integrations.clients.ntw import exceptions as ntw_exceptions
from src.integrations.live_inventory import base
from src.integrations.live_inventory import exceptions as live_inventory_exceptions

logger = logging.getLogger(__name__)
_LOG_PREFIX = "[NTW-LIVE-INVENTORY]"


def _ship_to_for(company_provider: src_models.CompanyProviders) -> typing.Optional[typing.List[typing.Dict]]:
    """
    The company's own address as TBC's ``shipTo``. Documented mandatory; measured as not enforced
    for a stock read, so this returns None rather than failing when a company has no location on
    file -- a missing address must not block a product page from showing availability.
    """
    location = (
        src_models.CompanyLocation.objects.filter(company_id=company_provider.company_id)
        .order_by("-is_primary", "id")
        .first()
    )
    if not location:
        return None
    return [{
        "address": {
            "name": location.name or "",
            "address": location.address1 or "",
            "address2": location.address2 or "",
            "city": location.city or "",
            "state": location.state or "",
            "country": location.country or "US",
            "zipCode": location.postal_code or "",
        },
        "phoneNumber": location.phone or "",
    }]


class NtwLiveInventoryProvider(base.LiveInventoryProvider):
    provider_kind = src_enums.BrandProviderKind.NTW.value

    def __init__(self, company_provider: src_models.CompanyProviders) -> None:
        base.LiveInventoryProvider.__init__(self, company_provider)
        order_credentials = credentials_helper.get_order_credentials(company_provider)
        self._customer_id = (
            order_credentials.get(constants.NTW_ORDER_CREDENTIALS_CUSTOMER_ID) or ""
        ).strip()
        client_id = (order_credentials.get(constants.NTW_ORDER_CREDENTIALS_CLIENT_ID) or "").strip()
        client_secret = (order_credentials.get(constants.NTW_ORDER_CREDENTIALS_CLIENT_SECRET) or "").strip()

        # ValueError is the registry's convention for "registered, but this connection cannot use
        # it" -- get_provider() returns None and the product page shows feed values only.
        if not self._customer_id:
            raise ValueError(
                "NTW connection {} has no customer_id; a live stock check needs the dealer's NTW "
                "account number.".format(company_provider.id)
            )
        if not client_id or not client_secret:
            raise ValueError(
                "NTW connection {} has no TBC API key/secret. Each dealer creates their own "
                "application in TBC's developer portal; add it under Ordering.".format(
                    company_provider.id
                )
            )
        self._client = ntw_api.NtwApiClient(client_id=client_id, client_secret=client_secret)
        self._ship_to = _ship_to_for(company_provider)

    # -- part resolution -----------------------------------------------------

    def _ntw_part_for(self, provider_part: src_models.ProviderPart) -> src_models.NtwParts:
        """
        Resolve our ProviderPart back to the feed row, via the composite
        ``{ntw_brand_id}_{part_number}`` external id that master_parts writes. The feed row is
        what carries the brand label TBC expects -- ``brandName`` is required by the API, and
        sending the part number alone makes their flow throw a 500.
        """
        external_id = provider_part.provider_external_id or ""
        brand_id, _, part_number = external_id.partition("_")
        if not brand_id.isdigit() or not part_number:
            raise live_inventory_exceptions.LiveInventoryNotFoundError(
                "NTW provider_external_id {!r} is not in the expected "
                "<ntw_brand_id>_<part_number> form.".format(external_id)
            )
        part = (
            src_models.NtwParts.objects.select_related("brand")
            .filter(brand_id=int(brand_id), part_number=part_number)
            .first()
        )
        if not part:
            raise live_inventory_exceptions.LiveInventoryNotFoundError(
                "No NtwParts row for brand {} part {}.".format(brand_id, part_number)
            )
        return part

    # -- refresh -------------------------------------------------------------

    def refresh(self, provider_part: src_models.ProviderPart) -> src_models.ProviderPartInventory:
        part = self._ntw_part_for(provider_part)

        try:
            row = self._client.find_tire_by_part_number(
                self._customer_id, part.part_number, part.brand.name, ship_to=self._ship_to
            )
        except ntw_exceptions.NtwApiNotConfiguredError as e:
            # Configuration, not transport. Surfaced as a transport error anyway so callers
            # outside this package keep one exception type, but logged distinctly.
            logger.error("{} Not configured: {}".format(_LOG_PREFIX, str(e)))
            raise live_inventory_exceptions.LiveInventoryTransportError(str(e)) from e
        except ntw_exceptions.NtwException as e:
            logger.error(
                "{} TBC API error refreshing provider_part_id={} part={}: {}".format(
                    _LOG_PREFIX, provider_part.id, part.part_number, str(e)
                )
            )
            raise live_inventory_exceptions.LiveInventoryTransportError(str(e)) from e

        now = timezone.now()
        if row is None:
            # TBC does not list this part for this customer at all. Recorded as a real zero
            # rather than left stale -- the same "absent means gone" rule the feed's zero-out
            # pass applies, here for one part on demand. Note the lookup already asks with
            # requestedQty=0, so an in-catalog part that is merely out of stock DOES come back
            # with quantityAvailable 0; None means genuinely not carried.
            logger.info(
                "{} TBC does not list part {} for customer {}; recording zero.".format(
                    _LOG_PREFIX, part.part_number, self._customer_id
                )
            )
            return self._write(provider_part, total_qty=0, sources=[], now=now)

        sources = row.get("availableSource") or []
        if isinstance(sources, dict):
            sources = [sources]
        total_qty = 0
        for source in sources:
            qty = source.get("quantityAvailable")
            if isinstance(qty, (int, float)):
                total_qty += int(qty)
        return self._write(provider_part, total_qty=total_qty, sources=sources, now=now)

    def _write(
        self,
        provider_part: src_models.ProviderPart,
        total_qty: int,
        sources: typing.List[typing.Dict],
        now: typing.Any,
    ) -> src_models.ProviderPartInventory:
        warehouse_availability: typing.Dict[str, typing.Any] = {"available_on_hand": total_qty}
        for source in sources:
            location_id = source.get("locationId")
            qty = source.get("quantityAvailable")
            if location_id and isinstance(qty, (int, float)):
                # Per-branch detail the feed cannot carry: the file is one dealer's one
                # warehouse, while the API answers across every location that can serve them.
                warehouse_availability[str(location_id)] = int(qty)

        inventory, _ = src_models.ProviderPartInventory.objects.update_or_create(
            provider_part=provider_part,
            defaults={
                "warehouse_total_qty": total_qty,
                "manufacturer_inventory": None,
                "manufacturer_esd": None,
                "warehouse_availability": warehouse_availability,
                "last_synced_at": now,
            },
        )
        return inventory
