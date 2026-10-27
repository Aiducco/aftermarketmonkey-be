"""
NtwLiveInventoryProvider -- on-demand single-item stock refresh against TBC's v2 tire inventory
API, for the part-detail "refresh" action. The feed (four files a day) stays the bulk source;
this is for the moment a buyer is looking at one tire and wants to know now.

Two things make this provider unlike every other one in this package.

**It cannot look a part up by its part number.** ``searchType: "ByPartNumber"`` returns errorCd
1006 for every identifier shape TBC's own docs list, so a single-SKU refresh has to ask for the
whole tire SIZE and filter the response down to the part we wanted. That makes a refresh cost one
size query -- fine for a user-triggered action on one part, and the reason nothing should ever
loop this.

**Its credentials come from two places.** The gateway client id/secret are account-level and live
in settings; ``customer_id`` is per-dealer and lives on the connection. Every other live-inventory
provider reads exactly one credential namespace (see this package's registry docstring), so this
split is deliberate rather than an oversight -- and the dealer-specific half is the half that is
usually missing, since TBC issues it separately from the feed login.
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


class NtwLiveInventoryProvider(base.LiveInventoryProvider):
    provider_kind = src_enums.BrandProviderKind.NTW.value

    def __init__(self, company_provider: src_models.CompanyProviders) -> None:
        base.LiveInventoryProvider.__init__(self, company_provider)
        feed_credentials = credentials_helper.get_feed_credentials(company_provider)
        self._customer_id = (feed_credentials.get(constants.NTW_CREDENTIALS_CUSTOMER_ID) or "").strip()
        if not self._customer_id:
            # Raising ValueError is the registry's convention for "registered but this connection
            # cannot use it" -- get_provider() returns None and the FE falls back to feed values.
            raise ValueError(
                "NTW connection {} has no customer_id; live inventory needs TBC's own customer "
                "number for this dealer.".format(company_provider.id)
            )
        self._client = ntw_api.NtwApiClient()

    # -- size resolution -----------------------------------------------------

    @staticmethod
    def _tire_size_for(part: src_models.NtwParts) -> typing.Optional[typing.Dict[str, str]]:
        """
        The ``{width, ratio, diameter}`` TBC's API expects, taken from the feed's own three size
        columns rather than from a parse of the sidewall string.

        Using TBC's numbers to query TBC is the point: they are what their backend indexed the
        tire under. A parse of "LT225/75R16" would also produce 225/75/16, but for the notations
        where the two disagree -- numeric sizes like "11X4.00-5", where the first column is an
        overall diameter and not a width -- the feed's values are what their API was built
        around, and ours would be a guess about their schema.

        Returns None when the row has no aspect ratio (commercial sizes like "10R22.5" leave
        column 8 blank), because the API's size object has no way to express that.
        """
        width = (part.size_component_1 or "").strip()
        ratio = (part.size_component_2 or "").strip()
        diameter = (part.size_component_3 or "").strip()
        if not width or not ratio or not diameter:
            return None
        return {"width": width, "ratio": ratio, "diameter": diameter}

    def _ntw_part_for(self, provider_part: src_models.ProviderPart) -> src_models.NtwParts:
        """
        Resolve our ProviderPart back to the feed row, via the composite
        ``{ntw_brand_id}_{part_number}`` external id that master_parts writes.
        """
        external_id = provider_part.provider_external_id or ""
        brand_id, _, part_number = external_id.partition("_")
        if not brand_id.isdigit() or not part_number:
            raise live_inventory_exceptions.LiveInventoryNotFoundError(
                "NTW provider_external_id {!r} is not in the expected "
                "<ntw_brand_id>_<part_number> form.".format(external_id)
            )
        part = src_models.NtwParts.objects.filter(
            brand_id=int(brand_id), part_number=part_number
        ).first()
        if not part:
            raise live_inventory_exceptions.LiveInventoryNotFoundError(
                "No NtwParts row for brand {} part {}.".format(brand_id, part_number)
            )
        return part

    # -- refresh -------------------------------------------------------------

    def refresh(self, provider_part: src_models.ProviderPart) -> src_models.ProviderPartInventory:
        part = self._ntw_part_for(provider_part)
        tire_size = self._tire_size_for(part)
        if not tire_size:
            raise live_inventory_exceptions.LiveInventoryNotFoundError(
                "NTW part {} has no complete metric size ({!r}), so TBC's size-keyed API cannot "
                "be queried for it.".format(part.part_number, part.size_description)
            )

        try:
            row = self._client.find_tire_by_part_number(
                self._customer_id, part.part_number, tire_size
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
            # TBC listed the size but not this part: a real zero, not a failure. Writing it is
            # the whole point -- it is the same "absent means gone" rule the feed's zero-out pass
            # implements, applied to one part on demand.
            logger.info(
                "{} TBC lists no stock for part {} in size {}; recording zero.".format(
                    _LOG_PREFIX, part.part_number, part.size_description
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
