"""
ATDLiveInventoryProvider -- on-demand single-item stock refresh against ATD's REST catalog API,
for the part-detail "refresh" action. The scheduled catalog sweep stays the bulk source; this is
for the moment a buyer is looking at one tire and wants to know now.

Unlike NTW's, this provider needs NO credentials beyond the ones the feed already has: ATD issues
one credential set per dealer covering catalog, availability and ordering alike. So wherever the
ATD feed is connected, this works -- no second onboarding step, and nothing to enable on ATD's
side (the order service is the part that needs separate entitlement; see orders/atd.py).

Three things about ``/product/product-availability``, all measured live against SunCoast's own
credentials on 2026-10-27 rather than read off the docs, because all three differ from the
``productcatalog`` response the bulk sweep parses:

**The buckets are flat on the product**, not nested under an ``availability`` key the way
productcatalog nests them. Reading ``product["availability"]`` here returns nothing and records a
silent zero.

**``sthinventory`` is never returned.** productcatalog sends all five buckets; this endpoint sends
four. Confirmed against parts whose stored sthinventory was 15, 20, 145 and 250 -- every one came
back without the key. It is therefore PRESERVED from the existing row rather than defaulted to
zero: the product page renders it as "Ship-to-Home Eligible" (see
api.services.parts._atd_company_inventory), so writing a zero would blank a real number on screen
and call it a refresh.

**An inaccessible product is HTTP 460 ``product-access-denied``**, not an empty list -- and the
client turns any ``errorMessage`` into ATDAuthError, which would otherwise surface to the dealer
as "your credentials are wrong" when the credentials are fine and ATD simply does not carry that
item for them. A single-part refresh is exactly the case that hits it, so that one message is
mapped to LiveInventoryNotFoundError. (In a MIXED request ATD returns 200 and silently omits the
inaccessible product instead, which is why the empty-result path below is also a not-found.)
"""
import logging
import typing

from django.utils import timezone

from src import enums as src_enums
from src import models as src_models
from src.integrations import credentials as credentials_helper
from src.integrations.clients.atd import client as atd_client
from src.integrations.clients.atd import exceptions as atd_exceptions
from src.integrations.live_inventory import base
from src.integrations.live_inventory import exceptions as live_inventory_exceptions

logger = logging.getLogger(__name__)
_LOG_PREFIX = "[ATD-LIVE-INVENTORY]"

# The exact errorMessage ATD returns (with HTTP 460) when it will not report on any requested
# product number. Matched on the whole value, not a substring: it is a fixed token, and a loose
# match would swallow the genuine credential errorMessages ("does not have access to this
# location", "enter a valid location") that must keep reporting as auth failures.
_PRODUCT_ACCESS_DENIED = "product-access-denied"

# Buckets this endpoint actually reports -> ATDCompanyPricing columns. sthinventory is absent on
# purpose; see the module docstring.
_BUCKETS = (
    ("local", "local_qty"),
    ("localplus", "localplus_qty"),
    ("nationwide", "nationwide_qty"),
    ("onhand", "onhand_qty"),
)


def _int_or_zero(value: typing.Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _buckets_from(product: typing.Dict) -> typing.Dict[str, int]:
    """
    The four availability buckets, read FLAT off the product.

    Its own function because this is the one line where this endpoint and the catalog sweep's
    disagree, and getting it wrong fails silently: ``productcatalog`` nests the buckets under an
    ``availability`` key, ``product-availability`` does not, and reading the wrong one yields a
    complete set of zeros that looks exactly like a part going out of stock.
    """
    return {api_key: _int_or_zero(product.get(api_key)) for api_key, _ in _BUCKETS}


class ATDLiveInventoryProvider(base.LiveInventoryProvider):
    provider_kind = src_enums.BrandProviderKind.ATD.value

    def __init__(self, company_provider: src_models.CompanyProviders) -> None:
        base.LiveInventoryProvider.__init__(self, company_provider)
        # ATDApiClient raises ValueError when username/password/location are missing, which is the
        # registry's convention for "registered, but this connection cannot use it" -- get_provider
        # returns None and the product page shows feed values only, as a disabled affordance
        # rather than a failing click.
        self._client = atd_client.ATDApiClient(
            credentials=credentials_helper.get_feed_credentials(company_provider)
        )

    # -- part resolution -----------------------------------------------------

    def _atd_part_for(self, provider_part: src_models.ProviderPart) -> src_models.ATDPart:
        """
        ``provider_external_id`` IS the atdproductnumber -- ATD's own globally-unique SKU, and
        what master_parts keys ProviderPart on. No composite to unpack, unlike NTW's
        ``<brand_id>_<part_number>``.
        """
        external_id = (provider_part.provider_external_id or "").strip()
        if not external_id:
            raise live_inventory_exceptions.LiveInventoryNotFoundError(
                "ProviderPart {} has no ATD product number.".format(provider_part.id)
            )
        part = src_models.ATDPart.objects.filter(atdproductnumber=external_id).first()
        if not part:
            raise live_inventory_exceptions.LiveInventoryNotFoundError(
                "No ATDPart row for atdproductnumber {}.".format(external_id)
            )
        return part

    # -- refresh -------------------------------------------------------------

    def refresh(self, provider_part: src_models.ProviderPart) -> src_models.ProviderPartInventory:
        part = self._atd_part_for(provider_part)

        try:
            products = self._client.get_product_availability([part.atdproductnumber])
        except atd_exceptions.ATDAuthError as e:
            if str(getattr(e, "args", [""])[0] or "").strip().lower() == _PRODUCT_ACCESS_DENIED:
                # Not a credential failure. ATD does not carry this item for this dealer's
                # location, which is a legitimate "no inventory record", recorded as such.
                logger.info(
                    "{} ATD will not report on product {} for company_provider_id={} "
                    "(product-access-denied).".format(
                        _LOG_PREFIX, part.atdproductnumber, self.company_provider.id
                    )
                )
                raise live_inventory_exceptions.LiveInventoryNotFoundError(
                    "ATD does not carry product {} for this account's location.".format(
                        part.atdproductnumber
                    )
                ) from e
            logger.error("{} ATD rejected the credentials: {}".format(_LOG_PREFIX, str(e)))
            raise live_inventory_exceptions.LiveInventoryTransportError(str(e)) from e
        except atd_exceptions.ATDAPIException as e:
            logger.error(
                "{} ATD API error refreshing provider_part_id={} product={}: {}".format(
                    _LOG_PREFIX, provider_part.id, part.atdproductnumber, str(e)
                )
            )
            raise live_inventory_exceptions.LiveInventoryTransportError(str(e)) from e

        match = None
        for product in products or []:
            if str(product.get("atdproductnumber") or "").strip() == part.atdproductnumber:
                match = product
                break
        if match is None:
            raise live_inventory_exceptions.LiveInventoryNotFoundError(
                "ATD returned no availability for product {}.".format(part.atdproductnumber)
            )

        return self._write(provider_part, part, match)

    # -- persistence ---------------------------------------------------------

    def _write(
        self,
        provider_part: src_models.ProviderPart,
        part: src_models.ATDPart,
        product: typing.Dict,
    ) -> src_models.ProviderPartInventory:
        now = timezone.now()
        company_id = self.company_provider.company_id
        company_key = str(company_id)

        buckets = _buckets_from(product)

        # The raw per-distributor table first, so the next bulk sweep reads back what the dealer
        # was just shown. sthinventory_qty is deliberately NOT in the update set.
        pricing, created = src_models.ATDCompanyPricing.objects.get_or_create(
            part=part, company_id=company_id,
            defaults={"last_synced_at": now, **{col: buckets[k] for k, col in _BUCKETS}},
        )
        if not created:
            for api_key, column in _BUCKETS:
                setattr(pricing, column, buckets[api_key])
            pricing.last_synced_at = now
            pricing.save(update_fields=[col for _, col in _BUCKETS] + ["last_synced_at", "updated_at"])

        inventory = src_models.ProviderPartInventory.objects.filter(provider_part=provider_part).first()
        warehouse_availability = (
            dict(inventory.warehouse_availability)
            if inventory and isinstance(inventory.warehouse_availability, dict)
            else {}
        )
        previous = warehouse_availability.get(company_key)
        previous = previous if isinstance(previous, dict) else {}

        # Merged onto this company's own key only -- every other company's entry in this global
        # row is left exactly as it was. sthinventory carries over from whatever the last full
        # sweep stored, since this endpoint does not speak about it.
        warehouse_availability[company_key] = dict(
            buckets, sthinventory=_int_or_zero(pricing.sthinventory_qty or previous.get("sthinventory"))
        )

        # Same approximation the bulk sweep makes: no single distributor-wide number exists for
        # ATD, so the global column carries the largest nationwide figure any company has seen.
        # The per-company truth stays in warehouse_availability.
        max_nationwide = max(
            (
                _int_or_zero(entry.get("nationwide"))
                for entry in warehouse_availability.values()
                if isinstance(entry, dict)
            ),
            default=0,
        )

        inventory, _ = src_models.ProviderPartInventory.objects.update_or_create(
            provider_part=provider_part,
            defaults={
                "warehouse_total_qty": max_nationwide,
                "manufacturer_inventory": None,
                "manufacturer_esd": None,
                "warehouse_availability": warehouse_availability,
                "last_synced_at": now,
            },
        )
        return inventory
