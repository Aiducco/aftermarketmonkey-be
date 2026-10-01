"""
Geoapify implementation of :class:`src.integrations.address.base.AddressProvider`.

Field mapping (Geoapify ``feature.properties`` -> our ``Address``):

    address_line1 / housenumber + street -> line1  (see _line1 -- house-number placement is
                                                   country-specific)
    city / town / village                -> city   (in that order -- small places carry only
                                                   the latter two)
    state_code                           -> state  (absent for countries with no state codes;
                                                   empty string, never None)
    postcode                             -> postal_code
    country_code.upper()                 -> country

``line2`` is never populated from the provider: Geoapify has no unit/suite concept, and
whatever the user typed there is theirs to keep.
"""
import logging
import typing

from django.conf import settings

from src import enums as src_enums
from src.integrations.address import base
from src.integrations.address import exceptions
from src.integrations.clients.geoapify import client as geoapify_client
from src.integrations.clients.geoapify import exceptions as geoapify_exceptions

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[GEOAPIFY-ADDRESS]"

# How precise a match is, is read from ``rank.confidence_building_level`` -- NOT from
# ``result_type``, which was verified against the live API to be useless for this:
#
#   "1600 Amphitheatre Parkway, Mountain View CA 94043"  -> result_type "amenity",
#                                                           confidence_building_level 1
#   "99281 Amphitheatre Parkway, Mountain View CA 94043" -> result_type "building",
#                                                           confidence_building_level 0
#
# i.e. a real delivery point reports "amenity" (Geoapify names the POI that sits on it) while
# a house number that doesn't exist still reports "building", with match_type "full_match" to
# boot. ``confidence_building_level`` is the only field that separated the two in every case
# probed; it is absent entirely when the match never reached building level.

# OSM ``building`` tags that imply more than one delivery point behind one street address, used
# only to hint "unit number may be missing". Deliberately conservative: a false hint is a
# pointless warning on a correct address. ``building=residential``/``house``/``yes`` are
# excluded for that reason -- they are single dwellings or simply untagged.
#
# CAVEAT: in practice this almost never fires. ``datasource.raw.building`` came back absent on
# every address probed against the live API, including the Empire State Building and
# 1 Rockefeller Plaza, so the free tier appears not to carry OSM building tags at all. The spec
# asked for this hint "if that data is available" -- it mostly isn't. Kept because it costs
# nothing and starts working if Geoapify (or a future provider) does expose it.
_MULTI_UNIT_BUILDING_TAGS = frozenset(
    {"apartments", "dormitory", "hotel", "office", "commercial", "retail"}
)

_MULTI_UNIT_MESSAGE = "Unit number may be missing"


def _first_non_empty(properties: dict, *keys: str) -> str:
    for key in keys:
        value = properties.get(key)
        if value:
            return str(value).strip()
    return ""


def _normalize(value: str) -> str:
    """Case/whitespace-insensitive comparison key. Street *spelling* differences are meant to
    surface as corrections, so nothing here tries to equate "St" with "Street"."""
    return " ".join((value or "").split()).strip().lower()


def _line1(properties: dict) -> str:
    """
    House-number placement is country-specific -- "1600 Amphitheatre Parkway" in the US,
    "Jovana Tomaševića 12" in Montenegro -- so composing ``housenumber + " " + street``
    produces a street line no local would recognize for most of the world.

    Geoapify's own ``address_line1`` already applies the right convention per country, so
    prefer it. The catch is that for an amenity hit ``address_line1`` is the place's NAME
    ("Googleplex") with the street relegated to ``address_line2`` -- useless in an ADDRESS
    field. Hence the containment check: use ``address_line1`` only when it was actually built
    from the street, and otherwise compose the parts ourselves.
    """
    street = _first_non_empty(properties, "street")
    address_line1 = _first_non_empty(properties, "address_line1")

    if street:
        if address_line1 and _normalize(street) in _normalize(address_line1):
            return address_line1
        housenumber = _first_non_empty(properties, "housenumber")
        return " ".join(part for part in (housenumber, street) if part)

    # No parsed street: amenities and some rural addresses carry only a pre-formatted line.
    return address_line1 or _first_non_empty(properties, "name")


def address_from_properties(properties: dict) -> base.Address:
    """Module-level (not a method) so the mapping can be unit-tested against fixtures without
    constructing a provider, which would need an API key."""
    country_code = _first_non_empty(properties, "country_code").upper()

    return base.Address(
        line1=_line1(properties),
        line2="",
        city=_first_non_empty(properties, "city", "town", "village"),
        state=_first_non_empty(properties, "state_code").upper(),
        postal_code=_first_non_empty(properties, "postcode"),
        country=country_code,
    )


def _label(properties: dict, address: base.Address) -> str:
    """Geoapify's own ``formatted`` string is what a user recognizes; fall back to rebuilding
    one from the mapped parts if it is missing."""
    formatted = _first_non_empty(properties, "formatted")
    return formatted or address.one_line()


def _is_multi_unit(properties: dict) -> bool:
    datasource = properties.get("datasource") or {}
    raw = datasource.get("raw") or {}
    building_tag = str(raw.get("building") or "").strip().lower()
    return building_tag in _MULTI_UNIT_BUILDING_TAGS


def _differing_fields(entered: base.Address, matched: base.Address) -> typing.List[str]:
    """
    Which of the three fields the spec treats as correctable differ. A field the user left
    empty is not a difference -- the provider filling in a blank postcode is an improvement we
    apply silently rather than a correction worth a modal.
    """
    differing = []
    for field_name in ("line1", "city", "postal_code"):
        entered_value = getattr(entered, field_name)
        matched_value = getattr(matched, field_name)
        if not entered_value or not matched_value:
            continue
        if _normalize(entered_value) != _normalize(matched_value):
            differing.append(field_name)
    return differing


class GeoapifyAddressProvider(base.AddressProvider):
    name = "geoapify"

    def __init__(self, client: typing.Optional[geoapify_client.GeoapifyApiClient] = None) -> None:
        if client is not None:
            self.client = client
            return
        try:
            self.client = geoapify_client.GeoapifyApiClient()
        except ValueError as e:
            raise exceptions.AddressProviderNotConfigured(str(e))

    # -- suggest ---------------------------------------------------------------------------

    def suggest(
        self, q: str, country: typing.Optional[str], session: str
    ) -> typing.List[base.Suggestion]:
        """``session`` is unused: Geoapify has no session-token concept (Google does, which is
        why it is in the interface at all)."""
        del session

        try:
            features = self.client.autocomplete(
                text=q, country_code=country, limit=settings.ADDRESS_SUGGEST_LIMIT
            )
        except geoapify_exceptions.GeoapifyTimeout as e:
            raise exceptions.AddressProviderTimeout(str(e))
        except geoapify_exceptions.GeoapifyAPIException as e:
            raise exceptions.AddressProviderError(str(e))

        suggestions = []
        for properties in features:
            address = address_from_properties(properties)
            if not address.line1:
                # Nothing to put in the ADDRESS field -- a country- or city-level hit. Showing
                # it would fill the form with a blank street.
                continue
            suggestions.append(
                base.Suggestion(
                    label=_label(properties, address),
                    provider_ref=str(properties.get("place_id") or ""),
                    address=address,
                )
            )
        return suggestions

    # -- resolve ---------------------------------------------------------------------------

    def resolve(self, provider_ref: str, session: str) -> typing.Optional[base.Address]:
        """
        Always None, and that is not a gap: Geoapify's autocomplete response already carried
        the structured address, so the API layer serves /resolve from its own cache and only
        reaches this method when that cache entry has expired -- at which point the provider
        reference is no help either (Geoapify has no place-details endpoint to replay it
        against) and the endpoint correctly answers 404.

        A Google adapter would do its Place Details call right here.
        """
        del provider_ref, session
        return None

    # -- validate --------------------------------------------------------------------------

    def validate(self, address: base.Address) -> base.ValidationResult:
        """
        Grading rules, each one derived from a live-API probe (see the module-level note on
        ``result_type``):

        * building-level match, nothing differs            -> VALID
        * anything differs, and the match is at least
          plausible at building OR street level            -> CORRECTED
        * street confirmed but the house number was not    -> UNVERIFIED
        * nothing recognizable                             -> UNVERIFIED

        The second rule uses a lower floor than the first on purpose. Geoapify does not
        penalize a wrong postcode at all -- "1600 Amphitheatre Parkway, Mountain View CA
        94044" comes back with confidence 1 and the *right* postcode silently substituted --
        so a correction is only ever detected by comparing fields, never by a low score. And
        a street typo ("Amphitheater Pkwy") scores 0.75 at both levels: below the bar for
        calling an address valid, but well above "we have no idea", and exactly the case the
        "Did you mean...?" prompt exists for.
        """
        threshold = settings.ADDRESS_VALIDATION_CONFIDENCE_THRESHOLD
        correction_floor = settings.ADDRESS_VALIDATION_CORRECTION_FLOOR

        try:
            features = self.client.search(
                text=address.one_line(),
                country_code=address.country,
                timeout_seconds=settings.ADDRESS_VALIDATE_TIMEOUT_SECONDS,
            )
        except geoapify_exceptions.GeoapifyTimeout as e:
            raise exceptions.AddressProviderTimeout(str(e))
        except geoapify_exceptions.GeoapifyAPIException as e:
            raise exceptions.AddressProviderError(str(e))

        if not features:
            return base.ValidationResult(
                status=src_enums.AddressValidationStatus.UNVERIFIED,
                messages=("We could not find this address.",),
            )

        properties = features[0]
        matched = address_from_properties(properties)
        rank = properties.get("rank") or {}
        # Both are absent (not zero) when the match never reached that level, hence the 0.0
        # default: "not graded at building level" and "graded 0 at building level" mean the
        # same thing to us.
        building_confidence = _as_float(rank.get("confidence_building_level"))
        street_confidence = _as_float(rank.get("confidence_street_level"))
        differing = _differing_fields(entered=address, matched=matched)

        messages: typing.List[str] = []
        if building_confidence >= threshold and not address.line2 and _is_multi_unit(properties):
            messages.append(_MULTI_UNIT_MESSAGE)

        if building_confidence >= threshold and not differing:
            return base.ValidationResult(
                status=src_enums.AddressValidationStatus.VALID,
                messages=tuple(messages),
            )

        if differing and max(building_confidence, street_confidence) >= correction_floor:
            return base.ValidationResult(
                status=src_enums.AddressValidationStatus.CORRECTED,
                # The user keeps their own line2: the provider never has it, so echoing its
                # empty value back would silently drop the unit number they typed.
                suggested=base.Address(
                    line1=matched.line1 or address.line1,
                    line2=address.line2,
                    city=matched.city or address.city,
                    state=matched.state or address.state,
                    postal_code=matched.postal_code or address.postal_code,
                    country=matched.country or address.country,
                ),
                messages=tuple(messages),
            )

        if street_confidence >= correction_floor:
            # The street is real but the house number isn't in the provider's data -- very
            # common for US addresses outside major cities, so this must stay a soft warning.
            messages.append("We could not confirm the house number for this street.")
        else:
            messages.append("We could not find this address.")
        return base.ValidationResult(
            status=src_enums.AddressValidationStatus.UNVERIFIED,
            messages=tuple(messages),
        )


def _as_float(value: typing.Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default
