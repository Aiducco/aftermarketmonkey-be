"""
Common interface every address-autocomplete/validation provider implements.

Only one implementation exists today (``geoapify.GeoapifyAddressProvider``), and this layer
would be over-engineering for one provider were it not for the actual reason it exists: the
provider is expected to change. Geoapify was picked because its free tier needs no credit
card, with Google Places as the planned upgrade for better house-number coverage; a paid
validator (Google Address Validation, Smarty) is a further possibility. The frontend must not
notice any of that, so it only ever talks to /api/address/{suggest,resolve,validate}/ and
those endpoints only ever talk to an :class:`AddressProvider`.

Kept beside ``src/integrations/live_inventory`` and ``src/integrations/orders`` -- the other
two "one interface, several vendors behind it" packages -- rather than inside
``src/integrations/clients/``, which holds raw per-vendor transports. The Geoapify transport
itself does live there (``src/integrations/clients/geoapify``); this package is the
vendor-agnostic layer on top.
"""
import abc
import dataclasses
import typing

from src import enums as src_enums


@dataclasses.dataclass(frozen=True)
class Address:
    """
    The one address shape shared by all three endpoints and by the quote request.

    ``line2`` is last only because dataclasses need defaulted fields last -- ``as_dict``
    emits the field order the API contract documents. ``state`` is empty for countries that
    have no state/region code (Montenegro, for instance), which is a valid address, not a
    missing field.
    """
    line1: str
    city: str
    state: str
    postal_code: str
    country: str
    line2: str = ""

    def as_dict(self) -> dict:
        return {
            "line1": self.line1,
            "line2": self.line2,
            "city": self.city,
            "state": self.state,
            "postal_code": self.postal_code,
            "country": self.country,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Address":
        """Tolerant of missing keys: every field defaults to "" so a half-filled form can
        still be sent to /validate (which answers "unverified" rather than erroring)."""
        return cls(
            line1=(data.get("line1") or "").strip(),
            line2=(data.get("line2") or "").strip(),
            city=(data.get("city") or "").strip(),
            state=(data.get("state") or "").strip(),
            postal_code=(data.get("postal_code") or "").strip(),
            country=(data.get("country") or "").strip().upper(),
        )

    def one_line(self) -> str:
        """Single-line form used when asking a provider to geocode this address."""
        city_line = " ".join(part for part in (self.city, self.state, self.postal_code) if part)
        return ", ".join(part for part in (self.line1, city_line, self.country) if part)


@dataclasses.dataclass(frozen=True)
class Suggestion:
    """
    One autocomplete hit. The id the frontend sees is NOT ``provider_ref`` -- the API layer
    mints its own opaque, session-scoped id (see src.api.services.address) so provider ids
    never reach the browser and a resolve from one session can't replay another's.

    ``address`` is pre-filled when the provider's autocomplete response already carries
    structured parts (Geoapify does), which is what lets /resolve answer from cache with no
    second billed call. A provider whose autocomplete returns only a label (Google Places)
    leaves it None and implements :meth:`AddressProvider.resolve` instead.
    """
    label: str
    provider_ref: str
    address: typing.Optional[Address] = None


@dataclasses.dataclass(frozen=True)
class ValidationResult:
    status: src_enums.AddressValidationStatus
    suggested: typing.Optional[Address] = None
    messages: typing.Tuple[str, ...] = ()


class AddressProvider(abc.ABC):
    """
    Implementations must be cheap to construct (one per request is fine) and must raise only
    :mod:`src.integrations.address.exceptions` types.
    """

    name: typing.ClassVar[str]

    @abc.abstractmethod
    def suggest(self, q: str, country: typing.Optional[str], session: str) -> typing.List[Suggestion]:
        """
        Type-ahead lookup for ``q``, optionally restricted to an ISO 3166-1 alpha-2
        ``country``. ``session`` is the frontend's per-typing-session uuid, passed through for
        providers that bill or group requests by session (Google's session tokens) -- Geoapify
        has no such concept and ignores it.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def resolve(self, provider_ref: str, session: str) -> typing.Optional[Address]:
        """
        Fetch the full address for a suggestion the provider returned earlier, for providers
        whose autocomplete response doesn't already contain it. Returns None when the
        reference is unknown or expired on the provider's side.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def validate(self, address: Address) -> ValidationResult:
        """
        Check a fully typed address. Never raises for "the address looks wrong" -- that is
        ``UNVERIFIED``/``CORRECTED``. Raises only on transport failure, and even then the
        service layer degrades it to ``UNVERIFIED``: validation is advisory and must never
        stop a user from getting a quote.
        """
        raise NotImplementedError
