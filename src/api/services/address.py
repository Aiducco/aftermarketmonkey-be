"""
Ship-to address autocomplete & validation, frontend-facing side.

Three things live here that deliberately do NOT live in the provider adapters, because they
are the same whichever provider is configured:

1. **Opaque suggestion ids.** The id handed to the browser is a uuid we mint, scoped to the
   typing session, mapped in the cache to the provider's own reference and (where the provider
   gave us one) the full address. The provider's ids never reach the frontend, and a leaked id
   is useless to another session.
2. **The cache that makes /resolve free.** Geoapify's autocomplete already returns structured
   parts, so resolve is a cache read, not a second billed call.
3. **Degrading, not failing.** Every provider error is swallowed here: /suggest returns an
   empty list and /validate returns ``unverified``. Autocomplete and validation are both
   conveniences layered onto a form that has always worked by hand, so neither may ever stop
   a user typing an address or getting a quote. Provider errors are logged WITHOUT the address
   text -- it is end-customer PII and the logs are not the place for it.
"""
import logging
import time
import typing
import uuid

from django.conf import settings
from django.core.cache import cache

from src import enums as src_enums
from src.integrations.address import base
from src.integrations.address import exceptions as address_exceptions
from src.integrations.address import registry

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[ADDRESS]"

_SUGGESTION_CACHE_PREFIX = "address:suggestion"
_RATE_LIMIT_CACHE_PREFIX = "address:suggest-rate"

# API wire values for the validation status. Spelled out rather than derived from the enum
# member names so renaming a member can't silently change the published contract.
_STATUS_TO_API = {
    src_enums.AddressValidationStatus.VALID: "valid",
    src_enums.AddressValidationStatus.CORRECTED: "corrected",
    src_enums.AddressValidationStatus.UNVERIFIED: "unverified",
}
_API_TO_STATUS = {value: key for key, value in _STATUS_TO_API.items()}

_PROVIDER_UNAVAILABLE_MESSAGE = "We could not check this address right now."


def status_to_api(status: src_enums.AddressValidationStatus) -> str:
    return _STATUS_TO_API[status]


def status_to_api_from_value(value: typing.Optional[int]) -> typing.Optional[str]:
    """Stored PositiveSmallIntegerField value -> wire string. None both for NULL ("validation
    never ran") and for a value that isn't a known member -- serializing a PO must not blow up
    over one odd row."""
    if not value:
        return None
    try:
        return _STATUS_TO_API[src_enums.AddressValidationStatus(value)]
    except ValueError:
        return None


def status_from_api(value: typing.Optional[str]) -> typing.Optional[src_enums.AddressValidationStatus]:
    """None for anything unrecognized (including None) -- callers treat that as "the frontend
    did not run validation", which is a legitimate state (saved location, ship-to-my-shop)."""
    if not value:
        return None
    return _API_TO_STATUS.get(str(value).strip().lower())


def _suggestion_cache_key(session: str, suggestion_id: str) -> str:
    return "{}:{}:{}".format(_SUGGESTION_CACHE_PREFIX, session, suggestion_id)


def _country_is_shippable(country: typing.Optional[str]) -> bool:
    allowed = getattr(settings, "ADDRESS_ALLOWED_COUNTRIES", None) or []
    if not allowed or not country:
        return True
    return country.strip().upper() in allowed


# -- rate limiting -------------------------------------------------------------------------


def consume_suggest_quota(identity: str) -> bool:
    """
    Fixed one-minute window per ``identity`` (user id, or IP for a request that somehow has
    no user). False means over the limit.

    Fails OPEN: if the cache is unreachable the user keeps their autocomplete rather than
    losing it to an outage in the thing that was only there to protect a quota.
    """
    limit = int(getattr(settings, "ADDRESS_SUGGEST_RATE_LIMIT_PER_MINUTE", 0) or 0)
    if limit <= 0:
        return True

    window = int(time.time() // 60)
    key = "{}:{}:{}".format(_RATE_LIMIT_CACHE_PREFIX, identity, window)
    try:
        if cache.add(key, 1, timeout=120):
            return True
        try:
            count = cache.incr(key)
        except ValueError:
            # The key expired between add() and incr(). Treat it as the first hit of a new
            # window rather than as an error.
            cache.set(key, 1, timeout=120)
            return True
    except Exception:
        logger.exception("%s Rate-limit cache unavailable; allowing the request.", _LOG_PREFIX)
        return True

    return count <= limit


# -- suggest -------------------------------------------------------------------------------


def suggest_addresses(
    q: str, country: typing.Optional[str], session: str
) -> typing.List[typing.Dict[str, str]]:
    """Returns at most ``settings.ADDRESS_SUGGEST_LIMIT`` ``{id, label}`` entries, or [] for
    any reason at all (no provider, provider down, nothing matched, country we don't ship to)."""
    if not _country_is_shippable(country):
        return []

    provider = registry.get_provider()
    if provider is None:
        return []

    try:
        suggestions = provider.suggest(q=q, country=country, session=session)
    except address_exceptions.AddressProviderError as e:
        # Never log `q`: it is the end customer's street address as typed.
        logger.warning(
            "%s Suggest failed (provider=%s): %s", _LOG_PREFIX, getattr(provider, "name", "?"), e
        )
        return []

    limit = int(getattr(settings, "ADDRESS_SUGGEST_LIMIT", 5) or 5)
    ttl = int(getattr(settings, "ADDRESS_SUGGESTION_CACHE_TTL_SECONDS", 600) or 600)

    results = []
    for suggestion in suggestions[:limit]:
        suggestion_id = uuid.uuid4().hex
        cache.set(
            _suggestion_cache_key(session=session, suggestion_id=suggestion_id),
            {
                "provider": getattr(provider, "name", ""),
                "provider_ref": suggestion.provider_ref,
                "address": suggestion.address.as_dict() if suggestion.address else None,
            },
            ttl,
        )
        results.append({"id": suggestion_id, "label": suggestion.label})
    return results


# -- resolve -------------------------------------------------------------------------------


def resolve_address(suggestion_id: str, session: str) -> typing.Optional[dict]:
    """
    None means "this id is not resolvable" and the view answers 404 -- the frontend's cue to
    leave whatever the user typed in place. That covers an expired cache entry, an id from a
    different session, and a provider whose details call failed.
    """
    cached = cache.get(_suggestion_cache_key(session=session, suggestion_id=suggestion_id))
    if cached and cached.get("address"):
        return cached["address"]

    if not cached:
        return None

    # Cache entry exists but holds no address: a provider whose autocomplete returns labels
    # only (not Geoapify) and needs a details call now.
    provider = registry.get_provider()
    if provider is None:
        return None
    try:
        address = provider.resolve(provider_ref=cached.get("provider_ref") or "", session=session)
    except address_exceptions.AddressProviderError as e:
        logger.warning(
            "%s Resolve failed (provider=%s): %s", _LOG_PREFIX, getattr(provider, "name", "?"), e
        )
        return None
    return address.as_dict() if address else None


# -- validate ------------------------------------------------------------------------------


def validate_address(address_data: dict) -> dict:
    """Returns ``{status, suggested?, messages}``. ``suggested`` is present only for
    ``corrected``."""
    address = base.Address.from_dict(address_data)

    provider = registry.get_provider()
    if provider is None:
        return {
            "status": _STATUS_TO_API[src_enums.AddressValidationStatus.UNVERIFIED],
            "messages": [_PROVIDER_UNAVAILABLE_MESSAGE],
        }

    try:
        result = provider.validate(address)
    except address_exceptions.AddressProviderError as e:
        logger.warning(
            "%s Validate failed (provider=%s): %s", _LOG_PREFIX, getattr(provider, "name", "?"), e
        )
        return {
            "status": _STATUS_TO_API[src_enums.AddressValidationStatus.UNVERIFIED],
            "messages": [_PROVIDER_UNAVAILABLE_MESSAGE],
        }

    payload: typing.Dict[str, typing.Any] = {
        "status": _STATUS_TO_API[result.status],
        "messages": list(result.messages),
    }
    if result.suggested is not None:
        payload["suggested"] = result.suggested.as_dict()
    return payload
