"""
Maps ``settings.ADDRESS_PROVIDER`` to its :class:`base.AddressProvider` implementation.

Same shape as ``src/integrations/live_inventory/registry.py``, with one difference: the choice
is a single global config value rather than per-company, because address lookup is a platform
capability on our own API key -- no customer connects their own Geoapify account.
"""
import logging
import typing

from django.conf import settings

from src.integrations.address import base
from src.integrations.address import exceptions
from src.integrations.address import geoapify

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[ADDRESS-REGISTRY]"

_PROVIDERS: typing.Dict[str, typing.Type[base.AddressProvider]] = {
    geoapify.GeoapifyAddressProvider.name: geoapify.GeoapifyAddressProvider,
}


def get_provider() -> typing.Optional[base.AddressProvider]:
    """
    Returns None -- never raises -- when ADDRESS_PROVIDER names something unknown or the
    configured provider has no API key. Callers degrade to "no suggestions / unverified"
    (see src.api.services.address), which is the same behavior as the provider being down,
    so a missing key can't take the quote flow with it.
    """
    provider_name = (getattr(settings, "ADDRESS_PROVIDER", "") or "").strip().lower()
    provider_cls = _PROVIDERS.get(provider_name)
    if provider_cls is None:
        logger.warning(
            "%s No address provider registered for ADDRESS_PROVIDER=%r; address lookup disabled.",
            _LOG_PREFIX,
            provider_name,
        )
        return None

    try:
        return provider_cls()
    except exceptions.AddressProviderNotConfigured as e:
        logger.warning("%s Provider %r is not configured: %s", _LOG_PREFIX, provider_name, e)
        return None
