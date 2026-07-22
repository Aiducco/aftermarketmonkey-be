"""
Maps a distributor's Providers.kind to its LiveInventoryProvider implementation. Distributors
with no entry here have no on-demand live-inventory refresh yet; the frontend falls back to
showing the last feed-synced ProviderPartInventory values only.

Providers are added one at a time as each distributor's live-inventory lookup is confirmed
against their API - this file starts with just Turn14 on purpose.
"""
import logging
import typing

from src import enums as src_enums
from src import models as src_models
from src.integrations.live_inventory import base

logger = logging.getLogger(__name__)

_PROVIDERS: typing.Dict[int, typing.Type[base.LiveInventoryProvider]] = {}


def register(provider_kind: int, provider_cls: typing.Type[base.LiveInventoryProvider]) -> None:
    _PROVIDERS[provider_kind] = provider_cls


def supports_live_inventory(provider_kind: int) -> bool:
    """Whether a live-inventory provider CLASS exists for this provider kind at all."""
    return provider_kind in _PROVIDERS


def get_provider(
    company_provider: src_models.CompanyProviders,
) -> typing.Optional[base.LiveInventoryProvider]:
    """
    Returns None both when no provider is registered for this kind AND when one is registered
    but this connection's feed credentials are missing/invalid - constructors raise ValueError
    in the latter case (matching Turn14ApiClient / orders.registry.get_adapter's convention).
    """
    provider_cls = _PROVIDERS.get(company_provider.provider.kind)
    if provider_cls is None:
        return None
    try:
        return provider_cls(company_provider)
    except ValueError:
        logger.info(
            "Live inventory provider for provider kind=%s company_provider_id=%s could not be "
            "constructed (feed credentials likely missing/invalid for this connection).",
            company_provider.provider.kind,
            company_provider.id,
        )
        return None


# Self-register each implemented provider. Imported here (rather than an AppConfig.ready()
# hook, which this project doesn't use) so registration happens as soon as anything imports
# this module.
from src.integrations.live_inventory import turn_14 as _turn_14  # noqa: E402

register(src_enums.BrandProviderKind.TURN_14.value, _turn_14.Turn14LiveInventoryProvider)
