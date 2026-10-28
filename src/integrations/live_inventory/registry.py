"""
Maps a distributor's Providers.kind to its LiveInventoryProvider implementation. Distributors
with no entry here have no on-demand live-inventory refresh yet; the frontend falls back to
showing the last feed-synced ProviderPartInventory values only.

Providers are added one at a time as each distributor's live-inventory lookup is confirmed
against their API - this file starts with just Turn14 on purpose.

Note: which credential namespace a provider needs (feed vs. order - see
src.integrations.credentials) is that provider's own choice, made in its __init__. Turn14 uses
feed credentials (its catalog OAuth client works for both feed and order); Keystone uses order
credentials (CheckInventory is part of its order-placement SOAP service, not its FTP feed).
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
    but this connection's credentials are missing/invalid - constructors raise ValueError in
    the latter case (matching Turn14ApiClient / orders.registry.get_adapter's convention).
    """
    provider_cls = _PROVIDERS.get(company_provider.provider.kind)
    if provider_cls is None:
        return None
    try:
        return provider_cls(company_provider)
    except ValueError:
        logger.info(
            "Live inventory provider for provider kind=%s company_provider_id=%s could not be "
            "constructed (credentials likely missing/invalid for this connection).",
            company_provider.provider.kind,
            company_provider.id,
        )
        return None


# Self-register each implemented provider. Imported here (rather than an AppConfig.ready()
# hook, which this project doesn't use) so registration happens as soon as anything imports
# this module.
from src.integrations.live_inventory import allpro as _allpro  # noqa: E402
from src.integrations.live_inventory import atd as _atd  # noqa: E402
from src.integrations.live_inventory import keystone as _keystone  # noqa: E402
from src.integrations.live_inventory import motorstate as _motorstate  # noqa: E402
from src.integrations.live_inventory import meyer as _meyer  # noqa: E402
from src.integrations.live_inventory import ntw as _ntw  # noqa: E402
from src.integrations.live_inventory import premier as _premier  # noqa: E402
from src.integrations.live_inventory import tristate as _tristate  # noqa: E402
from src.integrations.live_inventory import turn_14 as _turn_14  # noqa: E402
from src.integrations.live_inventory import wheelpros as _wheelpros  # noqa: E402

register(src_enums.BrandProviderKind.TURN_14.value, _turn_14.Turn14LiveInventoryProvider)
register(src_enums.BrandProviderKind.KEYSTONE.value, _keystone.KeystoneLiveInventoryProvider)
register(src_enums.BrandProviderKind.MEYER.value, _meyer.MeyerLiveInventoryProvider)
register(src_enums.BrandProviderKind.PREMIER_PERFORMANCE.value, _premier.PremierLiveInventoryProvider)
register(src_enums.BrandProviderKind.WHEELPROS.value, _wheelpros.WheelProsLiveInventoryProvider)
register(src_enums.BrandProviderKind.TRI_STATE.value, _tristate.TriStateLiveInventoryProvider)
register(src_enums.BrandProviderKind.NTW.value, _ntw.NtwLiveInventoryProvider)
# ATD needs no credentials of its own here: one dealer credential set covers catalog,
# availability and ordering, so this works wherever the ATD feed is connected. See
# live_inventory/atd.py on why the availability endpoint's response differs from the
# catalog sweep's.
register(src_enums.BrandProviderKind.ATD.value, _atd.ATDLiveInventoryProvider)
# AllPro reads its FM order credentials (not the FTP feed's) -- same split as NTW, since the
# live stock check is part of their ordering service.
register(src_enums.BrandProviderKind.ALLPRO_DISTRIBUTING.value, _allpro.AllProLiveInventoryProvider)
# Motor State has no stock endpoint at all, so the live check is a one-part OrderInquiry --
# non-committal, and its QuantityAvailable is the live figure. See live_inventory/motorstate.py.
register(src_enums.BrandProviderKind.MOTOR_STATE_DISTRIBUTING.value, _motorstate.MotorStateLiveInventoryProvider)
