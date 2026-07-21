"""
Maps a distributor's ``Providers.kind`` to its ``DistributorOrderAdapter`` implementation.
Distributors with no entry here (today: SDC, A-Tech, DLG, Wheel Pros, Rough Country) have no
in-app ordering — the API/frontend falls back to the existing catalog redirect link
(``_provider_go_to_link`` in ``src/api/services/parts.py``).

Adapters are added one at a time as each distributor's phase lands (see the Purchase Orders
plan) — this file starts empty on purpose so nothing changes for end users until an adapter
is registered.
"""
import logging
import typing

from src import enums as src_enums
from src import models as src_models
from src.integrations.orders import base

logger = logging.getLogger(__name__)

_ADAPTERS: typing.Dict[int, typing.Type[base.DistributorOrderAdapter]] = {}


def register(provider_kind: int, adapter_cls: typing.Type[base.DistributorOrderAdapter]) -> None:
    _ADAPTERS[provider_kind] = adapter_cls


def supports_ordering(provider_kind: int) -> bool:
    """
    Whether an order adapter CLASS exists for this provider kind at all — this says nothing
    about whether a given company's connection has actually configured order credentials for
    it (Keystone's order credentials are a separate, optional namespace from its catalog feed;
    see src/integrations/credentials.py). Callers deciding whether to show/allow an ordering
    action for a *specific* connection should use get_adapter() (or is_order_ready()) instead,
    since only that call actually knows whether this connection is usable, not just supported
    in the abstract.
    """
    return provider_kind in _ADAPTERS


def get_adapter(
    company_provider: src_models.CompanyProviders,
) -> typing.Optional[base.DistributorOrderAdapter]:
    """
    Returns None both when no adapter is registered for this provider kind AND when one is
    registered but this connection's order credentials are missing/invalid — adapter
    constructors raise ValueError in the latter case (e.g. KeystoneOrderApiClient requires
    account_number/security_key), and letting that propagate here would crash every caller
    that expects "no adapter" to be a normal, handleable outcome (job processing, capability
    listing, shipping-method lookup) rather than an unhandled exception. Use
    get_adapter_unavailable_reason() if a caller needs to distinguish the two cases for a more
    specific error message.
    """
    adapter_cls = _ADAPTERS.get(company_provider.provider.kind)
    if adapter_cls is None:
        return None
    try:
        return adapter_cls(company_provider)
    except ValueError:
        logger.info(
            "Order adapter for provider kind=%s company_provider_id=%s could not be "
            "constructed (order credentials likely missing/invalid for this connection).",
            company_provider.provider.kind,
            company_provider.id,
        )
        return None


def get_adapter_unavailable_reason(company_provider: src_models.CompanyProviders) -> str:
    """Human-readable reason get_adapter() returned None for this connection — distinguishes
    "not supported at all" from "supported, but this connection isn't configured for it yet",
    since those need different user-facing messages/next steps."""
    if not supports_ordering(company_provider.provider.kind):
        return "{} does not support in-app ordering yet.".format(company_provider.provider.kind_name)
    return "{} order credentials aren't configured for this connection yet.".format(
        company_provider.provider.kind_name
    )


# Self-register each implemented adapter. Imported here (rather than via a Django
# AppConfig.ready() hook, which this project doesn't use) so registration happens as soon as
# anything imports this module.
from src.integrations.orders import keystone as _keystone  # noqa: E402
from src.integrations.orders import turn_14 as _turn_14  # noqa: E402

register(src_enums.BrandProviderKind.TURN_14.value, _turn_14.Turn14OrderAdapter)
register(src_enums.BrandProviderKind.KEYSTONE.value, _keystone.KeystoneOrderAdapter)
