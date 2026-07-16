"""
Maps a distributor's ``Providers.kind`` to its ``DistributorOrderAdapter`` implementation.
Distributors with no entry here (today: SDC, A-Tech, DLG, Wheel Pros, Rough Country) have no
in-app ordering — the API/frontend falls back to the existing catalog redirect link
(``_provider_go_to_link`` in ``src/api/services/parts.py``).

Adapters are added one at a time as each distributor's phase lands (see the Purchase Orders
plan) — this file starts empty on purpose so nothing changes for end users until an adapter
is registered.
"""
import typing

from src import enums as src_enums
from src import models as src_models
from src.integrations.orders import base

_ADAPTERS: typing.Dict[int, typing.Type[base.DistributorOrderAdapter]] = {}


def register(provider_kind: int, adapter_cls: typing.Type[base.DistributorOrderAdapter]) -> None:
    _ADAPTERS[provider_kind] = adapter_cls


def supports_ordering(provider_kind: int) -> bool:
    return provider_kind in _ADAPTERS


def get_adapter(
    company_provider: src_models.CompanyProviders,
) -> typing.Optional[base.DistributorOrderAdapter]:
    adapter_cls = _ADAPTERS.get(company_provider.provider.kind)
    if adapter_cls is None:
        return None
    return adapter_cls(company_provider)


# Self-register each implemented adapter. Imported here (rather than via a Django
# AppConfig.ready() hook, which this project doesn't use) so registration happens as soon as
# anything imports this module.
from src.integrations.orders import turn_14 as _turn_14  # noqa: E402

register(src_enums.BrandProviderKind.TURN_14.value, _turn_14.Turn14OrderAdapter)
