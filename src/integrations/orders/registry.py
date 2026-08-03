"""
Maps a distributor's ``Providers.kind`` to its ``DistributorOrderAdapter`` implementation.
Distributors with no entry here (today: SDC, A-Tech, DLG, Rough Country) have no order API of
their own — but may still be order-capable via the Email channel (see get_adapter() below).

Adapters are added one at a time as each distributor's phase lands (see the Purchase Orders
plan) — this file starts empty on purpose so nothing changes for end users until an adapter
is registered.
"""
import logging
import typing

from src import enums as src_enums
from src import models as src_models
from src.integrations import credentials as order_credentials
from src.integrations.orders import base
from src.integrations.orders import email_order

logger = logging.getLogger(__name__)

_ADAPTERS: typing.Dict[int, typing.Type[base.DistributorOrderAdapter]] = {}


def register(provider_kind: int, adapter_cls: typing.Type[base.DistributorOrderAdapter]) -> None:
    _ADAPTERS[provider_kind] = adapter_cls


def supports_ordering(provider_kind: int) -> bool:
    """
    Whether a real, distributor-specific order adapter CLASS is registered for this provider
    kind — this is the API-channel question only. It says nothing about whether a given
    company's connection has actually configured order credentials for it (Keystone's order
    credentials are a separate, optional namespace from its catalog feed; see
    src/integrations/credentials.py), and nothing about Email-channel availability, which
    depends on the company's chosen CompanyProviderOrderAccount.order_method rather than on
    provider kind at all (see get_adapter()). Callers deciding whether to show/allow an
    ordering action for a *specific* connection should use get_adapter() (or
    is_order_ready()) instead, since only that call actually knows whether this connection is
    usable, not just supported in the abstract.
    """
    return provider_kind in _ADAPTERS


def get_adapter(
    company_provider: src_models.CompanyProviders,
    order_account: typing.Optional[src_models.CompanyProviderOrderAccount] = None,
) -> typing.Optional[base.DistributorOrderAdapter]:
    """
    ``order_account``, when given, selects which of this connection's order accounts the
    returned adapter places orders through (see base.DistributorOrderAdapter) — leave None to
    use the connection's implicit default account, which is what every caller meant before
    multi-account support existed and is still correct for the overwhelming majority of
    connections (single account).

    Channel-first: the resolved account's ``order_method`` (src.enums.OrderMethod) is checked
    BEFORE the provider-kind registry — EMAIL always routes to the generic EmailOrderAdapter
    regardless of whether a real, distributor-specific adapter also exists for this kind (a
    company can order some things through Keystone's API and switch to emailing their rep for
    others, from the same connection). API (the default for every account, including every one
    that existed before this field) falls through to the existing kind-registry lookup
    unchanged.

    Returns None when: no order method is usably configured at all, no adapter is registered
    for this provider kind (and it isn't set to Email), or one is registered/selected but the
    account's credentials for that channel are missing/invalid — adapter constructors raise
    ValueError in the credential case (e.g. KeystoneOrderApiClient requires
    account_number/security_key, EmailOrderAdapter requires rep_email), and letting that
    propagate here would crash every caller that expects "no adapter" to be a normal,
    handleable outcome (job processing, capability listing, shipping-method lookup) rather than
    an unhandled exception. Use get_adapter_unavailable_reason() if a caller needs to
    distinguish the cases for a more specific error message.
    """
    account = order_account or order_credentials.get_default_order_account(company_provider)
    if account is not None and account.order_method == src_enums.OrderMethod.EMAIL.value:
        try:
            return email_order.EmailOrderAdapter(company_provider, account)
        except ValueError:
            logger.info(
                "Email order adapter for company_provider_id=%s could not be constructed "
                "(rep_email likely missing/invalid for this account).",
                company_provider.id,
            )
            return None

    adapter_cls = _ADAPTERS.get(company_provider.provider.kind)
    if adapter_cls is None:
        return None
    try:
        return adapter_cls(company_provider, order_account)
    except ValueError:
        logger.info(
            "Order adapter for provider kind=%s company_provider_id=%s could not be "
            "constructed (order credentials likely missing/invalid for this connection).",
            company_provider.provider.kind,
            company_provider.id,
        )
        return None


def get_adapter_unavailable_reason(
    company_provider: src_models.CompanyProviders,
    order_account: typing.Optional[src_models.CompanyProviderOrderAccount] = None,
) -> str:
    """Human-readable reason get_adapter() returned None for this connection — distinguishes
    "not supported at all" from "supported, but this connection isn't configured for it yet",
    since those need different user-facing messages/next steps. ``order_account``, when the
    caller already resolved one, must match whatever was passed to the get_adapter() call this
    is explaining — otherwise this falls back to the connection's default account and can give
    a wrong reason for a non-default account (e.g. "does not support in-app ordering yet" for a
    kind with no API adapter, when the actual account in use has Email configured just fine)."""
    account = order_account or order_credentials.get_default_order_account(company_provider)
    if account is not None and account.order_method == src_enums.OrderMethod.EMAIL.value:
        return "Email ordering isn't fully configured for this connection yet — a rep email is required."
    if not supports_ordering(company_provider.provider.kind):
        return "{} does not support in-app ordering yet.".format(company_provider.provider.kind_name)
    return "{} order credentials aren't configured for this connection yet.".format(
        company_provider.provider.kind_name
    )


# Self-register each implemented adapter. Imported here (rather than via a Django
# AppConfig.ready() hook, which this project doesn't use) so registration happens as soon as
# anything imports this module.
from src.integrations.orders import keystone as _keystone  # noqa: E402
from src.integrations.orders import meyer as _meyer  # noqa: E402
from src.integrations.orders import premier as _premier  # noqa: E402
from src.integrations.orders import turn_14 as _turn_14  # noqa: E402
from src.integrations.orders import wheelpros as _wheelpros  # noqa: E402

register(src_enums.BrandProviderKind.TURN_14.value, _turn_14.Turn14OrderAdapter)
register(src_enums.BrandProviderKind.KEYSTONE.value, _keystone.KeystoneOrderAdapter)
register(src_enums.BrandProviderKind.MEYER.value, _meyer.MeyerOrderAdapter)
register(src_enums.BrandProviderKind.PREMIER_PERFORMANCE.value, _premier.PremierOrderAdapter)
register(src_enums.BrandProviderKind.WHEELPROS.value, _wheelpros.WheelProsOrderAdapter)
