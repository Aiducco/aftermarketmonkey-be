"""
Accessors for a connection's credentials. ``feed`` (catalog/pricing sync — FTP, SFTP, OAuth,
whatever the vendor's read-only feed needs) lives directly on
``CompanyProviders.credentials["feed"]``. ``order`` (order-placement API credentials, only
populated for vendors with a registered ``DistributorOrderAdapter`` — see
``src/integrations/orders/``) lives entirely in ``src.models.CompanyProviderOrderAccount`` —
every connection with order credentials configured has exactly one row there with
``is_default=True``, plus optionally more named accounts beyond that (e.g. a company that places
its own shop's orders through one Keystone account and drop-ships through a second).
``CompanyProviders.credentials`` never carries an "order" key — always go through
:func:`get_order_credentials` rather than reading either source directly.
"""
import typing

from src import models as src_models


def get_feed_credentials(company_provider: src_models.CompanyProviders) -> typing.Dict:
    return (company_provider.credentials or {}).get("feed", {})


def get_default_order_account(
    company_provider: src_models.CompanyProviders,
) -> typing.Optional[src_models.CompanyProviderOrderAccount]:
    return company_provider.order_accounts.filter(is_default=True, active=True).first()


def get_order_credentials(
    company_provider: src_models.CompanyProviders,
    order_account: typing.Optional[src_models.CompanyProviderOrderAccount] = None,
) -> typing.Dict:
    """
    ``order_account``, when given, must belong to this connection (callers are responsible for
    that — see purchase_orders_services._resolve_order_account) and its credentials are used
    directly. Otherwise resolves to this connection's default account (see
    :func:`get_default_order_account`), or ``{}`` if none is configured yet.
    """
    if order_account is not None:
        return order_account.credentials or {}
    default_account = get_default_order_account(company_provider)
    return (default_account.credentials if default_account else {}) or {}
