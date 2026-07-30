"""
Accessors for the two namespaces inside ``CompanyProviders.credentials``: ``feed`` (catalog/
pricing sync — FTP, SFTP, OAuth, whatever the vendor's read-only feed needs) and ``order``
(order-placement API credentials, only populated for vendors with a registered
``DistributorOrderAdapter`` — see ``src/integrations/orders/``).

These two namespaces are independent and always entered/validated separately, even when a
vendor's feed and order credentials happen to use the same values (Turn14: the same OAuth
client_id/client_secret pair works for both, but is submitted and tested against each API
independently — see ``_validate_turn14_order_connection`` — since catalog-API and order-API
access are separate permission grants on Turn14's side) or are entirely disjoint (Keystone: FTP
for the feed, a SOAP security key + account number for ordering) — callers should never read
``company_provider.credentials`` directly; always go through one of these two functions so the
right namespace is used regardless of which shape a given vendor happens to have.

A connection can also have one or more ADDITIONAL named order accounts beyond the implicit
default above (``src.models.CompanyProviderOrderAccount`` — e.g. a company that places its own
shop's orders through one Keystone account and drop-ships through a second). See
``get_order_credentials`` for the resolution order between the implicit default and these extra
accounts.
"""
import typing

from src import models as src_models


def get_feed_credentials(company_provider: src_models.CompanyProviders) -> typing.Dict:
    return (company_provider.credentials or {}).get("feed", {})


def get_order_credentials(
    company_provider: src_models.CompanyProviders,
    order_account: typing.Optional[src_models.CompanyProviderOrderAccount] = None,
) -> typing.Dict:
    """
    ``order_account``, when given, must belong to this connection (callers are responsible for
    that — see purchase_orders_services._resolve_order_account) and its credentials are used
    directly. Otherwise resolves to the implicit default: ``company_provider.credentials["order"]``
    if configured, else the earliest-created additional CompanyProviderOrderAccount, else {} (no
    order credentials configured at all).
    """
    if order_account is not None:
        return order_account.credentials or {}
    embedded = (company_provider.credentials or {}).get("order")
    if embedded:
        return embedded
    default_account = company_provider.order_accounts.filter(active=True).order_by("created_at").first()
    return default_account.credentials if default_account else {}
