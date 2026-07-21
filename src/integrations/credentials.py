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
"""
import typing

from src import models as src_models


def get_feed_credentials(company_provider: src_models.CompanyProviders) -> typing.Dict:
    return (company_provider.credentials or {}).get("feed", {})


def get_order_credentials(company_provider: src_models.CompanyProviders) -> typing.Dict:
    return (company_provider.credentials or {}).get("order", {})
