"""
Credential resolution for Turn 14's "global cache" -- the data that is fetched once and shared
by every customer.

Turn 14's integration model splits their API in two. Items, item data, fitment, inventory,
locations, dropship controllers and shipping estimates are the same for everybody, so they are
pulled once; pricing, quotes, orders, invoices and tracking are per customer and must use that
customer's own credentials. Our schema has always matched that split -- Turn14Items,
Turn14BrandData, Turn14BrandInventory, Turn14ItemFitment and Turn14Location carry no company
FK, and only Turn14BrandPricing does -- but the code populating those shared tables did not:
it used three different, mutually inconsistent ways of picking whose credentials to spend.

    CompanyProviders.filter(primary=True)          -> whichever connection is flagged primary
    Company.objects.filter(name='TICK_PERFORMANCE') -> a company matched by literal name
    CompanyBrands.filter(brand=brand).first()       -> whichever company sorted first

The third is the worst: shared catalog rows were written using an arbitrary customer's
credentials, non-deterministically, drawing down that customer's 5 000/hour budget to populate
data that was never theirs. This module replaces all three.

Handover: when Turn 14 issues integrator-level credentials, set TURN14_GLOBAL_CLIENT_ID and
TURN14_GLOBAL_CLIENT_SECRET. Until then the resolver falls back to the primary connection, so
behaviour is unchanged by default and the switch is a config change rather than a deploy.
"""
import logging
import typing

from django.conf import settings

from src import enums as src_enums
from src import models as src_models
from src.integrations import credentials as credentials_helper
from src.integrations.clients.turn_14 import client as turn_14_client

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[TURN-14-GLOBAL]"


class GlobalCredentialsUnavailable(Exception):
    """
    No usable credentials for the shared tables.

    Raised rather than logged-and-skipped on purpose. The previous code caught the equivalent
    ValueError per brand and continued, so a house account with blank credentials produced a
    clean-looking run that silently fetched nothing for all 464 brands -- which is exactly what
    was happening: the hardcoded TICK_PERFORMANCE connection has empty client_id/client_secret,
    so every code path keyed to it was dead. A scheduled task that cannot authenticate should
    fail loudly and show up as FAILED in scheduled_task_execution.
    """


def get_global_credentials() -> typing.Dict:
    """
    Credentials for the shared Turn 14 tables.

    Prefers the configured integrator credentials; falls back to the primary=True connection.
    Raises :class:`GlobalCredentialsUnavailable` rather than returning something unusable.
    """
    configured_id = getattr(settings, "TURN14_GLOBAL_CLIENT_ID", "") or ""
    configured_secret = getattr(settings, "TURN14_GLOBAL_CLIENT_SECRET", "") or ""
    if configured_id and configured_secret:
        logger.info("{} Using configured integrator credentials.".format(_LOG_PREFIX))
        return {"client_id": configured_id, "client_secret": configured_secret}

    company_provider = get_global_company_provider()
    if not company_provider:
        raise GlobalCredentialsUnavailable(
            "No Turn 14 connection is marked primary=True and TURN14_GLOBAL_CLIENT_ID is unset. "
            "The shared catalog tables have no credentials to sync with."
        )

    credentials = credentials_helper.get_feed_credentials(company_provider)
    if not credentials.get("client_id") or not credentials.get("client_secret"):
        raise GlobalCredentialsUnavailable(
            "Turn 14 connection id={} (company={}) is the global source but its credentials are "
            "empty. Either populate them, mark a different connection primary=True, or set "
            "TURN14_GLOBAL_CLIENT_ID/_SECRET.".format(
                company_provider.id, company_provider.company.name
            )
        )

    logger.info(
        "{} Using connection id={} (company={}) as the global source.".format(
            _LOG_PREFIX, company_provider.id, company_provider.company.name
        )
    )
    return credentials


def get_global_company_provider() -> typing.Optional[src_models.CompanyProviders]:
    """
    The connection acting as the house account for shared data.

    Note this can be a *customer's* connection today -- production has primary=True on a
    customer, not on the house account -- which is precisely why the settings override exists.
    """
    return (
        src_models.CompanyProviders.objects.select_related("company")
        .filter(
            provider__kind=src_enums.BrandProviderKind.TURN_14.value,
            provider__status=src_enums.BrandProviderStatus.ACTIVE.value,
            primary=True,
        )
        .first()
    )


def get_global_owner_company() -> typing.Optional[src_models.Company]:
    """
    The Company that owns CompanyBrands rows created for newly-discovered/shared Turn 14 brands.

    Every other provider anchors this to a company literally named 'TICK_PERFORMANCE'. For Turn
    14 that company's own Turn 14 credentials are empty, which is exactly the bug this module
    exists to fix -- so credentials and brand-ownership are resolved from the same place here:
    whichever company backs the global connection (today: the primary=True connection, i.e.
    Trident Motorsports). Falls back to TICK_PERFORMANCE-by-name only if the global connection
    is credential-only (TURN14_GLOBAL_CLIENT_ID/_SECRET set with no backing CompanyProviders row
    to derive a company from) and there is nothing else to anchor ownership to.
    """
    company_provider = get_global_company_provider()
    if company_provider:
        return company_provider.company
    return src_models.Company.objects.filter(name="TICK_PERFORMANCE").first()


def get_global_client() -> turn_14_client.Turn14ApiClient:
    """
    One client for a whole sweep.

    Callers must build this once and pass it down rather than constructing per brand: the
    rate limiter and token cache are keyed by client_id so a per-brand client is no longer
    expensive, but it is still 464 objects and 464 chances to resolve credentials differently.

    Base URL is set on this instance only (settings.TURN14_GLOBAL_BASE_URL, defaults to
    production) -- never on the Turn14ApiClient class -- so redirecting the shared catalog
    sweep to Turn 14's sandbox host never affects a client built elsewhere (per-company pricing,
    order placement), both of which always use that customer's own credentials against
    production regardless of this setting.
    """
    try:
        client = turn_14_client.Turn14ApiClient(credentials=get_global_credentials())
        client.API_BASE_URL = settings.TURN14_GLOBAL_BASE_URL
        return client
    except ValueError as e:
        raise GlobalCredentialsUnavailable(str(e)) from e
