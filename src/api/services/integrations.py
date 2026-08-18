import logging
import typing

from django.conf import settings
from django.core import exceptions as django_core_exceptions
from django.core import validators as django_validators
from django.core.paginator import Paginator
from django.db import IntegrityError, transaction
from django.db.models import ProtectedError
from django.utils import timezone

from src import constants as src_constants
from src import enums as src_enums
from src import models as src_models
from src.api.services import billing as billing_services
from src.integrations import credentials as credentials_helper
from src.integrations.clients.atech import client as atech_client
from src.integrations.clients.atech import exceptions as atech_exceptions
from src.integrations.clients.elite_wheel import client as elite_wheel_client
from src.integrations.clients.elite_wheel import exceptions as elite_wheel_exceptions
from src.integrations.clients.helmet_house import client as helmet_house_client
from src.integrations.clients.helmet_house import exceptions as helmet_house_exceptions
from src.integrations.clients.keystone import client as keystone_client
from src.integrations.clients.keystone import exceptions as keystone_exceptions
from src.integrations.clients.keystone import order_client as keystone_order_client
from src.integrations.clients.meyer import client as meyer_client
from src.integrations.clients.meyer import exceptions as meyer_exceptions
from src.integrations.clients.meyer import order_client as meyer_order_client
from src.integrations.clients.motorstate import client as motorstate_client
from src.integrations.clients.motorstate import exceptions as motorstate_exceptions
from src.integrations.clients.premier import client as premier_client
from src.integrations.clients.premier import exceptions as premier_exceptions
from src.integrations.clients.premier import order_client as premier_order_client
from src.integrations.clients.rough_country import client as rough_country_client
from src.integrations.clients.rough_country import exceptions as rough_country_exceptions
from src.integrations.clients.the_wheel_group import client as the_wheel_group_client
from src.integrations.clients.the_wheel_group import exceptions as the_wheel_group_exceptions
from src.integrations.clients.tirerack import client as tirerack_client
from src.integrations.clients.tirerack import exceptions as tirerack_exceptions
from src.integrations.clients.turn_14 import client as turn14_client
from src.integrations.clients.wps import client as wps_client
from src.integrations.clients.wps import exceptions as wps_exceptions
from src.integrations.clients.turn_14 import exceptions as turn14_exceptions
from src.integrations.clients.turn_14 import order_client as turn14_order_client
from src.integrations.clients.vossen import client as vossen_client
from src.integrations.clients.vossen import exceptions as vossen_exceptions
from src.integrations.clients.wheelpros import client as wheelpros_client
from src.integrations.clients.wheelpros import exceptions as wheelpros_exceptions
from src.integrations.clients.wheelpros import order_client as wheelpros_order_client
from src.integrations.orders import registry as order_registry
from src.integrations.services import integration_pricing_sync_jobs, relay_sftp_provisioning

logger = logging.getLogger(__name__)

_LOG_PREFIX = '[INTEGRATIONS-SERVICES]'

# Stable error codes returned alongside a human-readable "message" from connect_provider /
# update_connection, so the frontend can branch on `error_code` (e.g. highlight the password
# field, or show a "contact distributor support" banner) instead of parsing message text.
CONNECTION_ERROR_MISSING_FIELDS = "missing_fields"
CONNECTION_ERROR_INVALID_INPUT = "invalid_input"
CONNECTION_ERROR_INVALID_CREDENTIALS = "invalid_credentials"
CONNECTION_ERROR_PERMISSION_DENIED = "permission_denied"
CONNECTION_ERROR_CONNECTION_FAILED = "connection_failed"
CONNECTION_ERROR_NOT_FOUND = "not_found"
CONNECTION_ERROR_PLAN_LIMIT_REACHED = "plan_limit_reached"


def _render_relay_instructions_html(
    catalog_entry: typing.Dict[str, typing.Any],
    company: typing.Optional[src_models.Company],
) -> typing.Optional[str]:
    """
    For ``relay_provisioned`` catalog entries, substitute the company's own auto-provisioned
    relay SFTP username/password into the ``{{SFTP_USER}}`` / ``{{SFTP_PASSWORD}}`` placeholders
    so the distributor rep gets real, ready-to-use credentials instead of a request-by-email flow.
    """
    template = catalog_entry.get("installation_instructions_html") or ""
    if not catalog_entry.get("relay_provisioned"):
        return template or None
    username = getattr(company, "relay_sftp_username", None) if company else None
    password = getattr(company, "relay_sftp_password", None) if company else None
    if not username or not password:
        return (
            "<p>Your dedicated SFTP account is being created and will appear here shortly &mdash; "
            "check back in a few minutes. Still missing? Contact "
            "<a href=\"mailto:support@aftermarketscout.com\">support@aftermarketscout.com</a>.</p>"
        )
    return template.replace("{{SFTP_USER}}", username).replace("{{SFTP_PASSWORD}}", password)


def _normalize_credential_value(value: typing.Any) -> typing.Optional[typing.Any]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        if isinstance(value, float) and value != value:
            return None
        return value
    s = str(value).strip()
    return s if s else None


_SENSITIVE_CREDENTIAL_KEY_SUBSTRINGS = ("password", "secret", "key", "token")


def _credential_key_sensitive(key: str) -> bool:
    """
    Substring match against the field NAME (not a fixed allow-list), since every provider's
    credential fields are declared per-entry in PROVIDER_CATALOG and new ones get added as
    distributors are onboarded — a fixed list would silently miss a new provider's secret
    field the same way "password"/"secret" alone missed Keystone's "security_key" and
    "api_key"/"access_token" (neither contains "password" or "secret"). "key" and "token" are
    broad on purpose: over-redacting a field that turns out not to be secret is a harmless
    display nuisance, under-redacting a real secret is a credential leak.
    """
    lower = (key or "").lower()
    return any(s in lower for s in _SENSITIVE_CREDENTIAL_KEY_SUBSTRINGS)


def _merge_namespace_credentials(
    section: typing.Dict[str, typing.Any],
    required: typing.List[str],
    optional: typing.List[str],
    patch: typing.Dict[str, typing.Any],
) -> typing.Tuple[typing.Optional[str], typing.Optional[str]]:
    """Merges `patch` onto `section` in place. Only keys in required+optional are allowed.
    Non-empty values overwrite; empty/null for a **sensitive** key (password, secret) leaves the
    previous value unchanged so clients can update other fields without resubmitting secrets;
    empty/null for a non-sensitive key clears it. Returns (error_message, error_code)."""
    allowed = set(required) | set(optional)
    for k, v in (patch or {}).items():
        key = str(k)
        if key not in allowed:
            return "Unknown credential field: {}".format(key), CONNECTION_ERROR_INVALID_INPUT
        nv = _normalize_credential_value(v)
        if nv is not None:
            section[key] = nv
        elif not _credential_key_sensitive(key):
            section.pop(key, None)
    return None, None


def _merge_update_credentials(
    catalog_entry: typing.Dict[str, typing.Any],
    existing: typing.Optional[typing.Dict[str, typing.Any]],
    patch: typing.Dict[str, typing.Any],
) -> typing.Tuple[typing.Optional[typing.Dict[str, typing.Any]], typing.Optional[str], typing.Optional[str]]:
    """
    Apply a partial patch shaped ``{"feed": {...}, "order": {...}}`` onto the existing namespaced
    credentials dict. Either namespace may be omitted from the patch entirely, in which case that
    namespace is left untouched (this is how a feed-only update leaves an already-configured
    "order" section alone, and vice versa) — at least one of the two must be present, though.
    ``existing`` may itself be ``None``/``{}`` with no prior "feed" data at all: this is also how
    a brand-new connection can be created order-first, with "feed" configured later in a separate
    call — see :func:`connect_provider`, which uses this same function (not a separate "build"
    path) for exactly that reason. Returns (credentials, error_message, error_code).
    """
    patch = patch or {}
    if "feed" not in patch and "order" not in patch:
        # Neither namespace present — covers both a legacy/flat-shaped patch (bare credential
        # fields, no "feed"/"order" wrapper) and a plain empty body ({} or None). Either would
        # otherwise silently apply no changes at all and return a 200 with nothing actually
        # saved/created. Credentials are namespaced now; reject rather than no-op so a stale
        # client finds out immediately instead of believing a save succeeded.
        return (
            None,
            'Credentials must be nested under "feed" and/or "order".',
            CONNECTION_ERROR_INVALID_INPUT,
        )

    existing = existing or {}
    out = {"feed": dict(existing.get("feed") or {}), "order": dict(existing.get("order") or {})}

    feed_required = [str(f) for f in (catalog_entry.get("connection_required_fields") or [])]
    feed_optional = [str(f) for f in (catalog_entry.get("connection_optional_fields") or [])]
    if "feed" in patch:
        err, err_code = _merge_namespace_credentials(out["feed"], feed_required, feed_optional, patch.get("feed"))
        if err:
            return None, err, err_code

    order_required = [str(f) for f in (catalog_entry.get("order_connection_required_fields") or [])]
    order_optional = [str(f) for f in (catalog_entry.get("order_connection_optional_fields") or [])]
    if "order" in patch:
        err, err_code = _merge_namespace_credentials(out["order"], order_required, order_optional, patch.get("order"))
        if err:
            return None, err, err_code
    elif (
        "feed" in patch
        and not order_required
        and not order_optional
        and order_registry.supports_ordering(catalog_entry["kind"].value)
    ):
        # This provider's order credentials mirror "feed" (no distinct order field list declared)
        # — if the patch touched "feed" and didn't explicitly touch "order", re-derive "order" from
        # the freshly patched "feed" so rotating a shared credential (e.g. Turn14's client
        # id/secret) doesn't leave "order" silently pointing at the old, now-invalid value.
        out["order"] = dict(out["feed"])

    # Feed fields are all-or-nothing, same as order below: if the section has anything set at
    # all, every required feed field must be present. An entirely empty "feed" (never submitted,
    # by this request or any prior one) is valid — that's an order-only connection, feed just
    # isn't configured yet.
    if out["feed"] and feed_required:
        missing = [f for f in feed_required if not _normalize_credential_value(out["feed"].get(f))]
        if missing:
            return (
                None,
                "Missing required fields: {}".format(", ".join(missing)),
                CONNECTION_ERROR_MISSING_FIELDS,
            )

    # Order fields are all-or-nothing: if the section has anything set at all, every required
    # order field must be present, or every order-placement call against it would fail anyway.
    if out["order"] and order_required:
        missing_order = [f for f in order_required if not _normalize_credential_value(out["order"].get(f))]
        if missing_order:
            return (
                None,
                "Missing required order fields: {}".format(", ".join(missing_order)),
                CONNECTION_ERROR_MISSING_FIELDS,
            )

    return out, None, None


def _provider_ui_metadata(provider: src_models.Providers) -> typing.Dict[str, typing.Optional[str]]:
    """Display name and icon for catalog / connections UI (same sources as integrations catalog)."""
    kind_name = (provider.kind_name or "").strip()
    display = src_constants.PROVIDER_DISPLAY_NAMES.get(kind_name) or provider.name
    icon = src_constants.PROVIDER_IMAGE_URLS.get(kind_name)
    if not icon:
        for entry in src_constants.PROVIDER_CATALOG:
            if entry["kind"].value == provider.kind:
                icon = entry.get("icon_url") or ""
                break
    return {
        "provider_display_name": display,
        "provider_icon_url": icon or None,
    }


def get_providers_catalog(company_id: int) -> typing.Dict:
    """
    Get integrations catalog: all providers with connection status for the company.
    Includes active integrations (coming_soon=False) and coming-soon distributors
    (coming_soon=True, always connected=False, no connection fields).
    """
    logger.info('{} Fetching providers catalog for company_id: {}.'.format(
        _LOG_PREFIX, company_id
    ))

    company = src_models.Company.objects.filter(id=company_id).first()

    # All connections for this company, keyed by provider_id — avoids an N+1 query in the
    # catalog loop below (one query total instead of one per connected provider).
    company_providers_by_provider_id = {
        cp.provider_id: cp
        for cp in src_models.CompanyProviders.objects.filter(company_id=company_id)
    }

    # Get all providers from DB (by kind)
    providers_by_kind = {
        p.kind: p for p in src_models.Providers.objects.all()
    }

    catalog = []

    # Active integrations — driven by PROVIDER_CATALOG
    for entry in src_constants.visible_provider_catalog():
        kind_value = entry["kind"].value
        provider = providers_by_kind.get(kind_value)
        if not provider:
            continue

        company_provider = company_providers_by_provider_id.get(provider.id)
        # "Connected" tracks the Product feed specifically, not just row existence -- a company
        # can have a CompanyProviders row with only Ordering configured and feed credentials
        # empty (see connect_provider's docstring: a distributor can be "connected" via Ordering
        # alone), or with the feed explicitly disconnected (disconnect_provider(namespace="feed")
        # clears credentials.feed to {} without deleting the row if Ordering still exists). Either
        # way there's no catalog/pricing/inventory data flowing, so the card should read as "not
        # connected" even though company_provider_id is still populated below for managing the
        # Ordering side. order_status/order_status_* already carry Ordering's own connectivity.
        connected = bool(company_provider and (company_provider.credentials or {}).get("feed"))

        kind_name = provider.kind_name or ""
        display_name = src_constants.PROVIDER_DISPLAY_NAMES.get(
            kind_name, kind_name
        ) or provider.name

        catalog.append({
            "id": provider.id,
            "name": provider.name,
            "display_name": display_name,
            "description": entry.get("description", ""),
            "icon_url": entry.get("icon_url") or None,
            "category": entry.get("category", ""),
            "connection_required_fields": entry.get("connection_required_fields", []),
            "connection_optional_fields": entry.get("connection_optional_fields", []),
            "supports_ordering": _catalog_supports_ordering_display(entry),
            "order_credentials_mirror_feed": _catalog_order_credentials_mirror_feed(entry),
            "order_connection_required_fields": entry.get("order_connection_required_fields", []),
            "order_connection_optional_fields": entry.get("order_connection_optional_fields", []),
            "email_order_connection_required_fields": entry.get("email_order_connection_required_fields", []),
            "email_order_connection_optional_fields": entry.get("email_order_connection_optional_fields", []),
            "installation_instructions_html": _render_relay_instructions_html(entry, company),
            "relay_provisioned": bool(entry.get("relay_provisioned")),
            "connected": connected,
            "company_provider_id": company_provider.id if company_provider else None,
            "kind": kind_value,
            "kind_name": kind_name,
            "coming_soon": False,
            "integration_time": entry.get("integration_time") or None,
            # Live connectivity/sync status — see CompanyProviderConnectionStatus. Null when
            # not connected, or when connected but not checked yet (e.g. just created, cron
            # hasn't run). "connected"/"ingesting"/"waiting"/"failing".
            "status": company_provider.status if company_provider else None,
            "status_name": company_provider.status_name if company_provider else None,
            "status_reason": company_provider.status_reason if company_provider else None,
            "status_checked_at": (
                company_provider.status_checked_at.isoformat()
                if company_provider and company_provider.status_checked_at
                else None
            ),
            # Order-placement connectivity — see CompanyProviderOrderConnectionStatus. Null when
            # not connected, or connected but order credentials not configured.
            "order_status": company_provider.order_status if company_provider else None,
            "order_status_name": company_provider.order_status_name if company_provider else None,
            "order_status_reason": company_provider.order_status_reason if company_provider else None,
            "order_status_checked_at": (
                company_provider.order_status_checked_at.isoformat()
                if company_provider and company_provider.order_status_checked_at
                else None
            ),
        })

    # Coming soon distributors — driven by COMING_SOON_PROVIDERS
    for entry in src_constants.COMING_SOON_PROVIDERS:
        kind_value = entry["kind"].value
        provider = providers_by_kind.get(kind_value)
        if not provider:
            continue

        catalog.append({
            "id": provider.id,
            "name": provider.name,
            "display_name": provider.name,
            "description": "",
            "icon_url": entry.get("icon_url") or None,
            "category": entry.get("category", "Distributors"),
            "connection_required_fields": [],
            "connection_optional_fields": [],
            "installation_instructions_html": None,
            "connected": False,
            "company_provider_id": None,
            "kind": kind_value,
            "kind_name": provider.kind_name or "",
            "coming_soon": True,
        })

    # Order-capable distributors first (stable sort — preserves PROVIDER_CATALOG's declared
    # order within each group, and keeps coming-soon entries, which have no "supports_ordering"
    # key at all, at the back where they already were).
    catalog.sort(key=lambda row: not row.get("supports_ordering", False))

    logger.info('{} Found {} providers in catalog for company_id: {}.'.format(
        _LOG_PREFIX, len(catalog), company_id
    ))

    return {
        "data": catalog,
        "categories": list(dict.fromkeys(
            e.get("category", "") for e in src_constants.PROVIDER_CATALOG if e.get("category")
        )),
    }


def _get_catalog_entry_for_provider(provider_id: int) -> typing.Optional[typing.Dict]:
    """Get PROVIDER_CATALOG entry for a provider by id."""
    provider = src_models.Providers.objects.filter(id=provider_id).first()
    if not provider:
        return None
    for entry in src_constants.PROVIDER_CATALOG:
        if entry["kind"].value == provider.kind:
            return entry
    return None


def _catalog_supports_ordering_display(catalog_entry: typing.Dict[str, typing.Any]) -> bool:
    """
    The "supports_ordering" flag shown on the catalog/connection-detail endpoints — this drives
    whether the FE shows an ordering-credentials step, which is a *different* question from
    "can this specific connection place an order right now" (that one stays gated on
    order_registry.get_adapter() actually constructing an adapter — see get_order_capabilities()
    and parts.py's can_order_in_app).

    True when any of:
      - an order adapter is actually registered (order_registry.supports_ordering()), or
      - the catalog entry declares order-specific (API) credential fields, meaning we know what
        this distributor's ordering API needs even before its adapter is built (Meyer, Wheel
        Pros, Premier as of this writing) — staging the credentials form ahead of the adapter
        lets companies fill these in now instead of after the fact, or
      - the catalog entry declares email-order credential fields (rep_email/cc_email) — every
        distributor with a real feed client can be ordered by emailing a rep regardless of
        whether it also has a real order API (see src.enums.OrderMethod), so this is true for
        exactly the distributors PROVIDER_CATALOG declares email_order_connection_*_fields for.
    """
    kind_value = catalog_entry["kind"].value
    if order_registry.supports_ordering(kind_value):
        return True
    return bool(
        catalog_entry.get("order_connection_required_fields")
        or catalog_entry.get("order_connection_optional_fields")
        or catalog_entry.get("email_order_connection_required_fields")
        or catalog_entry.get("email_order_connection_optional_fields")
    )


def _catalog_order_credentials_mirror_feed(catalog_entry: typing.Dict[str, typing.Any]) -> bool:
    """
    True only when the catalog entry declares NO distinct order field list at all for a
    registered order adapter — i.e. its order credentials would be silently auto-mirrored from
    "feed" (see _merge_update_credentials's fallback ``elif`` branch). As of this
    writing every registered adapter (Turn14, Keystone) declares its own order fields and is
    validated independently — even Turn14, whose order credential *values* happen to be the
    same OAuth client_id/client_secret as its feed, still requires them to be entered and
    validated separately (catalog-API and order-API access are separate permission grants on
    Turn14's side) — so this is currently always False. Kept for any future adapter that
    genuinely has nothing distinct to validate.
    """
    if catalog_entry.get("order_connection_required_fields") or catalog_entry.get("order_connection_optional_fields"):
        return False
    return order_registry.supports_ordering(catalog_entry["kind"].value)


def _validate_wheelpros_markup_fields(credentials: typing.Dict[str, typing.Any]) -> typing.Optional[str]:
    """wheel_markup / tire_markup / accessories_markup must be numeric percentages in [0, 100]."""
    for key in ("wheel_markup", "tire_markup", "accessories_markup"):
        raw = credentials.get(key)
        try:
            value = float(raw)
        except (TypeError, ValueError):
            return "{} must be a number between 0 and 100.".format(key)
        if not (0 <= value <= 100):
            return "{} must be between 0 and 100 (got {}).".format(key, raw)
    return None


_ValidatorResult = typing.Tuple[typing.Optional[str], typing.Optional[str]]  # (error_message, error_code)


def _validate_turn14_connection(credentials: typing.Dict[str, typing.Any]) -> _ValidatorResult:
    try:
        client = turn14_client.Turn14ApiClient(credentials=credentials)
        client.test_connection()
    except turn14_exceptions.Turn14PermissionError as e:
        return e.message, CONNECTION_ERROR_PERMISSION_DENIED
    except turn14_exceptions.Turn14APIBadResponseCodeError as e:
        code = (
            CONNECTION_ERROR_INVALID_CREDENTIALS
            if e.code in (401, 403)
            else CONNECTION_ERROR_CONNECTION_FAILED
        )
        return e.message, code
    except (turn14_exceptions.Turn14APIException, ValueError) as e:
        return str(e), CONNECTION_ERROR_CONNECTION_FAILED
    return None, None


def _validate_keystone_connection(credentials: typing.Dict[str, typing.Any]) -> _ValidatorResult:
    try:
        client = keystone_client.KeystoneFTPClient(credentials=credentials)
        client.test_connection()
    except keystone_exceptions.KeystoneFTPAuthError as e:
        return str(e), CONNECTION_ERROR_INVALID_CREDENTIALS
    except (keystone_exceptions.KeystoneException, ValueError) as e:
        return str(e), CONNECTION_ERROR_CONNECTION_FAILED
    return None, None


def _validate_premier_connection(credentials: typing.Dict[str, typing.Any]) -> _ValidatorResult:
    try:
        client = premier_client.PremierFTPClient(credentials=credentials)
        client.test_connection()
    except premier_exceptions.PremierFTPAuthError as e:
        return str(e), CONNECTION_ERROR_INVALID_CREDENTIALS
    except (premier_exceptions.PremierException, ValueError) as e:
        return str(e), CONNECTION_ERROR_CONNECTION_FAILED
    return None, None


def _validate_wheelpros_connection(credentials: typing.Dict[str, typing.Any]) -> _ValidatorResult:
    markup_error = _validate_wheelpros_markup_fields(credentials)
    if markup_error:
        return markup_error, CONNECTION_ERROR_INVALID_INPUT
    try:
        client = wheelpros_client.WheelProsSFTPClient(credentials=credentials)
        # Check auth against all three feeds (wheel/tire/accessories), not just a bare login —
        # some accounts authenticate fine but lack permission on a specific feed's directory.
        client.test_connection(remote_paths=src_constants.WHEELPROS_FEED_PATHS.values())
    except wheelpros_exceptions.WheelProsAuthError as e:
        return str(e), CONNECTION_ERROR_INVALID_CREDENTIALS
    except wheelpros_exceptions.WheelProsPermissionError as e:
        return str(e), CONNECTION_ERROR_PERMISSION_DENIED
    except (wheelpros_exceptions.WheelProsException, ValueError) as e:
        return str(e), CONNECTION_ERROR_CONNECTION_FAILED
    return None, None


def _validate_rough_country_connection(credentials: typing.Dict[str, typing.Any]) -> _ValidatorResult:
    url = credentials.get(src_constants.ROUGH_COUNTRY_CREDENTIALS_FEED_URL)
    try:
        client = rough_country_client.RoughCountryFeedClient(file_url=url)
    except ValueError as e:
        return str(e), CONNECTION_ERROR_INVALID_INPUT
    try:
        client.test_connection()
    except (rough_country_exceptions.RoughCountryException, ValueError) as e:
        return str(e), CONNECTION_ERROR_CONNECTION_FAILED
    return None, None


def _validate_vossen_discount_percent(credentials: typing.Dict[str, typing.Any]) -> typing.Optional[str]:
    """discount_percent must be a numeric percentage in [0, 100] — see vossen.dealer_cost_from_price."""
    key = src_constants.VOSSEN_CREDENTIALS_DISCOUNT_PERCENT
    raw = credentials.get(key)
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return "{} must be a number between 0 and 100.".format(key)
    if not (0 <= value <= 100):
        return "{} must be between 0 and 100 (got {}).".format(key, raw)
    return None


def _validate_vossen_connection(credentials: typing.Dict[str, typing.Any]) -> _ValidatorResult:
    discount_error = _validate_vossen_discount_percent(credentials)
    if discount_error:
        return discount_error, CONNECTION_ERROR_INVALID_INPUT
    url = credentials.get(src_constants.VOSSEN_CREDENTIALS_FEED_URL)
    try:
        client = vossen_client.VossenFeedClient(file_url=url)
    except ValueError as e:
        return str(e), CONNECTION_ERROR_INVALID_INPUT
    try:
        client.test_connection()
    except (vossen_exceptions.VossenException, ValueError) as e:
        return str(e), CONNECTION_ERROR_CONNECTION_FAILED
    return None, None


def _validate_elite_wheel_connection(credentials: typing.Dict[str, typing.Any]) -> _ValidatorResult:
    """
    Validates the dealer's own Elite SFTP account: connect, list the feed directory, and confirm a
    ``TriWeeklyUpdate*.xlsx`` workbook is actually there — an account that authenticates but is
    pointed at an empty directory would otherwise look connected and never sync anything.
    Credentials that carry no SFTP details fall back to Elite's public inventory share, which this
    checks the same way (see EliteWheelFeedClient.source_mode).
    """
    try:
        client = elite_wheel_client.EliteWheelFeedClient(credentials=credentials)
    except ValueError as e:
        return str(e), CONNECTION_ERROR_INVALID_INPUT
    try:
        client.test_connection()
    except elite_wheel_exceptions.EliteWheelSFTPConnectionError as e:
        return e.message, CONNECTION_ERROR_INVALID_CREDENTIALS
    except elite_wheel_exceptions.EliteWheelFileNotFoundError as e:
        return str(e), CONNECTION_ERROR_NOT_FOUND
    except (elite_wheel_exceptions.EliteWheelException, ValueError) as e:
        return str(e), CONNECTION_ERROR_CONNECTION_FAILED
    return None, None


def _validate_motorstate_connection(credentials: typing.Dict[str, typing.Any]) -> _ValidatorResult:
    """
    Validates the dealer's Motor State API key against GET /api/Brands — the cheapest real
    endpoint (single unpaginated call, no account state needed).

    Motor State answers 403 with an empty body both for an unknown/expired key and for a valid
    key that lacks access to an endpoint, so the two are indistinguishable from the response
    alone; the client raises MotorStatePermissionError for either and it is reported as bad
    credentials, which is the actionable case for someone pasting a key into this form.
    """
    try:
        client = motorstate_client.MotorStateApiClient(credentials=credentials)
    except ValueError as e:
        return str(e), CONNECTION_ERROR_INVALID_INPUT
    try:
        client.test_connection()
    except motorstate_exceptions.MotorStatePermissionError as e:
        return e.message, CONNECTION_ERROR_INVALID_CREDENTIALS
    except motorstate_exceptions.MotorStateAPIBadResponseCodeError as e:
        return e.message, CONNECTION_ERROR_CONNECTION_FAILED
    except motorstate_exceptions.MotorStateAPIException as e:
        return str(e), CONNECTION_ERROR_CONNECTION_FAILED
    return None, None


def _validate_wps_connection(credentials: typing.Dict[str, typing.Any]) -> _ValidatorResult:
    """
    Validates the dealer's WPS Data Depot bearer token by requesting a single item -- the
    cheapest real read. WPS answers 401/403 for a revoked or unknown token; either way the
    actionable message is the same for someone pasting a token into this form.
    """
    try:
        client = wps_client.WpsApiClient(credentials=credentials)
    except ValueError as e:
        return str(e), CONNECTION_ERROR_INVALID_INPUT
    try:
        client.test_connection()
    except wps_exceptions.WpsPermissionError as e:
        return e.message, CONNECTION_ERROR_INVALID_CREDENTIALS
    except wps_exceptions.WpsAPIBadResponseCodeError as e:
        return e.message, CONNECTION_ERROR_CONNECTION_FAILED
    except wps_exceptions.WpsAPIException as e:
        return str(e), CONNECTION_ERROR_CONNECTION_FAILED
    return None, None


def _validate_helmet_house_connection(credentials: typing.Dict[str, typing.Any]) -> _ValidatorResult:
    """
    Validates a Helmet House FTP login: connect on port 21, then confirm a catalog file
    (masterv.csv, or master.csv as a fallback) is actually present. Never downloads the ~10 MB
    file — an account that authenticates but is pointed at a directory without the catalog would
    otherwise look connected and never sync anything.

    Helmet House publishes one shared login rather than issuing an account per dealer, so a
    rejected login usually means the shared credential has been rotated rather than that this
    dealer's access was revoked. The message the user sees says as much (see the HELMHOUSE entry
    in PROVIDER_CATALOG).
    """
    try:
        client = helmet_house_client.HelmetHouseFTPClient(credentials=credentials)
    except ValueError as e:
        return str(e), CONNECTION_ERROR_INVALID_INPUT
    try:
        client.test_connection()
    except helmet_house_exceptions.HelmetHouseFTPAuthError as e:
        return str(e), CONNECTION_ERROR_INVALID_CREDENTIALS
    except helmet_house_exceptions.HelmetHouseFileNotFoundError as e:
        return str(e), CONNECTION_ERROR_NOT_FOUND
    except (helmet_house_exceptions.HelmetHouseException, ValueError) as e:
        return str(e), CONNECTION_ERROR_CONNECTION_FAILED
    return None, None


def _validate_tirerack_connection(credentials: typing.Dict[str, typing.Any]) -> _ValidatorResult:
    try:
        client = tirerack_client.TireRackSFTPClient(credentials=credentials)
        client.test_connection()
    except ValueError as e:
        return str(e), CONNECTION_ERROR_INVALID_INPUT
    except tirerack_exceptions.TireRackSFTPConnectionError as e:
        return str(e), CONNECTION_ERROR_INVALID_CREDENTIALS
    except tirerack_exceptions.TireRackException as e:
        return str(e), CONNECTION_ERROR_CONNECTION_FAILED
    return None, None


# Connection validators run synchronously at connect/update time, before credentials are saved,
# so bad credentials fail the request instead of silently failing the first background sync.
# Kinds without an entry here are not validated (relay-provisioned kinds, where credentials are
# system-generated rather than user-entered, and providers with no real backend client yet —
# see get_distributor_credentials_info for what each kind needs).
_CONNECTION_VALIDATORS: typing.Dict[int, typing.Callable[[typing.Dict[str, typing.Any]], _ValidatorResult]] = {
    src_enums.BrandProviderKind.TURN_14.value: _validate_turn14_connection,
    src_enums.BrandProviderKind.KEYSTONE.value: _validate_keystone_connection,
    src_enums.BrandProviderKind.PREMIER_PERFORMANCE.value: _validate_premier_connection,
    src_enums.BrandProviderKind.WHEELPROS.value: _validate_wheelpros_connection,
    src_enums.BrandProviderKind.ROUGH_COUNTRY.value: _validate_rough_country_connection,
    src_enums.BrandProviderKind.VOSSEN.value: _validate_vossen_connection,
    src_enums.BrandProviderKind.TIRERACK.value: _validate_tirerack_connection,
    src_enums.BrandProviderKind.ELITE_WHEEL.value: _validate_elite_wheel_connection,
    src_enums.BrandProviderKind.MOTOR_STATE_DISTRIBUTING.value: _validate_motorstate_connection,
    src_enums.BrandProviderKind.WESTERN_POWER_SPORTS.value: _validate_wps_connection,
    src_enums.BrandProviderKind.HELMHOUSE.value: _validate_helmet_house_connection,
}


def _validate_connection(
    kind: int, credentials: typing.Dict[str, typing.Any]
) -> typing.Tuple[typing.Optional[bool], typing.Optional[str], typing.Optional[str]]:
    """
    Run the feed connection validator for this provider kind, if one exists.
    Returns (validated, error_message, error_code):
      (True, None, None)     — validator ran and the connection is good.
      (False, message, code) — validator ran and the connection failed; caller should reject the request.
      (None, None, None)     — no validator for this kind yet; not attempted.
    """
    validator = _CONNECTION_VALIDATORS.get(kind)
    if not validator:
        return None, None, None
    message, code = validator(credentials)
    if message:
        return False, message, code
    return True, None, None


def _validate_keystone_order_connection(credentials: typing.Dict[str, typing.Any]) -> _ValidatorResult:
    try:
        client = keystone_order_client.KeystoneOrderApiClient(credentials=credentials)
        client.test_connection()
    except (
        keystone_exceptions.KeystoneOrderAuthError,
        keystone_exceptions.KeystoneOrderPermissionError,
    ) as e:
        return str(e), CONNECTION_ERROR_INVALID_CREDENTIALS
    except (keystone_exceptions.KeystoneException, ValueError) as e:
        return str(e), CONNECTION_ERROR_CONNECTION_FAILED
    return None, None


def _validate_turn14_order_connection(credentials: typing.Dict[str, typing.Any]) -> _ValidatorResult:
    """
    Tests the submitted client_id/client_secret against Turn 14's ORDER API specifically (not
    the catalog API — see clients/turn_14/client.py's own validator), since the two are
    separate permission grants even when the credential values happen to be identical. Order
    credentials are entered explicitly for Turn 14 like every other distributor now — they are
    no longer silently mirrored from "feed" without being independently confirmed to work for
    order placement.
    """
    try:
        environment = getattr(settings, "TURN14_ORDER_ENVIRONMENT", "testing")
        client = turn14_order_client.Turn14OrderApiClient(credentials=credentials, environment=environment)
        client.test_connection()
    except turn14_exceptions.Turn14PermissionError as e:
        return e.message, CONNECTION_ERROR_PERMISSION_DENIED
    except turn14_exceptions.Turn14APIBadResponseCodeError as e:
        code = (
            CONNECTION_ERROR_INVALID_CREDENTIALS
            if e.code in (401, 403)
            else CONNECTION_ERROR_CONNECTION_FAILED
        )
        return e.message, code
    except (turn14_exceptions.Turn14APIException, ValueError) as e:
        return str(e), CONNECTION_ERROR_CONNECTION_FAILED
    return None, None


def _validate_meyer_order_connection(credentials: typing.Dict[str, typing.Any]) -> _ValidatorResult:
    try:
        environment = getattr(settings, "MEYER_ORDER_ENVIRONMENT", "testing")
        client = meyer_order_client.MeyerOrderApiClient(credentials=credentials, environment=environment)
        client.test_connection()
    except meyer_exceptions.MeyerOrderAuthError as e:
        return str(e), CONNECTION_ERROR_INVALID_CREDENTIALS
    except (meyer_exceptions.MeyerException, ValueError) as e:
        return str(e), CONNECTION_ERROR_CONNECTION_FAILED
    return None, None


def _validate_premier_order_connection(credentials: typing.Dict[str, typing.Any]) -> _ValidatorResult:
    try:
        environment = getattr(settings, "PREMIER_ORDER_ENVIRONMENT", "production")
        client = premier_order_client.PremierOrderApiClient(credentials=credentials, environment=environment)
        client.test_connection()
    except premier_exceptions.PremierOrderAuthError as e:
        return str(e), CONNECTION_ERROR_INVALID_CREDENTIALS
    except (premier_exceptions.PremierException, ValueError) as e:
        return str(e), CONNECTION_ERROR_CONNECTION_FAILED
    return None, None


def _validate_wheelpros_order_connection(credentials: typing.Dict[str, typing.Any]) -> _ValidatorResult:
    """
    Tests the submitted username/password against Wheel Pros' Orders API specifically (not the
    SFTP feed — see clients/wheelpros/client.py's own validator), since order placement is a
    separate permission grant from catalog/feed access on Wheel Pros' side, requested
    independently per their onboarding docs.
    """
    try:
        environment = getattr(settings, "WHEELPROS_ORDER_ENVIRONMENT", "production")
        client = wheelpros_order_client.WheelProsOrderApiClient(credentials=credentials, environment=environment)
        client.test_connection()
    except wheelpros_exceptions.WheelProsOrderAuthError as e:
        return str(e), CONNECTION_ERROR_INVALID_CREDENTIALS
    except wheelpros_exceptions.WheelProsOrderPermissionError as e:
        return str(e), CONNECTION_ERROR_PERMISSION_DENIED
    except (wheelpros_exceptions.WheelProsException, ValueError) as e:
        return str(e), CONNECTION_ERROR_CONNECTION_FAILED
    return None, None


# Order-credential validators — parallel to _CONNECTION_VALIDATORS but for the "order" namespace,
# only run when a company actually submits order credentials (see _merge_update_credentials —
# both "feed" and "order" are optional at connect/update time, either may be submitted alone).
# Populated per vendor as each order adapter's transport client is built (see
# src/integrations/orders/).
_ORDER_CONNECTION_VALIDATORS: typing.Dict[int, typing.Callable[[typing.Dict[str, typing.Any]], _ValidatorResult]] = {
    src_enums.BrandProviderKind.TURN_14.value: _validate_turn14_order_connection,
    src_enums.BrandProviderKind.KEYSTONE.value: _validate_keystone_order_connection,
    src_enums.BrandProviderKind.MEYER.value: _validate_meyer_order_connection,
    src_enums.BrandProviderKind.PREMIER_PERFORMANCE.value: _validate_premier_order_connection,
    src_enums.BrandProviderKind.WHEELPROS.value: _validate_wheelpros_order_connection,
}


def _validate_order_connection(
    kind: int, credentials: typing.Dict[str, typing.Any]
) -> typing.Tuple[typing.Optional[bool], typing.Optional[str], typing.Optional[str]]:
    """Same contract as _validate_connection, but against _ORDER_CONNECTION_VALIDATORS."""
    validator = _ORDER_CONNECTION_VALIDATORS.get(kind)
    if not validator:
        return None, None, None
    message, code = validator(credentials)
    if message:
        return False, message, code
    return True, None, None


def _validate_email_order_connection(credentials: typing.Dict[str, typing.Any]) -> _ValidatorResult:
    """
    Validator for the Email order channel (src.enums.OrderMethod.EMAIL) — checked whenever an
    order account's order_method is EMAIL, regardless of provider kind. Unlike
    _ORDER_CONNECTION_VALIDATORS above (one live-API-testing validator per distributor kind),
    there's no live endpoint to test against here — just that rep_email/cc_email/reply_to, when
    present, are actually well-formed addresses. No network call, so this always runs
    synchronously and cheaply, same as the format-only feed validators elsewhere in this module.
    """
    for field in ("rep_email", "cc_email", "reply_to"):
        value = (credentials.get(field) or "").strip()
        if not value:
            continue
        try:
            django_validators.validate_email(value)
        except django_core_exceptions.ValidationError:
            return "{} must be a valid email address.".format(field), CONNECTION_ERROR_INVALID_INPUT
    return None, None


def _validate_order_connection_for_method(
    kind: int, order_method: int, credentials: typing.Dict[str, typing.Any]
) -> typing.Tuple[typing.Optional[bool], typing.Optional[str], typing.Optional[str]]:
    """
    Channel-first validation dispatch, mirroring orders.registry.get_adapter()'s channel-first
    adapter resolution — EMAIL always validates against _validate_email_order_connection
    regardless of provider kind; API falls through to the existing per-kind
    _validate_order_connection lookup unchanged.
    """
    if order_method == src_enums.OrderMethod.EMAIL.value:
        message, code = _validate_email_order_connection(credentials)
        if message:
            return False, message, code
        return True, None, None
    return _validate_order_connection(kind, credentials)


# Relay-provisioned kinds we can actually check something for — see
# _relay_feed_connection_status. Other relay-provisioned kinds (CTP, Crown, DIX) have no ingest
# client built yet, so there's nothing to check against.
_RELAY_FEED_CHECK_KINDS = {
    src_enums.BrandProviderKind.MEYER.value,
    src_enums.BrandProviderKind.ATECH.value,
    src_enums.BrandProviderKind.THE_WHEEL_GROUP.value,
}


def _relay_feed_connection_status(
    company: typing.Optional[src_models.Company], kind: int
) -> typing.Tuple[typing.Optional["src_enums.CompanyProviderConnectionStatus"], typing.Optional[str]]:
    """
    For relay-provisioned kinds in _RELAY_FEED_CHECK_KINDS: log into our relay with the
    company's own relay credentials and check whether the expected feed file(s) have arrived.
    Returns (None, None) for any other kind — nothing to check. Single source of truth for this
    logic — used both here (to set an initial status at connect time) and by the
    check_company_provider_connections cron (to keep it fresh afterwards).

    The Wheel Group is the one exception to "check the relay folder": their catalog and list
    pricing come from the public mastersheet they publish, not from a relay drop, so a TWG
    connection is already receiving data on the day it's made. It's checked against whichever
    source it will actually read — the client decides, so this switches to the dealer's own relay
    drop by itself once THE_WHEEL_GROUP_FORCE_PUBLIC_SHARE is turned off.
    """
    if kind not in _RELAY_FEED_CHECK_KINDS:
        return None, None
    if not company or not company.relay_sftp_username or not company.relay_sftp_password:
        return (
            src_enums.CompanyProviderConnectionStatus.WAITING,
            "Your relay SFTP account is still being created.",
        )

    creds = {"sftp_user": company.relay_sftp_username, "sftp_password": company.relay_sftp_password}

    if kind == src_enums.BrandProviderKind.THE_WHEEL_GROUP.value:
        try:
            the_wheel_group_client.TheWheelGroupFeedClient(credentials=creds).test_connection()
        except (the_wheel_group_exceptions.TheWheelGroupException, ValueError) as e:
            return (
                src_enums.CompanyProviderConnectionStatus.FAILING,
                "Could not reach The Wheel Group's feed: {}".format(e),
            )
        return (
            src_enums.CompanyProviderConnectionStatus.INGESTING,
            "Reading The Wheel Group's catalog — your own cost and stock follow once they deliver "
            "your feed.",
        )

    try:
        if kind == src_enums.BrandProviderKind.MEYER.value:
            client = meyer_client.MeyerSFTPClient(credentials=creds)
        else:
            client = atech_client.AtechSFTPClient(credentials=creds)
        present = client.feed_present()
    except (meyer_exceptions.MeyerException, atech_exceptions.AtechException, ValueError) as e:
        return (
            src_enums.CompanyProviderConnectionStatus.FAILING,
            "Could not reach our relay to check for your file: {}".format(e),
        )

    if present:
        return (
            src_enums.CompanyProviderConnectionStatus.INGESTING,
            "File received — waiting for it to be processed.",
        )
    return (
        src_enums.CompanyProviderConnectionStatus.WAITING,
        "Waiting for your first file to arrive on our relay.",
    )


def _status_enum_from_stored(
    raw_status: typing.Optional[int],
) -> typing.Optional["src_enums.CompanyProviderConnectionStatus"]:
    """
    Reconstructs a CompanyProviderConnectionStatus enum from the raw int stored on
    CompanyProviders.status — used when a request skipped re-validating "feed" (already synced,
    not touched by this call) but order's CONNECTED-vs-WAITING gating still needs to know feed's
    TRUE current status, not None (which would incorrectly read as "feed not connected").
    """
    if not raw_status:
        return None
    return src_enums.CompanyProviderConnectionStatus(raw_status)


def _order_gating_feed_status_enum(
    company_provider: src_models.CompanyProviders,
) -> typing.Optional["src_enums.CompanyProviderConnectionStatus"]:
    """
    The feed status order's CONNECTED-vs-WAITING gating should use — CONNECTED whenever
    ``initial_sync_completed`` is True, regardless of what the narrower ``status`` enum
    currently holds. ``initial_sync_completed`` is the durable "this feed has synced
    successfully at least once" fact (see integration_pricing_sync_jobs, which sets it True the
    moment the first sync completes, at the same time it sets status=CONNECTED); ``status``
    itself can go back to None afterward for reasons unrelated to the feed actually being
    broken — e.g. disconnect_provider(namespace="feed") clears status without resetting
    initial_sync_completed, since the historical fact that a sync once completed doesn't
    change. Falling back to _status_enum_from_stored(status) alone would then incorrectly gate
    order status to WAITING for a feed that is, in every functional sense, still connected.
    """
    if company_provider.initial_sync_completed:
        return src_enums.CompanyProviderConnectionStatus.CONNECTED
    return _status_enum_from_stored(company_provider.status)


def _connection_status_fields(
    status: typing.Optional["src_enums.CompanyProviderConnectionStatus"],
    reason: typing.Optional[str],
) -> typing.Dict[str, typing.Any]:
    """
    Field values for CompanyProviders.status/status_name/status_reason/status_checked_at —
    a dict so it works both as ``.create(**fields)`` kwargs and via setattr on an existing row.
    """
    return {
        "status": status.value if status else None,
        "status_name": status.name if status else None,
        "status_reason": reason,
        "status_checked_at": timezone.now() if status else None,
    }


_ORDER_WAITING_ON_FEED_REASON = (
    "Order credentials are valid, but ordering stays disabled until the feed connection "
    "finishes syncing."
)


def _order_connection_status_fields(
    status: typing.Optional["src_enums.CompanyProviderOrderConnectionStatus"],
    reason: typing.Optional[str],
) -> typing.Dict[str, typing.Any]:
    """
    Field values for CompanyProviders.order_status/order_status_name/order_status_reason/
    order_status_checked_at — parallel to :func:`_connection_status_fields` but for the order
    namespace. ``status=None`` (order credentials not submitted, or no validator for this kind)
    leaves all four fields null/unset rather than touching them.
    """
    return {
        "order_status": status.value if status else None,
        "order_status_name": status.name if status else None,
        "order_status_reason": reason,
        "order_status_checked_at": timezone.now() if status else None,
    }


def _resolve_order_status(
    order_validated: typing.Optional[bool],
    order_val_error: typing.Optional[str],
    feed_status_enum: typing.Optional["src_enums.CompanyProviderConnectionStatus"],
) -> typing.Tuple[typing.Optional["src_enums.CompanyProviderOrderConnectionStatus"], typing.Optional[str]]:
    """
    Turn an order validator's result into an order status, gated on feed status: ordering can
    never be CONNECTED while the feed itself isn't — see CompanyProviderOrderConnectionStatus.
      order_val_error set        -> ERROR, that message.
      order_validated True       -> CONNECTED if feed is CONNECTED, else WAITING.
      order_validated None       -> (None, None) — no validator for this kind; leave untouched.
    """
    if order_val_error:
        return src_enums.CompanyProviderOrderConnectionStatus.ERROR, order_val_error
    if order_validated:
        if feed_status_enum == src_enums.CompanyProviderConnectionStatus.CONNECTED:
            return src_enums.CompanyProviderOrderConnectionStatus.CONNECTED, None
        return src_enums.CompanyProviderOrderConnectionStatus.WAITING, _ORDER_WAITING_ON_FEED_REASON
    return None, None


def _set_order_account_status(
    account: src_models.CompanyProviderOrderAccount,
    status: typing.Optional["src_enums.CompanyProviderOrderConnectionStatus"],
    reason: typing.Optional[str],
) -> None:
    """
    Sets order_status/order_status_name/order_status_reason/order_status_checked_at on ONE
    CompanyProviderOrderAccount row — this is the source of truth for order status per account
    (including the default's own row; see the model docstring). Callers that also need
    CompanyProviders.order_status* kept in sync for the default account should follow this with
    :func:`_refresh_default_order_status`.
    """
    status_fields = _order_connection_status_fields(status, reason)
    for field, value in status_fields.items():
        setattr(account, field, value)
    account.save(update_fields=list(status_fields.keys()) + ["updated_at"])


def _validate_and_resolve_order_status(
    kind: int,
    order_creds: typing.Optional[typing.Dict[str, typing.Any]],
    feed_status_enum: typing.Optional["src_enums.CompanyProviderConnectionStatus"],
) -> typing.Tuple[
    typing.Optional[bool],
    typing.Optional["src_enums.CompanyProviderOrderConnectionStatus"],
    typing.Optional[str],
    typing.Optional[str],
    typing.Optional[str],
]:
    """
    Shared by connect_provider and update_connection's relay AND non-relay branches (four call
    sites, identical order-side handling regardless of how the feed side is validated): runs the
    order validator when order credentials are present, then resolves order_status from the
    result. Submitted order credentials that fail live validation reject the whole request, the
    same way feed credential failures already do — callers must check the last two return values
    and return early when set, mirroring the existing ``if val_error: return None, val_error,
    val_error_code`` pattern used for feed. (The cron connectivity re-check
    (check_company_provider_connections) calls :func:`_resolve_order_status` directly instead of
    this wrapper — it can't "reject" a background check, so it still records ERROR status on an
    already-saved connection rather than blocking anything.)

    Returns (order_validated, order_status_enum, order_status_reason, error_message, error_code).
    """
    if not order_creds:
        return None, None, None, None, None
    order_validated, order_val_error, order_val_error_code = _validate_order_connection(kind, order_creds)
    if order_val_error:
        return False, None, None, order_val_error, order_val_error_code
    order_status_enum, order_status_reason = _resolve_order_status(order_validated, None, feed_status_enum)
    return order_validated, order_status_enum, order_status_reason, None, None


def connect_provider(
    company_id: int,
    provider_id: int,
    credentials: typing.Dict[str, typing.Any],
) -> typing.Tuple[typing.Optional[typing.Dict], typing.Optional[str], typing.Optional[str]]:
    """
    Create, or idempotently update, a ``CompanyProviders`` row (keyed by company + provider).
    ``credentials`` is shaped ``{"feed": {...}, "order": {...}}`` — either namespace may be
    omitted (at least one must be present), and each is validated/stored independently via
    :func:`_merge_update_credentials`, the same merge this uses for PATCH. This is what lets a
    never-connected distributor be "connected" via Ordering alone, with Product feed filled in
    later (or vice versa) — there is no requirement that "feed" exist first.

    Calling this again for a company+provider pair that's already connected does NOT replace
    the whole credentials blob — it merges the submitted namespace(s) onto what's already
    stored, exactly like PATCH does, so e.g. resubmitting "order" here after "feed" was already
    connected can't silently wipe the feed credentials. Use :func:`update_connection` (PATCH by
    connection id) when you already know the ``company_provider_id``; this is for "connect via
    catalog by provider id," which also happens to be idempotent.

    Tests each namespace's connection with a validator when one exists (see
    :data:`_CONNECTION_VALIDATORS` / :data:`_ORDER_CONNECTION_VALIDATORS`) and enqueues
    ``integration_pricing_sync_jobs.enqueue_company_provider_pricing_sync`` when the provider has
    per-company pricing sync. Returns (data, error_message, error_code) — error_code is one of
    the ``CONNECTION_ERROR_*`` constants, for the frontend to branch on.
    """
    provider = src_models.Providers.objects.filter(id=provider_id).first()
    if not provider:
        return None, "Provider not found", CONNECTION_ERROR_NOT_FOUND

    catalog_entry = _get_catalog_entry_for_provider(provider_id)
    if not catalog_entry:
        return None, "Provider not found in catalog", CONNECTION_ERROR_NOT_FOUND

    existing = src_models.CompanyProviders.objects.filter(
        company_id=company_id,
        provider_id=provider_id,
    ).first()
    # existing.credentials never carries "order" (see CompanyProviderOrderAccount) — seed the
    # merge/relay baseline below from this connection's actual default account instead, so a
    # feed-only request still correctly carries forward whatever order credentials already exist.
    existing_creds = dict(existing.credentials or {}) if existing else None
    if existing:
        existing_creds["order"] = credentials_helper.get_order_credentials(existing)

    # Distributor-connection cap: only relevant when this call would newly connect this
    # provider's PRODUCT FEED for the first time — relay-provisioned providers always populate
    # "feed" on first connect; non-relay providers only if this request's payload includes
    # "feed". An already-feed-connected provider being re-submitted (credential rotation, a
    # later order-only call) never counts as "new" and is never blocked here.
    already_has_feed = bool(existing and (existing.credentials or {}).get("feed"))
    will_add_new_feed = not already_has_feed and (
        bool(catalog_entry.get("relay_provisioned"))
        or bool(isinstance(credentials, dict) and credentials.get("feed"))
    )
    if will_add_new_feed:
        allowed, plan_reason = billing_services.check_distributor_connection_limit(company_id)
        if not allowed:
            return None, plan_reason, CONNECTION_ERROR_PLAN_LIMIT_REACHED

    if catalog_entry.get("relay_provisioned"):
        company = src_models.Company.objects.filter(id=company_id).first()
        if not company:
            return None, "Company not found", CONNECTION_ERROR_NOT_FOUND
        if not company.relay_sftp_username or not company.relay_sftp_password:
            try:
                relay_sftp_provisioning.provision_company_sftp_account(company)
            except Exception as e:
                logger.error("{} Relay SFTP provisioning failed for company_id={}: {}".format(
                    _LOG_PREFIX, company_id, e
                ))
                return (
                    None,
                    "Your SFTP account could not be created. Please contact support@aftermarketscout.com.",
                    CONNECTION_ERROR_CONNECTION_FAILED,
                )
        # "feed" is always system-generated for relay providers — never user-submitted, so it's
        # not run through _merge_update_credentials. "order" (e.g. Meyer's order API creds) is
        # still a normal user-submitted, independently-saveable namespace, same as non-relay.
        user_field, password_field = catalog_entry.get("relay_credential_fields", ("sftp_user", "sftp_password"))
        order_required = [str(f) for f in (catalog_entry.get("order_connection_required_fields") or [])]
        order_optional = [str(f) for f in (catalog_entry.get("order_connection_optional_fields") or [])]
        order_creds = dict((existing_creds or {}).get("order") or {})
        order_raw = credentials.get("order") if isinstance(credentials, dict) else None
        if order_raw:
            err, err_code = _merge_namespace_credentials(order_creds, order_required, order_optional, order_raw)
            if err:
                return None, err, err_code
        if order_creds and order_required:
            missing_order = [f for f in order_required if not _normalize_credential_value(order_creds.get(f))]
            if missing_order:
                return (
                    None,
                    "Missing required order fields: {}".format(", ".join(missing_order)),
                    CONNECTION_ERROR_MISSING_FIELDS,
                )
        creds = {
            "feed": {user_field: company.relay_sftp_username, password_field: company.relay_sftp_password},
            "order": order_creds,
        }
        # Once the initial sync has completed, don't keep re-running the relay file-presence
        # check on every unrelated (e.g. order-only) call — it can only report WAITING/INGESTING,
        # never CONNECTED, so re-running it after CONNECTED would regress the stored status for
        # no reason. Still re-check on every call until sync completes, matching the cron's own
        # scoping (see check_company_provider_connections / CompanyProviders.initial_sync_completed).
        already_synced = bool(existing and existing.initial_sync_completed)
        feed_fields_touched = existing is None or not already_synced
        if feed_fields_touched:
            validated = None
            status_enum, status_reason = _relay_feed_connection_status(company, provider.kind)
        else:
            validated, status_enum, status_reason = None, None, None
        effective_feed_status_enum = (
            status_enum if feed_fields_touched
            else (_order_gating_feed_status_enum(existing) if existing else None)
        )

        order_fields_touched = isinstance(credentials, dict) and "order" in credentials
        if order_fields_touched:
            order_validated, order_status_enum, order_status_reason, order_val_error, order_val_error_code = (
                _validate_and_resolve_order_status(provider.kind, order_creds, effective_feed_status_enum)
            )
            if order_val_error:
                return None, order_val_error, order_val_error_code
        else:
            order_validated, order_status_enum, order_status_reason = None, None, None
    else:
        creds, err, err_code = _merge_update_credentials(catalog_entry, existing_creds, credentials)
        if err:
            return None, err, err_code

        # Only re-validate "feed" (and touch its status fields) when this call actually
        # submitted it, or the initial sync hasn't completed yet — otherwise an order-only call
        # against an already-CONNECTED feed would re-test (and could regress) a connection
        # nothing about this request touched. See _status_enum_from_stored for why order's
        # CONNECTED-gating below still needs the TRUE current feed status even when skipped here.
        already_synced = bool(existing and existing.initial_sync_completed)
        feed_fields_touched = (isinstance(credentials, dict) and "feed" in credentials) or not already_synced
        if feed_fields_touched:
            if creds.get("feed"):
                validated, val_error, val_error_code = _validate_connection(provider.kind, creds["feed"])
                if val_error:
                    return None, val_error, val_error_code
            else:
                # "feed" was never submitted (by this call or any prior one) — nothing to
                # validate yet, distinct from a real feed failure (which returns above).
                validated = None
            # validated is True (validator ran and passed) or None (no validator for this kind,
            # or no feed data yet) — False already returned above via val_error. A successful
            # re-validation preserves CONNECTED if the initial sync already completed instead of
            # regressing to INGESTING — nothing about the sync itself changed, just re-confirmed
            # the same credentials still work (e.g. after a rotation).
            status_enum = (
                (
                    src_enums.CompanyProviderConnectionStatus.CONNECTED if already_synced
                    else src_enums.CompanyProviderConnectionStatus.INGESTING
                )
                if validated else None
            )
            status_reason = None
        else:
            validated, status_enum, status_reason = None, None, None
        effective_feed_status_enum = (
            status_enum if feed_fields_touched
            else (_order_gating_feed_status_enum(existing) if existing else None)
        )

        # Order credentials are validated separately from feed credentials (different transport,
        # different client, different failure modes — e.g. Keystone's FTP feed vs its SOAP order
        # API) but a failure here rejects the whole request too, same as feed above — invalid
        # order credentials aren't saved just because the feed happened to be fine. Only run
        # this when "order" was actually part of THIS request, for the same reason as feed above.
        order_fields_touched = isinstance(credentials, dict) and "order" in credentials
        if order_fields_touched:
            order_validated, order_status_enum, order_status_reason, order_val_error, order_val_error_code = (
                _validate_and_resolve_order_status(provider.kind, creds.get("order"), effective_feed_status_enum)
            )
            if order_val_error:
                return None, order_val_error, order_val_error_code
        else:
            order_validated, order_status_enum, order_status_reason = None, None, None

    # Only include a namespace's status fields in what actually gets written when this call
    # touched that namespace — otherwise leave the existing row's stored values alone entirely
    # (as opposed to overwriting them with the None a skipped validation would otherwise produce).
    status_fields = {}
    if feed_fields_touched:
        status_fields.update(_connection_status_fields(status_enum, status_reason))
    if order_fields_touched:
        status_fields.update(_order_connection_status_fields(order_status_enum, order_status_reason))

    # "order" is never persisted onto CompanyProviders.credentials — only "feed" lives there;
    # order credentials go to this connection's default CompanyProviderOrderAccount row instead
    # (see _sync_default_order_account), which needs cp to already have an id, hence the call
    # placement after save/create below rather than folding it into `creds` first.
    order_creds_to_sync = creds.pop("order", None)

    if existing:
        existing.credentials = creds
        for field, value in status_fields.items():
            setattr(existing, field, value)
        existing.save()
        cp = existing
    else:
        cp = src_models.CompanyProviders.objects.create(
            company_id=company_id,
            provider_id=provider_id,
            credentials=creds,
            primary=False,
            **status_fields,
        )
    if order_fields_touched:
        _sync_default_order_account(cp, order_creds_to_sync or {}, order_status_enum, order_status_reason)

    # Only (re-)enqueue a pricing sync when feed was actually part of this request (or the
    # initial sync hasn't completed yet) — an order-only save has nothing to do with the feed
    # and shouldn't trigger a full re-sync against the distributor's API.
    if feed_fields_touched and integration_pricing_sync_jobs.should_enqueue_pricing_sync(provider.kind):
        integration_pricing_sync_jobs.enqueue_company_provider_pricing_sync(cp.id)

    result = {
        "id": cp.id,
        "company_provider_id": cp.id,
        "company_id": cp.company_id,
        "provider_id": cp.provider_id,
        "provider_name": provider.name,
        "primary": cp.primary,
        "connection_validated": validated,
        "order_connection_validated": order_validated,
        "status": cp.status,
        "status_name": cp.status_name,
        "status_reason": cp.status_reason,
        "status_checked_at": cp.status_checked_at.isoformat() if cp.status_checked_at else None,
        "order_status": cp.order_status,
        "order_status_name": cp.order_status_name,
        "order_status_reason": cp.order_status_reason,
        "order_status_checked_at": cp.order_status_checked_at.isoformat() if cp.order_status_checked_at else None,
        "created_at": cp.created_at.isoformat() if cp.created_at else None,
        "updated_at": cp.updated_at.isoformat() if cp.updated_at else None,
    }
    result.update(
        _redacted_credentials_for_catalog_entry(
            catalog_entry, cp.credentials, credentials_helper.get_order_credentials(cp)
        )
    )
    return result, None, None


def update_connection(
    company_id: int,
    company_provider_id: int,
    credentials: typing.Dict[str, typing.Any],
) -> typing.Tuple[typing.Optional[typing.Dict], typing.Optional[str], typing.Optional[str]]:
    """
    Patch credentials for an existing connection (``CompanyProviders`` by id and company).
    Merges with stored JSON (see :func:`_merge_update_credentials`); re-enqueues
    :func:`integration_pricing_sync_jobs.enqueue_company_provider_pricing_sync` the same
    way as :func:`connect_provider` on success. Returns (data, error_message, error_code) —
    error_code is one of the ``CONNECTION_ERROR_*`` constants, for the frontend to branch on.
    """
    cp = src_models.CompanyProviders.objects.filter(
        id=company_provider_id,
        company_id=company_id,
    ).select_related("provider", "company").first()
    if not cp or not cp.provider:
        return None, "Connection not found", CONNECTION_ERROR_NOT_FOUND

    catalog_entry = _get_catalog_entry_for_provider(cp.provider_id)
    if not catalog_entry:
        return None, "Provider not found in catalog", CONNECTION_ERROR_NOT_FOUND

    # cp.credentials never carries "order" (see CompanyProviderOrderAccount) — seed the merge
    # baseline from this connection's actual default account instead, so a feed-only PATCH still
    # correctly carries forward whatever order credentials already exist.
    merge_baseline = dict(cp.credentials or {})
    merge_baseline["order"] = credentials_helper.get_order_credentials(cp)
    creds, err, err_code = _merge_update_credentials(
        catalog_entry,
        merge_baseline,
        credentials,
    )
    if err:
        return None, err, err_code

    already_synced = cp.initial_sync_completed
    feed_submitted = isinstance(credentials, dict) and "feed" in credentials
    order_submitted = isinstance(credentials, dict) and "order" in credentials

    if catalog_entry.get("relay_provisioned"):
        # "feed" is system-generated for relay providers (never part of `credentials`/`creds`
        # here). Once the initial sync has completed, don't keep re-running the relay
        # file-presence check on every unrelated (e.g. order-only) PATCH — it can only report
        # WAITING/INGESTING, never CONNECTED, so re-running it after CONNECTED would regress the
        # stored status for no reason. "order" (e.g. Meyer's order API creds) is a normal
        # user-submitted namespace and must still be validated on every PATCH that touches it,
        # same as the non-relay branch below — it was previously validated unconditionally on
        # every PATCH regardless of whether "order" was actually submitted.
        feed_fields_touched = not already_synced
        if feed_fields_touched:
            validated = None
            status_enum, status_reason = _relay_feed_connection_status(cp.company, cp.provider.kind)
        else:
            validated, status_enum, status_reason = None, None, None
        effective_feed_status_enum = (
            status_enum if feed_fields_touched else _order_gating_feed_status_enum(cp)
        )

        order_fields_touched = order_submitted
        if order_fields_touched:
            order_validated, order_status_enum, order_status_reason, order_val_error, order_val_error_code = (
                _validate_and_resolve_order_status(cp.provider.kind, creds.get("order"), effective_feed_status_enum)
            )
            if order_val_error:
                return None, order_val_error, order_val_error_code
        else:
            order_validated, order_status_enum, order_status_reason = None, None, None
    else:
        # Only re-validate "feed" (and touch its status fields) when this call actually
        # submitted it, or the initial sync hasn't completed yet — otherwise an order-only PATCH
        # against an already-CONNECTED feed would re-test (and could regress) a connection
        # nothing about this request touched.
        feed_fields_touched = feed_submitted or not already_synced
        if feed_fields_touched:
            if creds.get("feed"):
                validated, val_error, val_error_code = _validate_connection(cp.provider.kind, creds["feed"])
                if val_error:
                    return None, val_error, val_error_code
            else:
                # "feed" isn't configured on this connection yet (order-only so far) — nothing
                # to validate, distinct from a real feed failure (which returns above).
                validated = None
            # A successful re-validation preserves CONNECTED if the initial sync already
            # completed instead of regressing to INGESTING — nothing about the sync itself
            # changed, just re-confirmed the same credentials still work (e.g. after rotation).
            status_enum = (
                (
                    src_enums.CompanyProviderConnectionStatus.CONNECTED if already_synced
                    else src_enums.CompanyProviderConnectionStatus.INGESTING
                )
                if validated else None
            )
            status_reason = None
        else:
            validated, status_enum, status_reason = None, None, None
        effective_feed_status_enum = (
            status_enum if feed_fields_touched else _order_gating_feed_status_enum(cp)
        )

        # Order credentials validate independently of feed credentials — a failure here rejects
        # the whole request too, same as feed above; see connect_provider for the same logic.
        # Only run when "order" was actually part of THIS request, for the same reason as feed.
        order_fields_touched = order_submitted
        if order_fields_touched:
            order_validated, order_status_enum, order_status_reason, order_val_error, order_val_error_code = (
                _validate_and_resolve_order_status(cp.provider.kind, creds.get("order"), effective_feed_status_enum)
            )
            if order_val_error:
                return None, order_val_error, order_val_error_code
        else:
            order_validated, order_status_enum, order_status_reason = None, None, None

    # "order" is never persisted onto CompanyProviders.credentials — see connect_provider.
    order_creds_to_sync = creds.pop("order", None)
    cp.credentials = creds
    # Only include a namespace's status fields in what actually gets written when this call
    # touched that namespace — otherwise leave the existing row's stored values alone entirely.
    status_fields = {}
    if feed_fields_touched:
        status_fields.update(_connection_status_fields(status_enum, status_reason))
    if order_fields_touched:
        status_fields.update(_order_connection_status_fields(order_status_enum, order_status_reason))
    for field, value in status_fields.items():
        setattr(cp, field, value)
    cp.save()
    if order_fields_touched:
        _sync_default_order_account(cp, order_creds_to_sync or {}, order_status_enum, order_status_reason)

    # Only (re-)enqueue a pricing sync when feed was actually part of this request (or the
    # initial sync hasn't completed yet) — an order-only PATCH has nothing to do with the feed
    # and shouldn't trigger a full re-sync against the distributor's API.
    if feed_fields_touched and integration_pricing_sync_jobs.should_enqueue_pricing_sync(cp.provider.kind):
        integration_pricing_sync_jobs.enqueue_company_provider_pricing_sync(cp.id)

    result = {
        "id": cp.id,
        "company_provider_id": cp.id,
        "company_id": cp.company_id,
        "provider_id": cp.provider_id,
        "provider_name": cp.provider.name,
        "primary": cp.primary,
        "connection_validated": validated,
        "order_connection_validated": order_validated,
        "status": cp.status,
        "status_name": cp.status_name,
        "status_reason": cp.status_reason,
        "status_checked_at": cp.status_checked_at.isoformat() if cp.status_checked_at else None,
        "order_status": cp.order_status,
        "order_status_name": cp.order_status_name,
        "order_status_reason": cp.order_status_reason,
        "order_status_checked_at": cp.order_status_checked_at.isoformat() if cp.order_status_checked_at else None,
        "created_at": cp.created_at.isoformat() if cp.created_at else None,
        "updated_at": cp.updated_at.isoformat() if cp.updated_at else None,
    }
    result.update(
        _redacted_credentials_for_catalog_entry(
            catalog_entry, cp.credentials, credentials_helper.get_order_credentials(cp)
        )
    )
    return result, None, None


def disconnect_provider(
    company_id: int,
    company_provider_id: int,
    namespace: typing.Optional[str] = None,
) -> typing.Tuple[bool, typing.Optional[str]]:
    """
    Disconnect a ``CompanyProviders`` connection. Must belong to company.

    ``namespace=None`` (default): delete the whole row — the original, still-default behavior.

    ``namespace="feed"`` / ``namespace="order"``: clear just that namespace's credentials and
    status fields, leaving the other namespace's connection intact — the disconnect-side
    counterpart to being able to save "feed" and "order" independently via connect/PATCH (see
    connect_provider). If clearing a namespace leaves BOTH empty, the whole row is deleted, same
    as the no-namespace case — an all-empty connection has no reason to exist.

    Returns (success, error_message). On success error_message is None.
    """
    cp = src_models.CompanyProviders.objects.filter(
        id=company_provider_id,
        company_id=company_id,
    ).first()
    if not cp:
        return False, "Connection not found"

    if namespace is None:
        cp.delete()
        return True, None

    if namespace not in ("feed", "order"):
        return False, 'namespace must be "feed" or "order"'

    if namespace == "feed":
        creds = dict(cp.credentials or {})
        creds["feed"] = {}
        cp.credentials = creds
        cp.status = None
        cp.status_name = None
        cp.status_reason = None
        cp.status_checked_at = None
        # Also reset the durable "has synced at least once" fact, not just the narrower status
        # enum — otherwise a later order-only call would see initial_sync_completed still True
        # and incorrectly treat this now-disconnected feed as connected for order-gating purposes
        # (see _order_gating_feed_status_enum). A genuine reconnect re-earns CONNECTED the normal
        # way once its sync actually completes.
        cp.initial_sync_completed = False
        # Order can never be CONNECTED once feed isn't — demote rather than clear, since the
        # order credentials themselves are still valid/unaffected by disconnecting feed. Demote
        # the default account's own row too (it's the source of truth for its status now), then
        # mirror the same values onto cp.
        default_account = credentials_helper.get_default_order_account(cp)
        if default_account and default_account.order_status == src_enums.CompanyProviderOrderConnectionStatus.CONNECTED.value:
            _set_order_account_status(
                default_account,
                src_enums.CompanyProviderOrderConnectionStatus.WAITING,
                _ORDER_WAITING_ON_FEED_REASON,
            )
        if cp.order_status == src_enums.CompanyProviderOrderConnectionStatus.CONNECTED.value:
            cp.order_status = src_enums.CompanyProviderOrderConnectionStatus.WAITING.value
            cp.order_status_name = src_enums.CompanyProviderOrderConnectionStatus.WAITING.name
            cp.order_status_reason = _ORDER_WAITING_ON_FEED_REASON
            cp.order_status_checked_at = timezone.now()
    else:
        # Order credentials live on the default CompanyProviderOrderAccount row, not
        # cp.credentials — delete it outright when nothing protects it (no order history yet),
        # else deactivate/clear it in place so PurchaseOrder.order_account (PROTECT) never
        # breaks. Either way, promote the next remaining account (if any) to be the new default
        # so a company with other configured accounts isn't left unable to order at all.
        default_account = cp.order_accounts.filter(is_default=True).first()
        if default_account:
            try:
                default_account.delete()
            except ProtectedError:
                default_account.credentials = {}
                default_account.active = False
                default_account.is_default = False
                status_fields = _order_connection_status_fields(None, None)
                for field, value in status_fields.items():
                    setattr(default_account, field, value)
                default_account.save(
                    update_fields=["credentials", "active", "is_default"]
                    + list(status_fields.keys())
                    + ["updated_at"]
                )
            _promote_next_default_order_account(cp)
        # Mirrors whichever account is now the default (freshly promoted, with its own already-
        # known status) or nulls out if none remain — rather than unconditionally nulling cp's
        # fields, which would incorrectly wipe out a newly-promoted account's real status.
        _refresh_default_order_status(cp)

    feed_empty = not (cp.credentials or {}).get("feed")
    order_empty = not credentials_helper.get_order_credentials(cp)
    if feed_empty and order_empty:
        cp.delete()
        return True, None

    cp.save()
    return True, None


# -- Order accounts (src.models.CompanyProviderOrderAccount) --------------------------------
#
# A connection's normal "Ordering" section is what connect_provider/update_connection/
# disconnect_provider manage below — it always writes to this connection's is_default=True
# CompanyProviderOrderAccount row (via _sync_default_order_account), never to
# CompanyProviders.credentials directly. create_order_account/update_order_account/
# delete_order_account further down manage ADDITIONAL, explicitly-named, always-non-default
# accounts beyond that one (e.g. a company that places its own shop's orders through one
# Keystone account and drop-ships through a second).


def _sync_default_order_account(
    company_provider: src_models.CompanyProviders,
    order_credentials: typing.Dict[str, typing.Any],
    status: typing.Optional["src_enums.CompanyProviderOrderConnectionStatus"] = None,
    reason: typing.Optional[str] = None,
) -> None:
    """
    Keeps this connection's is_default=True account in sync with the "order" credentials
    connect_provider/update_connection just merged/validated — creating it on first use,
    updating it thereafter. ``order_credentials`` being empty (order never configured, or just
    disconnected) deactivates rather than deletes an existing default row, so
    PurchaseOrder.order_account (PROTECT) never breaks over a routine credentials change; use
    disconnect_provider for an explicit, intentional removal.

    ``status``/``reason`` are this call's already-computed order-status result (see
    _validate_and_resolve_order_status) — stored directly on the account row itself, since
    CompanyProviderOrderAccount is the source of truth for order status now, including the
    default's. Callers still need to mirror onto CompanyProviders separately (they already do —
    see connect_provider/update_connection, which set the same computed values there directly).
    """
    # Looked up regardless of `active` (unlike credentials_helper.get_default_order_account,
    # which callers use to find a USABLE default for placing orders) — a previously-deactivated
    # default must be found and reactivated here, not shadowed by a second is_default=True row.
    account = company_provider.order_accounts.filter(is_default=True).first()
    if not order_credentials:
        if account:
            account.credentials = {}
            account.active = False
            account.is_default = False
            status_fields = _order_connection_status_fields(None, None)
            for field, value in status_fields.items():
                setattr(account, field, value)
            account.save(
                update_fields=["credentials", "active", "is_default"] + list(status_fields.keys()) + ["updated_at"]
            )
            _promote_next_default_order_account(company_provider)
        return
    status_fields = _order_connection_status_fields(status, reason)
    if account:
        account.credentials = order_credentials
        account.active = True
        for field, value in status_fields.items():
            setattr(account, field, value)
        account.save(update_fields=["credentials", "active"] + list(status_fields.keys()) + ["updated_at"])
    else:
        src_models.CompanyProviderOrderAccount.objects.create(
            company_provider=company_provider,
            label="Default",
            credentials=order_credentials,
            is_default=True,
            **status_fields,
        )


def _promote_next_default_order_account(company_provider: src_models.CompanyProviders) -> None:
    """After the is_default account is deleted outright (see disconnect_provider), promotes the
    oldest remaining active additional account to is_default so ordering isn't silently left
    unusable when other configured accounts could serve as the default — same "promote the next
    one" idiom as company_locations.delete_company_location for is_primary."""
    next_account = company_provider.order_accounts.filter(active=True).order_by("created_at").first()
    if next_account:
        next_account.is_default = True
        next_account.save(update_fields=["is_default", "updated_at"])


def _refresh_default_order_status(company_provider: src_models.CompanyProviders) -> None:
    """
    Mirrors CompanyProviders.order_status/order_status_name/order_status_reason/
    order_status_checked_at from whichever CompanyProviderOrderAccount row is currently this
    connection's default — that account is the source of truth for order status (including the
    default's own), so this is a straight copy, not a recompute. Called after
    create_order_account/update_order_account/delete_order_account change the default account's
    identity, credentials, or active state, so the catalog listing's Ordering badge (which reads
    these same four fields on CompanyProviders) never goes stale regardless of whether a caller
    used these endpoints or the legacy connect_provider/update_connection "order" namespace path
    (which sets the mirrored fields directly, from the same computation it already stores on the
    account via _sync_default_order_account). Non-default accounts have their own status but
    don't feed into this mirror — CompanyProviders.order_status has always represented the
    connection's one primary ordering capability, not a per-account concept.
    """
    default_account = credentials_helper.get_default_order_account(company_provider)
    if default_account is None:
        status_fields = _order_connection_status_fields(None, None)
    else:
        status_fields = {
            "order_status": default_account.order_status,
            "order_status_name": default_account.order_status_name,
            "order_status_reason": default_account.order_status_reason,
            "order_status_checked_at": default_account.order_status_checked_at,
        }
    for field, value in status_fields.items():
        setattr(company_provider, field, value)
    company_provider.save(update_fields=list(status_fields.keys()) + ["updated_at"])


def _order_credential_fields_for_method(
    catalog_entry: typing.Dict[str, typing.Any], order_method: int
) -> typing.Tuple[typing.List[str], typing.List[str]]:
    """(required, optional) field lists for whichever channel order_method selects — API reads
    the existing order_connection_*_fields catalog keys unchanged; EMAIL reads the parallel
    email_order_connection_*_fields keys (see src/constants.py PROVIDER_CATALOG)."""
    if order_method == src_enums.OrderMethod.EMAIL.value:
        return (
            list(catalog_entry.get("email_order_connection_required_fields") or []),
            list(catalog_entry.get("email_order_connection_optional_fields") or []),
        )
    return (
        list(catalog_entry.get("order_connection_required_fields") or []),
        list(catalog_entry.get("order_connection_optional_fields") or []),
    )


def _serialize_order_account(
    catalog_entry: typing.Dict[str, typing.Any], account: src_models.CompanyProviderOrderAccount
) -> typing.Dict[str, typing.Any]:
    required, optional = _order_credential_fields_for_method(catalog_entry, account.order_method)
    credentials, secrets_configured = _redacted_credentials(required, optional, account.credentials)
    return {
        "id": account.id,
        "company_provider_id": account.company_provider_id,
        "label": account.label,
        "active": account.active,
        "is_default": account.is_default,
        # 'api' | 'email' — see src.enums.OrderMethod. Determines which credential fields above
        # are the "current" ones; the other channel's values (if any — see the non-destructive
        # merge in create_order_account/update_order_account) are still present in `credentials`
        # under "any other keys in storage" but aren't part of `required`/`optional` for display.
        "order_method": account.order_method_name.lower(),
        "credentials": credentials,
        "secrets_configured": secrets_configured,
        "order_status": account.order_status,
        "order_status_name": account.order_status_name,
        "order_status_reason": account.order_status_reason,
        "order_status_checked_at": (
            account.order_status_checked_at.isoformat() if account.order_status_checked_at else None
        ),
        "created_at": account.created_at.isoformat() if account.created_at else None,
        "updated_at": account.updated_at.isoformat() if account.updated_at else None,
    }


def list_order_accounts(company_id: int, company_provider_id: int) -> typing.Optional[typing.List[typing.Dict]]:
    """
    Every order account for this connection, for the Settings > Integrations "Ordering
    accounts" management list — the default account (whatever was entered through the
    connection's normal "Ordering" section) first, then any additional named accounts, oldest
    first. Returns None if the connection doesn't exist for this company, or an empty list if
    order credentials have never been configured at all.
    """
    cp = (
        src_models.CompanyProviders.objects.filter(id=company_provider_id, company_id=company_id)
        .select_related("provider")
        .first()
    )
    if not cp or not cp.provider:
        return None
    catalog_entry = _get_catalog_entry_for_provider(cp.provider_id) or {}
    accounts = cp.order_accounts.order_by("-is_default", "created_at")
    return [_serialize_order_account(catalog_entry, account) for account in accounts]


def create_order_account(
    company_id: int,
    company_provider_id: int,
    label: str,
    credentials: typing.Dict[str, typing.Any],
    is_default: typing.Optional[bool] = None,
    order_method: typing.Optional[str] = None,
) -> typing.Tuple[typing.Optional[typing.Dict], typing.Optional[str], typing.Optional[str]]:
    """
    Adds an order account to a connection — this is the ONE path for creating an order account,
    whether it's the very first one (which always becomes the default, same as
    CompanyLocation's "first location defaults to primary") or an additional one alongside an
    existing default (which becomes the default too, but only if ``is_default=True`` is passed
    explicitly; otherwise it's added as a non-default alternative). ``credentials`` is a flat
    dict of this provider's order-connection fields (same fields as the "order" namespace in
    the legacy connect_provider/update_connection path, NOT nested under "order" again since
    it's already scoped by this endpoint) — validated live the same way, before being saved.

    ``order_method`` ('api' | 'email', see src.enums.OrderMethod) selects which field set
    ``credentials`` is validated against and which channel orders.registry.get_adapter() routes
    this account through. Defaults to 'api' (the only channel that existed before this
    parameter), matching CompanyProviderOrderAccount.order_method's own default. 'email' is
    always available regardless of whether this provider kind has a real order adapter — see
    _order_credential_fields_for_method.

    Returns (data, error_message, error_code); error_code is one of the ``CONNECTION_ERROR_*``
    constants, for the frontend to branch on.
    """
    label = (label or "").strip()
    if not label:
        return None, "label is required.", CONNECTION_ERROR_INVALID_INPUT

    method_value = (
        src_enums.OrderMethod.EMAIL.value
        if (order_method or "").strip().lower() == "email"
        else src_enums.OrderMethod.API.value
    )

    cp = (
        src_models.CompanyProviders.objects.filter(id=company_provider_id, company_id=company_id)
        .select_related("provider")
        .first()
    )
    if not cp or not cp.provider:
        return None, "Connection not found", CONNECTION_ERROR_NOT_FOUND

    catalog_entry = _get_catalog_entry_for_provider(cp.provider_id)
    if not catalog_entry:
        return None, "Provider not found in catalog", CONNECTION_ERROR_NOT_FOUND

    order_required, order_optional = _order_credential_fields_for_method(catalog_entry, method_value)
    if not order_required and not order_optional:
        return (
            None,
            "{} doesn't support order accounts yet.".format(cp.provider.name),
            CONNECTION_ERROR_INVALID_INPUT,
        )

    creds: typing.Dict[str, typing.Any] = {}
    err, err_code = _merge_namespace_credentials(creds, order_required, order_optional, credentials)
    if err:
        return None, err, err_code
    missing = [f for f in order_required if not _normalize_credential_value(creds.get(f))]
    if missing:
        return None, "Missing required fields: {}".format(", ".join(missing)), CONNECTION_ERROR_MISSING_FIELDS

    validated, val_error, val_error_code = _validate_order_connection_for_method(cp.provider.kind, method_value, creds)
    if val_error:
        return None, val_error, val_error_code

    make_default = is_default is True or not cp.order_accounts.exists()

    try:
        with transaction.atomic():
            if make_default:
                cp.order_accounts.filter(is_default=True).update(is_default=False)
            account = src_models.CompanyProviderOrderAccount.objects.create(
                company_provider=cp,
                label=label,
                credentials=creds,
                is_default=make_default,
                order_method=method_value,
                order_method_name=src_enums.OrderMethod(method_value).name,
            )
    except IntegrityError:
        return (
            None,
            'An order account named "{}" already exists for this connection.'.format(label),
            CONNECTION_ERROR_INVALID_INPUT,
        )

    status_enum, reason = _resolve_order_status(True, None, _order_gating_feed_status_enum(cp))
    _set_order_account_status(account, status_enum, reason)
    if make_default:
        _refresh_default_order_status(cp)

    result = _serialize_order_account(catalog_entry, account)
    result["connection_validated"] = validated
    return result, None, None


def update_order_account(
    company_id: int,
    order_account_id: int,
    label: typing.Optional[str] = None,
    credentials: typing.Optional[typing.Dict[str, typing.Any]] = None,
    active: typing.Optional[bool] = None,
    is_default: typing.Optional[bool] = None,
    order_method: typing.Optional[str] = None,
) -> typing.Tuple[typing.Optional[typing.Dict], typing.Optional[str], typing.Optional[str]]:
    """
    Partial update of ANY order account, including the default one — label/credentials/active/
    is_default/order_method may each be omitted to leave them unchanged. ``credentials`` is
    merged onto the stored dict the same way the legacy connect_provider/update_connection path
    merges "order" (non-empty values overwrite, empty/null for a sensitive field leaves it
    unchanged), then re-validated live if anything actually changed. ``is_default=True``
    promotes this account to be the connection's default (demoting whichever one currently is);
    ``is_default=False`` on the current default demotes it and promotes the next-oldest active
    account, same as deactivating it (see below) — there always at most one default, never zero
    once at least one active account exists.

    ``order_method`` ('api' | 'email') switches which channel this account places orders
    through — see orders.registry.get_adapter()'s channel-first dispatch. Switching is
    non-destructive: credentials are validated/merged against whichever channel's field list is
    now active, but _merge_namespace_credentials only ever touches keys in that list, so the
    OTHER channel's previously-entered values (e.g. Keystone's account_number/security_key when
    switching to Email, or rep_email/cc_email when switching back to API) stay in `credentials`
    untouched — switching back later doesn't require re-entering them. When ``order_method`` is
    given WITHOUT ``credentials`` (a bare channel switch), the account's already-stored values
    for that channel are used as-is and re-validated only if that channel requires live
    API-credential validation (email needs none — see _validate_email_order_connection).
    """
    account = (
        src_models.CompanyProviderOrderAccount.objects.filter(
            id=order_account_id, company_provider__company_id=company_id
        )
        .select_related("company_provider__provider")
        .first()
    )
    if not account:
        return None, "Order account not found", CONNECTION_ERROR_NOT_FOUND

    cp = account.company_provider
    catalog_entry = _get_catalog_entry_for_provider(cp.provider_id)
    if not catalog_entry:
        return None, "Provider not found in catalog", CONNECTION_ERROR_NOT_FOUND

    method_value = account.order_method
    switching_method = False
    if order_method is not None:
        new_method_value = (
            src_enums.OrderMethod.EMAIL.value
            if order_method.strip().lower() == "email"
            else src_enums.OrderMethod.API.value
        )
        switching_method = new_method_value != method_value
        method_value = new_method_value

    validated = None
    if credentials or switching_method:
        order_required, order_optional = _order_credential_fields_for_method(catalog_entry, method_value)
        if not order_required and not order_optional:
            return (
                None,
                "{} doesn't support {} ordering.".format(
                    cp.provider.name, "email" if method_value == src_enums.OrderMethod.EMAIL.value else "API"
                ),
                CONNECTION_ERROR_INVALID_INPUT,
            )
        creds = dict(account.credentials or {})
        if credentials:
            err, err_code = _merge_namespace_credentials(creds, order_required, order_optional, credentials)
            if err:
                return None, err, err_code
        missing = [f for f in order_required if not _normalize_credential_value(creds.get(f))]
        if missing:
            return None, "Missing required fields: {}".format(", ".join(missing)), CONNECTION_ERROR_MISSING_FIELDS
        validated, val_error, val_error_code = _validate_order_connection_for_method(cp.provider.kind, method_value, creds)
        if val_error:
            return None, val_error, val_error_code
        account.credentials = creds
        account.order_method = method_value
        account.order_method_name = src_enums.OrderMethod(method_value).name
        status_enum, reason = _resolve_order_status(True, None, _order_gating_feed_status_enum(cp))
        status_fields = _order_connection_status_fields(status_enum, reason)
        for field, value in status_fields.items():
            setattr(account, field, value)

    if label is not None:
        label = label.strip()
        if not label:
            return None, "label is required.", CONNECTION_ERROR_INVALID_INPUT
        account.label = label

    was_default = account.is_default
    if active is not None:
        account.active = bool(active)

    try:
        account.save()
    except IntegrityError:
        return (
            None,
            'An order account named "{}" already exists for this connection.'.format(account.label),
            CONNECTION_ERROR_INVALID_INPUT,
        )

    if is_default is True and not account.is_default:
        cp.order_accounts.filter(is_default=True).exclude(id=account.id).update(is_default=False)
        account.is_default = True
        account.save(update_fields=["is_default", "updated_at"])
    elif (
        (is_default is False or (is_default is None and active is False))
        and was_default
        and account.is_default
    ):
        # Deactivating the current default (or explicitly un-defaulting it) leaves ordering
        # silently unusable if this connection has other configured accounts that could serve
        # instead — demote and promote the next one, same as disconnect_provider does when the
        # default is removed outright.
        account.is_default = False
        account.save(update_fields=["is_default", "updated_at"])
        _promote_next_default_order_account(cp)

    if credentials or switching_method or was_default or account.is_default:
        _refresh_default_order_status(cp)

    result = _serialize_order_account(catalog_entry, account)
    if validated is not None:
        result["connection_validated"] = validated
    return result, None, None


def delete_order_account(company_id: int, order_account_id: int) -> typing.Tuple[bool, typing.Optional[str]]:
    """
    Deletes ANY order account, including the default (promoting the oldest remaining active
    account to be the new default, if any — see _promote_next_default_order_account). Blocked
    (rather than silently orphaning history) while any PurchaseOrder still references it —
    PurchaseOrder.order_account is on_delete=PROTECT for exactly this reason; deactivate it via
    update_order_account(active=False) instead if it has order history.
    """
    account = (
        src_models.CompanyProviderOrderAccount.objects.filter(
            id=order_account_id, company_provider__company_id=company_id
        )
        .select_related("company_provider")
        .first()
    )
    if not account:
        return False, "Order account not found"
    cp = account.company_provider
    was_default = account.is_default
    try:
        account.delete()
    except ProtectedError:
        return False, "This account has order history and can't be deleted — deactivate it instead."
    if was_default:
        _promote_next_default_order_account(cp)
    _refresh_default_order_status(cp)
    return True, None


def get_company_providers(company_id: int) -> typing.List[typing.Dict]:
    """
    Get company providers for a given company_id.
    Left joins with Providers to get provider details.
    
    Args:
        company_id: The ID of the company
        
    Returns:
        List of dictionaries containing company provider data with provider details
    """
    logger.info('{} Fetching company providers for company_id: {}.'.format(
        _LOG_PREFIX, company_id
    ))
    
    company_providers = src_models.CompanyProviders.objects.filter(
        company_id=company_id
    ).select_related('provider').all()
    
    data = []
    for cp in company_providers:
        provider = cp.provider
        row = {
            "id": cp.id,
            "company_id": cp.company_id,
            "provider_id": cp.provider_id,
            "provider_name": provider.name if provider else None,
            "provider_status": provider.status if provider else None,
            "provider_status_name": provider.status_name if provider else None,
            "provider_type": provider.type if provider else None,
            "provider_type_name": provider.type_name if provider else None,
            "provider_kind": provider.kind if provider else None,
            "provider_kind_name": provider.kind_name if provider else None,
            "primary": cp.primary,
            "created_at": cp.created_at.isoformat() if cp.created_at else None,
            "updated_at": cp.updated_at.isoformat() if cp.updated_at else None,
        }
        catalog_entry = {}
        if provider:
            for entry in src_constants.PROVIDER_CATALOG:
                if entry["kind"].value == provider.kind:
                    catalog_entry = entry
                    break
        row.update(
            _redacted_credentials_for_catalog_entry(
                catalog_entry, cp.credentials, credentials_helper.get_order_credentials(cp)
            )
        )
        if provider:
            row.update(_provider_ui_metadata(provider))
        data.append(row)
    
    logger.info('{} Found {} company providers for company_id: {}.'.format(
        _LOG_PREFIX, len(data), company_id
    ))
    
    return data


def get_company_provider_by_id(company_id: int, provider_id: int) -> typing.Optional[typing.Dict]:
    """
    Get a single company provider by ID for a given company_id.
    Left joins with Providers to get provider details.
    
    Args:
        company_id: The ID of the company
        provider_id: The ID of the company provider
        
    Returns:
        Dictionary containing company provider data with provider details, or None if not found
    """
    logger.info('{} Fetching company provider with id: {} for company_id: {}.'.format(
        _LOG_PREFIX, provider_id, company_id
    ))
    
    try:
        company_provider = src_models.CompanyProviders.objects.filter(
            id=provider_id,
            company_id=company_id
        ).select_related('provider').first()
        
        if not company_provider:
            logger.warning('{} Company provider with id: {} not found for company_id: {}.'.format(
                _LOG_PREFIX, provider_id, company_id
            ))
            return None
        
        provider = company_provider.provider
        catalog_entry = {}
        if provider:
            for entry in src_constants.PROVIDER_CATALOG:
                if entry["kind"].value == provider.kind:
                    catalog_entry = entry
                    break
        data = {
            "id": company_provider.id,
            "company_id": company_provider.company_id,
            "provider_id": company_provider.provider_id,
            "provider_name": provider.name if provider else None,
            "provider_status": provider.status if provider else None,
            "provider_status_name": provider.status_name if provider else None,
            "provider_type": provider.type if provider else None,
            "provider_type_name": provider.type_name if provider else None,
            "provider_kind": provider.kind if provider else None,
            "provider_kind_name": provider.kind_name if provider else None,
            "primary": company_provider.primary,
            "connection_required_fields": list(catalog_entry.get("connection_required_fields") or []),
            "connection_optional_fields": list(catalog_entry.get("connection_optional_fields") or []),
            "order_connection_required_fields": list(catalog_entry.get("order_connection_required_fields") or []),
            "order_connection_optional_fields": list(catalog_entry.get("order_connection_optional_fields") or []),
            "email_order_connection_required_fields": list(catalog_entry.get("email_order_connection_required_fields") or []),
            "email_order_connection_optional_fields": list(catalog_entry.get("email_order_connection_optional_fields") or []),
            "supports_ordering": _catalog_supports_ordering_display(catalog_entry) if catalog_entry else False,
            "order_credentials_mirror_feed": (
                _catalog_order_credentials_mirror_feed(catalog_entry) if catalog_entry else False
            ),
            "created_at": company_provider.created_at.isoformat() if company_provider.created_at else None,
            "updated_at": company_provider.updated_at.isoformat() if company_provider.updated_at else None,
        }
        data.update(
            _redacted_credentials_for_catalog_entry(
                catalog_entry, company_provider.credentials, credentials_helper.get_order_credentials(company_provider)
            )
        )
        if provider:
            data.update(_provider_ui_metadata(provider))

        logger.info('{} Found company provider with id: {} for company_id: {}.'.format(
            _LOG_PREFIX, provider_id, company_id
        ))
        
        return data
    except Exception as e:
        logger.error('{} Error fetching company provider with id: {} for company_id: {}. Error: {}.'.format(
            _LOG_PREFIX, provider_id, company_id, str(e)
        ))
        raise


def _redacted_credentials(
    required: typing.List[str],
    optional: typing.List[str],
    raw: typing.Optional[typing.Dict[str, typing.Any]],
) -> typing.Tuple[typing.Dict[str, typing.Any], typing.Dict[str, bool]]:
    """
    Keys: `required` + `optional` (in order), then any other keys in storage.
    Non-sensitive: stored values. Sensitive: always ``null`` in the returned dict; if a value is
    stored, ``secrets_configured[key]`` is True (so the FE can show "password set" without
    echoing the secret). Used for both the "feed" and "order" namespaces of a connection.
    """
    required = list(required or [])
    optional = list(optional or [])
    raw = raw or {}
    key_order: typing.List[str] = []
    seen: typing.Set[str] = set()
    for k in required + optional:
        ks = str(k).strip() if k is not None else ""
        if ks and ks not in seen:
            seen.add(ks)
            key_order.append(ks)
    for k in sorted(raw.keys(), key=str):
        ks = str(k).strip() if k is not None else ""
        if ks and ks not in seen:
            seen.add(ks)
            key_order.append(ks)

    credentials: typing.Dict[str, typing.Any] = {}
    secrets_configured: typing.Dict[str, bool] = {}
    for key in key_order:
        val = _normalize_credential_value(raw.get(key))
        if _credential_key_sensitive(key):
            credentials[key] = None
            if val is not None:
                secrets_configured[key] = True
        else:
            credentials[key] = val
    return credentials, secrets_configured


def _redacted_credentials_for_catalog_entry(
    catalog_entry: typing.Dict[str, typing.Any],
    stored: typing.Optional[typing.Dict[str, typing.Any]],
    order_credentials: typing.Optional[typing.Dict[str, typing.Any]] = None,
) -> typing.Dict[str, typing.Any]:
    """
    feed_credentials/secrets_configured/order_credentials/order_secrets_configured for one
    connection, built the same way as get_company_provider_connection_detail — factored out so
    every endpoint that returns a connection's credentials redacts them the same way, instead
    of some endpoints returning cp.credentials raw. Relay-provisioned connections' FEED is the
    one exception (shown plainly, not redacted — see get_company_provider_connection_detail's
    comment on why: these are AMS-generated, meant to be handed to the distributor rep, not
    secret from the company itself). ORDER is always redacted normally against the catalog's
    order field lists, relay-provisioned or not — Meyer's order credentials (relay-provisioned
    feed, but real user-entered order API creds) are genuine secrets like any other provider's.

    ``stored`` is a connection's ``credentials`` JSON — "feed" only; it never carries "order"
    (see CompanyProviderOrderAccount). ``order_credentials`` is passed in explicitly by the
    caller (via ``credentials_helper.get_order_credentials``) since it now lives in a separate
    table this function has no model access point for.
    """
    stored = stored or {}
    order_redacted, order_secrets = _redacted_credentials(
        catalog_entry.get("order_connection_required_fields") or [],
        catalog_entry.get("order_connection_optional_fields") or [],
        order_credentials,
    )
    if catalog_entry.get("relay_provisioned"):
        feed_stored = stored.get("feed") or {}
        return {
            "feed_credentials": dict(feed_stored),
            "secrets_configured": {k: True for k in feed_stored.keys()},
            "order_credentials": order_redacted,
            "order_secrets_configured": order_secrets,
        }
    feed_redacted, feed_secrets = _redacted_credentials(
        catalog_entry.get("connection_required_fields") or [],
        catalog_entry.get("connection_optional_fields") or [],
        stored.get("feed"),
    )
    return {
        "feed_credentials": feed_redacted,
        "secrets_configured": feed_secrets,
        "order_credentials": order_redacted,
        "order_secrets_configured": order_secrets,
    }


def get_company_provider_connection_detail(
    company_id: int,
    company_provider_id: int,
) -> typing.Optional[typing.Dict[str, typing.Any]]:
    """
    One connection: provider row, ``company_provider_id`` (same as ``id``, for PATCH URL),
    catalog copy, ``connection_required_fields`` / ``connection_optional_fields``,
    and ``credentials`` (secrets redacted; see ``secrets_configured`` for sensitive fields that are set).
    """
    logger.info(
        "{} Fetching connection detail for company_provider_id={} company_id={}.".format(
            _LOG_PREFIX, company_provider_id, company_id,
        )
    )
    company_provider = (
        src_models.CompanyProviders.objects.filter(
            id=company_provider_id,
            company_id=company_id,
        )
        .select_related("provider")
        .first()
    )
    if not company_provider:
        return None

    provider = company_provider.provider
    if not provider:
        return None

    catalog_entry = _get_catalog_entry_for_provider(provider.id)
    if not catalog_entry:
        return None

    base: typing.Dict[str, typing.Any] = {
        "id": company_provider.id,
        "company_provider_id": company_provider.id,
        "company_id": company_provider.company_id,
        "provider_id": company_provider.provider_id,
        "provider_name": provider.name,
        "provider_status": provider.status,
        "provider_status_name": provider.status_name,
        "provider_type": provider.type,
        "provider_type_name": provider.type_name,
        "provider_kind": provider.kind,
        "provider_kind_name": provider.kind_name,
        "primary": company_provider.primary,
        "status": company_provider.status,
        "status_name": company_provider.status_name,
        "status_reason": company_provider.status_reason,
        "status_checked_at": (
            company_provider.status_checked_at.isoformat() if company_provider.status_checked_at else None
        ),
        "order_status": company_provider.order_status,
        "order_status_name": company_provider.order_status_name,
        "order_status_reason": company_provider.order_status_reason,
        "order_status_checked_at": (
            company_provider.order_status_checked_at.isoformat()
            if company_provider.order_status_checked_at else None
        ),
        "created_at": company_provider.created_at.isoformat() if company_provider.created_at else None,
        "updated_at": company_provider.updated_at.isoformat() if company_provider.updated_at else None,
    }
    base.update(_provider_ui_metadata(provider))

    out: typing.Dict[str, typing.Any] = dict(base)
    out["description"] = catalog_entry.get("description", "")
    out["category"] = catalog_entry.get("category", "")
    out["connection_required_fields"] = list(catalog_entry.get("connection_required_fields") or [])
    out["connection_optional_fields"] = list(catalog_entry.get("connection_optional_fields") or [])
    out["order_connection_required_fields"] = list(catalog_entry.get("order_connection_required_fields") or [])
    out["order_connection_optional_fields"] = list(catalog_entry.get("order_connection_optional_fields") or [])
    out["email_order_connection_required_fields"] = list(catalog_entry.get("email_order_connection_required_fields") or [])
    out["email_order_connection_optional_fields"] = list(catalog_entry.get("email_order_connection_optional_fields") or [])
    out["relay_provisioned"] = bool(catalog_entry.get("relay_provisioned"))
    out["supports_ordering"] = _catalog_supports_ordering_display(catalog_entry)
    out["order_credentials_mirror_feed"] = _catalog_order_credentials_mirror_feed(catalog_entry)

    if catalog_entry.get("relay_provisioned"):
        # These credentials are meant to be handed to the distributor's rep, not kept secret from
        # the company itself — show them plainly instead of redacting like a normal password field.
        company = src_models.Company.objects.filter(id=company_provider.company_id).first()
        out["installation_instructions_html"] = _render_relay_instructions_html(catalog_entry, company)
    else:
        out["installation_instructions_html"] = catalog_entry.get("installation_instructions_html") or None
    out.update(
        _redacted_credentials_for_catalog_entry(
            catalog_entry, company_provider.credentials, credentials_helper.get_order_credentials(company_provider)
        )
    )
    out["order_accounts"] = list_order_accounts(company_id=company_id, company_provider_id=company_provider_id)
    return out


def get_all_brands_with_providers() -> typing.List[typing.Dict]:
    """
    Get all brands with their associated providers.
    Left joins with BrandProviders and Providers to get provider details.
    
    Returns:
        List of dictionaries containing brand data with their providers
    """
    logger.info('{} Fetching all brands with providers.'.format(_LOG_PREFIX))
    
    brands = src_models.Brands.objects.prefetch_related(
        'providers__provider'
    ).all()
    
    data = []
    for brand in brands:
        providers_data = []
        for brand_provider in brand.providers.all():
            provider = brand_provider.provider
            providers_data.append({
                "id": provider.id if provider else None,
                "name": provider.name if provider else None,
                "status": provider.status if provider else None,
                "status_name": provider.status_name if provider else None,
                "type": provider.type if provider else None,
                "type_name": provider.type_name if provider else None,
                "kind": provider.kind if provider else None,
                "kind_name": provider.kind_name if provider else None,
                "created_at": brand_provider.created_at.isoformat() if brand_provider.created_at else None,
                "updated_at": brand_provider.updated_at.isoformat() if brand_provider.updated_at else None,
            })
        
        data.append({
            "id": brand.id,
            "name": brand.name,
            "status": brand.status,
            "status_name": brand.status_name,
            "data": brand.data,
            "providers": providers_data,
            "created_at": brand.created_at.isoformat() if brand.created_at else None,
            "updated_at": brand.updated_at.isoformat() if brand.updated_at else None,
        })
    
    logger.info('{} Found {} brands with providers.'.format(
        _LOG_PREFIX, len(data)
    ))
    
    return data


def get_company_destinations_with_brands(company_id: int) -> typing.List[typing.Dict]:
    """
    Get all destinations for a company with their associated brands.
    Joins through CompanyBrandDestination -> CompanyBrands -> Brands.
    
    Args:
        company_id: The ID of the company
        
    Returns:
        List of dictionaries containing destination data with their brands
    """
    logger.info('{} Fetching company destinations with brands for company_id: {}.'.format(
        _LOG_PREFIX, company_id
    ))
    
    destinations = src_models.CompanyDestinations.objects.filter(
        company_id=company_id
    ).prefetch_related(
        'company_brands__company_brand__brand'
    ).all()
    
    data = []
    for destination in destinations:
        brands_data = []
        for company_brand_destination in destination.company_brands.all():
            company_brand = company_brand_destination.company_brand
            brand = company_brand.brand
            
            brands_data.append({
                "id": brand.id if brand else None,
                "name": brand.name if brand else None,
                "status": company_brand.status if company_brand else None,
                "status_name": company_brand.status_name if company_brand else None,
                "data": brand.data if brand else None,
                "company_brand_id": company_brand.id if company_brand else None,
                "created_at": company_brand.created_at.isoformat() if company_brand.created_at else None,
                "updated_at": company_brand.updated_at.isoformat() if company_brand.updated_at else None,
            })
        
        data.append({
            "id": destination.id,
            "status": destination.status,
            "status_name": destination.status_name,
            "destination_type": destination.destination_type,
            "destination_type_name": destination.destination_type_name,
            "credentials": destination.credentials,
            "company_id": destination.company_id,
            "brands": brands_data,
            "created_at": destination.created_at.isoformat() if destination.created_at else None,
            "updated_at": destination.updated_at.isoformat() if destination.updated_at else None,
        })
    
    logger.info('{} Found {} destinations with brands for company_id: {}.'.format(
        _LOG_PREFIX, len(data), company_id
    ))
    
    return data


def get_company_destination_by_id(company_id: int, destination_id: int) -> typing.Optional[typing.Dict]:
    """
    Get a single company destination by ID for a given company_id.
    Includes all destination details including credentials.
    
    Args:
        company_id: The ID of the company
        destination_id: The ID of the company destination
        
    Returns:
        Dictionary containing destination data, or None if not found
    """
    logger.info('{} Fetching company destination with id: {} for company_id: {}.'.format(
        _LOG_PREFIX, destination_id, company_id
    ))
    
    try:
        destination = src_models.CompanyDestinations.objects.filter(
            id=destination_id,
            company_id=company_id
        ).first()
        
        if not destination:
            logger.warning('{} Company destination with id: {} not found for company_id: {}.'.format(
                _LOG_PREFIX, destination_id, company_id
            ))
            return None
        
        data = {
            "id": destination.id,
            "status": destination.status,
            "status_name": destination.status_name,
            "destination_type": destination.destination_type,
            "destination_type_name": destination.destination_type_name,
            "credentials": destination.credentials,
            "company_id": destination.company_id,
            "created_at": destination.created_at.isoformat() if destination.created_at else None,
            "updated_at": destination.updated_at.isoformat() if destination.updated_at else None,
        }
        
        logger.info('{} Found company destination with id: {} for company_id: {}.'.format(
            _LOG_PREFIX, destination_id, company_id
        ))
        
        return data
    except Exception as e:
        logger.error('{} Error fetching company destination with id: {} for company_id: {}. Error: {}.'.format(
            _LOG_PREFIX, destination_id, company_id, str(e)
        ))
        raise


def get_company_execution_runs(
    company_id: int,
    destination_id: typing.Optional[int] = None,
    page: int = 1,
    page_size: int = 20
) -> typing.Dict:
    """
    Get execution runs for a company with pagination.
    Optionally filter by destination_id.
    Includes brand and destination information.
    
    Args:
        company_id: The ID of the company
        destination_id: Optional destination ID to filter by
        page: Page number (default: 1)
        page_size: Number of items per page (default: 20)
        
    Returns:
        Dictionary containing paginated execution runs data with brand and destination info
    """
    logger.info('{} Fetching execution runs for company_id: {}, destination_id: {}, page: {}, page_size: {}.'.format(
        _LOG_PREFIX, company_id, destination_id, page, page_size
    ))
    
    # Filter execution runs by company_id through the relationships
    execution_runs = src_models.CompanyDestinationExecutionRun.objects.filter(
        company_brand_destination__company_brand__company_id=company_id
    )
    
    # Optionally filter by destination_id
    if destination_id:
        execution_runs = execution_runs.filter(
            company_brand_destination__destination_id=destination_id
        )
    
    execution_runs = execution_runs.select_related(
        'company_brand_destination__company_brand__brand',
        'company_brand_destination__destination'
    ).order_by('-created_at')
    
    # Paginate the results
    paginator = Paginator(execution_runs, page_size)
    
    try:
        page_obj = paginator.page(page)
    except Exception as e:
        logger.warning('{} Invalid page number: {}. Error: {}. Returning first page.'.format(
            _LOG_PREFIX, page, str(e)
        ))
        page_obj = paginator.page(1)
    
    data = []
    for execution_run in page_obj:
        company_brand_destination = execution_run.company_brand_destination
        company_brand = company_brand_destination.company_brand if company_brand_destination else None
        brand = company_brand.brand if company_brand else None
        destination = company_brand_destination.destination if company_brand_destination else None
        
        data.append({
            "id": execution_run.id,
            "status": execution_run.status,
            "status_name": execution_run.status_name,
            "products_processed": execution_run.products_processed,
            "products_created": execution_run.products_created,
            "products_updated": execution_run.products_updated,
            "products_failed": execution_run.products_failed,
            "error_message": execution_run.error_message,
            "message": execution_run.message,
            "brand": {
                "id": brand.id if brand else None,
                "name": brand.name if brand else None,
                "status": brand.status if brand else None,
                "status_name": brand.status_name if brand else None,
            } if brand else None,
            "company_brand": {
                "id": company_brand.id if company_brand else None,
                "status": company_brand.status if company_brand else None,
                "status_name": company_brand.status_name if company_brand else None,
            } if company_brand else None,
            "destination": {
                "id": destination.id if destination else None,
                "status": destination.status if destination else None,
                "status_name": destination.status_name if destination else None,
                "destination_type": destination.destination_type if destination else None,
                "destination_type_name": destination.destination_type_name if destination else None,
            } if destination else None,
            "created_at": execution_run.created_at.isoformat() if execution_run.created_at else None,
            "updated_at": execution_run.updated_at.isoformat() if execution_run.updated_at else None,
            "completed_at": execution_run.completed_at.isoformat() if execution_run.completed_at else None,
        })
    
    result = {
        "data": data,
        "pagination": {
            "page": page_obj.number,
            "page_size": page_size,
            "total_pages": paginator.num_pages,
            "total_count": paginator.count,
            "has_next": page_obj.has_next(),
            "has_previous": page_obj.has_previous(),
        }
    }
    
    logger.info('{} Found {} execution runs for company_id: {} (page {} of {}).'.format(
        _LOG_PREFIX, len(data), company_id, page_obj.number, paginator.num_pages
    ))
    
    return result


def get_execution_run_parts_history(
    company_id: int,
    execution_run_id: int,
    page: int = 1,
    page_size: int = 20
) -> typing.Dict:
    """
    Get parts history for a specific execution run with pagination.
    Includes destination part, brand, and destination information.
    
    Args:
        company_id: The ID of the company
        execution_run_id: The ID of the execution run
        page: Page number (default: 1)
        page_size: Number of items per page (default: 20)
        
    Returns:
        Dictionary containing paginated parts history data with part, brand, and destination info
    """
    logger.info('{} Fetching parts history for execution_run_id: {}, company_id: {}, page: {}, page_size: {}.'.format(
        _LOG_PREFIX, execution_run_id, company_id, page, page_size
    ))
    
    # First verify the execution run belongs to the company
    execution_run = src_models.CompanyDestinationExecutionRun.objects.filter(
        id=execution_run_id,
        company_brand_destination__company_brand__company_id=company_id
    ).first()
    
    if not execution_run:
        logger.warning('{} Execution run with id: {} not found for company_id: {}.'.format(
            _LOG_PREFIX, execution_run_id, company_id
        ))
        return None
    
    # Get parts history for this execution run
    parts_history = src_models.CompanyDestinationPartsHistory.objects.filter(
        execution_run_id=execution_run_id
    ).select_related(
        'destination_part__brand',
        'destination_part__company_destination'
    ).order_by('-created_at')
    
    # Paginate the results
    paginator = Paginator(parts_history, page_size)
    
    try:
        page_obj = paginator.page(page)
    except Exception as e:
        logger.warning('{} Invalid page number: {}. Error: {}. Returning first page.'.format(
            _LOG_PREFIX, page, str(e)
        ))
        page_obj = paginator.page(1)
    
    data = []
    for history in page_obj:
        destination_part = history.destination_part
        brand = destination_part.brand if destination_part else None
        destination = destination_part.company_destination if destination_part else None
        
        data.append({
            "id": history.id,
            "data": history.data,
            "changes": history.changes,
            "synced": history.synced,
            "destination_part": {
                "id": destination_part.id if destination_part else None,
                "part_unique_key": destination_part.part_unique_key if destination_part else None,
                "source_external_id": destination_part.source_external_id if destination_part else None,
                "destination_external_id": destination_part.destination_external_id if destination_part else None,
            } if destination_part else None,
            "brand": {
                "id": brand.id if brand else None,
                "name": brand.name if brand else None,
                "status": brand.status if brand else None,
                "status_name": brand.status_name if brand else None,
            } if brand else None,
            "destination": {
                "id": destination.id if destination else None,
                "status": destination.status if destination else None,
                "status_name": destination.status_name if destination else None,
                "destination_type": destination.destination_type if destination else None,
                "destination_type_name": destination.destination_type_name if destination else None,
            } if destination else None,
            "created_at": history.created_at.isoformat() if history.created_at else None,
            "updated_at": history.updated_at.isoformat() if history.updated_at else None,
        })
    
    result = {
        "data": data,
        "pagination": {
            "page": page_obj.number,
            "page_size": page_size,
            "total_pages": paginator.num_pages,
            "total_count": paginator.count,
            "has_next": page_obj.has_next(),
            "has_previous": page_obj.has_previous(),
        }
    }
    
    logger.info('{} Found {} parts history records for execution_run_id: {} (page {} of {}).'.format(
        _LOG_PREFIX, len(data), execution_run_id, page_obj.number, paginator.num_pages
    ))

    return result


# ---------------------------------------------------------------------------
# Integration requests
# ---------------------------------------------------------------------------

def create_integration_request(company_id: int, provider_id: int) -> typing.Tuple[bool, typing.Optional[str]]:
    """
    Create an IntegrationRequest for the given company + provider.
    Idempotent — if one already exists, return success without error.
    Returns (ok, error_message).
    """
    provider = src_models.Providers.objects.filter(id=provider_id).first()
    if not provider:
        return False, "Provider not found"

    src_models.IntegrationRequest.objects.get_or_create(
        company_id=company_id,
        provider=provider,
    )
    return True, None


def get_integration_requests(company_id: int) -> typing.List[int]:
    """Return list of provider IDs the company has already requested."""
    return list(
        src_models.IntegrationRequest.objects.filter(company_id=company_id)
        .values_list("provider_id", flat=True)
    )


# Custom (free-text) integration requests
# ---------------------------------------------------------------------------

def create_custom_integration_request(
    company_id: int, distributor_name: str
) -> typing.Tuple[bool, typing.Optional[str]]:
    """
    Create a CustomIntegrationRequest for a distributor not in our system.
    Idempotent — if one already exists for this company + name, return success.
    Returns (ok, error_message).
    """
    name = (distributor_name or "").strip()
    if not name:
        return False, "distributor_name is required"
    if len(name) > 255:
        return False, "distributor_name must be 255 characters or fewer"

    src_models.CustomIntegrationRequest.objects.get_or_create(
        company_id=company_id,
        distributor_name=name,
    )
    return True, None


def get_custom_integration_requests(company_id: int) -> typing.List[str]:
    """Return list of distributor names the company has already requested."""
    return list(
        src_models.CustomIntegrationRequest.objects.filter(company_id=company_id)
        .values_list("distributor_name", flat=True)
    )


