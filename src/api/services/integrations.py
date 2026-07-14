import logging
import typing

from django.core.paginator import Paginator

from src import constants as src_constants
from src import enums as src_enums
from src import models as src_models
from src.integrations.clients.keystone import client as keystone_client
from src.integrations.clients.keystone import exceptions as keystone_exceptions
from src.integrations.clients.premier import client as premier_client
from src.integrations.clients.premier import exceptions as premier_exceptions
from src.integrations.clients.rough_country import client as rough_country_client
from src.integrations.clients.rough_country import exceptions as rough_country_exceptions
from src.integrations.clients.turn_14 import client as turn14_client
from src.integrations.clients.turn_14 import exceptions as turn14_exceptions
from src.integrations.clients.wheelpros import client as wheelpros_client
from src.integrations.clients.wheelpros import exceptions as wheelpros_exceptions
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
            "<a href=\"mailto:info@aftermarketscout.com\">info@aftermarketscout.com</a>.</p>"
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


def _credential_key_sensitive(key: str) -> bool:
    lower = (key or "").lower()
    if "password" in lower or "secret" in lower:
        return True
    return False


def _build_credentials_from_catalog_entry(
    catalog_entry: typing.Dict[str, typing.Any],
    credentials: typing.Dict[str, typing.Any],
) -> typing.Tuple[typing.Optional[typing.Dict[str, typing.Any]], typing.Optional[str], typing.Optional[str]]:
    """
    Validate connection_required_fields and merge optional non-empty values from connection_optional_fields.
    Returns (credentials, error_message, error_code).
    """
    required = catalog_entry.get("connection_required_fields", []) or []
    optional = catalog_entry.get("connection_optional_fields", []) or []
    if required:
        missing = [f for f in required if not _normalize_credential_value(credentials.get(f))]
        if missing:
            return (
                None,
                "Missing required fields: {}".format(", ".join(missing)),
                CONNECTION_ERROR_MISSING_FIELDS,
            )
    creds: typing.Dict[str, typing.Any] = {}
    for k in required:
        v = _normalize_credential_value(credentials.get(k))
        if v is not None:
            creds[k] = v
    for k in optional:
        v = _normalize_credential_value(credentials.get(k))
        if v is not None:
            creds[k] = v
    return creds, None, None


def _merge_update_credentials(
    catalog_entry: typing.Dict[str, typing.Any],
    existing: typing.Optional[typing.Dict[str, typing.Any]],
    patch: typing.Dict[str, typing.Any],
) -> typing.Tuple[typing.Optional[typing.Dict[str, typing.Any]], typing.Optional[str], typing.Optional[str]]:
    """
    Apply a partial credential patch onto stored credentials. Only keys in the catalog
    (required + optional) are allowed. Non-empty values overwrite. Empty / null for a
    **sensitive** key (password, secret) leaves the previous value unchanged so clients
    can update other fields without resubmitting secrets. Returns (credentials, error_message, error_code).
    """
    required = [str(f) for f in (catalog_entry.get("connection_required_fields") or [])]
    optional = [str(f) for f in (catalog_entry.get("connection_optional_fields") or [])]
    allowed: typing.Set[str] = set(required) | set(optional)
    out: typing.Dict[str, typing.Any] = dict(existing or {})

    for k, v in (patch or {}).items():
        key = str(k)
        if key not in allowed:
            return None, "Unknown credential field: {}".format(key), CONNECTION_ERROR_INVALID_INPUT
        nv = _normalize_credential_value(v)
        if nv is not None:
            out[key] = nv
        else:
            if _credential_key_sensitive(key):
                continue
            out.pop(key, None)

    missing = [f for f in required if not _normalize_credential_value(out.get(f))]
    if missing:
        return (
            None,
            "Missing required fields: {}".format(", ".join(missing)),
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
    for entry in src_constants.PROVIDER_CATALOG:
        kind_value = entry["kind"].value
        provider = providers_by_kind.get(kind_value)
        if not provider:
            continue

        company_provider = company_providers_by_provider_id.get(provider.id)
        connected = company_provider is not None

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
}


def _validate_connection(
    kind: int, credentials: typing.Dict[str, typing.Any]
) -> typing.Tuple[typing.Optional[bool], typing.Optional[str], typing.Optional[str]]:
    """
    Run the connection validator for this provider kind, if one exists.
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


def connect_provider(
    company_id: int,
    provider_id: int,
    credentials: typing.Dict[str, typing.Any],
) -> typing.Tuple[typing.Optional[typing.Dict], typing.Optional[str], typing.Optional[str]]:
    """
    Create or replace credentials for a ``CompanyProviders`` row (keyed by company + provider).
    Validates required fields from the catalog, tests the connection for kinds with a validator
    (see :data:`_CONNECTION_VALIDATORS`), saves, and enqueues
    ``integration_pricing_sync_jobs.enqueue_company_provider_pricing_sync`` when the provider
    has per-company pricing sync. Use :func:`update_connection` for partial PATCH updates by
    connection id. Returns (data, error_message, error_code) — error_code is one of the
    ``CONNECTION_ERROR_*`` constants, for the frontend to branch on.
    """
    provider = src_models.Providers.objects.filter(id=provider_id).first()
    if not provider:
        return None, "Provider not found", CONNECTION_ERROR_NOT_FOUND

    catalog_entry = _get_catalog_entry_for_provider(provider_id)
    if not catalog_entry:
        return None, "Provider not found in catalog", CONNECTION_ERROR_NOT_FOUND

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
                    "Your SFTP account could not be created. Please contact info@aftermarketscout.com.",
                    CONNECTION_ERROR_CONNECTION_FAILED,
                )
        user_field, password_field = catalog_entry.get("relay_credential_fields", ("sftp_user", "sftp_password"))
        creds = {user_field: company.relay_sftp_username, password_field: company.relay_sftp_password}
        validated = None
    else:
        creds, err, err_code = _build_credentials_from_catalog_entry(catalog_entry, credentials)
        if err:
            return None, err, err_code
        validated, val_error, val_error_code = _validate_connection(provider.kind, creds)
        if val_error:
            return None, val_error, val_error_code

    # Check if already connected
    existing = src_models.CompanyProviders.objects.filter(
        company_id=company_id,
        provider_id=provider_id,
    ).first()
    if existing:
        existing.credentials = creds
        existing.save()
        cp = existing
    else:
        cp = src_models.CompanyProviders.objects.create(
            company_id=company_id,
            provider_id=provider_id,
            credentials=creds,
            primary=False,
        )

    if integration_pricing_sync_jobs.should_enqueue_pricing_sync(provider.kind):
        integration_pricing_sync_jobs.enqueue_company_provider_pricing_sync(cp.id)

    return {
        "id": cp.id,
        "company_provider_id": cp.id,
        "company_id": cp.company_id,
        "provider_id": cp.provider_id,
        "provider_name": provider.name,
        "credentials": cp.credentials,
        "primary": cp.primary,
        "connection_validated": validated,
        "created_at": cp.created_at.isoformat() if cp.created_at else None,
        "updated_at": cp.updated_at.isoformat() if cp.updated_at else None,
    }, None, None


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
    ).select_related("provider").first()
    if not cp or not cp.provider:
        return None, "Connection not found", CONNECTION_ERROR_NOT_FOUND

    catalog_entry = _get_catalog_entry_for_provider(cp.provider_id)
    if not catalog_entry:
        return None, "Provider not found in catalog", CONNECTION_ERROR_NOT_FOUND

    creds, err, err_code = _merge_update_credentials(
        catalog_entry,
        cp.credentials,
        credentials,
    )
    if err:
        return None, err, err_code

    if catalog_entry.get("relay_provisioned"):
        validated = None
    else:
        validated, val_error, val_error_code = _validate_connection(cp.provider.kind, creds)
        if val_error:
            return None, val_error, val_error_code

    cp.credentials = creds
    cp.save()

    if integration_pricing_sync_jobs.should_enqueue_pricing_sync(cp.provider.kind):
        integration_pricing_sync_jobs.enqueue_company_provider_pricing_sync(cp.id)

    return {
        "id": cp.id,
        "company_provider_id": cp.id,
        "company_id": cp.company_id,
        "provider_id": cp.provider_id,
        "provider_name": cp.provider.name,
        "credentials": cp.credentials,
        "primary": cp.primary,
        "connection_validated": validated,
        "created_at": cp.created_at.isoformat() if cp.created_at else None,
        "updated_at": cp.updated_at.isoformat() if cp.updated_at else None,
    }, None, None


def disconnect_provider(
    company_id: int,
    company_provider_id: int,
) -> typing.Tuple[bool, typing.Optional[str]]:
    """
    Delete CompanyProviders record. Must belong to company.
    Returns (success, error_message). On success error_message is None.
    """
    cp = src_models.CompanyProviders.objects.filter(
        id=company_provider_id,
        company_id=company_id,
    ).first()
    if not cp:
        return False, "Connection not found"
    cp.delete()
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
            "credentials": cp.credentials,
            "primary": cp.primary,
            "created_at": cp.created_at.isoformat() if cp.created_at else None,
            "updated_at": cp.updated_at.isoformat() if cp.updated_at else None,
        }
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
            "credentials": company_provider.credentials,
            "primary": company_provider.primary,
            "connection_required_fields": list(catalog_entry.get("connection_required_fields") or []),
            "connection_optional_fields": list(catalog_entry.get("connection_optional_fields") or []),
            "created_at": company_provider.created_at.isoformat() if company_provider.created_at else None,
            "updated_at": company_provider.updated_at.isoformat() if company_provider.updated_at else None,
        }
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


def _redacted_credentials_for_connection_detail(
    catalog_entry: typing.Dict[str, typing.Any],
    raw: typing.Optional[typing.Dict[str, typing.Any]],
) -> typing.Tuple[typing.Dict[str, typing.Any], typing.Dict[str, bool]]:
    """
    Keys: catalog required + optional (in order), then any other keys in storage.
    Non-sensitive: stored values. Sensitive: always ``null`` in ``credentials``; if a value is stored,
    ``secrets_configured[key]`` is True (so the FE can show “password set” without echoing the secret).
    """
    required = list(catalog_entry.get("connection_required_fields") or [])
    optional = list(catalog_entry.get("connection_optional_fields") or [])
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
        "created_at": company_provider.created_at.isoformat() if company_provider.created_at else None,
        "updated_at": company_provider.updated_at.isoformat() if company_provider.updated_at else None,
    }
    base.update(_provider_ui_metadata(provider))

    out: typing.Dict[str, typing.Any] = dict(base)
    out["description"] = catalog_entry.get("description", "")
    out["category"] = catalog_entry.get("category", "")
    out["connection_required_fields"] = list(catalog_entry.get("connection_required_fields") or [])
    out["connection_optional_fields"] = list(catalog_entry.get("connection_optional_fields") or [])
    out["relay_provisioned"] = bool(catalog_entry.get("relay_provisioned"))

    if catalog_entry.get("relay_provisioned"):
        # These credentials are meant to be handed to the distributor's rep, not kept secret from
        # the company itself — show them plainly instead of redacting like a normal password field.
        company = src_models.Company.objects.filter(id=company_provider.company_id).first()
        out["installation_instructions_html"] = _render_relay_instructions_html(catalog_entry, company)
        out["credentials"] = dict(company_provider.credentials or {})
        out["secrets_configured"] = {k: True for k in (company_provider.credentials or {}).keys()}
    else:
        out["installation_instructions_html"] = catalog_entry.get("installation_instructions_html") or None
        creds_redacted, secrets_configured = _redacted_credentials_for_connection_detail(
            catalog_entry,
            company_provider.credentials,
        )
        out["credentials"] = creds_redacted
        out["secrets_configured"] = secrets_configured
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


