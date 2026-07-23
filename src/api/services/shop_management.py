"""
Services for company shop-management-system connections (ShopMonkey, ...). Mirrors the
catalog/connect/update/list slice of ``src/api/services/integrations.py``, but deliberately
simpler: one flat credentials dict (e.g. ``{"api_key": "..."}``, no feed/order namespace split),
one connectivity status, no relay provisioning, no background sync enqueue — none of that
distributor-specific machinery applies here. Kept in its own module rather than extending
integrations.py since this is a structurally separate system — see ``ShopManagementProviders``/
``CompanyShopManagementProviders`` in ``src/models.py``.

Push/sync capability (e.g. pushing parts onto a repair order) is intentionally not implemented
yet — this module only covers the base connection lifecycle.
"""
import logging
import typing

from django.utils import timezone

from src import constants as src_constants
from src import enums as src_enums
from src import models as src_models
from src.integrations.clients.shopmonkey import client as shopmonkey_client
from src.integrations.clients.shopmonkey import exceptions as shopmonkey_exceptions

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[SHOP-MANAGEMENT-SERVICES]"

# Same stable error-code contract as src/api/services/integrations.py, so the frontend can reuse
# its existing branching logic.
CONNECTION_ERROR_MISSING_FIELDS = "missing_fields"
CONNECTION_ERROR_INVALID_INPUT = "invalid_input"
CONNECTION_ERROR_INVALID_CREDENTIALS = "invalid_credentials"
CONNECTION_ERROR_CONNECTION_FAILED = "connection_failed"
CONNECTION_ERROR_NOT_FOUND = "not_found"

_SENSITIVE_CREDENTIAL_KEY_SUBSTRINGS = ("password", "secret", "key", "token")


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
    return any(s in lower for s in _SENSITIVE_CREDENTIAL_KEY_SUBSTRINGS)


def _get_catalog_entry(kind_value: int) -> typing.Optional[typing.Dict[str, typing.Any]]:
    for entry in src_constants.SHOP_MANAGEMENT_PROVIDER_CATALOG:
        if entry["kind"].value == kind_value:
            return entry
    return None


def _get_catalog_entry_for_provider(provider_id: int) -> typing.Optional[typing.Dict[str, typing.Any]]:
    provider = src_models.ShopManagementProviders.objects.filter(id=provider_id).first()
    if not provider:
        return None
    return _get_catalog_entry(provider.kind)


def _merge_credentials(
    catalog_entry: typing.Dict[str, typing.Any],
    existing: typing.Optional[typing.Dict[str, typing.Any]],
    patch: typing.Optional[typing.Dict[str, typing.Any]],
) -> typing.Tuple[typing.Optional[typing.Dict[str, typing.Any]], typing.Optional[str], typing.Optional[str]]:
    """
    Merge a flat credentials patch onto the existing stored dict. Only fields declared in the
    catalog entry's required/optional lists are accepted. Once anything is stored, every
    required field must be present (all-or-nothing), same rule as distributor connections.
    Returns (credentials, error_message, error_code).
    """
    if patch is None:
        patch = {}
    if not isinstance(patch, dict):
        return None, "Credentials must be an object.", CONNECTION_ERROR_INVALID_INPUT

    required = [str(f) for f in (catalog_entry.get("connection_required_fields") or [])]
    optional = [str(f) for f in (catalog_entry.get("connection_optional_fields") or [])]
    allowed = set(required) | set(optional)

    out = dict(existing or {})
    for k, v in patch.items():
        key = str(k)
        if key not in allowed:
            return None, "Unknown credential field: {}".format(key), CONNECTION_ERROR_INVALID_INPUT
        nv = _normalize_credential_value(v)
        if nv is not None:
            out[key] = nv
        elif not _credential_key_sensitive(key):
            out.pop(key, None)

    if required:
        missing = [f for f in required if not _normalize_credential_value(out.get(f))]
        if missing:
            return None, "Missing required fields: {}".format(", ".join(missing)), CONNECTION_ERROR_MISSING_FIELDS

    return out, None, None


_ValidatorResult = typing.Tuple[typing.Optional[str], typing.Optional[str]]  # (error_message, error_code)


def _validate_shopmonkey_connection(credentials: typing.Dict[str, typing.Any]) -> _ValidatorResult:
    try:
        client = shopmonkey_client.ShopMonkeyApiClient(credentials=credentials)
        client.test_connection()
    except shopmonkey_exceptions.ShopMonkeyAuthError as e:
        return str(e), CONNECTION_ERROR_INVALID_CREDENTIALS
    except (shopmonkey_exceptions.ShopMonkeyException, ValueError) as e:
        return str(e), CONNECTION_ERROR_CONNECTION_FAILED
    return None, None


# Connection validators run synchronously at connect/update time, before credentials are saved,
# so bad credentials fail the request instead of silently failing later. Populated per vendor as
# each is onboarded — mirrors _CONNECTION_VALIDATORS in src/api/services/integrations.py.
_CONNECTION_VALIDATORS: typing.Dict[int, typing.Callable[[typing.Dict[str, typing.Any]], _ValidatorResult]] = {
    src_enums.ShopManagementProviderKind.SHOPMONKEY.value: _validate_shopmonkey_connection,
}


def _validate_connection(
    kind: int, credentials: typing.Dict[str, typing.Any]
) -> typing.Tuple[typing.Optional[bool], typing.Optional[str], typing.Optional[str]]:
    """Same contract as integrations.py's _validate_connection: (validated, error_message, error_code)."""
    validator = _CONNECTION_VALIDATORS.get(kind)
    if not validator:
        return None, None, None
    message, code = validator(credentials)
    if message:
        return False, message, code
    return True, None, None


def _connection_status_fields(
    status_enum: typing.Optional["src_enums.ShopManagementConnectionStatus"],
    status_reason: typing.Optional[str],
) -> typing.Dict[str, typing.Any]:
    if status_enum is None:
        return {}
    return {
        "status": status_enum.value,
        "status_name": status_enum.name,
        "status_reason": status_reason,
        "status_checked_at": timezone.now(),
    }


def _redacted_credentials(
    catalog_entry: typing.Dict[str, typing.Any],
    stored: typing.Optional[typing.Dict[str, typing.Any]],
) -> typing.Dict[str, typing.Any]:
    """Sensitive fields (password/secret/key/token substrings) come back as null with
    secrets_configured[key]=True when set; everything else is echoed as-is."""
    required = list(catalog_entry.get("connection_required_fields") or [])
    optional = list(catalog_entry.get("connection_optional_fields") or [])
    stored = stored or {}

    key_order: typing.List[str] = []
    seen: typing.Set[str] = set()
    for k in required + optional:
        ks = str(k).strip() if k is not None else ""
        if ks and ks not in seen:
            seen.add(ks)
            key_order.append(ks)
    for k in sorted(stored.keys(), key=str):
        ks = str(k).strip() if k is not None else ""
        if ks and ks not in seen:
            seen.add(ks)
            key_order.append(ks)

    credentials: typing.Dict[str, typing.Any] = {}
    secrets_configured: typing.Dict[str, bool] = {}
    for key in key_order:
        val = _normalize_credential_value(stored.get(key))
        if _credential_key_sensitive(key):
            credentials[key] = None
            if val is not None:
                secrets_configured[key] = True
        else:
            credentials[key] = val

    return {"credentials": credentials, "secrets_configured": secrets_configured}


def get_shop_management_catalog(company_id: int) -> typing.Dict:
    """Catalog of connectable shop-management systems with per-company connection status."""
    logger.info("{} Fetching shop management catalog for company_id: {}.".format(_LOG_PREFIX, company_id))

    company_connections_by_provider_id = {
        c.provider_id: c
        for c in src_models.CompanyShopManagementProviders.objects.filter(company_id=company_id)
    }
    providers_by_kind = {p.kind: p for p in src_models.ShopManagementProviders.objects.all()}

    catalog = []
    for entry in src_constants.SHOP_MANAGEMENT_PROVIDER_CATALOG:
        kind_value = entry["kind"].value
        provider = providers_by_kind.get(kind_value)
        if not provider:
            continue

        connection = company_connections_by_provider_id.get(provider.id)
        catalog.append({
            "id": provider.id,
            "name": provider.name,
            "description": entry.get("description", ""),
            "icon_url": entry.get("icon_url") or None,
            "category": entry.get("category", ""),
            "connection_required_fields": entry.get("connection_required_fields", []),
            "connection_optional_fields": entry.get("connection_optional_fields", []),
            "installation_instructions_html": entry.get("installation_instructions_html") or None,
            "connected": connection is not None,
            "company_provider_id": connection.id if connection else None,
            "kind": kind_value,
            "kind_name": provider.kind_name or "",
            "coming_soon": bool(provider.coming_soon),
            "status": connection.status if connection else None,
            "status_name": connection.status_name if connection else None,
            "status_reason": connection.status_reason if connection else None,
            "status_checked_at": (
                connection.status_checked_at.isoformat()
                if connection and connection.status_checked_at
                else None
            ),
        })

    return {
        "data": catalog,
        "categories": list(dict.fromkeys(
            e.get("category", "") for e in src_constants.SHOP_MANAGEMENT_PROVIDER_CATALOG if e.get("category")
        )),
    }


def _connection_result(
    cp: "src_models.CompanyShopManagementProviders",
    catalog_entry: typing.Dict[str, typing.Any],
    validated: typing.Optional[bool] = None,
) -> typing.Dict[str, typing.Any]:
    result = {
        "id": cp.id,
        "company_provider_id": cp.id,
        "company_id": cp.company_id,
        "provider_id": cp.provider_id,
        "provider_name": cp.provider.name if cp.provider_id else None,
        "connection_validated": validated,
        "status": cp.status,
        "status_name": cp.status_name,
        "status_reason": cp.status_reason,
        "status_checked_at": cp.status_checked_at.isoformat() if cp.status_checked_at else None,
        "created_at": cp.created_at.isoformat() if cp.created_at else None,
        "updated_at": cp.updated_at.isoformat() if cp.updated_at else None,
    }
    result.update(_redacted_credentials(catalog_entry, cp.credentials))
    return result


def connect_provider(
    company_id: int,
    provider_id: int,
    credentials: typing.Dict[str, typing.Any],
) -> typing.Tuple[typing.Optional[typing.Dict], typing.Optional[str], typing.Optional[str]]:
    """
    Create, or idempotently update, a ``CompanyShopManagementProviders`` row (keyed by
    company + provider). Validates credentials with a live ``test_connection()`` call before
    persisting anything. Returns (data, error_message, error_code).
    """
    provider = src_models.ShopManagementProviders.objects.filter(id=provider_id).first()
    if not provider:
        return None, "Provider not found", CONNECTION_ERROR_NOT_FOUND

    catalog_entry = _get_catalog_entry_for_provider(provider_id)
    if not catalog_entry:
        return None, "Provider not found in catalog", CONNECTION_ERROR_NOT_FOUND

    existing = src_models.CompanyShopManagementProviders.objects.filter(
        company_id=company_id,
        provider_id=provider_id,
    ).first()

    creds, err, err_code = _merge_credentials(
        catalog_entry, existing.credentials if existing else None, credentials
    )
    if err:
        return None, err, err_code

    validated, val_error, val_error_code = _validate_connection(provider.kind, creds)
    if val_error:
        return None, val_error, val_error_code

    status_enum = src_enums.ShopManagementConnectionStatus.CONNECTED if validated else None
    status_fields = _connection_status_fields(status_enum, None)

    if existing:
        existing.credentials = creds
        for field, value in status_fields.items():
            setattr(existing, field, value)
        existing.save()
        cp = existing
    else:
        cp = src_models.CompanyShopManagementProviders.objects.create(
            company_id=company_id,
            provider_id=provider_id,
            credentials=creds,
            **status_fields,
        )
    cp.provider = provider

    return _connection_result(cp, catalog_entry, validated), None, None


def update_connection(
    company_id: int,
    company_provider_id: int,
    credentials: typing.Dict[str, typing.Any],
) -> typing.Tuple[typing.Optional[typing.Dict], typing.Optional[str], typing.Optional[str]]:
    """PATCH equivalent of connect_provider, addressed by connection id."""
    cp = src_models.CompanyShopManagementProviders.objects.filter(
        id=company_provider_id, company_id=company_id
    ).select_related("provider").first()
    if not cp:
        return None, "Connection not found", CONNECTION_ERROR_NOT_FOUND

    provider = cp.provider
    catalog_entry = _get_catalog_entry(provider.kind)
    if not catalog_entry:
        return None, "Provider not found in catalog", CONNECTION_ERROR_NOT_FOUND

    creds, err, err_code = _merge_credentials(catalog_entry, cp.credentials, credentials)
    if err:
        return None, err, err_code

    validated, val_error, val_error_code = _validate_connection(provider.kind, creds)
    if val_error:
        return None, val_error, val_error_code

    status_enum = src_enums.ShopManagementConnectionStatus.CONNECTED if validated else None
    status_fields = _connection_status_fields(status_enum, None)

    cp.credentials = creds
    for field, value in status_fields.items():
        setattr(cp, field, value)
    cp.save()

    return _connection_result(cp, catalog_entry, validated), None, None


def disconnect(company_id: int, company_provider_id: int) -> typing.Tuple[bool, typing.Optional[str]]:
    cp = src_models.CompanyShopManagementProviders.objects.filter(
        id=company_provider_id, company_id=company_id
    ).first()
    if not cp:
        return False, "Connection not found"
    cp.delete()
    return True, None


def list_company_connections(company_id: int) -> typing.List[typing.Dict[str, typing.Any]]:
    connections = src_models.CompanyShopManagementProviders.objects.filter(
        company_id=company_id
    ).select_related("provider")

    data = []
    for cp in connections:
        provider = cp.provider
        catalog_entry = _get_catalog_entry(provider.kind) if provider else {}
        row = {
            "id": cp.id,
            "company_id": cp.company_id,
            "provider_id": cp.provider_id,
            "provider_name": provider.name if provider else None,
            "provider_kind": provider.kind if provider else None,
            "provider_kind_name": provider.kind_name if provider else None,
            "active": cp.active,
            "status": cp.status,
            "status_name": cp.status_name,
            "status_reason": cp.status_reason,
            "status_checked_at": cp.status_checked_at.isoformat() if cp.status_checked_at else None,
            "created_at": cp.created_at.isoformat() if cp.created_at else None,
            "updated_at": cp.updated_at.isoformat() if cp.updated_at else None,
        }
        row.update(_redacted_credentials(catalog_entry or {}, cp.credentials))
        data.append(row)
    return data


def get_connection_detail(
    company_id: int, company_provider_id: int
) -> typing.Optional[typing.Dict[str, typing.Any]]:
    cp = src_models.CompanyShopManagementProviders.objects.filter(
        id=company_provider_id, company_id=company_id
    ).select_related("provider").first()
    if not cp:
        return None

    provider = cp.provider
    catalog_entry = _get_catalog_entry(provider.kind) if provider else {}
    data = {
        "id": cp.id,
        "company_id": cp.company_id,
        "provider_id": cp.provider_id,
        "provider_name": provider.name if provider else None,
        "provider_kind": provider.kind if provider else None,
        "provider_kind_name": provider.kind_name if provider else None,
        "active": cp.active,
        "connection_required_fields": list((catalog_entry or {}).get("connection_required_fields") or []),
        "connection_optional_fields": list((catalog_entry or {}).get("connection_optional_fields") or []),
        "status": cp.status,
        "status_name": cp.status_name,
        "status_reason": cp.status_reason,
        "status_checked_at": cp.status_checked_at.isoformat() if cp.status_checked_at else None,
        "created_at": cp.created_at.isoformat() if cp.created_at else None,
        "updated_at": cp.updated_at.isoformat() if cp.updated_at else None,
    }
    data.update(_redacted_credentials(catalog_entry or {}, cp.credentials))
    return data
