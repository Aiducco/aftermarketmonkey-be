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

from django.db import IntegrityError, transaction
from django.utils import timezone

from src import constants as src_constants
from src import enums as src_enums
from src import models as src_models
from src.api.services import feature_flags as feature_flags_services
from src.api.services import spaces as spaces_services
from src.domain import account_fingerprint as account_fingerprint_domain
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
CONNECTION_ERROR_FEATURE_NOT_AVAILABLE = "feature_not_available"
# Space rules (multi-spaces S5); raised as spaces_services.SpaceError so the body carries the ``space``.
ERROR_SPACE_HAS_SHOP_SYSTEM = "space_has_shop_system"
ERROR_SHOP_ACCOUNT_IN_USE = "shop_account_in_use"

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


def _space_ref(scope: spaces_services.SpaceScope, space_id: int) -> typing.Dict[str, typing.Any]:
    return {"id": space_id, "name": scope.space_names.get(space_id)}


def _fingerprint(catalog_entry: typing.Dict[str, typing.Any], credentials: typing.Dict[str, typing.Any]):
    """Hash of the credential fields the system's catalog entry names as the account identity
    (``account_identity_fields``; default: its required fields). None when the account cannot be recognised."""
    fields = catalog_entry.get("account_identity_fields")
    if fields is None:
        fields = catalog_entry.get("connection_required_fields") or []
    return account_fingerprint_domain.account_fingerprint(fields, credentials)


def _ensure_account_free(
    scope: spaces_services.SpaceScope,
    company_id: int,
    provider_id: int,
    fingerprint: typing.Optional[str],
    space_id: int,
) -> None:
    """AC-43: raises 400 shop_account_in_use (naming the space that has it) when another space of the company
    already holds this account."""
    if not fingerprint:
        return
    other = (
        src_models.CompanyShopManagementProviders.objects.filter(
            company_id=company_id, provider_id=provider_id, credentials_fingerprint=fingerprint
        )
        .exclude(space_id=space_id)
        .first()
    )
    if other is not None:
        raise spaces_services.SpaceError(
            "This account is already connected in {}.".format(scope.space_names.get(other.space_id)),
            ERROR_SHOP_ACCOUNT_IN_USE,
            extra={"space": _space_ref(scope, other.space_id)},
        )


def _ensure_space_free_of_other_system(
    scope: spaces_services.SpaceScope, space_id: int, provider_id: int
) -> typing.Optional["src_models.CompanyShopManagementProviders"]:
    """AC-42: the space's connection when it is of this system (connect then merges onto it); raises 400
    space_has_shop_system when the space already has a connection of another system."""
    existing = (
        src_models.CompanyShopManagementProviders.objects.filter(space_id=space_id).select_related("provider").first()
    )
    if existing is not None and existing.provider_id != provider_id:
        name = scope.space_names.get(space_id)
        raise spaces_services.SpaceError(
            "{} already has a shop system ({}). Disconnect it first.".format(name, existing.provider.name),
            ERROR_SPACE_HAS_SHOP_SYSTEM,
            extra={"space": _space_ref(scope, space_id)},
        )
    return existing


def _connection_ref(
    scope: spaces_services.SpaceScope, cp: "src_models.CompanyShopManagementProviders"
) -> typing.Dict[str, typing.Any]:
    return {
        "id": cp.id,
        "space": _space_ref(scope, cp.space_id),
        "status": cp.status,
        "status_name": cp.status_name,
        "status_reason": cp.status_reason,
        "status_checked_at": cp.status_checked_at.isoformat() if cp.status_checked_at else None,
    }


def get_shop_management_catalog(
    company_id: int, scope: typing.Optional[spaces_services.SpaceScope] = None
) -> typing.Dict:
    """
    Catalog of connectable shop-management systems with per-company connection status.

    The whole category is staged behind FEATURE_SHOP_MANAGEMENT (see
    src.api.services.feature_flags) -- a company without the flag gets an empty catalog, same
    shape as "no shop-management systems exist yet", so the frontend needs no special case for
    "not available to you" vs "nothing to show".

    With a ``scope`` of a company that has the multi_spaces flag, an item describes the connection of the current
    space (the main space's among the visible ones in "all", else the first) and adds ``space`` and
    ``connections``. Without one nothing changes.
    """
    logger.info("{} Fetching shop management catalog for company_id: {}.".format(_LOG_PREFIX, company_id))

    if not feature_flags_services.company_has_feature(company_id, feature_flags_services.FEATURE_SHOP_MANAGEMENT):
        return {"data": [], "categories": []}

    spaced = scope is not None and scope.enabled
    if spaced:
        connections = list(
            src_models.CompanyShopManagementProviders.objects.filter(
                company_id=company_id, space_id__in=scope.list_space_ids
            ).order_by("id")
        )
    else:
        connections = list(src_models.CompanyShopManagementProviders.objects.filter(company_id=company_id))
    company_connections_by_provider_id = {c.provider_id: c for c in connections}
    providers_by_kind = {p.kind: p for p in src_models.ShopManagementProviders.objects.all()}

    catalog = []
    for entry in src_constants.SHOP_MANAGEMENT_PROVIDER_CATALOG:
        kind_value = entry["kind"].value
        provider = providers_by_kind.get(kind_value)
        if not provider:
            continue

        provider_connections = [c for c in connections if c.provider_id == provider.id]
        connection = company_connections_by_provider_id.get(provider.id)
        if spaced:
            connection = next((c for c in provider_connections if c.space_id == scope.main_space_id), None) or (
                provider_connections[0] if provider_connections else None
            )
        item = {
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
                connection.status_checked_at.isoformat() if connection and connection.status_checked_at else None
            ),
        }
        if spaced:
            item["space"] = _space_ref(scope, connection.space_id) if connection else None
            item["connections"] = [_connection_ref(scope, c) for c in provider_connections]
        catalog.append(item)

    return {
        "data": catalog,
        "categories": list(
            dict.fromkeys(
                e.get("category", "") for e in src_constants.SHOP_MANAGEMENT_PROVIDER_CATALOG if e.get("category")
            )
        ),
    }


def _connection_result(
    cp: "src_models.CompanyShopManagementProviders",
    catalog_entry: typing.Dict[str, typing.Any],
    validated: typing.Optional[bool] = None,
    scope: typing.Optional[spaces_services.SpaceScope] = None,
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
    if scope is not None and scope.enabled:
        result["space"] = _space_ref(scope, cp.space_id)
    return result


def connect_provider(
    company_id: int,
    provider_id: int,
    credentials: typing.Dict[str, typing.Any],
    scope: spaces_services.SpaceScope,
) -> typing.Tuple[typing.Optional[typing.Dict], typing.Optional[str], typing.Optional[str]]:
    """
    Create, or idempotently update, the write space's ``CompanyShopManagementProviders`` row (AC-42). Validates
    credentials with a live ``test_connection()`` call before persisting anything. Returns
    (data, error_message, error_code).

    Gated behind FEATURE_SHOP_MANAGEMENT independently of the catalog endpoint -- the catalog
    hides the option, but a direct API call must not be able to connect around that.

    Flagged companies (multi_spaces): admin only, the connection goes to the write space (400 space_required in
    "all"), a space with a connection of another system is refused (SpaceError 400 space_has_shop_system) and so
    is an account another space of the company holds (SpaceError 400 shop_account_in_use). Unflagged companies
    write to the main space and are otherwise unchanged.
    """
    if not feature_flags_services.company_has_feature(company_id, feature_flags_services.FEATURE_SHOP_MANAGEMENT):
        return (
            None,
            "Shop management integrations are not available for this company yet.",
            CONNECTION_ERROR_FEATURE_NOT_AVAILABLE,
        )

    provider = src_models.ShopManagementProviders.objects.filter(id=provider_id).first()
    if not provider:
        return None, "Provider not found", CONNECTION_ERROR_NOT_FOUND

    catalog_entry = _get_catalog_entry_for_provider(provider_id)
    if not catalog_entry:
        return None, "Provider not found in catalog", CONNECTION_ERROR_NOT_FOUND

    if scope.enabled:
        spaces_services.require_admin(scope)
    space_id = spaces_services.write_space(scope)
    existing = _ensure_space_free_of_other_system(scope, space_id, provider_id)

    creds, err, err_code = _merge_credentials(catalog_entry, existing.credentials if existing else None, credentials)
    if err:
        return None, err, err_code
    fingerprint = _fingerprint(catalog_entry, creds)
    _ensure_account_free(scope, company_id, provider_id, fingerprint, space_id)

    validated, val_error, val_error_code = _validate_connection(provider.kind, creds)
    if val_error:
        return None, val_error, val_error_code

    status_enum = src_enums.ShopManagementConnectionStatus.CONNECTED if validated else None
    status_fields = _connection_status_fields(status_enum, None)

    try:
        # The company row lock serializes the checks and the write, so two spaces cannot take one account.
        with transaction.atomic():
            spaces_services._lock_company(company_id)
            existing = _ensure_space_free_of_other_system(scope, space_id, provider_id)
            _ensure_account_free(scope, company_id, provider_id, fingerprint, space_id)
            if existing:
                existing.credentials = creds
                existing.credentials_fingerprint = fingerprint
                for field, value in status_fields.items():
                    setattr(existing, field, value)
                existing.save()
                cp = existing
            else:
                cp = src_models.CompanyShopManagementProviders.objects.create(
                    company_id=company_id,
                    provider_id=provider_id,
                    space_id=space_id,
                    credentials=creds,
                    credentials_fingerprint=fingerprint,
                    **status_fields,
                )
    except IntegrityError:
        # A unique (the space's one connection, or the account) was taken after the checks: same answers.
        _ensure_space_free_of_other_system(scope, space_id, provider_id)
        _ensure_account_free(scope, company_id, provider_id, fingerprint, space_id)
        raise
    cp.provider = provider

    return _connection_result(cp, catalog_entry, validated, scope), None, None


def _get_connection(
    company_id: int, company_provider_id: int, scope: spaces_services.SpaceScope
) -> typing.Optional["src_models.CompanyShopManagementProviders"]:
    """The company's connection by id, visible to the caller (SpaceError 403 space_access_denied otherwise)."""
    cp = (
        src_models.CompanyShopManagementProviders.objects.filter(id=company_provider_id, company_id=company_id)
        .select_related("provider")
        .first()
    )
    if cp is not None:
        spaces_services.ensure_visible(scope, cp.space_id)
    return cp


def update_connection(
    company_id: int,
    company_provider_id: int,
    credentials: typing.Dict[str, typing.Any],
    scope: spaces_services.SpaceScope,
) -> typing.Tuple[typing.Optional[typing.Dict], typing.Optional[str], typing.Optional[str]]:
    """PATCH equivalent of connect_provider, addressed by connection id. Flagged: visible to the caller, admin
    only, and a new account identity another space holds is refused (SpaceError 400 shop_account_in_use)."""
    cp = _get_connection(company_id, company_provider_id, scope)
    if not cp:
        return None, "Connection not found", CONNECTION_ERROR_NOT_FOUND
    if scope.enabled:
        spaces_services.require_admin(scope)

    provider = cp.provider
    catalog_entry = _get_catalog_entry(provider.kind)
    if not catalog_entry:
        return None, "Provider not found in catalog", CONNECTION_ERROR_NOT_FOUND

    creds, err, err_code = _merge_credentials(catalog_entry, cp.credentials, credentials)
    if err:
        return None, err, err_code
    fingerprint = _fingerprint(catalog_entry, creds)
    _ensure_account_free(scope, company_id, cp.provider_id, fingerprint, cp.space_id)

    validated, val_error, val_error_code = _validate_connection(provider.kind, creds)
    if val_error:
        return None, val_error, val_error_code

    status_enum = src_enums.ShopManagementConnectionStatus.CONNECTED if validated else None
    status_fields = _connection_status_fields(status_enum, None)

    try:
        with transaction.atomic():
            spaces_services._lock_company(company_id)
            _ensure_account_free(scope, company_id, cp.provider_id, fingerprint, cp.space_id)
            cp.credentials = creds
            cp.credentials_fingerprint = fingerprint
            for field, value in status_fields.items():
                setattr(cp, field, value)
            cp.save()
    except IntegrityError:
        _ensure_account_free(scope, company_id, cp.provider_id, fingerprint, cp.space_id)
        raise

    return _connection_result(cp, catalog_entry, validated, scope), None, None


def disconnect(
    company_id: int, company_provider_id: int, scope: spaces_services.SpaceScope
) -> typing.Tuple[bool, typing.Optional[str]]:
    cp = _get_connection(company_id, company_provider_id, scope)
    if not cp:
        return False, "Connection not found"
    if scope.enabled:
        spaces_services.require_admin(scope)
    cp.delete()
    return True, None


def _connection_row(
    cp: "src_models.CompanyShopManagementProviders",
    catalog_entry: typing.Optional[typing.Dict[str, typing.Any]],
    scope: spaces_services.SpaceScope,
    detail: bool,
) -> typing.Dict[str, typing.Any]:
    provider = cp.provider
    row = {
        "id": cp.id,
        "company_id": cp.company_id,
        "provider_id": cp.provider_id,
        "provider_name": provider.name if provider else None,
        "provider_kind": provider.kind if provider else None,
        "provider_kind_name": provider.kind_name if provider else None,
        "active": cp.active,
    }
    if detail:
        row["connection_required_fields"] = list((catalog_entry or {}).get("connection_required_fields") or [])
        row["connection_optional_fields"] = list((catalog_entry or {}).get("connection_optional_fields") or [])
    row.update(
        {
            "status": cp.status,
            "status_name": cp.status_name,
            "status_reason": cp.status_reason,
            "status_checked_at": cp.status_checked_at.isoformat() if cp.status_checked_at else None,
            "created_at": cp.created_at.isoformat() if cp.created_at else None,
            "updated_at": cp.updated_at.isoformat() if cp.updated_at else None,
        }
    )
    row.update(_redacted_credentials(catalog_entry or {}, cp.credentials))
    if scope.enabled:
        row["space"] = _space_ref(scope, cp.space_id)
    return row


def list_company_connections(
    company_id: int, scope: spaces_services.SpaceScope
) -> typing.List[typing.Dict[str, typing.Any]]:
    connections = src_models.CompanyShopManagementProviders.objects.filter(company_id=company_id).select_related(
        "provider"
    )
    if scope.enabled:
        connections = connections.filter(space_id__in=scope.list_space_ids).order_by("id")

    data = []
    for cp in connections:
        provider = cp.provider
        catalog_entry = _get_catalog_entry(provider.kind) if provider else {}
        data.append(_connection_row(cp, catalog_entry, scope, detail=False))
    return data


def get_connection_detail(
    company_id: int, company_provider_id: int, scope: spaces_services.SpaceScope
) -> typing.Optional[typing.Dict[str, typing.Any]]:
    cp = _get_connection(company_id, company_provider_id, scope)
    if not cp:
        return None

    provider = cp.provider
    catalog_entry = _get_catalog_entry(provider.kind) if provider else {}
    return _connection_row(cp, catalog_entry, scope, detail=True)
