"""
Spaces (multi-spaces, ADR 0001): which space a request is about and which spaces the caller may see.

The fe sends the switcher's choice as the optional header ``X-Space-Id: <id>|all``. ``resolve_scope`` turns it
into a SpaceScope; lists filter by it, ``ensure_visible`` checks an object's space and ``write_space`` picks the
space a new row goes to. Admin status and membership are read from the DB, never from the JWT. Companies without
the multi_spaces flag are not filtered at all and write to the main space, so their API is unchanged.
"""
import dataclasses
import logging
import typing

from django.db import IntegrityError, transaction
from django.db.models import ProtectedError

from src import enums as src_enums
from src import models as src_models
from src.api.services import billing as billing_services
from src.api.services import company_locations as company_locations_services
from src.api.services import feature_flags as feature_flags_services
from src.domain import space_prefix

logger = logging.getLogger(__name__)

SPACE_HEADER = "HTTP_X_SPACE_ID"

ERROR_SPACE_ACCESS_DENIED = "space_access_denied"
ERROR_SPACE_REQUIRED = "space_required"
ERROR_FEATURE_NOT_AVAILABLE = "feature_not_available"
ERROR_SPACE_NOT_FOUND = "space_not_found"
ERROR_PREFIX_REQUIRED = "prefix_required"
ERROR_PREFIX_INVALID = "prefix_invalid"
ERROR_PREFIX_CONFLICT = "prefix_conflict"
ERROR_ADDRESS_REQUIRED = "address_required"
ERROR_NAME_TAKEN = "name_taken"
ERROR_MAIN_SPACE_NO_PREFIX = "main_space_no_prefix"
ERROR_MAIN_SPACE_PROTECTED = "main_space_protected"
ERROR_SPACE_NOT_EMPTY = "space_not_empty"
ERROR_ONLY_SPACE_OF_USERS = "only_space_of_users"
ERROR_ONLY_SPACE_OF_ACCOUNTS = "only_space_of_accounts"

_ADDRESS_FIELDS = ("name", "attention", "address1", "address2", "city", "state", "postal_code", "country", "phone")


class SpaceError(Exception):
    """A space rule refused the request. ``body()`` is the JSON error body; ``status_code`` its HTTP status."""

    def __init__(
        self,
        message: str,
        error_code: str,
        status_code: int = 400,
        extra: typing.Optional[dict] = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.error_code = error_code
        self.status_code = status_code
        self.extra = extra or {}

    def body(self) -> dict:
        return {"message": self.message, "error_code": self.error_code, **self.extra}


@dataclasses.dataclass
class SpaceScope:
    company_id: int
    enabled: bool
    is_admin: bool
    main_space_id: int
    # Every space of the company (id -> name): lets an object check name the space without another query.
    space_names: typing.Dict[int, str]
    # Spaces the caller may see: every space for an admin, the member's spaces (else the main one) otherwise.
    visible_space_ids: typing.List[int]
    # The header's space id when it is a space of this company (visible or not), else None.
    requested_space_id: typing.Optional[int]

    @property
    def current_space_id(self) -> typing.Optional[int]:
        """The header's space when the caller can see it; None means "all"."""
        if self.requested_space_id in self.visible_space_ids:
            return self.requested_space_id
        return None

    @property
    def list_space_ids(self) -> typing.List[int]:
        """The space ids a list should filter by: the current space, else every visible one."""
        current = self.current_space_id
        return [current] if current is not None else list(self.visible_space_ids)


def _parse_header(request) -> typing.Optional[int]:
    raw = request.META.get(SPACE_HEADER)
    if raw is None:
        return None
    raw = raw.strip()
    if not (raw.isascii() and raw.isdigit()):  # "all", empty or garbage: treated as absent
        return None
    return int(raw)


def resolve_scope(request) -> SpaceScope:
    """The caller's space scope. Needs an authenticated request with a company (callers check that first)."""
    company_id = request.company_id
    main = src_models.Space.objects.main_for(company_id)

    if not feature_flags_services.company_has_feature(company_id, feature_flags_services.FEATURE_MULTI_SPACES):
        return SpaceScope(
            company_id=company_id,
            enabled=False,
            is_admin=_is_company_admin(request.user.id, company_id),
            main_space_id=main.id,
            space_names={main.id: main.name},
            visible_space_ids=[main.id],
            requested_space_id=None,
        )

    space_names = dict(src_models.Space.objects.filter(company_id=company_id).values_list("id", "name"))
    profile = src_models.UserProfile.objects.filter(user_id=request.user.id, company_id=company_id).first()
    is_admin = bool(profile and profile.is_company_admin)
    if is_admin:
        visible = sorted(space_names)
    elif profile is None:
        # A removed member with a still-valid token: nothing to see, and writes answer space_required.
        visible = []
    else:
        member_of = sorted(profile.spaces.filter(company_id=company_id).values_list("id", flat=True))
        # No rows means the main space only (ADR 0001).
        visible = member_of or [main.id]

    requested = _parse_header(request)
    return SpaceScope(
        company_id=company_id,
        enabled=True,
        is_admin=is_admin,
        main_space_id=main.id,
        space_names=space_names,
        visible_space_ids=visible,
        requested_space_id=requested if requested in space_names else None,
    )


def _is_company_admin(user_id: int, company_id: int) -> bool:
    return src_models.UserProfile.objects.filter(user_id=user_id, company_id=company_id, is_company_admin=True).exists()


def _access_denied(scope: SpaceScope, space_id: int) -> SpaceError:
    name = scope.space_names.get(space_id)
    return SpaceError(
        "This belongs to {}".format(name),
        ERROR_SPACE_ACCESS_DENIED,
        status_code=403,
        extra={"space": {"id": space_id, "name": name}},
    )


def ensure_visible(scope: SpaceScope, space_id: int) -> None:
    """Object check: raises SpaceError (403 space_access_denied, naming the space) when the object's space
    is not one the caller can see. Always passes for a company without the flag."""
    if not scope.enabled or space_id in scope.visible_space_ids:
        return
    if space_id not in scope.space_names:
        # Not a space of this company: never name it. Objects are already filtered by company_id, so this
        # cannot happen for a real row.
        raise SpaceError("Space not found", ERROR_SPACE_ACCESS_DENIED, status_code=403)
    raise _access_denied(scope, space_id)


def write_space(scope: SpaceScope) -> int:
    """The space id a new row is written to, first rule that applies (ADR 0001):
    (1) a caller with exactly one visible space writes there, whatever the header says;
    (2) a header naming a visible space; (3) a header naming a space of the company the caller cannot see:
    403 space_access_denied; (4) "all", no header, or an id that is not a space of this company: 400
    space_required, never the main space. A company without the flag always writes to the main space."""
    if not scope.enabled:
        return scope.main_space_id
    if not scope.visible_space_ids:  # a removed member: nowhere to write, and no space to name in a 403
        raise SpaceError("Choose a space for this.", ERROR_SPACE_REQUIRED, status_code=400)
    if len(scope.visible_space_ids) == 1:
        return scope.visible_space_ids[0]
    requested = scope.requested_space_id
    if requested is not None:
        if requested in scope.visible_space_ids:
            return requested
        raise _access_denied(scope, requested)
    raise SpaceError("Choose a space for this.", ERROR_SPACE_REQUIRED, status_code=400)


# ---------------------------------------------------------------------------
# Serializing
# ---------------------------------------------------------------------------


def serialize_address(location: src_models.CompanyLocation) -> dict:
    data = company_locations_services._serialize(location)
    data["space_id"] = location.space_id
    return data


def serialize_space(space: src_models.Space) -> dict:
    return {
        "id": space.id,
        "name": space.name,
        "kind": src_enums.SpaceKind(space.kind).name.lower(),
        "prefix": space.prefix,
        "is_main": space.is_main,
        "address": serialize_address(space.location) if space.location_id else None,
    }


def get_spaces_overview(scope: SpaceScope) -> dict:
    """GET /api/spaces/: the caller's scope and the spaces they may see (an admin: every space)."""
    spaces = (
        src_models.Space.objects.filter(company_id=scope.company_id, id__in=scope.visible_space_ids)
        .select_related("location")
        .order_by("-is_main", "name")
    )
    data = {
        "enabled": scope.enabled,
        "main_space_id": scope.main_space_id,
        "current_space_id": scope.current_space_id,
        "is_admin": scope.is_admin,
        "visible_space_ids": list(scope.visible_space_ids),
        "spaces": [serialize_space(s) for s in spaces],
    }
    if scope.is_admin:
        company = src_models.Company.objects.get(id=scope.company_id)
        data["limit"] = {
            "used": billing_services.spaces_in_use(scope.company_id),
            "limit": billing_services.space_allowance(company),
        }
    return data


# ---------------------------------------------------------------------------
# Create, edit, delete (company admins of a flagged company)
# ---------------------------------------------------------------------------


def require_admin(scope: SpaceScope) -> None:
    """403 unless the company has the flag and the caller is one of its admins (read from the DB)."""
    if not scope.enabled:
        raise SpaceError("Spaces are not available for this company.", ERROR_FEATURE_NOT_AVAILABLE, status_code=403)
    if not scope.is_admin:
        raise SpaceError("Company admin access required", "admin_required", status_code=403)


def _lock_company(company_id: int) -> src_models.Company:
    """Serializes the allowance check, the prefix and name checks and the write for one company."""
    return src_models.Company.objects.select_for_update().get(id=company_id)


def _get_space(company_id: int, space_id: int) -> src_models.Space:
    space = src_models.Space.objects.select_related("location").filter(id=space_id, company_id=company_id).first()
    if space is None:
        raise SpaceError("Space not found", ERROR_SPACE_NOT_FOUND, status_code=404)
    return space


def _checked_prefix(company_id: int, prefix: typing.Optional[str], exclude_space_id: typing.Optional[int]) -> str:
    prefix = (prefix or "").strip()
    if not prefix:
        raise SpaceError("A prefix is required.", ERROR_PREFIX_REQUIRED)
    problem = space_prefix.format_error(prefix)
    if problem:
        raise SpaceError(problem, ERROR_PREFIX_INVALID)
    others = src_models.Space.objects.filter(company_id=company_id).exclude(id=exclude_space_id)
    clash = space_prefix.conflicting_prefix(prefix, others.values_list("prefix", flat=True))
    if clash:
        raise SpaceError(
            "The prefix {} clashes with {}, which another space already uses.".format(prefix, clash),
            ERROR_PREFIX_CONFLICT,
        )
    return prefix


def _checked_name(company_id: int, name: str, exclude_space_id: typing.Optional[int]) -> str:
    name = name.strip()
    if not name:
        raise SpaceError("Invalid payload", "invalid_payload")
    taken = src_models.Space.objects.filter(company_id=company_id, name__iexact=name).exclude(id=exclude_space_id)
    if taken.exists():
        raise SpaceError("A space named {} already exists.".format(name), ERROR_NAME_TAKEN)
    return name


def _address_values(address: dict) -> dict:
    return {key: address.get(key) for key in _ADDRESS_FIELDS}


def create_space(scope: SpaceScope, data: dict) -> dict:
    require_admin(scope)
    is_location = data["kind"] == "location"
    address = data.get("address")
    if is_location and not address:
        raise SpaceError("A Location needs an address.", ERROR_ADDRESS_REQUIRED)
    if not is_location and address:
        raise SpaceError("An Online store has no address.", "invalid_payload")

    kind = src_enums.SpaceKind.LOCATION if is_location else src_enums.SpaceKind.ONLINE_STORE
    with transaction.atomic():
        company = _lock_company(scope.company_id)
        name = _checked_name(company.id, data["name"], None)
        prefix = _checked_prefix(company.id, data.get("prefix"), None)
        allowed, reason = billing_services.check_space_limit(company.id)
        if not allowed:
            extra = {"upgrade_required": True}
            if billing_services.can_buy_space_addon(company):
                extra["upgrade_target"] = "additional_spaces"
            raise SpaceError(reason, billing_services.PLAN_LIMIT_ERROR_CODE, status_code=402, extra=extra)
        try:
            with transaction.atomic():
                space = src_models.Space.objects.create(
                    company_id=company.id, kind=kind.value, kind_name=kind.name, name=name, prefix=prefix
                )
        except IntegrityError:
            raise SpaceError("A space named {} already exists.".format(name), ERROR_NAME_TAKEN)
        if is_location:
            space.location = src_models.CompanyLocation.objects.create(
                company_id=company.id, space=space, label=name, is_primary=False, **_address_values(address)
            )
            space.save(update_fields=["location", "updated_at"])
    return serialize_space(space)


def update_space(scope: SpaceScope, space_id: int, data: dict) -> dict:
    require_admin(scope)
    with transaction.atomic():
        company = _lock_company(scope.company_id)
        space = _get_space(company.id, space_id)

        if "name" in data and data["name"].strip() != space.name:
            space.name = _checked_name(company.id, data["name"], space.id)
            if space.location_id:
                # The address row is labelled with the space's name (the label sent by the fe is ignored).
                space.location.label = space.name
                space.location.save(update_fields=["label", "updated_at"])

        if "prefix" in data:
            if space.is_main:
                if (data["prefix"] or "").strip():
                    raise SpaceError("The main space has no prefix.", ERROR_MAIN_SPACE_NO_PREFIX)
            else:
                space.prefix = _checked_prefix(company.id, data["prefix"], space.id)

        address = data.get("address")
        if address:
            if space.kind == src_enums.SpaceKind.ONLINE_STORE.value:
                raise SpaceError("An Online store has no address.", "invalid_payload")
            values = _address_values(address)
            if space.location_id:
                for key, value in values.items():
                    setattr(space.location, key, value)
                space.location.label = space.name
                space.location.save()
            else:
                # A migrated main space without an address: the new row is the company's primary one, which
                # is what NTW's ship-to reads.
                space.location = src_models.CompanyLocation.objects.create(
                    company_id=company.id, space=space, label=space.name, is_primary=space.is_main, **values
                )

        try:
            with transaction.atomic():
                space.save()
        except IntegrityError:
            raise SpaceError("A space named {} already exists.".format(space.name), ERROR_NAME_TAKEN)
    return serialize_space(space)


def _people_to_remove(space: src_models.Space) -> typing.List[dict]:
    """Non-admin members whose only space is this one (deleting it would silently widen them to the main space)."""
    profiles = (
        src_models.UserProfile.objects.filter(company_id=space.company_id, is_company_admin=False, spaces=space)
        .select_related("user")
        .prefetch_related("spaces")
    )
    return [
        {"id": p.user_id, "email": p.user.email, "first_name": p.user.first_name, "last_name": p.user.last_name}
        for p in profiles
        if [s.id for s in p.spaces.all() if s.company_id == space.company_id] == [space.id]
    ]


def _accounts_to_remove(space: src_models.Space) -> typing.List[dict]:
    """Order accounts whose only Used-by space is this one (no rows would mean "the owner" instead)."""
    accounts = (
        src_models.CompanyProviderOrderAccount.objects.filter(spaces=space)
        .select_related("company_provider__provider")
        .prefetch_related("spaces")
    )
    return [
        {
            "id": a.id,
            "label": a.label,
            "provider_name": a.company_provider.provider.name if a.company_provider.provider_id else None,
        }
        for a in accounts
        if [s.id for s in a.spaces.all()] == [space.id]
    ]


def delete_space(scope: SpaceScope, space_id: int) -> None:
    """Deletes a space and its one address. Refused while it is the main space, holds orders or carts, or is
    the only space of a non-admin member or of an order account."""
    require_admin(scope)
    try:
        _delete_space(scope, space_id)
    except (ProtectedError, IntegrityError):
        # Something referenced the space after the checks (a race): same answer as the check itself.
        raise SpaceError("This space has orders or open carts, so it cannot be deleted.", ERROR_SPACE_NOT_EMPTY)


def _delete_space(scope: SpaceScope, space_id: int) -> None:
    with transaction.atomic():
        company = _lock_company(scope.company_id)
        space = _get_space(company.id, space_id)
        if space.is_main:
            raise SpaceError("The main space cannot be deleted.", ERROR_MAIN_SPACE_PROTECTED)
        if (
            src_models.PurchaseOrder.objects.filter(space=space).exists()
            or src_models.ExternalPurchaseOrder.objects.filter(space=space).exists()
        ):
            raise SpaceError("This space has orders or open carts, so it cannot be deleted.", ERROR_SPACE_NOT_EMPTY)
        users = _people_to_remove(space)
        if users:
            raise SpaceError(
                "It is the only space of {}. Give them another space first.".format(
                    ", ".join(u["email"] for u in users)
                ),
                ERROR_ONLY_SPACE_OF_USERS,
                extra={"users": users},
            )
        accounts = _accounts_to_remove(space)
        if accounts:
            raise SpaceError(
                "It is the only space that uses {}. Give them another space first.".format(
                    ", ".join(a["label"] for a in accounts)
                ),
                ERROR_ONLY_SPACE_OF_ACCOUNTS,
                extra={"accounts": accounts},
            )
        # The space's address rows go with it (CompanyLocation.space is CASCADE).
        space.delete()
