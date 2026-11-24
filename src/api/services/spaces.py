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

from src import enums as src_enums
from src import models as src_models
from src.api.services import billing as billing_services
from src.api.services import company_locations as company_locations_services
from src.api.services import feature_flags as feature_flags_services

logger = logging.getLogger(__name__)

SPACE_HEADER = "HTTP_X_SPACE_ID"

ERROR_SPACE_ACCESS_DENIED = "space_access_denied"
ERROR_SPACE_REQUIRED = "space_required"


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
    if not raw.isdigit():  # "all", empty or garbage: treated as absent
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
    else:
        member_of = sorted(profile.spaces.filter(company_id=company_id).values_list("id", flat=True)) if profile else []
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
