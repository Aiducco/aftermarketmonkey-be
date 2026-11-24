"""
Spaces (multi-spaces, ADR 0001): which space a request is about and which spaces the caller may see.

The fe sends the switcher's choice as the optional header ``X-Space-Id: <id>|all``. ``resolve_scope`` turns it
into a SpaceScope; lists filter by it, ``ensure_visible`` checks an object's space and ``write_space`` picks the
space a new row goes to. Admin status and membership are read from the DB, never from the JWT. Companies without
the multi_spaces flag are not filtered at all and write to the main space, so their API is unchanged.
"""
import dataclasses
import logging
import secrets
import typing

from django.db import IntegrityError, transaction
from django.db.models import Count, ProtectedError, Sum
from django.db.models.functions import Trim, Upper

from src import enums as src_enums
from src import models as src_models
from src.api.services import billing as billing_services
from src.api.services import company_locations as company_locations_services
from src.api.services import feature_flags as feature_flags_services
from src.domain import space_prefix

logger = logging.getLogger(__name__)

SPACE_HEADER = "HTTP_X_SPACE_ID"

_TENANT_SLUG_ATTEMPTS = 5

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
ERROR_FEED_IN_USE = "feed_in_use"
ERROR_OWN_FEED_USE_DISCONNECT = "own_feed_use_disconnect"
ERROR_SPACE_FEED_NOT_FOUND = "space_feed_not_found"
ERROR_SPACE_NOT_ON_FEED = "space_not_on_feed"

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
# Feeds per space (ADR 0002)
# ---------------------------------------------------------------------------


def family_company_ids(company_id: int) -> typing.List[int]:
    """The customer company's id followed by its feed tenants' ids, for any company id of the family (a tenant
    maps to its parent first). A company with no tenants returns just its own id."""
    parent_id = src_models.Company.objects.values_list("parent_company_id", flat=True).filter(id=company_id).first()
    customer_id = parent_id or company_id
    tenant_ids = (
        src_models.Company.objects.filter(parent_company_id=customer_id).order_by("id").values_list("id", flat=True)
    )
    return [customer_id, *tenant_ids]


def feeds_for(space: typing.Union[src_models.Space, int]) -> typing.Dict[int, src_models.CompanyProviders]:
    """The connections a space uses, ``{provider_id: connection}``, active connections only (a deactivated
    tenant connection reads as not connected). Callers read it only for flagged companies; an unflagged company
    keeps reading its own connections."""
    space_id = space.id if isinstance(space, src_models.Space) else space
    rows = (
        src_models.SpaceProvider.objects.filter(space_id=space_id, company_provider__active=True)
        .select_related("company_provider")
        .order_by("provider_id")
    )
    return {row.provider_id: row.company_provider for row in rows}


def ensure_feed_tenant(space: src_models.Space) -> src_models.Company:
    """The hidden child company that holds the space's own distributor connections, created on first use (ADR 0002).
    Never created for the main space (its own feeds are the real company's). Call it on its own, outside any other
    transaction and after the request is validated: it takes the space row lock for one short transaction (two
    first connects cannot create two tenants) and does nothing slow while holding it. A caller whose connect then
    fails removes a tenant it just created with ``delete_new_feed_tenant``."""
    with transaction.atomic():
        locked = src_models.Space.objects.select_for_update().get(id=space.id)
        if locked.feed_company_id:
            space.feed_company = locked.feed_company
            return locked.feed_company
        parent = src_models.Company.objects.get(id=locked.company_id)
        name = "{} - {} feeds".format(parent.name, locked.name)[:255]
        tenant = None
        for attempt in range(_TENANT_SLUG_ATTEMPTS):
            suffix = "" if attempt == 0 else "-{}".format(secrets.token_hex(3))
            slug = "{}-space-{}-feeds{}".format(parent.slug[:200], locked.id, suffix)
            try:
                with transaction.atomic():
                    tenant = src_models.Company.objects.create(
                        name=name,
                        slug=slug,
                        status=parent.status,
                        status_name=parent.status_name,
                        is_internal=True,
                        onboarding_step=None,
                        parent_company=parent,
                    )
                break
            except IntegrityError:
                if attempt == _TENANT_SLUG_ATTEMPTS - 1:
                    raise
        locked.feed_company = tenant
        locked.save(update_fields=["feed_company", "updated_at"])
        space.feed_company = tenant
        return tenant


def delete_new_feed_tenant(tenant: src_models.Company) -> bool:
    """Removes a feed tenant a connect just created and then failed to use. Safe: does nothing (returns False)
    unless the tenant is a tenant and holds no connection at all, so it can never drop a feed or its prices. The
    space's link to it is cleared first."""
    with transaction.atomic():
        locked = (
            src_models.Company.objects.select_for_update().filter(id=tenant.id, parent_company__isnull=False).first()
        )
        if locked is None or src_models.CompanyProviders.objects.filter(company_id=locked.id).exists():
            return False
        src_models.Space.objects.filter(feed_company_id=locked.id).update(feed_company=None)
        locked.delete()
        return True


# ---------------------------------------------------------------------------
# Order accounts per space
# ---------------------------------------------------------------------------


def owner_space_id_of_connection(
    company_provider: src_models.CompanyProviders, main_space_id: typing.Optional[int] = None
) -> int:
    """The space that owns a connection. A feed tenant's connection is owned by the space whose ``feed_company``
    is that tenant (ADR 0002); every other connection is the company's own and belongs to its main space. Pass
    the scope's ``main_space_id`` to skip the main-space lookup (a read must not get-or-create per connection)."""
    company_id = company_provider.company_id
    parent_id = src_models.Company.objects.filter(id=company_id).values_list("parent_company_id", flat=True).first()
    if parent_id is not None:
        space_id = src_models.Space.objects.filter(feed_company_id=company_id).values_list("id", flat=True).first()
        if space_id is not None:
            return space_id
        # An orphan tenant (no space points at it): the customer company's main space.
        if main_space_id is not None:
            return main_space_id
        return src_models.Space.objects.main_for(parent_id).id
    if main_space_id is not None:
        return main_space_id
    return src_models.Space.objects.main_for(company_id).id


def order_account_space_ids(account: src_models.CompanyProviderOrderAccount, owner_space_id: int) -> typing.List[int]:
    """The spaces that use an order account: its Used-by rows, or the connection's owner space when it has
    none (ADR 0001). Reads ``account.spaces.all()``, so a prefetch avoids a query per account."""
    ids = sorted(space.id for space in account.spaces.all())
    return ids or [owner_space_id]


def owner_spaces_by_company(company_id: int, main_space_id: int) -> typing.Dict[int, int]:
    """``{company id: owner space id}`` for a customer company and its feed tenants: the company's own connections
    belong to its main space, a tenant's to the space whose ``feed_company`` it is."""
    owners = {company_id: main_space_id}
    owners.update(
        src_models.Space.objects.filter(company_id=company_id, feed_company_id__isnull=False).values_list(
            "feed_company_id", "id"
        )
    )
    return owners


def ensure_connection_visible(scope: SpaceScope, company_provider: src_models.CompanyProviders) -> None:
    """Object check for a connection of the family: visible when its owner space or any space that uses it is
    visible, else 403 space_access_denied naming the owner space. Always passes for a company without the flag."""
    if not scope.enabled:
        return
    owner_space_id = owner_space_id_of_connection(company_provider, scope.main_space_id)
    if owner_space_id in scope.visible_space_ids:
        return
    if src_models.SpaceProvider.objects.filter(
        company_provider=company_provider, space_id__in=scope.visible_space_ids
    ).exists():
        return
    raise _access_denied(scope, owner_space_id)


def visible_connections(
    scope: SpaceScope, connections: typing.List[src_models.CompanyProviders]
) -> typing.List[src_models.CompanyProviders]:
    """The connections of the family the caller can see (same rule as ``ensure_connection_visible``)."""
    owners = owner_spaces_by_company(scope.company_id, scope.main_space_id)
    visible = set(scope.visible_space_ids)
    used_by_visible = set(
        src_models.SpaceProvider.objects.filter(
            company_provider_id__in=[cp.id for cp in connections], space_id__in=visible
        ).values_list("company_provider_id", flat=True)
    )
    return [
        cp
        for cp in connections
        if owners.get(cp.company_id, scope.main_space_id) in visible or cp.id in used_by_visible
    ]


def connection_ids_used_by(scope: SpaceScope, space_ids: typing.List[int]) -> typing.Set[int]:
    """Ids of the family's connections that one of the spaces uses: owns, or has a ``SpaceProvider`` row for."""
    owners = owner_spaces_by_company(scope.company_id, scope.main_space_id)
    owned_companies = [company_id for company_id, space_id in owners.items() if space_id in space_ids]
    ids = set(src_models.CompanyProviders.objects.filter(company_id__in=owned_companies).values_list("id", flat=True))
    ids.update(
        src_models.SpaceProvider.objects.filter(space_id__in=space_ids).values_list("company_provider_id", flat=True)
    )
    return ids


def other_spaces_using(company_provider: src_models.CompanyProviders, scope: SpaceScope) -> typing.List[dict]:
    """The spaces other than the connection's owner that use it, as ``{"id", "name"}`` refs, limited to the
    spaces the caller can see."""
    owner_space_id = owner_space_id_of_connection(company_provider, scope.main_space_id)
    space_ids = (
        src_models.SpaceProvider.objects.filter(company_provider=company_provider)
        .exclude(space_id=owner_space_id)
        .values_list("space_id", flat=True)
    )
    return [
        {"id": space_id, "name": scope.space_names.get(space_id)}
        for space_id in sorted(space_ids)
        if space_id in scope.visible_space_ids
    ]


def stop_using_feed(scope: SpaceScope, space_id: int, provider_id: int) -> None:
    """DELETE /spaces/<id>/feeds/<provider_id>/: a space stops using a feed another space owns. Its
    ``SpaceProvider`` row and its Used-by rows on that connection's order accounts go; the feed, its owner and the
    other sharers are untouched. Admin only, flagged companies only."""
    if not scope.enabled:
        raise SpaceError("Spaces are not available for this company.", ERROR_FEATURE_NOT_AVAILABLE, status_code=403)
    require_admin(scope)
    with transaction.atomic():
        space = _get_space(scope.company_id, space_id)
        row = (
            src_models.SpaceProvider.objects.select_related("company_provider")
            .filter(space=space, provider_id=provider_id)
            .first()
        )
        if row is None:
            raise SpaceError("This space uses no feed of that distributor.", ERROR_SPACE_FEED_NOT_FOUND, 404)
        connection = row.company_provider
        if owner_space_id_of_connection(connection, scope.main_space_id) == space.id:
            raise SpaceError(
                "This is the space's own account. Disconnect it instead.", ERROR_OWN_FEED_USE_DISCONNECT, 400
            )
        for account in connection.order_accounts.filter(spaces=space):
            account.spaces.remove(space)
        row.delete()


# ---------------------------------------------------------------------------
# Sorting orders from distributors into spaces
# ---------------------------------------------------------------------------


class ExternalOrderSorter:
    """The sorting rule for an order from a distributor that we did not place (spec Definitions). Step 1 (placed
    in the app) never applies here: those orders are dropped before they get this far. Then, in order:
    2. the PO number starts with a space's prefix; 3. the connection's space is unambiguous (every active order
    account of the connection is used by the same single space, or it has only one account, used by one space);
    4. the main space. Step 3 looks at the connection, not the account that polled: Keystone gives every account of
    a company the same history, so the polling account says nothing about where the order was placed. Built once
    per batch (it caches prefixes and each connection's spaces)."""

    def __init__(self, company_id: int) -> None:
        self.main_space_id = src_models.Space.objects.main_for(company_id).id
        # Longest first; prefixes never start one another, so the order is only a safeguard.
        self._prefixes = sorted(
            (
                (prefix, space_id)
                for space_id, prefix in src_models.Space.objects.filter(company_id=company_id)
                .exclude(prefix__isnull=True)
                .exclude(prefix="")
                .values_list("id", "prefix")
            ),
            key=lambda pair: -len(pair[0]),
        )
        self._connection_space: typing.Dict[int, typing.Optional[int]] = {}
        # A feed tenant's connection is owned by the space the tenant belongs to (ADR 0002); every other
        # connection of the customer company is owned by the main space.
        self._tenant_space = dict(
            src_models.Space.objects.filter(company_id=company_id, feed_company_id__isnull=False).values_list(
                "feed_company_id", "id"
            )
        )

    def _unambiguous_space(self, account: src_models.CompanyProviderOrderAccount) -> typing.Optional[int]:
        """The one space every active account of the account's connection is used by, else None."""
        accounts = list(
            src_models.CompanyProviderOrderAccount.objects.filter(
                company_provider_id=account.company_provider_id, active=True
            ).prefetch_related("spaces")
        )
        if not any(a.id == account.id for a in accounts):
            accounts.append(account)
        owner_space_id = self._tenant_space.get(account.company_provider.company_id, self.main_space_id)
        used_by = {tuple(order_account_space_ids(a, owner_space_id)) for a in accounts}
        if len(used_by) == 1:
            space_ids = next(iter(used_by))
            if len(space_ids) == 1:
                return space_ids[0]
        return None

    def sort(
        self, account: src_models.CompanyProviderOrderAccount, customer_po_number: typing.Optional[str]
    ) -> typing.Tuple[int, "src_enums.ExternalOrderSpaceReason", typing.Optional[str]]:
        """(space id, reason, the prefix that matched or None)."""
        po = (customer_po_number or "").strip().upper()
        if po:
            for prefix, space_id in self._prefixes:
                if po.startswith(prefix.upper()):
                    return space_id, src_enums.ExternalOrderSpaceReason.PREFIX, prefix
        if account.company_provider_id not in self._connection_space:
            self._connection_space[account.company_provider_id] = self._unambiguous_space(account)
        space_id = self._connection_space[account.company_provider_id]
        if space_id is not None:
            return space_id, src_enums.ExternalOrderSpaceReason.ACCOUNT, None
        return self.main_space_id, src_enums.ExternalOrderSpaceReason.MAIN, None


def external_order_sorter(company_id: int) -> typing.Optional[ExternalOrderSorter]:
    """The sorter for a flagged company (the customer company, never a feed tenant); None for an unflagged one
    (its orders keep going to the main space with no reason, exactly as before)."""
    if not feature_flags_services.company_has_feature(company_id, feature_flags_services.FEATURE_MULTI_SPACES):
        return None
    return ExternalOrderSorter(company_id)


def _resort_candidates(company_id: int, space: src_models.Space):
    """Past orders from distributors whose PO number starts with the space's prefix and that sit in another
    space. Orders an admin moved by hand are never re-sorted (spec OQ-5)."""
    if not space.prefix:
        return src_models.ExternalPurchaseOrder.objects.none()
    # Same matching as ingest (ExternalOrderSorter.sort): the PO is trimmed and compared in upper case.
    return (
        src_models.ExternalPurchaseOrder.objects.annotate(_po=Upper(Trim("customer_po_number")))
        .filter(company_id=company_id, _po__startswith=space.prefix.strip().upper())
        .exclude(space_id=space.id)
        .exclude(space_reason=src_enums.ExternalOrderSpaceReason.MOVED.value)
    )


def resort_preview(scope: SpaceScope, space_id: int) -> dict:
    """GET /spaces/<id>/resort-preview/ (admin): how many past orders would move into the space, and their
    value per order account (AC-31)."""
    require_admin(scope)
    space = _get_space(scope.company_id, space_id)
    rows = (
        _resort_candidates(scope.company_id, space)
        .values("order_account_id", "order_account__label", "company_provider__provider__name")
        .annotate(count=Count("id"), total=Sum("total"))
        .order_by("company_provider__provider__name", "order_account__label", "order_account_id")
    )
    by_account = [
        {
            "order_account_id": r["order_account_id"],
            "order_account_label": r["order_account__label"],
            "provider_name": r["company_provider__provider__name"],
            "count": r["count"],
            "total": round(float(r["total"] or 0), 2),
        }
        for r in rows
    ]
    return {
        "count": sum(r["count"] for r in by_account),
        "total": round(sum(r["total"] for r in by_account), 2),
        "by_account": by_account,
    }


def resort_apply(scope: SpaceScope, space_id: int) -> dict:
    """POST /spaces/<id>/resort/ (admin): move those orders into the space, reason prefix. Returns {"moved": n}."""
    require_admin(scope)
    with transaction.atomic():
        _lock_company(scope.company_id)
        space = _get_space(scope.company_id, space_id)
        ids = list(_resort_candidates(scope.company_id, space).select_for_update().values_list("id", flat=True))
        moved = src_models.ExternalPurchaseOrder.objects.filter(id__in=ids).update(
            space_id=space.id,
            space_reason=src_enums.ExternalOrderSpaceReason.PREFIX.value,
            space_reason_name=src_enums.ExternalOrderSpaceReason.PREFIX.name,
            space_reason_prefix=space.prefix,
            space_moved_by_id=None,
            space_moved_at=None,
        )
    return {"moved": moved}


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
    the only space of a non-admin member or of an order account, or has its own distributor connections or a
    shop-system connection."""
    require_admin(scope)
    try:
        _delete_space(scope, space_id)
    except (ProtectedError, IntegrityError):
        # Something referenced the space after the checks (a race): same answer as the check itself.
        raise SpaceError("This space has orders or open carts, so it cannot be deleted.", ERROR_SPACE_NOT_EMPTY)


def _delete_space_sync_rows(space: src_models.Space) -> None:
    """The space's ShopMonkey sync rows (kept after a disconnect) go with it, children first: executions, parts,
    brands (their extra vendors cascade), then vendors. Only this space's rows; the PROTECT on `space` would
    otherwise block the delete forever."""
    src_models.ShopMonkeySyncExecution.objects.filter(space=space).delete()
    src_models.ShopMonkeySyncPart.objects.filter(sync_brand__space=space).delete()
    src_models.ShopMonkeySyncBrand.objects.filter(space=space).delete()
    src_models.ShopMonkeySyncVendor.objects.filter(space=space).delete()


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
        if (
            space.feed_company_id
            and src_models.CompanyProviders.objects.filter(company_id=space.feed_company_id).exists()
        ):
            raise SpaceError(
                "This space has its own distributor accounts. Disconnect them first.", ERROR_SPACE_NOT_EMPTY
            )
        if src_models.CompanyShopManagementProviders.objects.filter(space=space).exists():
            raise SpaceError("This space has a shop system connected. Disconnect it first.", ERROR_SPACE_NOT_EMPTY)
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
        _delete_space_sync_rows(space)
        # The space's address rows go with it (CompanyLocation.space is CASCADE).
        space.delete()
