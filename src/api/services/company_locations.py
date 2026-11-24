"""
Company locations (shop/warehouse address book) — lets checkout offer "ship to one of my
locations" instead of typing the address every time.
"""
import logging

from django.db import transaction

from src import models as src_models

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[COMPANY-LOCATIONS]"


def _serialize(location: src_models.CompanyLocation) -> dict:
    return {
        "id": location.id,
        "label": location.label,
        "name": location.name,
        "attention": location.attention,
        "address1": location.address1,
        "address2": location.address2,
        "city": location.city,
        "state": location.state,
        "postal_code": location.postal_code,
        "country": location.country,
        "phone": location.phone,
        "is_primary": location.is_primary,
        "created_at": location.created_at.isoformat() if location.created_at else None,
        "updated_at": location.updated_at.isoformat() if location.updated_at else None,
    }


def list_company_locations(company_id: int) -> list:
    locations = src_models.CompanyLocation.objects.filter(company_id=company_id).order_by("-is_primary", "label")
    return [_serialize(loc) for loc in locations]


def list_space_addresses(company_id: int, space_ids: list) -> list:
    """Flagged companies: the one address of each given space (never the old extra rows), each with its space_id."""
    locations = src_models.CompanyLocation.objects.filter(
        company_id=company_id, address_of_space__in=space_ids
    ).order_by("-is_primary", "label")
    rows = []
    for loc in locations:
        row = _serialize(loc)
        row["space_id"] = loc.space_id
        rows.append(row)
    return rows


def list_for_request(request, company_id: int) -> list:
    """GET /company/locations/: a flagged company gets the addresses of the spaces the caller can see (raises
    SpaceError 403 when the header names a space they cannot); any other company gets its address book."""
    # Imported here: services.spaces imports this module.
    from src.api.services import feature_flags as feature_flags_services
    from src.api.services import spaces as spaces_services

    if not feature_flags_services.company_has_feature(company_id, feature_flags_services.FEATURE_MULTI_SPACES):
        return list_company_locations(company_id)
    scope = spaces_services.resolve_scope(request)
    if scope.requested_space_id is not None:
        spaces_services.ensure_visible(scope, scope.requested_space_id)
    return list_space_addresses(company_id, scope.list_space_ids)


@transaction.atomic
def create_company_location(company_id: int, **fields) -> dict:
    is_primary = bool(fields.pop("is_primary", False))
    if is_primary:
        src_models.CompanyLocation.objects.filter(company_id=company_id, is_primary=True).update(is_primary=False)
    elif not src_models.CompanyLocation.objects.filter(company_id=company_id).exists():
        # First location for a company defaults to primary so there's always one to fall back to.
        is_primary = True

    location = src_models.CompanyLocation.objects.create(company_id=company_id, is_primary=is_primary, **fields)
    return _serialize(location)


@transaction.atomic
def update_company_location(company_id: int, location_id: int, **fields) -> dict | str:
    location = (
        src_models.CompanyLocation.objects.select_for_update().filter(id=location_id, company_id=company_id).first()
    )
    if not location:
        return "Location not found"

    is_primary = fields.pop("is_primary", None)
    for key, value in fields.items():
        setattr(location, key, value)

    if is_primary is True and not location.is_primary:
        src_models.CompanyLocation.objects.filter(company_id=company_id, is_primary=True).exclude(
            id=location.id
        ).update(is_primary=False)
        location.is_primary = True
    elif is_primary is False:
        location.is_primary = False

    location.save()
    return _serialize(location)


@transaction.atomic
def delete_company_location(company_id: int, location_id: int) -> str | None:
    location = (
        src_models.CompanyLocation.objects.select_for_update().filter(id=location_id, company_id=company_id).first()
    )
    if not location:
        return "Location not found"

    was_primary = location.is_primary
    location.delete()

    if was_primary:
        next_location = src_models.CompanyLocation.objects.filter(company_id=company_id).order_by("label").first()
        if next_location:
            next_location.is_primary = True
            next_location.save(update_fields=["is_primary", "updated_at"])

    return None
