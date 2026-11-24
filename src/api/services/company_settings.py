"""
Company settings and team management for admins.
"""
import logging

from django.contrib.auth import models as auth_models
from django.db import transaction

from src import models as src_models
from src.api.services import billing as billing_services
from src.api.services import feature_flags as feature_flags_services
from src.api.services import spaces as spaces_services

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[COMPANY-SETTINGS]"


def get_user_profile(user) -> dict | None:
    """Get current user's profile for settings page."""
    if not user or not user.is_authenticated:
        return None
    try:
        profile = src_models.UserProfile.objects.select_related("company").get(user=user)
    except src_models.UserProfile.DoesNotExist:
        return {
            "id": user.id,
            "email": user.email,
            "first_name": user.first_name,
            "last_name": user.last_name,
            "company_id": None,
            "company_name": None,
            "is_company_admin": False,
        }
    company = profile.company
    return {
        "id": user.id,
        "email": user.email,
        "first_name": user.first_name,
        "last_name": user.last_name,
        "company_id": company.id if company else None,
        "company_name": company.name if company else None,
        "is_company_admin": profile.is_company_admin,
    }


def update_user_profile(
    user,
    first_name: str | None = None,
    last_name: str | None = None,
    email: str | None = None,
) -> dict | str:
    """
    Update current user's profile. Returns updated profile dict or error message.
    """
    if not user or not user.is_authenticated:
        return "User not authenticated"
    if email:
        email = email.strip().lower()
        if auth_models.User.objects.filter(email=email).exclude(id=user.id).exists():
            return "A user with this email already exists"
        user.email = email
        user.username = email
    if first_name is not None:
        user.first_name = first_name.strip()
    if last_name is not None:
        user.last_name = last_name.strip()
    user.save()
    return get_user_profile(user)


def _is_company_admin(user, company_id: int) -> tuple[bool, str | None]:
    """Check if user is company admin. Returns (ok, error_message)."""
    if not user or not user.is_authenticated:
        return False, "User not authenticated"
    if not company_id:
        return False, "No company found in token"
    try:
        profile = src_models.UserProfile.objects.get(user=user, company_id=company_id)
    except src_models.UserProfile.DoesNotExist:
        return False, "User not in company"
    if not profile.is_company_admin:
        return False, "Company admin access required"
    return True, None


def get_company_settings(company_id: int, user) -> dict | None:
    """Get company details for settings page. User must be in company."""
    try:
        profile = src_models.UserProfile.objects.get(user=user, company_id=company_id)
    except src_models.UserProfile.DoesNotExist:
        return None

    company = profile.company
    if not company:
        return None

    return {
        "id": company.id,
        "name": company.name,
        "slug": company.slug,
        "business_type": company.business_type,
        "country": company.country,
        "state_province": company.state_province,
        "city": company.city,
        "postal_code": company.postal_code,
        "tax_id": company.tax_id,
        "is_admin": profile.is_company_admin,
    }


def update_company_settings(
    company_id: int,
    user,
    name: str | None = None,
    business_type: list | None = None,
    country: str | None = None,
    state_province: str | None = None,
    city: str | None = None,
    postal_code: str | None = None,
    tax_id: str | None = None,
) -> dict | None:
    """Update company. User must be company admin."""
    ok, err = _is_company_admin(user, company_id)
    if not ok:
        return None

    try:
        company = src_models.Company.objects.get(id=company_id)
    except src_models.Company.DoesNotExist:
        return None

    if name is not None:
        company.name = name.strip()
    if business_type is not None:
        company.business_type = [str(bt).strip() for bt in business_type if str(bt).strip()]
    if country is not None:
        company.country = country.strip() or None
    if state_province is not None:
        company.state_province = state_province.strip() or None
    if city is not None:
        company.city = city.strip() or None
    if postal_code is not None:
        company.postal_code = postal_code.strip() or None
    if tax_id is not None:
        company.tax_id = tax_id.strip() or None
    company.save()

    return get_company_settings(company_id, user)


def list_company_users(company_id: int, user) -> list | None:
    """List all users in company. User must be in company."""
    try:
        src_models.UserProfile.objects.get(user=user, company_id=company_id)
    except src_models.UserProfile.DoesNotExist:
        return None

    profiles = src_models.UserProfile.objects.filter(company_id=company_id).select_related("user")
    space_ids_of = _space_ids_by_profile(company_id, profiles) if _spaces_enabled(company_id) else None
    return [
        {
            "id": p.user_id,
            "email": p.user.email,
            "first_name": p.user.first_name,
            "last_name": p.user.last_name,
            "is_company_admin": p.is_company_admin,
            # Plan-limit suspension (see plan_lifecycle): a suspended member can still sign in but
            # every other route answers "account needs upgrade"; the Members page badges them.
            "state": "suspended" if p.suspended_at else "active",
            "suspended_at": p.suspended_at.strftime("%Y-%m-%dT%H:%M:%SZ") if p.suspended_at else None,
            "created_at": p.created_at.isoformat() if p.created_at else None,
            **({"space_ids": space_ids_of[p.id]} if space_ids_of is not None else {}),
        }
        for p in profiles
    ]


def _spaces_enabled(company_id: int) -> bool:
    return feature_flags_services.company_has_feature(company_id, feature_flags_services.FEATURE_MULTI_SPACES)


def _space_ids_by_profile(company_id: int, profiles) -> dict:
    """profile id -> space ids: an admin has every space, a member their rows, else the main space (ADR 0001)."""
    all_ids = sorted(src_models.Space.objects.filter(company_id=company_id).values_list("id", flat=True))
    main_id = src_models.Space.objects.main_for(company_id).id
    linked: dict = {}
    for profile_id, space_id in src_models.Space.members.through.objects.filter(
        userprofile__company_id=company_id, space__company_id=company_id
    ).values_list("userprofile_id", "space_id"):
        linked.setdefault(profile_id, []).append(space_id)
    return {p.id: all_ids if p.is_company_admin else sorted(linked.get(p.id, [])) or [main_id] for p in profiles}


def _checked_space_ids(company_id: int, space_ids: list | None) -> list:
    """Distinct ids, each a space of the company; empty is space_required (a user has at least one space)."""
    ids = sorted(set(space_ids or []))
    if not ids:
        raise spaces_services.SpaceError("Choose at least one space.", spaces_services.ERROR_SPACE_REQUIRED)
    found = set(src_models.Space.objects.filter(company_id=company_id, id__in=ids).values_list("id", flat=True))
    if found != set(ids):
        raise spaces_services.SpaceError("Space not found", spaces_services.ERROR_SPACE_NOT_FOUND)
    return ids


@transaction.atomic
def set_user_spaces(company_id: int, admin_user, target_user_id: int, space_ids: list) -> dict:
    """PUT .../team/<id>/spaces/: replaces a member's spaces. Admin only, flagged companies only.
    Raises SpaceError (403, 400 space_required / space_not_found, 404 user not in company)."""
    ok, err = _is_company_admin(admin_user, company_id)
    if not ok:
        raise spaces_services.SpaceError(err or "Company admin access required", "admin_required", status_code=403)
    if not _spaces_enabled(company_id):
        raise spaces_services.SpaceError(
            "Spaces are not available for this company.", spaces_services.ERROR_FEATURE_NOT_AVAILABLE, status_code=403
        )
    profile = src_models.UserProfile.objects.filter(user_id=target_user_id, company_id=company_id).first()
    if profile is None:
        raise spaces_services.SpaceError("User not found in company", "user_not_found", status_code=404)
    spaces_services._lock_company(company_id)  # same lock as delete_space, so a delete cannot race this
    ids = _checked_space_ids(company_id, space_ids)
    profile.spaces.set(ids)
    return {"id": target_user_id, "space_ids": ids}


@transaction.atomic
def add_company_user(
    company_id: int,
    admin_user,
    email: str,
    first_name: str,
    last_name: str,
    password: str,
    is_company_admin: bool = False,
    space_ids: list | None = None,
) -> dict | str:
    """
    Add user to company. Admin only. Flagged company: a non-admin needs space_ids (SpaceError space_required).
    - If user exists: add them to company (update profile).
    - If user doesn't exist: create user + profile in one atomic transaction.
    Returns user dict on success, error message string on failure.
    """
    ok, err = _is_company_admin(admin_user, company_id)
    if not ok:
        return err

    email = email.strip().lower()
    try:
        company = src_models.Company.objects.get(id=company_id)
    except src_models.Company.DoesNotExist:
        return "Company not found"

    # Flagged: a non-admin gets exactly the given spaces; an admin sees every space and ignores space_ids.
    spaces_enabled = _spaces_enabled(company_id)
    member_space_ids = []
    if spaces_enabled and not is_company_admin:
        spaces_services._lock_company(company_id)  # same lock as delete_space, so a delete cannot race this
        member_space_ids = _checked_space_ids(company_id, space_ids)

    existing_user = auth_models.User.objects.filter(email=email).first()

    if existing_user:
        # User exists: add to company (or update if already in company)
        try:
            profile = src_models.UserProfile.objects.get(user=existing_user)
        except src_models.UserProfile.DoesNotExist:
            profile = None

        if profile and profile.company_id == company_id:
            return "User is already in this company"

    # Every path past this point (re-adding an existing user to this company, or creating a
    # brand-new user below) consumes one of this company's seats — gate once, here, before
    # either branch. Must come AFTER the "already in this company" early-return above (that's
    # not a new seat) but BEFORE both the existing-user and new-user creation paths.
    allowed, seat_reason = billing_services.check_seat_limit(company_id)
    if not allowed:
        return seat_reason

    if existing_user:
        if profile:
            profile.company = company
            profile.is_company_admin = is_company_admin
            profile.save()
            # Space rows belong to the company the profile was in.
            profile.spaces.clear()
        else:
            profile = src_models.UserProfile.objects.create(
                user=existing_user,
                company=company,
                is_company_admin=is_company_admin,
            )

        result = {
            "id": existing_user.id,
            "email": existing_user.email,
            "first_name": existing_user.first_name,
            "last_name": existing_user.last_name,
            "is_company_admin": is_company_admin,
        }
        return _with_space_ids(result, profile, company_id, spaces_enabled, member_space_ids)

    # New user: create user + profile atomically
    user = auth_models.User(
        username=email,
        email=email,
        first_name=first_name.strip(),
        last_name=last_name.strip(),
    )
    user.set_password(password)
    user.save()

    profile = src_models.UserProfile.objects.create(
        user=user,
        company=company,
        is_company_admin=is_company_admin,
    )

    result = {
        "id": user.id,
        "email": user.email,
        "first_name": user.first_name,
        "last_name": user.last_name,
        "is_company_admin": is_company_admin,
    }
    return _with_space_ids(result, profile, company_id, spaces_enabled, member_space_ids)


def _with_space_ids(result: dict, profile, company_id: int, spaces_enabled: bool, member_space_ids: list) -> dict:
    """Flagged: stores a non-admin's spaces and adds space_ids to the response. Unflagged: result as is."""
    if not spaces_enabled:
        return result
    if profile.is_company_admin:
        result["space_ids"] = sorted(
            src_models.Space.objects.filter(company_id=company_id).values_list("id", flat=True)
        )
    else:
        profile.spaces.set(member_space_ids)
        result["space_ids"] = member_space_ids
    return result


def update_company_user_role(
    company_id: int,
    target_user_id: int,
    admin_user,
    is_company_admin: bool,
) -> dict | str:
    """Update user's admin role. Admin only. Cannot demote yourself if you're the last admin."""
    ok, err = _is_company_admin(admin_user, company_id)
    if not ok:
        return err

    try:
        profile = src_models.UserProfile.objects.get(
            user_id=target_user_id,
            company_id=company_id,
        )
    except src_models.UserProfile.DoesNotExist:
        return "User not found in company"

    # If demoting, ensure at least one admin remains
    if profile.is_company_admin and not is_company_admin:
        admin_count = src_models.UserProfile.objects.filter(
            company_id=company_id,
            is_company_admin=True,
        ).count()
        if admin_count <= 1:
            return "Cannot remove the last company admin"

    profile.is_company_admin = is_company_admin
    profile.save()

    return {
        "id": profile.user_id,
        "is_company_admin": profile.is_company_admin,
    }


@transaction.atomic
def remove_company_user(
    company_id: int,
    target_user_id: int,
    admin_user,
) -> str | None:
    """
    Remove user from company (sets company to null). Admin only.
    Cannot remove yourself if you're the last admin.
    Returns error message on failure, None on success.
    """
    ok, err = _is_company_admin(admin_user, company_id)
    if not ok:
        return err

    try:
        profile = src_models.UserProfile.objects.get(
            user_id=target_user_id,
            company_id=company_id,
        )
    except src_models.UserProfile.DoesNotExist:
        return "User not found in company"

    # Cannot remove the last admin
    if profile.is_company_admin:
        admin_count = src_models.UserProfile.objects.filter(
            company_id=company_id,
            is_company_admin=True,
        ).count()
        if admin_count <= 1:
            return "Cannot remove the last company admin"

    profile.company_id = None
    profile.save()
    # Space rows belong to the company the profile left.
    profile.spaces.clear()

    return None


def reset_company_user_password(
    company_id: int,
    target_user_id: int,
    admin_user,
    new_password: str,
) -> str | None:
    """
    Reset a team member's password. Admin only. Target user must belong to the same
    company as the admin — scoped the same way as update_company_user_role/
    remove_company_user, so an admin can't reach a user in another company.
    Returns error message on failure, None on success.
    """
    ok, err = _is_company_admin(admin_user, company_id)
    if not ok:
        return err

    try:
        profile = src_models.UserProfile.objects.select_related("user").get(
            user_id=target_user_id,
            company_id=company_id,
        )
    except src_models.UserProfile.DoesNotExist:
        return "User not found in company"

    profile.user.set_password(new_password)
    profile.user.save(update_fields=["password"])

    return None


# ---------------------------------------------------------------------------
# Account deletion
# ---------------------------------------------------------------------------


def delete_user_account(user, company_id: int) -> tuple[bool, str | None]:
    """
    Hard-delete the authenticated user's own account.
    Blocks if the user is the sole admin of their company.
    Returns (ok, error_message).
    """
    try:
        profile = src_models.UserProfile.objects.get(user=user, company_id=company_id)
    except src_models.UserProfile.DoesNotExist:
        # No profile — still allow deleting the auth user
        profile = None

    if profile and profile.is_company_admin:
        remaining_admins = (
            src_models.UserProfile.objects.filter(
                company_id=company_id,
                is_company_admin=True,
            )
            .exclude(user=user)
            .count()
        )
        if remaining_admins == 0:
            return False, "Cannot delete account: you are the last admin. Transfer admin rights first."

    # Delete the Django auth user (cascades to UserProfile via OneToOneField)
    user.delete()
    return True, None


# ---------------------------------------------------------------------------
# Company data wipe
# ---------------------------------------------------------------------------


def delete_all_company_data(user, company_id: int) -> tuple[bool, str | None]:
    """
    Admin-only: wipe all data for the company inside a single transaction.
    Deletes all company users, integration connections, integration requests,
    destinations, onboarding preferences, company brands, and the company row.
    Returns (ok, error_message).
    """
    ok, err = _is_company_admin(user, company_id)
    if not ok:
        return False, err

    # Feed tenants hold full copies of distributor pricing: never deleted inside a request (ADR 0002).
    if src_models.Company.objects.filter(parent_company_id=company_id).exists():
        return False, (
            "This company has space feeds that must be removed first. Contact support to delete the account."
        )

    with transaction.atomic():
        # Collect all Django auth user IDs in the company before deleting profiles
        user_ids = list(src_models.UserProfile.objects.filter(company_id=company_id).values_list("user_id", flat=True))

        # Company-scoped data — Django cascades handle most FK children,
        # but we delete explicitly for clarity and safety.
        src_models.IntegrationRequest.objects.filter(company_id=company_id).delete()
        src_models.CompanyProviders.objects.filter(company_id=company_id).delete()
        src_models.CompanyDestinations.objects.filter(company_id=company_id).delete()
        src_models.CompanyLocation.objects.filter(company_id=company_id).delete()
        src_models.CompanyBrands.objects.filter(company_id=company_id).delete()
        src_models.CompanyOnboardingPreferences.objects.filter(company_id=company_id).delete()

        # Delete all auth users (cascades to UserProfile)
        auth_models.User.objects.filter(id__in=user_ids).delete()

        # Finally delete the company itself
        src_models.Company.objects.filter(id=company_id).delete()

    return True, None
