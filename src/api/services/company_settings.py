"""
Company settings and team management for admins.
"""
import logging

from django.contrib.auth import models as auth_models
from django.db import transaction

from src import models as src_models

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
        "tax_id": company.tax_id,
        "is_admin": profile.is_company_admin,
    }


def update_company_settings(
    company_id: int,
    user,
    name: str | None = None,
    business_type: str | None = None,
    country: str | None = None,
    state_province: str | None = None,
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
        company.business_type = business_type.strip() or None
    if country is not None:
        company.country = country.strip() or None
    if state_province is not None:
        company.state_province = state_province.strip() or None
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
    return [
        {
            "id": p.user_id,
            "email": p.user.email,
            "first_name": p.user.first_name,
            "last_name": p.user.last_name,
            "is_company_admin": p.is_company_admin,
            "created_at": p.created_at.isoformat() if p.created_at else None,
        }
        for p in profiles
    ]


@transaction.atomic
def add_company_user(
    company_id: int,
    admin_user,
    email: str,
    first_name: str,
    last_name: str,
    password: str,
    is_company_admin: bool = False,
) -> dict | str:
    """
    Add user to company. Admin only.
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

    existing_user = auth_models.User.objects.filter(email=email).first()

    if existing_user:
        # User exists: add to company (or update if already in company)
        try:
            profile = src_models.UserProfile.objects.get(user=existing_user)
        except src_models.UserProfile.DoesNotExist:
            profile = None

        if profile and profile.company_id == company_id:
            return "User is already in this company"

        if profile:
            profile.company = company
            profile.is_company_admin = is_company_admin
            profile.save()
        else:
            src_models.UserProfile.objects.create(
                user=existing_user,
                company=company,
                is_company_admin=is_company_admin,
            )

        return {
            "id": existing_user.id,
            "email": existing_user.email,
            "first_name": existing_user.first_name,
            "last_name": existing_user.last_name,
            "is_company_admin": is_company_admin,
        }

    # New user: create user + profile atomically
    user = auth_models.User(
        username=email,
        email=email,
        first_name=first_name.strip(),
        last_name=last_name.strip(),
    )
    user.set_password(password)
    user.save()

    src_models.UserProfile.objects.create(
        user=user,
        company=company,
        is_company_admin=is_company_admin,
    )

    return {
        "id": user.id,
        "email": user.email,
        "first_name": user.first_name,
        "last_name": user.last_name,
        "is_company_admin": is_company_admin,
    }


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

    return None
