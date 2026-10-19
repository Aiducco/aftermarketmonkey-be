import datetime
import logging
import secrets
import string
import typing

from django.conf import settings
from django.contrib.auth import models as auth_models
from django.db import transaction
from django.db.models import Count, Q
from django.utils import timezone

from src import enums as src_enums
from src import models as src_models

_LOG_PREFIX = "[ADMIN-SERVICE]"

logger = logging.getLogger(__name__)

# Alphanumeric only (no symbols) so a generated password is easy to read aloud/retype when staff
# relay it to a customer over the phone or in a support ticket -- length makes up for the smaller
# alphabet (62^24 is still astronomically large).
_GENERATED_PASSWORD_ALPHABET = string.ascii_letters + string.digits
_GENERATED_PASSWORD_LENGTH = 24


def _generate_password() -> str:
    return "".join(secrets.choice(_GENERATED_PASSWORD_ALPHABET) for _ in range(_GENERATED_PASSWORD_LENGTH))

# The internal company used to group staff/ops users — see seed_admin_company management command.
# Excluded from the "all companies" list since it's not a real customer.
ADMIN_COMPANY_SLUG = "aftermarketscout"


def list_all_companies_for_admin() -> typing.List[typing.Dict]:
    """
    Every company (excluding the internal admin company itself) with its subscription state and
    an aggregate count of connected vs. total providers — one query via annotate, no N+1 across
    companies.
    """
    logger.info("{} Fetching all companies for admin panel.".format(_LOG_PREFIX))

    companies = (
        src_models.Company.objects.exclude(slug=ADMIN_COMPANY_SLUG)
        .annotate(
            total_providers_count=Count("company_providers"),
            connected_providers_count=Count(
                "company_providers", filter=Q(company_providers__status_name="CONNECTED")
            ),
        )
        .order_by("-created_at")
    )

    data = []
    for company in companies:
        data.append(
            {
                "id": company.id,
                "name": company.name,
                "slug": company.slug,
                "status": company.status,
                "status_name": company.status_name,
                "subscription_plan": company.subscription_plan,
                "subscription_status": company.subscription_status,
                "subscription_period_end": (
                    company.subscription_period_end.isoformat() if company.subscription_period_end else None
                ),
                "onboarding_step": company.onboarding_step,
                "created_at": company.created_at.isoformat() if company.created_at else None,
                "connected_providers_count": company.connected_providers_count,
                "total_providers_count": company.total_providers_count,
            }
        )

    logger.info("{} Found {} companies.".format(_LOG_PREFIX, len(data)))
    return data


def get_admin_company_detail(company_id: int) -> typing.Optional[typing.Dict]:
    """Single company's summary (same shape as list_all_companies_for_admin's rows) plus every CompanyProviders row for it — provider name/status only, no credentials."""
    logger.info("{} Fetching company detail for company_id: {}.".format(_LOG_PREFIX, company_id))

    company = (
        src_models.Company.objects.filter(id=company_id)
        .annotate(
            total_providers_count=Count("company_providers"),
            connected_providers_count=Count(
                "company_providers", filter=Q(company_providers__status_name="CONNECTED")
            ),
        )
        .first()
    )
    if not company:
        return None

    company_providers = (
        src_models.CompanyProviders.objects.filter(company_id=company_id)
        .select_related("provider")
        .order_by("provider__name")
    )

    providers = []
    for cp in company_providers:
        providers.append(
            {
                "id": cp.id,
                "provider_id": cp.provider_id,
                "provider_name": cp.provider.name if cp.provider else None,
                "status": cp.status,
                "status_name": cp.status_name,
                "status_reason": cp.status_reason,
                "status_checked_at": cp.status_checked_at.isoformat() if cp.status_checked_at else None,
                "order_status": cp.order_status,
                "order_status_name": cp.order_status_name,
                "active": cp.active,
                "initial_sync_completed": cp.initial_sync_completed,
                "created_at": cp.created_at.isoformat() if cp.created_at else None,
            }
        )

    # Same shape list_company_users (settings/company/team/) returns for a company's own members —
    # here for any company, since this is the staff view. No password/credential fields.
    profiles = src_models.UserProfile.objects.filter(company_id=company_id).select_related("user")
    users = [
        {
            "id": p.user_id,
            "email": p.user.email,
            "first_name": p.user.first_name,
            "last_name": p.user.last_name,
            "is_company_admin": p.is_company_admin,
            "is_staff": p.user.is_staff,
            "is_active": p.user.is_active,
            "last_login": p.user.last_login.isoformat() if p.user.last_login else None,
            "created_at": p.created_at.isoformat() if p.created_at else None,
        }
        for p in profiles
    ]

    return {
        "id": company.id,
        "name": company.name,
        "slug": company.slug,
        "status": company.status,
        "status_name": company.status_name,
        "subscription_plan": company.subscription_plan,
        "subscription_status": company.subscription_status,
        "subscription_period_end": (
            company.subscription_period_end.isoformat() if company.subscription_period_end else None
        ),
        "onboarding_step": company.onboarding_step,
        "created_at": company.created_at.isoformat() if company.created_at else None,
        "connected_providers_count": company.connected_providers_count,
        "total_providers_count": company.total_providers_count,
        "providers": providers,
        "users": users,
    }


def list_all_providers_for_admin() -> typing.List[typing.Dict]:
    """Every distributor/provider with a count of companies connected to it (CONNECTED status) vs. total companies that have ever set it up — the inverse of list_all_companies_for_admin."""
    logger.info("{} Fetching all providers for admin panel.".format(_LOG_PREFIX))

    providers = src_models.Providers.objects.annotate(
        total_companies_count=Count("brand_providers"),
        connected_companies_count=Count(
            "brand_providers", filter=Q(brand_providers__status_name="CONNECTED")
        ),
    ).order_by("name")

    data = []
    for provider in providers:
        data.append(
            {
                "id": provider.id,
                "name": provider.name,
                "status": provider.status,
                "status_name": provider.status_name,
                "kind_name": provider.kind_name,
                "coming_soon": provider.coming_soon,
                "connected_companies_count": provider.connected_companies_count,
                "total_companies_count": provider.total_companies_count,
            }
        )

    logger.info("{} Found {} providers.".format(_LOG_PREFIX, len(data)))
    return data


def get_admin_provider_detail(provider_id: int) -> typing.Optional[typing.Dict]:
    """Single provider's summary plus every company connected to it (company name/slug/subscription + the CompanyProviders connection status) — the inverse of get_admin_company_detail. No credentials."""
    logger.info("{} Fetching provider detail for provider_id: {}.".format(_LOG_PREFIX, provider_id))

    provider = (
        src_models.Providers.objects.filter(id=provider_id)
        .annotate(
            total_companies_count=Count("brand_providers"),
            connected_companies_count=Count(
                "brand_providers", filter=Q(brand_providers__status_name="CONNECTED")
            ),
        )
        .first()
    )
    if not provider:
        return None

    company_providers = (
        src_models.CompanyProviders.objects.filter(provider_id=provider_id)
        .exclude(company__slug=ADMIN_COMPANY_SLUG)
        .select_related("company")
        .order_by("company__name")
    )

    companies = []
    for cp in company_providers:
        companies.append(
            {
                "id": cp.id,
                "company_id": cp.company_id,
                "company_name": cp.company.name if cp.company else None,
                "company_slug": cp.company.slug if cp.company else None,
                "subscription_plan": cp.company.subscription_plan if cp.company else None,
                "status": cp.status,
                "status_name": cp.status_name,
                "status_reason": cp.status_reason,
                "status_checked_at": cp.status_checked_at.isoformat() if cp.status_checked_at else None,
                "order_status": cp.order_status,
                "order_status_name": cp.order_status_name,
                "active": cp.active,
                "initial_sync_completed": cp.initial_sync_completed,
                "created_at": cp.created_at.isoformat() if cp.created_at else None,
            }
        )

    return {
        "id": provider.id,
        "name": provider.name,
        "status": provider.status,
        "status_name": provider.status_name,
        "kind_name": provider.kind_name,
        "coming_soon": provider.coming_soon,
        "connected_companies_count": provider.connected_companies_count,
        "total_companies_count": provider.total_companies_count,
        "companies": companies,
    }


def _generate_slug(company_name: str) -> str:
    """Same scheme as onboarding._generate_slug -- kept as its own copy rather than imported
    since the two are allowed to diverge (admin-created companies have no self-signup form
    shaping their input), not because of a real import-cycle concern."""
    import re
    import uuid

    base = re.sub(r"[^a-z0-9]+", "-", company_name.lower()).strip("-") or "company"
    return "{}-{}".format(base, uuid.uuid4().hex[:8])


@transaction.atomic
def create_company_for_admin(
    *,
    company_name: str,
    email: str,
    first_name: str,
    last_name: str,
    password: typing.Optional[str] = None,
    trial_days: typing.Optional[int] = None,
    subscription_plan: typing.Optional[str] = None,
) -> typing.Dict:
    """
    Staff-triggered equivalent of onboarding.register_user -- same Company+User+UserProfile
    shape and the same comp'd-trial defaults (demote_expired_trials.py still finds and expires
    these the same way), but:
      - onboarding_step is set to 4 (complete), not 2 -- a company an admin creates on a
        customer's behalf should land straight in the app, not re-enter the self-signup wizard.
      - returns the plaintext password used (generated here if the caller didn't supply one),
        since there is no "the user set their own password" step to skip that return -- staff
        need it to relay to the customer. Never logged.
      - does not return a JWT -- the admin isn't logging in as the new company.

    Raises ValueError (caller maps to a 400) if company_name is blank or email is taken.
    """
    email = email.strip().lower()
    company_name = company_name.strip()
    if not company_name:
        raise ValueError("Company name is required.")
    if auth_models.User.objects.filter(email=email).exists():
        raise ValueError("A user with this email already exists.")

    now = timezone.now()
    resolved_trial_days = trial_days or getattr(settings, "NEW_SIGNUP_TRIAL_DAYS", 30)
    resolved_plan = subscription_plan or getattr(settings, "NEW_SIGNUP_TRIAL_PLAN", "hunter")
    resolved_password = password or _generate_password()

    company = src_models.Company.objects.create(
        name=company_name,
        slug=_generate_slug(company_name),
        status=src_enums.CompanyStatus.ACTIVE.value,
        status_name=src_enums.CompanyStatus.ACTIVE.name,
        onboarding_step=4,
        subscription_plan=resolved_plan,
        subscription_status="trialing",
        subscription_period_end=now + datetime.timedelta(days=resolved_trial_days),
        manual_trial_granted_at=now,
    )

    user = auth_models.User(
        username=email,
        email=email,
        first_name=first_name.strip(),
        last_name=last_name.strip(),
    )
    user.set_password(resolved_password)
    user.save()

    src_models.UserProfile.objects.create(user=user, company=company, is_company_admin=True)

    logger.info(
        "{} Created company_id={} (slug={}) with first user_id={} via admin panel.".format(
            _LOG_PREFIX, company.id, company.slug, user.id
        )
    )

    return {
        "company_id": company.id,
        "company_slug": company.slug,
        "user_id": user.id,
        "email": email,
        "password": resolved_password,
        "subscription_plan": resolved_plan,
        "subscription_period_end": company.subscription_period_end.isoformat(),
    }


def admin_reset_user_password(
    company_id: int, user_id: int, new_password: typing.Optional[str] = None
) -> typing.Tuple[typing.Optional[typing.Dict], typing.Optional[str]]:
    """
    Staff-only password reset -- unlike company_settings.reset_company_user_password, the acting
    user only needs is_staff (checked by the view's _require_staff), not membership in the
    target company. Still scoped by (user_id, company_id) together so a staff member resetting
    a password for the company they're looking at in the admin panel can't typo their way into
    resetting an unrelated user's password elsewhere.

    Returns (result, error) -- result is None on failure, with error set; a generated password
    (when the caller didn't supply one) is returned in plaintext in result, same reasoning as
    create_company_for_admin, and is never logged.
    """
    try:
        profile = src_models.UserProfile.objects.select_related("user").get(
            user_id=user_id, company_id=company_id
        )
    except src_models.UserProfile.DoesNotExist:
        return None, "User not found in company"

    resolved_password = new_password or _generate_password()
    profile.user.set_password(resolved_password)
    profile.user.save(update_fields=["password"])

    logger.info(
        "{} Reset password for user_id={} (company_id={}) via admin panel.".format(
            _LOG_PREFIX, user_id, company_id
        )
    )

    return {"user_id": user_id, "email": profile.user.email, "password": resolved_password}, None


def extend_company_trial(
    company_id: int, days: int, plan: typing.Optional[str] = None
) -> typing.Tuple[typing.Optional[typing.Dict], typing.Optional[str]]:
    """
    Extend (or (re)grant) a company's comp'd trial by `days`, counted from whichever is later:
    now, or the company's current subscription_period_end -- so extending a still-active trial
    adds to its real remaining time instead of resetting the clock back from today.

    Sets manual_trial_granted_at to now unconditionally (even if already set), which is what
    demote_expired_trials.py keys off to find and expire these later -- so an extension always
    keeps this company tracked for its own future expiry, including the case where it had
    previously been cleared by a real Stripe subscription (see billing._sync_company_subscription).
    That edge case -- extending a company that's on a real paid plan -- is allowed rather than
    blocked here; it is an unusual admin action (comp some extra time on top of a paid plan) but
    not a wrong one, and the subscription_plan passed in (or kept as-is) reflects that choice.

    Returns (result, error) -- result is None on failure (company not found), with error set.
    """
    try:
        company = src_models.Company.objects.get(id=company_id)
    except src_models.Company.DoesNotExist:
        return None, "Company not found"

    now = timezone.now()
    base = max(now, company.subscription_period_end) if company.subscription_period_end else now
    company.subscription_period_end = base + datetime.timedelta(days=days)
    company.subscription_status = "trialing"
    company.manual_trial_granted_at = now
    if plan:
        company.subscription_plan = plan
    elif not company.subscription_plan:
        company.subscription_plan = getattr(settings, "NEW_SIGNUP_TRIAL_PLAN", "hunter")
    company.save(
        update_fields=["subscription_period_end", "subscription_status", "manual_trial_granted_at", "subscription_plan"]
    )

    logger.info(
        "{} Extended trial for company_id={} by {} day(s), now ending {}.".format(
            _LOG_PREFIX, company_id, days, company.subscription_period_end.isoformat()
        )
    )

    return {
        "company_id": company.id,
        "subscription_plan": company.subscription_plan,
        "subscription_status": company.subscription_status,
        "subscription_period_end": company.subscription_period_end.isoformat(),
    }, None
