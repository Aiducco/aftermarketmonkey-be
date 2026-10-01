import logging
import typing

from django.db.models import Count, Q

from src import models as src_models

_LOG_PREFIX = "[ADMIN-SERVICE]"

logger = logging.getLogger(__name__)

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
