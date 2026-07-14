"""
Transactional notification emails around distributor integrations, sent via Resend
(https://resend.com). Separate from the legacy SMTP-based DEFAULT_FROM_EMAIL used for
support tickets and API key emails.
"""
import logging

import requests
from django.conf import settings
from django.template.loader import render_to_string

from src import enums as src_enums
from src import models as src_models

logger = logging.getLogger(__name__)

RESEND_API_URL = "https://api.resend.com/emails"


def _company_admin_email(company: src_models.Company) -> str | None:
    profile = (
        src_models.UserProfile.objects.filter(company=company, is_company_admin=True)
        .select_related("user")
        .first()
    )
    return profile.user.email if profile and profile.user.email else None


def _log_email(
    *,
    email_type: src_enums.NotificationEmailType,
    to_email: str,
    from_email: str,
    subject: str,
    company: src_models.Company | None,
    company_provider: src_models.CompanyProviders | None,
    status: src_enums.NotificationEmailStatus,
    provider_message_id: str | None = None,
    error_message: str | None = None,
) -> None:
    src_models.NotificationEmailLog.objects.create(
        email_type=email_type.value,
        email_type_name=email_type.name,
        to_email=to_email,
        from_email=from_email,
        subject=subject,
        company=company,
        company_provider=company_provider,
        status=status.value,
        status_name=status.name,
        provider_message_id=provider_message_id,
        error_message=error_message,
    )


def send_first_sync_completed_email(company_provider: src_models.CompanyProviders) -> None:
    """Notify the company admin the first time a distributor integration finishes syncing."""
    company = company_provider.company
    provider = company_provider.provider

    to_email = _company_admin_email(company)
    if not to_email:
        logger.warning(
            "Skipping first-sync email for company_provider_id=%s: no company admin email found.",
            company_provider.id,
        )
        return

    from_email = settings.NOTIFICATIONS_FROM_EMAIL
    subject = "{} is live on AftermarketScout".format(provider.name)
    context = {
        "provider_name": provider.name,
        "company_name": company.name,
        "app_url": settings.FRONTEND_BASE_URL,
    }

    try:
        response = requests.post(
            RESEND_API_URL,
            headers={"Authorization": "Bearer {}".format(settings.RESEND_API_KEY)},
            json={
                "from": from_email,
                "to": [to_email],
                "subject": subject,
                "html": render_to_string("first_sync_completed_email.html", context),
                "text": render_to_string("first_sync_completed_email.txt", context),
            },
            timeout=10,
        )
        response.raise_for_status()
    except Exception as e:
        logger.exception(
            "Failed to send first-sync email for company_provider_id=%s.", company_provider.id
        )
        _log_email(
            email_type=src_enums.NotificationEmailType.FIRST_SYNC_COMPLETED,
            to_email=to_email,
            from_email=from_email,
            subject=subject,
            company=company,
            company_provider=company_provider,
            status=src_enums.NotificationEmailStatus.FAILED,
            error_message=str(e)[:4000],
        )
        return

    _log_email(
        email_type=src_enums.NotificationEmailType.FIRST_SYNC_COMPLETED,
        to_email=to_email,
        from_email=from_email,
        subject=subject,
        company=company,
        company_provider=company_provider,
        status=src_enums.NotificationEmailStatus.SENT,
        provider_message_id=response.json().get("id"),
    )
