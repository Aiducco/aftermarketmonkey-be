"""
Transactional notification emails around distributor integrations, sent via Resend
(https://resend.com). Separate from the legacy SMTP-based DEFAULT_FROM_EMAIL used for
support tickets and API key emails.
"""
import base64
import logging
import typing

import requests
from django.conf import settings
from django.template.loader import render_to_string

from src import enums as src_enums
from src import models as src_models
from src.integrations.orders import base as order_base
from src.integrations.orders import exceptions as order_exceptions

logger = logging.getLogger(__name__)

RESEND_API_URL = "https://api.resend.com/emails"


def _company_admin_email(company: src_models.Company) -> str | None:
    profile = (
        src_models.UserProfile.objects.filter(company=company, is_company_admin=True).select_related("user").first()
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
    # The customer: the parent for a feed tenant's connection, so the admin and the name are the shop's.
    company = company_provider.company.customer_company
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
        "app_url": "{}/parts".format(settings.FRONTEND_BASE_URL.rstrip("/")),
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
        logger.exception("Failed to send first-sync email for company_provider_id=%s.", company_provider.id)
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


def send_purchase_order_email(
    *,
    company_provider: src_models.CompanyProviders,
    purchase_order: src_models.PurchaseOrder,
    to_email: str,
    cc_email: typing.Optional[str],
    reply_to: typing.Optional[str] = None,
    pdf_bytes: bytes,
    pdf_filename: str,
) -> str:
    """
    Emails a generated purchase-order PDF to a distributor rep, called by
    EmailOrderAdapter.submit_order() (src/integrations/orders/email_order.py). Returns Resend's
    message id on success.

    ``reply_to``, when set (from the order account's optional "reply_to" credential field --
    see PROVIDER_CATALOG's email_order_connection_optional_fields), routes the rep's reply
    directly wherever the company wants it. Without it, a rep hitting "Reply" defaults to
    ``from_email`` -- a generic platform address nobody actively monitors for order-specific
    replies, not the company that actually placed the order.

    Unlike send_first_sync_completed_email above (fire-and-forget — a missed "you're live" email
    isn't worth failing a sync job over), this one RAISES on failure as OrderValidationError. The
    caller is inside submit_order(), and purchase_order_jobs._run_submit expects submit_order()
    to raise OrderAdapterError on any failure so the PO is correctly marked FAILED with a real
    error_message — silently swallowing a failed send here would leave the PO looking SUBMITTED
    when no email actually went out.
    """
    if not (to_email or "").strip():
        # Belt-and-suspenders: EmailOrderAdapter.__init__ already refuses to construct without a
        # rep_email, but that guard lives one layer up and a future caller (or a bug in that
        # guard) must never reach Resend with an empty "to" -- that's a PO silently marked
        # SUBMITTED/EMAILED with no email actually sent to anyone.
        raise order_exceptions.OrderValidationError(
            "Refusing to send the purchase order email: no recipient (rep) email address."
        )

    # The customer: the parent for a feed tenant's connection, so the admin and the name are the shop's.
    company = company_provider.company.customer_company
    provider = company_provider.provider
    from_email = settings.NOTIFICATIONS_FROM_EMAIL
    po_reference = order_base.resolve_po_number(purchase_order)
    subject = "Purchase Order {} — {}".format(po_reference, company.name)
    context = {
        "provider_name": provider.name,
        "company_name": company.name,
        "po_number": po_reference,
    }

    cc_list = [addr for addr in (cc_email, settings.PURCHASE_ORDER_INTERNAL_CC_EMAIL) if addr]
    payload: typing.Dict[str, typing.Any] = {
        "from": from_email,
        "to": [to_email],
        "subject": subject,
        "html": render_to_string("purchase_order_notification_email.html", context),
        "text": render_to_string("purchase_order_notification_email.txt", context),
        "attachments": [
            {
                "filename": pdf_filename,
                "content": base64.b64encode(pdf_bytes).decode("ascii"),
            }
        ],
    }
    if cc_list:
        payload["cc"] = cc_list
    if reply_to:
        payload["reply_to"] = reply_to

    try:
        response = requests.post(
            RESEND_API_URL,
            headers={"Authorization": "Bearer {}".format(settings.RESEND_API_KEY)},
            json=payload,
            timeout=15,
        )
        response.raise_for_status()
    except Exception as e:
        logger.exception("Failed to email purchase order id=%s to %s.", purchase_order.id, to_email)
        _log_email(
            email_type=src_enums.NotificationEmailType.PURCHASE_ORDER_EMAILED,
            to_email=to_email,
            from_email=from_email,
            subject=subject,
            company=company,
            company_provider=company_provider,
            status=src_enums.NotificationEmailStatus.FAILED,
            error_message=str(e)[:4000],
        )
        raise order_exceptions.OrderValidationError("Failed to email the purchase order to {}: {}".format(to_email, e))

    message_id = response.json().get("id")
    _log_email(
        email_type=src_enums.NotificationEmailType.PURCHASE_ORDER_EMAILED,
        to_email=to_email,
        from_email=from_email,
        subject=subject,
        company=company,
        company_provider=company_provider,
        status=src_enums.NotificationEmailStatus.SENT,
        provider_message_id=message_id,
    )
    return message_id


# --- Plan expiry / downgrade lifecycle emails (see src.api.services.plan_lifecycle) -----------
#
# Four emails: the day-7 and day-1 trial warnings, the day-0 "your trial has ended" summary and
# the "everything's back on" upgrade confirmation. plan_lifecycle builds the context (it owns the
# kept/paused/suspended numbers) and decides *when* to send; this module only renders, delivers
# and audits.
_LIFECYCLE_EMAILS = {
    src_enums.NotificationEmailType.TRIAL_ENDING_7D: (
        "trial_ending_7d_email",
        "Your {trial_plan_name} trial ends in {days_left_label}",
    ),
    src_enums.NotificationEmailType.TRIAL_ENDING_1D: (
        "trial_ending_1d_email",
        "Your trial ends tomorrow: choose what to keep",
    ),
    src_enums.NotificationEmailType.TRIAL_ENDED: (
        "trial_ended_email",
        "Your trial has ended: here's what changed",
    ),
    src_enums.NotificationEmailType.PLAN_RESTORED: (
        "plan_restored_email",
        "Everything's back on",
    ),
}


def _company_active_admin_emails(company: src_models.Company) -> list[str]:
    """Every admin who can still sign in -- a suspended admin cannot act on these emails."""
    profiles = src_models.UserProfile.objects.filter(
        company=company, is_company_admin=True, suspended_at__isnull=True
    ).select_related("user")
    emails = []
    for profile in profiles.order_by("created_at", "id"):
        email = (profile.user.email or "").strip()
        if email and email not in emails:
            emails.append(email)
    return emails


def send_plan_lifecycle_email(
    *,
    company: src_models.Company,
    email_type: src_enums.NotificationEmailType,
    context: dict,
) -> bool:
    """
    Sends one plan-lifecycle email to every active admin of ``company`` and returns True when it
    should be recorded as sent (Resend accepted it, or there is nobody to send to), False when the
    send failed and should be retried by a later cron run.

    Fire-and-forget like send_first_sync_completed_email -- it never raises, because a Resend
    outage must not abort the downgrade or the Stripe webhook that called it. The caller stamps
    Company.warning_emails_sent only on True, which is what makes a failed send retry and a
    successful one never repeat.

    "Nobody to send to" counts as sent: a company with no admin email cannot be emailed by
    retrying either, and returning False would make the hourly cron log the same warning for
    every remaining hour of the send window.
    """
    template_base, subject_template = _LIFECYCLE_EMAILS[email_type]
    to_emails = _company_active_admin_emails(company)
    if not to_emails:
        logger.warning(
            "Skipping %s email for company_id=%s: no active company admin email found.",
            email_type.name,
            company.id,
        )
        return True

    from_email = settings.NOTIFICATIONS_FROM_EMAIL
    subject = subject_template.format(**context)
    # The log column is a single address; a multi-admin send is one row listing every recipient.
    logged_to = ", ".join(to_emails)[:255]

    try:
        response = requests.post(
            RESEND_API_URL,
            headers={"Authorization": "Bearer {}".format(settings.RESEND_API_KEY)},
            json={
                "from": from_email,
                "to": to_emails,
                "subject": subject,
                "html": render_to_string("{}.html".format(template_base), context),
                "text": render_to_string("{}.txt".format(template_base), context),
            },
            timeout=10,
        )
        response.raise_for_status()
    except Exception as e:
        logger.exception("Failed to send %s email for company_id=%s.", email_type.name, company.id)
        _log_email(
            email_type=email_type,
            to_email=logged_to,
            from_email=from_email,
            subject=subject,
            company=company,
            company_provider=None,
            status=src_enums.NotificationEmailStatus.FAILED,
            error_message=str(e)[:4000],
        )
        return False

    # Resend has accepted the email: from here on this must return True whatever happens, because the
    # caller stamps Company.warning_emails_sent on True and a False would make the next cron run send
    # the same email again. An unreadable response body or a failed audit-log write is only logged.
    try:
        _log_email(
            email_type=email_type,
            to_email=logged_to,
            from_email=from_email,
            subject=subject,
            company=company,
            company_provider=None,
            status=src_enums.NotificationEmailStatus.SENT,
            provider_message_id=response.json().get("id"),
        )
    except Exception:
        logger.exception("Sent %s email for company_id=%s but could not audit-log it.", email_type.name, company.id)
    return True
