import logging

from django.conf import settings
from django.core.mail import send_mail

from src import models as src_models

logger = logging.getLogger(__name__)


def _ticket_to_dict(ticket: src_models.SupportTicket) -> dict:
    return {
        "id": ticket.id,
        "subject": ticket.subject,
        "message": ticket.message,
        "status": ticket.status,
        "created_at": ticket.created_at.isoformat(),
        "updated_at": ticket.updated_at.isoformat(),
    }


def create_ticket(company_id: int, user_id: int, subject: str, message: str) -> dict:
    ticket = src_models.SupportTicket.objects.create(
        company_id=company_id,
        user_id=user_id,
        subject=subject,
        message=message,
    )

    _notify_support(ticket)

    return _ticket_to_dict(ticket)


def list_tickets(user_id: int) -> list[dict]:
    tickets = src_models.SupportTicket.objects.filter(user_id=user_id)
    return [_ticket_to_dict(t) for t in tickets]


def _notify_support(ticket: src_models.SupportTicket) -> None:
    recipients = getattr(settings, "SUPPORT_EMAIL_RECIPIENT_LIST", None)
    if not recipients:
        recipients = getattr(settings, "LOGGING_EMAIL_RECIPIENT_LIST", [])
    if not recipients:
        return

    try:
        user = ticket.user
        from_email = getattr(settings, "DEFAULT_FROM_EMAIL", "noreply@aftermarketscout.com")
        send_mail(
            subject=f"[Support] #{ticket.id} — {ticket.subject}",
            message=(
                f"New support ticket from {user.email}\n\n"
                f"Subject: {ticket.subject}\n"
                f"Ticket ID: #{ticket.id}\n"
                f"Company ID: {ticket.company_id}\n\n"
                f"{ticket.message}"
            ),
            from_email=from_email,
            recipient_list=recipients,
            fail_silently=True,
        )
    except Exception as e:
        logger.exception("Failed to send support ticket email for ticket #%s: %s", ticket.id, e)
