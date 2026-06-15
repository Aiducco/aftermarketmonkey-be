"""
Billing service for Stripe subscriptions, portal, checkout, and usage tracking.
"""
import logging
from datetime import datetime, timezone
from typing import Optional

import stripe

from django.conf import settings
from django.utils import timezone as django_tz

from src import models as src_models

logger = logging.getLogger(__name__)

PLAN_DISPLAY_NAMES = {
    "starter": "Starter",
    "pro": "Pro",
    "growth": "Growth",
}

ACTIVE_STATUSES = {"active", "trialing"}
DISPLAYABLE_STATUSES = {"active", "trialing", "past_due"}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _api_key() -> Optional[str]:
    key = getattr(settings, "STRIPE_SECRET_KEY", None)
    if not key:
        logger.error("STRIPE_SECRET_KEY not configured")
    return key


def _plan_limits(plan_id: Optional[str]) -> dict:
    limits = getattr(settings, "PLAN_LIMITS", {})
    return limits.get(plan_id, limits.get(None, {}))


def _product_to_plan_map() -> dict:
    plans = getattr(settings, "STRIPE_PLANS", {})
    mapping = {v: k for k, v in plans.items()}
    # Legacy test product
    mapping["prod_UAc2GCQQHcZwSz"] = "starter"
    return mapping


def _sync_company_subscription(company: src_models.Company, sub: dict) -> None:
    """Write Stripe subscription state to the Company record."""
    product_to_plan = _product_to_plan_map()
    items = sub.get("items", {}).get("data", [])
    plan_id = None
    if items:
        price_obj = items[0].get("price", {})
        product_ref = price_obj.get("product")
        product_id = product_ref if isinstance(product_ref, str) else getattr(product_ref, "id", None)
        plan_id = product_to_plan.get(product_id)

    period_end_ts = sub.get("current_period_end")
    period_end = None
    if period_end_ts:
        period_end = datetime.fromtimestamp(period_end_ts, tz=timezone.utc)

    company.subscription_id = sub.get("id")
    company.subscription_plan = plan_id
    company.subscription_status = sub.get("status")
    company.subscription_period_end = period_end
    company.save(update_fields=[
        "subscription_id", "subscription_plan",
        "subscription_status", "subscription_period_end",
    ])


def _clear_company_subscription(company: src_models.Company) -> None:
    company.subscription_id = None
    company.subscription_plan = None
    company.subscription_status = "canceled"
    company.subscription_period_end = None
    company.save(update_fields=[
        "subscription_id", "subscription_plan",
        "subscription_status", "subscription_period_end",
    ])


# ---------------------------------------------------------------------------
# Customer management
# ---------------------------------------------------------------------------

def get_or_create_stripe_customer(company_id: int, email: str, name: str) -> Optional[str]:
    company = src_models.Company.objects.filter(id=company_id).first()
    if not company:
        return None

    if company.stripe_customer_id:
        return company.stripe_customer_id

    if not email or not email.strip():
        logger.error("Email required to create Stripe customer")
        return None

    api_key = _api_key()
    if not api_key:
        return None

    try:
        customer = stripe.Customer.create(
            api_key=api_key,
            email=email,
            name=name or None,
            metadata={"company_id": str(company_id)},
        )
        company.stripe_customer_id = customer.id
        company.save(update_fields=["stripe_customer_id"])
        return customer.id
    except stripe.StripeError as e:
        logger.exception("Stripe customer creation failed: %s", e)
        return None


# ---------------------------------------------------------------------------
# Checkout / Portal
# ---------------------------------------------------------------------------

def create_portal_session(
    company_id: int,
    return_url: str,
    customer_email: str,
    customer_name: str = "",
) -> Optional[str]:
    api_key = _api_key()
    if not api_key:
        return None

    customer_id = get_or_create_stripe_customer(
        company_id=company_id, email=customer_email, name=customer_name,
    )
    if not customer_id:
        return None

    try:
        session = stripe.billing_portal.Session.create(
            api_key=api_key,
            customer=customer_id,
            return_url=return_url,
        )
        return session.url
    except stripe.StripeError as e:
        logger.exception("Stripe portal session creation failed: %s", e)
        return None


def create_checkout_session(
    company_id: int,
    plan_id: str,
    success_url: str,
    cancel_url: str,
    customer_email: str,
    customer_name: str = "",
) -> Optional[str]:
    api_key = _api_key()
    if not api_key:
        return None

    plans = getattr(settings, "STRIPE_PLANS", {})
    amounts = getattr(settings, "STRIPE_PLAN_AMOUNTS", {})
    currencies = getattr(settings, "STRIPE_PLAN_CURRENCIES", {})
    product_id = plans.get(plan_id)
    unit_amount = amounts.get(plan_id)
    currency = currencies.get(plan_id, "usd")

    if not product_id or unit_amount is None:
        logger.error("Invalid plan_id: %s", plan_id)
        return None

    customer_id = get_or_create_stripe_customer(
        company_id=company_id, email=customer_email, name=customer_name,
    )
    if not customer_id:
        return None

    try:
        session = stripe.checkout.Session.create(
            api_key=api_key,
            customer=customer_id,
            mode="subscription",
            line_items=[
                {
                    "price_data": {
                        "currency": currency,
                        "product": product_id,
                        "unit_amount": unit_amount,
                        "recurring": {"interval": "month"},
                    },
                    "quantity": 1,
                }
            ],
            success_url=success_url,
            cancel_url=cancel_url,
            metadata={"company_id": str(company_id), "plan_id": plan_id},
        )
        return session.url
    except stripe.StripeError as e:
        logger.exception("Stripe checkout session creation failed: %s", e)
        return None


# ---------------------------------------------------------------------------
# Subscription state (local-first, Stripe as fallback)
# ---------------------------------------------------------------------------

def get_subscription(company_id: int) -> Optional[dict]:
    """
    Return subscription info for the company.
    Reads local DB first (fast); falls back to Stripe API if no local state.
    """
    company = src_models.Company.objects.filter(id=company_id).first()
    if not company:
        return None

    # Use locally cached state if available
    if company.subscription_status and company.subscription_status in DISPLAYABLE_STATUSES:
        plan_id = company.subscription_plan
        amounts = getattr(settings, "STRIPE_PLAN_AMOUNTS", {})
        currencies = getattr(settings, "STRIPE_PLAN_CURRENCIES", {})
        unit_amount = amounts.get(plan_id)
        currency = currencies.get(plan_id, "usd")
        symbol = "€" if currency == "eur" else "$"
        price_display = f"{symbol}{unit_amount / 100:.2f}/mo" if unit_amount else "—"
        renewal_date = (
            company.subscription_period_end.strftime("%Y-%m-%d")
            if company.subscription_period_end else None
        )
        return {
            "plan_id": plan_id,
            "plan": PLAN_DISPLAY_NAMES.get(plan_id, (plan_id or "").title()),
            "price": price_display,
            "renewal_date": renewal_date,
            "status": company.subscription_status,
        }

    if not company.stripe_customer_id:
        return None

    # Fallback: fetch from Stripe and sync locally
    api_key = _api_key()
    if not api_key:
        return None

    try:
        subs = stripe.Subscription.list(
            api_key=api_key,
            customer=company.stripe_customer_id,
            status="all",
            limit=20,
            expand=["data.items.data.price"],
        )
        displayable = [s for s in subs.data if s.get("status") in DISPLAYABLE_STATUSES]
        if not displayable:
            return None

        sub = displayable[0]
        _sync_company_subscription(company, sub)

        # Re-read from updated company
        company.refresh_from_db()
        return get_subscription(company_id)
    except stripe.StripeError as e:
        logger.exception("Stripe subscription fetch failed: %s", e)
        return None


# ---------------------------------------------------------------------------
# Usage tracking
# ---------------------------------------------------------------------------

def _period_start(company: src_models.Company):
    """Return the start of the current billing period."""
    from datetime import timedelta
    now = django_tz.now()
    period_end = company.subscription_period_end
    if period_end and period_end > now:
        return period_end - timedelta(days=30)
    return now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def get_usage(company_id: int) -> dict:
    """
    Return current billing-period product-view usage and plan limit.
    Searches are always unlimited — only detail views are metered.
    """
    company = src_models.Company.objects.filter(id=company_id).first()
    if not company:
        return {}

    plan_id = company.subscription_plan if company.subscription_status in ACTIVE_STATUSES else None
    limits = _plan_limits(plan_id)
    period_start = _period_start(company)
    period_end = company.subscription_period_end

    detail_views_used = src_models.PartRequestAudit.objects.filter(
        company_id=company_id,
        action="detail",
        created_at__gte=period_start,
    ).count()

    return {
        "plan_id": plan_id,
        "plan": PLAN_DISPLAY_NAMES.get(plan_id, "Free") if plan_id else "Free",
        "period_start": period_start.strftime("%Y-%m-%d"),
        "period_end": period_end.strftime("%Y-%m-%d") if period_end else None,
        "detail_views": {
            "used": detail_views_used,
            "limit": limits.get("detail_views_per_month", 0),
        },
    }


def check_detail_view_limit(company_id: int) -> tuple[bool, str]:
    """
    Check if company can view a product detail page.
    Returns (allowed: bool, reason: str).
    -1 limit means unlimited.
    """
    company = src_models.Company.objects.filter(id=company_id).only(
        "subscription_plan", "subscription_status", "subscription_period_end"
    ).first()
    if not company:
        return False, "Company not found"

    plan_id = company.subscription_plan if company.subscription_status in ACTIVE_STATUSES else None
    limits = _plan_limits(plan_id)
    limit = limits.get("detail_views_per_month", 0)

    if limit == -1:
        return True, ""

    period_start = _period_start(company)
    used = src_models.PartRequestAudit.objects.filter(
        company_id=company_id,
        action="detail",
        created_at__gte=period_start,
    ).count()

    if used >= limit:
        plan_name = PLAN_DISPLAY_NAMES.get(plan_id, "Free") if plan_id else "Free"
        return False, f"Monthly product view limit reached for {plan_name} plan ({used}/{limit})"

    return True, ""


# ---------------------------------------------------------------------------
# Webhook event handlers
# ---------------------------------------------------------------------------

def handle_webhook_event(payload: bytes, sig_header: str) -> tuple[bool, str]:
    """
    Verify Stripe webhook signature and dispatch event to the right handler.
    Returns (success: bool, message: str).
    """
    webhook_secret = getattr(settings, "STRIPE_WEBHOOK_SECRET", None)
    api_key = _api_key()
    if not webhook_secret:
        logger.error("STRIPE_WEBHOOK_SECRET not configured")
        return False, "Webhook secret not configured"

    try:
        event = stripe.Webhook.construct_event(
            payload=payload,
            sig_header=sig_header,
            secret=webhook_secret,
        )
    except stripe.SignatureVerificationError as e:
        logger.warning("Stripe webhook signature verification failed: %s", e)
        return False, "Invalid signature"
    except Exception as e:
        logger.exception("Stripe webhook parsing failed: %s", e)
        return False, "Parse error"

    event_type = event.get("type")
    data_object = event.get("data", {}).get("object", {})

    handlers = {
        "checkout.session.completed": _handle_checkout_completed,
        "customer.subscription.created": _handle_subscription_upsert,
        "customer.subscription.updated": _handle_subscription_upsert,
        "customer.subscription.deleted": _handle_subscription_deleted,
        "invoice.payment_succeeded": _handle_invoice_payment_succeeded,
        "invoice.payment_failed": _handle_invoice_payment_failed,
    }

    handler = handlers.get(event_type)
    if handler:
        try:
            handler(data_object, api_key=api_key)
        except Exception as e:
            logger.exception("Error handling webhook event %s: %s", event_type, e)
            return False, f"Handler error: {e}"
    else:
        logger.debug("Unhandled Stripe event type: %s", event_type)

    return True, "ok"


def _company_by_customer(customer_id: str) -> Optional[src_models.Company]:
    return src_models.Company.objects.filter(stripe_customer_id=customer_id).first()


def _fetch_subscription(sub_id: str, api_key: str) -> Optional[dict]:
    try:
        return stripe.Subscription.retrieve(
            sub_id,
            expand=["items.data.price"],
            api_key=api_key,
        )
    except stripe.StripeError as e:
        logger.exception("Failed to retrieve subscription %s: %s", sub_id, e)
        return None


def _handle_checkout_completed(obj: dict, api_key: str) -> None:
    if obj.get("mode") != "subscription":
        return
    customer_id = obj.get("customer")
    sub_id = obj.get("subscription")
    if not customer_id or not sub_id:
        return

    company = _company_by_customer(customer_id)
    if not company:
        # Try metadata fallback
        meta_company_id = (obj.get("metadata") or {}).get("company_id")
        if meta_company_id:
            company = src_models.Company.objects.filter(id=meta_company_id).first()
            if company and not company.stripe_customer_id:
                company.stripe_customer_id = customer_id
                company.save(update_fields=["stripe_customer_id"])
    if not company:
        logger.warning("checkout.session.completed: no company for customer %s", customer_id)
        return

    sub = _fetch_subscription(sub_id, api_key)
    if sub:
        _sync_company_subscription(company, sub)
        logger.info(
            "checkout.session.completed: synced company %s → plan=%s status=%s",
            company.id, company.subscription_plan, company.subscription_status,
        )


def _handle_subscription_upsert(obj: dict, api_key: str) -> None:
    customer_id = obj.get("customer")
    company = _company_by_customer(customer_id)
    if not company:
        logger.warning("subscription event: no company for customer %s", customer_id)
        return
    _sync_company_subscription(company, obj)
    logger.info(
        "subscription upsert: company %s → plan=%s status=%s",
        company.id, company.subscription_plan, company.subscription_status,
    )


def _handle_subscription_deleted(obj: dict, api_key: str) -> None:
    customer_id = obj.get("customer")
    company = _company_by_customer(customer_id)
    if not company:
        logger.warning("subscription.deleted: no company for customer %s", customer_id)
        return
    _clear_company_subscription(company)
    logger.info("subscription.deleted: cleared subscription for company %s", company.id)


def _handle_invoice_payment_succeeded(obj: dict, api_key: str) -> None:
    customer_id = obj.get("customer")
    sub_id = obj.get("subscription")
    company = _company_by_customer(customer_id)
    if not company or not sub_id:
        return

    sub = _fetch_subscription(sub_id, api_key)
    if sub:
        _sync_company_subscription(company, sub)
        logger.info("invoice.payment_succeeded: refreshed subscription for company %s", company.id)


def _handle_invoice_payment_failed(obj: dict, api_key: str) -> None:
    customer_id = obj.get("customer")
    company = _company_by_customer(customer_id)
    if not company:
        return
    if company.subscription_status not in (None, "canceled"):
        company.subscription_status = "past_due"
        company.save(update_fields=["subscription_status"])
        logger.info("invoice.payment_failed: marked company %s as past_due", company.id)
