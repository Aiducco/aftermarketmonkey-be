"""
Billing service for Stripe Customer Portal.
"""
import logging
from datetime import datetime
from typing import Optional

import stripe

from django.conf import settings

from src import models as src_models

logger = logging.getLogger(__name__)

PLAN_DISPLAY_NAMES = {
    "starter": "Starter",
    "pro": "Pro",
    "growth": "Growth",
}


def get_or_create_stripe_customer(company_id: int, email: str, name: str) -> Optional[str]:
    """
    Get existing Stripe customer ID for company, or create one if missing.
    Returns customer ID or None on error.
    """
    company = src_models.Company.objects.filter(id=company_id).first()
    if not company:
        return None

    if company.stripe_customer_id:
        return company.stripe_customer_id

    if not email or not email.strip():
        logger.error("Email required to create Stripe customer")
        return None

    api_key = getattr(settings, "STRIPE_SECRET_KEY", None)
    if not api_key:
        logger.error("STRIPE_SECRET_KEY not configured")
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


def create_portal_session(
    company_id: int,
    return_url: str,
    customer_email: str,
    customer_name: str = "",
) -> Optional[str]:
    """
    Create a Stripe Billing Portal session for the company.
    Returns the portal session URL or None on error.
    """
    api_key = getattr(settings, "STRIPE_SECRET_KEY", None)
    if not api_key:
        logger.error("STRIPE_SECRET_KEY not configured")
        return None

    customer_id = get_or_create_stripe_customer(
        company_id=company_id,
        email=customer_email,
        name=customer_name,
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
    """
    Create a Stripe Checkout session for subscription signup.
    Returns the checkout session URL or None on error.
    """
    api_key = getattr(settings, "STRIPE_SECRET_KEY", None)
    if not api_key:
        logger.error("STRIPE_SECRET_KEY not configured")
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
        company_id=company_id,
        email=customer_email,
        name=customer_name,
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


def get_subscription(company_id: int) -> Optional[dict]:
    """
    Get the company's current active subscription from Stripe.
    Returns dict with plan_id, plan, price, renewal_date, status or None if no subscription.
    """
    company = src_models.Company.objects.filter(id=company_id).first()
    if not company or not company.stripe_customer_id:
        return None

    api_key = getattr(settings, "STRIPE_SECRET_KEY", None)
    if not api_key:
        logger.error("STRIPE_SECRET_KEY not configured")
        return None

    plans = getattr(settings, "STRIPE_PLANS", {})
    amounts = getattr(settings, "STRIPE_PLAN_AMOUNTS", {})
    currencies = getattr(settings, "STRIPE_PLAN_CURRENCIES", {})
    product_to_plan = {v: k for k, v in plans.items()}
    # Include test product so existing subscriptions display correctly
    product_to_plan["prod_UAc2GCQQHcZwSz"] = "starter"

    try:
        # Fetch active, trialing, or past_due subscriptions (Stripe allows one status per call)
        all_subs = []
        for sub_status in ("active", "trialing", "past_due"):
            subs = stripe.Subscription.list(
                api_key=api_key,
                customer=company.stripe_customer_id,
                status=sub_status,
                limit=10,
            )
            all_subs.extend(subs.data)
        all_subs = [s for s in all_subs if s.get("status") in ("active", "trialing", "past_due")]
        if not all_subs:
            logger.info(
                "No active/trialing subscription for customer %s (company_id=%s)",
                company.stripe_customer_id,
                company_id,
            )
            return None

        sub = all_subs[0]
        price_obj = sub["items"]["data"][0]["price"]
        product_ref = price_obj.get("product")
        product_id = product_ref if isinstance(product_ref, str) else (product_ref.id if product_ref else None)
        plan_id = product_to_plan.get(product_id) if product_id else None
        if not plan_id:
            plan_id = "unknown"

        # Prefer price from Stripe subscription; fallback to config
        price_cents = price_obj.get("unit_amount")
        currency = price_obj.get("currency", "usd")
        if price_cents is None:
            price_cents = amounts.get(plan_id)
            currency = currencies.get(plan_id, "usd")
        symbol = "€" if currency == "eur" else "$"
        price_display = f"{symbol}{price_cents / 100:.2f}/mo" if price_cents is not None else "—"
        renewal_ts = sub.get("current_period_end")
        if renewal_ts:
            if isinstance(renewal_ts, int):
                renewal_date = datetime.utcfromtimestamp(renewal_ts).strftime("%Y-%m-%d")
            else:
                renewal_date = renewal_ts.strftime("%Y-%m-%d")
        else:
            renewal_date = None

        return {
            "plan_id": plan_id,
            "plan": PLAN_DISPLAY_NAMES.get(plan_id, plan_id.title()),
            "price": price_display,
            "renewal_date": renewal_date,
            "status": sub.get("status", "active"),
        }
    except stripe.StripeError as e:
        logger.exception("Stripe subscription fetch failed: %s", e)
        return None
