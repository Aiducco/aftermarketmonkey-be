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

ACTIVE_STATUSES = {"active", "trialing"}
DISPLAYABLE_STATUSES = {"active", "trialing", "past_due"}

# Shared error code for every plan-tier gate (distributor connections, seats, PO checkout) so
# the frontend can branch on one constant regardless of which endpoint blocked the request.
# integrations.CONNECTION_ERROR_PLAN_LIMIT_REACHED references this same value.
PLAN_LIMIT_ERROR_CODE = "plan_limit_reached"


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _api_key() -> Optional[str]:
    key = getattr(settings, "STRIPE_SECRET_KEY", None)
    if not key:
        logger.error("STRIPE_SECRET_KEY not configured")
    return key


def _plan_key(company: src_models.Company) -> str:
    """Which PLANS key applies: the company's Stripe plan_id if actively subscribed, else
    "scout" (the free tier — replaces the old None-key convention on PLAN_LIMITS)."""
    plan_id = company.subscription_plan if company.subscription_status in ACTIVE_STATUSES else None
    return plan_id or "scout"


def _plan_entry(plan_key: str) -> dict:
    plans = getattr(settings, "PLANS", {})
    return plans.get(plan_key, plans.get("scout", {}))


def _plan_features(plan_key: str) -> dict:
    return _plan_entry(plan_key).get("features", {})


def _plan_name(plan_key: str) -> str:
    return _plan_entry(plan_key).get("name", plan_key.title())


def _product_to_plan_map() -> dict:
    plans = getattr(settings, "PLANS", {})
    return {
        entry["stripe_product_id"]: plan_id
        for plan_id, entry in plans.items()
        if entry.get("stripe_product_id")
    }


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
    billing_period: str,
    success_url: str,
    cancel_url: str,
    customer_email: str,
    customer_name: str = "",
    quantity: int = 1,
) -> Optional[str]:
    """
    ``billing_period`` is "monthly" or "annual" — each paid plan has two pre-created Stripe
    Prices (see settings.PLANS), so checkout always references a real Price id rather than
    building price_data inline. ``quantity`` is only meaningful for plan_id="pack" (sold
    per-location via an adjustable-quantity line item); every other plan is always quantity 1.
    """
    api_key = _api_key()
    if not api_key:
        return None

    plans = getattr(settings, "PLANS", {})
    entry = plans.get(plan_id)
    if not entry:
        logger.error("Invalid plan_id: %s", plan_id)
        return None

    price_field = "stripe_price_id_monthly" if billing_period == "monthly" else "stripe_price_id_annual"
    price_id = entry.get(price_field)
    if not price_id:
        logger.error("No Stripe price configured for plan_id=%s billing_period=%s", plan_id, billing_period)
        return None

    customer_id = get_or_create_stripe_customer(
        company_id=company_id, email=customer_email, name=customer_name,
    )
    if not customer_id:
        return None

    # Only Pack is sold per-unit/location; every other plan is a fixed quantity-1 subscription.
    effective_quantity = quantity if plan_id == "pack" else 1
    line_item: dict = {"price": price_id, "quantity": effective_quantity}
    if plan_id == "pack":
        max_qty = getattr(settings, "PACK_LOCATION_MAX_QUANTITY", 20)
        line_item["adjustable_quantity"] = {"enabled": True, "minimum": 1, "maximum": max_qty}

    try:
        session = stripe.checkout.Session.create(
            api_key=api_key,
            customer=customer_id,
            mode="subscription",
            line_items=[line_item],
            success_url=success_url,
            cancel_url=cancel_url,
            metadata={
                "company_id": str(company_id), "plan_id": plan_id, "billing_period": billing_period,
            },
        )
        return session.url
    except stripe.StripeError as e:
        logger.exception("Stripe checkout session creation failed: %s", e)
        return None


# ---------------------------------------------------------------------------
# Subscription state (local-first, Stripe as fallback)
# ---------------------------------------------------------------------------

def _live_price_display(company: src_models.Company, plan_key: str) -> tuple[str, Optional[str]]:
    """
    One extra live Stripe call to read the real current price/interval off the company's active
    subscription. Needed because each paid plan now has TWO Prices (monthly/annual) and Company
    doesn't locally cache which one a subscription is on — a static settings lookup alone can't
    tell "$40/mo" (annual, effective) from "$49/mo" (monthly). Not a hot path — this only runs on
    a billing-settings page load, unlike check_detail_view_limit. Falls back to the plan's listed
    monthly amount if the live call fails for any reason.
    Returns (price_display, billing_period) where billing_period is "monthly"/"annual"/None.
    """
    entry = _plan_entry(plan_key)
    currency = entry.get("currency", "usd")
    symbol = "€" if currency == "eur" else "$"
    fallback_amount = entry.get("amount_monthly")
    fallback = f"{symbol}{fallback_amount / 100:.2f}/mo" if fallback_amount else "—"

    api_key = _api_key()
    if not api_key or not company.subscription_id:
        return fallback, None

    try:
        sub = stripe.Subscription.retrieve(
            company.subscription_id, expand=["items.data.price"], api_key=api_key,
        )
        items = sub.get("items", {}).get("data", [])
        if not items:
            return fallback, None
        price_obj = items[0].get("price", {})
        unit_amount = price_obj.get("unit_amount")
        interval = (price_obj.get("recurring") or {}).get("interval")
        billing_period = {"month": "monthly", "year": "annual"}.get(interval)
        if unit_amount is None:
            return fallback, billing_period
        price_currency = price_obj.get("currency", currency)
        price_symbol = "€" if price_currency == "eur" else "$"
        suffix = {"month": "/mo", "year": "/yr"}.get(interval, "")
        return f"{price_symbol}{unit_amount / 100:.2f}{suffix}", billing_period
    except stripe.StripeError as e:
        logger.exception("Failed to fetch live price for company %s: %s", company.id, e)
        return fallback, None


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
        plan_key = _plan_key(company)
        price_display, billing_period = (
            _live_price_display(company, plan_key) if plan_key != "scout" else ("Free", None)
        )
        renewal_date = (
            company.subscription_period_end.strftime("%Y-%m-%d")
            if company.subscription_period_end else None
        )
        return {
            "plan_id": company.subscription_plan,
            "plan": _plan_name(plan_key),
            "price": price_display,
            "billing_period": billing_period,
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


def _connected_distributor_count(company_id: int) -> int:
    """Count CompanyProviders rows whose credentials carry a truthy "feed" namespace — the same
    "connected" convention used at integrations.py:282,1553 and parts.py:275,581. Order-only rows
    (feed empty) don't count."""
    creds_list = src_models.CompanyProviders.objects.filter(
        company_id=company_id
    ).values_list("credentials", flat=True)
    return sum(1 for c in creds_list if (c or {}).get("feed"))


def _seat_count(company_id: int) -> int:
    return src_models.UserProfile.objects.filter(company_id=company_id).count()


def get_usage(company_id: int) -> dict:
    """
    Return current billing-period usage vs plan limits: product-view (usage-counted, resets
    monthly), distributor connections and seats (point-in-time counts, not usage-counted — plan
    tier just caps how many can exist, no reset window). Searches are always unlimited.
    """
    company = src_models.Company.objects.filter(id=company_id).first()
    if not company:
        return {}

    plan_key = _plan_key(company)
    features = _plan_features(plan_key)
    period_start = _period_start(company)
    period_end = company.subscription_period_end

    detail_views_used = src_models.PartRequestAudit.objects.filter(
        company_id=company_id,
        action="detail",
        created_at__gte=period_start,
    ).count()

    return {
        "plan_id": company.subscription_plan,
        "plan": _plan_name(plan_key),
        "period_start": period_start.strftime("%Y-%m-%d"),
        "period_end": period_end.strftime("%Y-%m-%d") if period_end else None,
        # Full boolean/limit feature map for the current plan — lets the FE gate UI proactively
        # (e.g. disable "Add to PO" before the click) instead of only reacting to a 402/400 after
        # the fact, without hardcoding its own plan->feature map that would drift from PLANS.
        "features": features,
        "detail_views": {
            "used": detail_views_used,
            "limit": features.get("detail_views_per_month", 0),
        },
        "distributor_connections": {
            "used": _connected_distributor_count(company_id),
            "limit": features.get("distributor_connections", 0),
        },
        "seats": {
            "used": _seat_count(company_id),
            "limit": features.get("seats", 0),
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

    plan_key = _plan_key(company)
    limit = _plan_features(plan_key).get("detail_views_per_month", 0)

    if limit == -1:
        return True, ""

    period_start = _period_start(company)
    used = src_models.PartRequestAudit.objects.filter(
        company_id=company_id,
        action="detail",
        created_at__gte=period_start,
    ).count()

    if used >= limit:
        return False, f"Monthly product view limit reached for {_plan_name(plan_key)} plan ({used}/{limit})"

    return True, ""


def check_distributor_connection_limit(company_id: int) -> tuple[bool, str]:
    """
    Check if company can connect one more distributor's product feed.
    Returns (allowed: bool, reason: str). -1 limit means unlimited.
    """
    company = src_models.Company.objects.filter(id=company_id).only(
        "subscription_plan", "subscription_status",
    ).first()
    if not company:
        return False, "Company not found"

    plan_key = _plan_key(company)
    limit = _plan_features(plan_key).get("distributor_connections", 0)
    if limit == -1:
        return True, ""

    used = _connected_distributor_count(company_id)
    if used >= limit:
        return False, (
            f"Distributor connection limit reached for {_plan_name(plan_key)} plan "
            f"({used}/{limit}). Upgrade to connect more distributors."
        )
    return True, ""


def check_seat_limit(company_id: int) -> tuple[bool, str]:
    """
    Check if company can add one more user account.
    Returns (allowed: bool, reason: str). -1 limit means unlimited.
    """
    company = src_models.Company.objects.filter(id=company_id).only(
        "subscription_plan", "subscription_status",
    ).first()
    if not company:
        return False, "Company not found"

    plan_key = _plan_key(company)
    limit = _plan_features(plan_key).get("seats", 1)
    if limit == -1:
        return True, ""

    used = _seat_count(company_id)
    if used >= limit:
        return False, (
            f"Seat limit reached for {_plan_name(plan_key)} plan ({used}/{limit}). "
            f"Upgrade your plan to add more team members."
        )
    return True, ""


def check_po_checkout_allowed(company_id: int) -> tuple[bool, str]:
    """
    Whether this company's plan allows placing purchase orders in-app at all (Hunter/Pack only).
    Unlike the other checks here, this isn't a usage counter — it's a binary feature gate.
    Returns (allowed: bool, reason: str).
    """
    company = src_models.Company.objects.filter(id=company_id).only(
        "subscription_plan", "subscription_status",
    ).first()
    if not company:
        return False, "Company not found"

    plan_key = _plan_key(company)
    if _plan_features(plan_key).get("po_checkout_allowed", False):
        return True, ""
    return False, (
        f"Purchase order checkout isn't available on the {_plan_name(plan_key)} plan. "
        f"Upgrade to Hunter or Pack to place orders in-app."
    )


def has_multi_warehouse_visibility(company_id: Optional[int]) -> bool:
    """
    Whether this company's plan shows the full per-warehouse stock breakdown
    (warehouse_availability) vs only the aggregate warehouse_total_qty. Scout=False, all paid
    plans=True. Used by parts.get_part_detail's inventory block — a display toggle, never
    raises/blocks, so no (bool, str) tuple like the check_* functions above.
    """
    if company_id is None:
        return False
    company = src_models.Company.objects.filter(id=company_id).only(
        "subscription_plan", "subscription_status",
    ).first()
    if not company:
        return False
    plan_key = _plan_key(company)
    return bool(_plan_features(plan_key).get("multi_warehouse_visibility", False))


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
