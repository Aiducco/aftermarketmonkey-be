"""
Billing views for Stripe subscriptions, portal, checkout, webhook, and usage.
"""
import json
import logging
import typing

import simplejson
from django import http, views
from django.conf import settings
from django.views.decorators.csrf import csrf_exempt
from django.utils.decorators import method_decorator

from common import exceptions as common_exceptions
from common import utils as common_utils

from src.api.schemas import billing as billing_schema
from src.api.services import billing as billing_services

logger = logging.getLogger(__name__)


def _json_response(data: dict, status: int = 200) -> http.HttpResponse:
    return http.HttpResponse(
        headers={"Content-Type": "application/json"},
        content=simplejson.dumps(data),
        status=status,
    )


def _auth_and_company(request) -> tuple[int | None, str | None]:
    """Returns (company_id, error_message)."""
    if not request.user or not request.user.is_authenticated:
        return None, "User not authenticated"
    company_id = getattr(request, "company_id", None)
    if not company_id:
        return None, "No company found in token"
    return company_id, None


@method_decorator(csrf_exempt, name="dispatch")
class SubscriptionView(views.View):
    """
    GET /api/billing/subscription/
    Returns current subscription (plan, price, renewal_date, status) or null if none.
    """

    def get(self, request: http.HttpRequest, *args: typing.Any, **kwargs: typing.Any) -> http.HttpResponse:
        company_id, err = _auth_and_company(request)
        if err:
            return _json_response({"message": err}, status=401 if "authenticated" in err else 400)

        subscription = billing_services.get_subscription(company_id)
        return _json_response({"subscription": subscription})


@method_decorator(csrf_exempt, name="dispatch")
class UsageView(views.View):
    """
    GET /api/billing/usage/
    Returns current billing period usage counts and plan limits.
    Response:
    {
      "plan_id": "starter",
      "plan": "Starter",
      "period_start": "2026-05-15",
      "period_end": "2026-06-15",
      "searches": { "used": 42, "limit": 500 },
      "detail_views": { "used": 12, "limit": 1000 }
    }
    """

    def get(self, request: http.HttpRequest, *args: typing.Any, **kwargs: typing.Any) -> http.HttpResponse:
        company_id, err = _auth_and_company(request)
        if err:
            return _json_response({"message": err}, status=401 if "authenticated" in err else 400)

        usage = billing_services.get_usage(company_id)
        return _json_response({"usage": usage})


@method_decorator(csrf_exempt, name="dispatch")
class CreatePortalSessionView(views.View):
    """
    POST /api/billing/create-portal-session/
    Creates a Stripe Billing Portal session and returns the redirect URL.
    Handles plan upgrades, downgrades, cancellation, and payment method updates.
    Request body: { "return_url": "https://yourapp.com/settings" }
    Response: { "url": "https://billing.stripe.com/session/xxx" }
    """

    def post(self, request: http.HttpRequest, *args: typing.Any, **kwargs: typing.Any) -> http.HttpResponse:
        company_id, err = _auth_and_company(request)
        if err:
            return _json_response({"message": err}, status=401 if "authenticated" in err else 400)

        try:
            body = json.loads(request.body) if request.body else {}
            validated = common_utils.validate_data_schema(
                data=body,
                schema=billing_schema.CreatePortalSessionSchema(),
            )
        except common_exceptions.ValidationSchemaException as e:
            return _json_response(
                {"message": "Invalid payload", "data": common_utils.get_exception_message(exception=e)},
                status=400,
            )
        except json.JSONDecodeError:
            return _json_response({"message": "Invalid JSON body"}, status=400)

        return_url = validated.get("return_url")
        if not return_url:
            return_url = getattr(settings, "BILLING_PORTAL_RETURN_URL", None)
        if not return_url:
            return _json_response(
                {"message": "return_url is required for billing portal"},
                status=400,
            )

        customer_email = (request.user.email or "").strip()
        if not customer_email:
            return _json_response(
                {"message": "User email is required for billing portal"},
                status=400,
            )

        url = billing_services.create_portal_session(
            company_id=company_id,
            return_url=return_url,
            customer_email=customer_email,
            customer_name=f"{request.user.first_name or ''} {request.user.last_name or ''}".strip() or customer_email,
        )
        if not url:
            return _json_response(
                {"message": "Failed to create billing portal session"},
                status=500,
            )
        return _json_response({"url": url})


@method_decorator(csrf_exempt, name="dispatch")
class CreateCheckoutSessionView(views.View):
    """
    POST /api/billing/create-checkout-session/
    Creates a Stripe Checkout session for new plan subscriptions.
    Use the Billing Portal (create-portal-session) for upgrade/downgrade of existing subscriptions.
    Request body: { "plan_id": "starter"|"pro"|"growth", "success_url": "...", "cancel_url": "..." }
    Response: { "url": "https://checkout.stripe.com/c/pay/cs_xxx" }
    """

    def post(self, request: http.HttpRequest, *args: typing.Any, **kwargs: typing.Any) -> http.HttpResponse:
        company_id, err = _auth_and_company(request)
        if err:
            return _json_response({"message": err}, status=401 if "authenticated" in err else 400)

        try:
            body = json.loads(request.body) if request.body else {}
            validated = common_utils.validate_data_schema(
                data=body,
                schema=billing_schema.CreateCheckoutSessionSchema(),
            )
        except common_exceptions.ValidationSchemaException as e:
            return _json_response(
                {"message": "Invalid payload", "data": common_utils.get_exception_message(exception=e)},
                status=400,
            )
        except json.JSONDecodeError:
            return _json_response({"message": "Invalid JSON body"}, status=400)

        customer_email = (request.user.email or "").strip()
        if not customer_email:
            return _json_response(
                {"message": "User email is required for checkout"},
                status=400,
            )

        url = billing_services.create_checkout_session(
            company_id=company_id,
            plan_id=validated["plan_id"],
            success_url=validated["success_url"],
            cancel_url=validated["cancel_url"],
            customer_email=customer_email,
            customer_name=f"{request.user.first_name or ''} {request.user.last_name or ''}".strip() or customer_email,
        )
        if not url:
            return _json_response(
                {"message": "Failed to create checkout session"},
                status=500,
            )
        return _json_response({"url": url})


@method_decorator(csrf_exempt, name="dispatch")
class WebhookView(views.View):
    """
    POST /api/billing/webhook/
    Receives Stripe webhook events and syncs subscription state to the DB.
    Must be registered in Stripe dashboard with the signing secret set in STRIPE_WEBHOOK_SECRET.

    Required events:
      - checkout.session.completed
      - customer.subscription.created
      - customer.subscription.updated
      - customer.subscription.deleted
      - invoice.payment_succeeded
      - invoice.payment_failed
    """

    def post(self, request: http.HttpRequest, *args: typing.Any, **kwargs: typing.Any) -> http.HttpResponse:
        sig_header = request.META.get("HTTP_STRIPE_SIGNATURE", "")
        if not sig_header:
            return _json_response({"message": "Missing Stripe-Signature header"}, status=400)

        success, message = billing_services.handle_webhook_event(
            payload=request.body,
            sig_header=sig_header,
        )
        if not success:
            status_code = 400 if "signature" in message.lower() or "secret" in message.lower() else 500
            return _json_response({"message": message}, status=status_code)

        return _json_response({"received": True})
