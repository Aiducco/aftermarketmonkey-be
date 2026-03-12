"""
Onboarding flow API views.
"""
import json
import logging
import typing

import simplejson
from django import http, views
from django.views.decorators.csrf import csrf_exempt
from django.utils.decorators import method_decorator

from common import exceptions as common_exceptions
from common import utils as common_utils

from src import models as src_models
from src.api.schemas.onboarding import (
    CompanyDetailsSchemaAllowAny,
    PersonalizationSchema,
    RegisterSchema,
)
from src.api.services import onboarding as onboarding_services

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[ONBOARDING-VIEW]"


def _json_response(data: dict, status: int = 200) -> http.HttpResponse:
    return http.HttpResponse(
        headers={"Content-Type": "application/json"},
        content=simplejson.dumps(data),
        status=status,
    )


@method_decorator(csrf_exempt, name="dispatch")
class RegisterView(views.View):
    """
    POST /onboarding/register/
    Step 1+2 (atomic): Create account + company in one request.
    No partial state - user is never created without company details.
    No auth required.
    """

    def post(self, request: http.HttpRequest, *args: typing.Any, **kwargs: typing.Any) -> http.HttpResponse:
        try:
            body = json.loads(request.body) if request.body else {}
            validated = common_utils.validate_data_schema(data=body, schema=RegisterSchema())
        except common_exceptions.ValidationSchemaException as e:
            return _json_response(
                {"message": "Invalid payload", "data": common_utils.get_exception_message(exception=e)},
                status=400,
            )
        except json.JSONDecodeError:
            return _json_response({"message": "Invalid JSON body"}, status=400)

        try:
            result = onboarding_services.register_user(
                first_name=validated["first_name"],
                last_name=validated["last_name"],
                email=validated["email"],
                password=validated["password"],
                company_name=validated["company_name"],
                business_type=validated.get("business_type"),
                country=validated.get("country"),
                state_province=validated.get("state_province"),
                tax_id=validated.get("tax_id"),
            )
        except ValueError as e:
            return _json_response({"message": str(e)}, status=400)
        except Exception as e:
            logger.exception("%s Register error: %s", _LOG_PREFIX, str(e))
            return _json_response({"message": "Registration failed"}, status=500)

        return _json_response({"message": "Account created", "data": result}, status=201)


@method_decorator(csrf_exempt, name="dispatch")
class CompanyDetailsView(views.View):
    """
    POST /onboarding/company-details/
    Step 2: Update company details. Requires auth.
    """

    def post(self, request: http.HttpRequest, *args: typing.Any, **kwargs: typing.Any) -> http.HttpResponse:
        if not request.user or not request.user.is_authenticated:
            return _json_response({"message": "User not authenticated"}, status=401)

        company_id = getattr(request, "company_id", None)
        if not company_id:
            return _json_response({"message": "No company found in token"}, status=400)

        try:
            body = json.loads(request.body) if request.body else {}
            validated = common_utils.validate_data_schema(data=body, schema=CompanyDetailsSchemaAllowAny())
        except common_exceptions.ValidationSchemaException as e:
            return _json_response(
                {"message": "Invalid payload", "data": common_utils.get_exception_message(exception=e)},
                status=400,
            )
        except json.JSONDecodeError:
            return _json_response({"message": "Invalid JSON body"}, status=400)

        try:
            result = onboarding_services.update_company_details(
                company_id=company_id,
                company_name=validated["company_name"],
                business_type=validated.get("business_type"),
                country=validated.get("country"),
                state_province=validated.get("state_province"),
                tax_id=validated.get("tax_id"),
            )
        except src_models.Company.DoesNotExist:
            return _json_response({"message": "Company not found"}, status=404)
        except Exception as e:
            logger.exception("%s Company details error: %s", _LOG_PREFIX, str(e))
            return _json_response({"message": "Failed to update company details"}, status=500)

        return _json_response({"message": "Company details updated", "data": result}, status=200)


@method_decorator(csrf_exempt, name="dispatch")
class PersonalizationView(views.View):
    """
    POST /onboarding/personalization/
    Step 3: Save preferences and optional distributor credentials. Requires auth.
    """

    def post(self, request: http.HttpRequest, *args: typing.Any, **kwargs: typing.Any) -> http.HttpResponse:
        if not request.user or not request.user.is_authenticated:
            return _json_response({"message": "User not authenticated"}, status=401)

        company_id = getattr(request, "company_id", None)
        if not company_id:
            return _json_response({"message": "No company found in token"}, status=400)

        try:
            body = json.loads(request.body) if request.body else {}
            validated = common_utils.validate_data_schema(data=body, schema=PersonalizationSchema())
        except common_exceptions.ValidationSchemaException as e:
            return _json_response(
                {"message": "Invalid payload", "data": common_utils.get_exception_message(exception=e)},
                status=400,
            )
        except json.JSONDecodeError:
            return _json_response({"message": "Invalid JSON body"}, status=400)

        try:
            result = onboarding_services.save_personalization(
                company_id=company_id,
                preferred_distributor_ids=validated.get("preferred_distributor_ids"),
                top_categories=validated.get("top_categories"),
                distributor_credentials=validated.get("distributor_credentials"),
            )
        except src_models.Company.DoesNotExist:
            return _json_response({"message": "Company not found"}, status=404)
        except Exception as e:
            logger.exception("%s Personalization error: %s", _LOG_PREFIX, str(e))
            return _json_response({"message": "Failed to save personalization"}, status=500)

        return _json_response({"message": "Personalization saved", "data": result}, status=200)


@method_decorator(csrf_exempt, name="dispatch")
class OnboardingStatusView(views.View):
    """
    GET /onboarding/status/
    Returns current onboarding step and available options. Requires auth.
    """

    def get(self, request: http.HttpRequest, *args: typing.Any, **kwargs: typing.Any) -> http.HttpResponse:
        if not request.user or not request.user.is_authenticated:
            return _json_response({"message": "User not authenticated"}, status=401)

        company_id = getattr(request, "company_id", None)
        if not company_id:
            return _json_response({"message": "No company found in token"}, status=400)

        try:
            data = onboarding_services.get_onboarding_status(company_id=company_id)
        except Exception as e:
            logger.exception("%s Status error: %s", _LOG_PREFIX, str(e))
            return _json_response({"message": "Failed to get status"}, status=500)

        return _json_response({"data": data}, status=200)


@method_decorator(csrf_exempt, name="dispatch")
class DistributorCredentialsInfoView(views.View):
    """
    GET /onboarding/distributor-credentials-info/
    Returns what credentials each distributor needs. No auth required.
    """

    def get(self, request: http.HttpRequest, *args: typing.Any, **kwargs: typing.Any) -> http.HttpResponse:
        data = onboarding_services.get_distributor_credentials_info()
        return _json_response({"data": data}, status=200)
