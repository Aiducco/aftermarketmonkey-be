import json
import logging
import typing

import simplejson
from django import http, views
from django.views.decorators.csrf import csrf_exempt
from django.utils.decorators import method_decorator

from common import exceptions as common_exceptions
from common import utils as common_utils
from src.api.schemas import admin as admin_schema
from src.api.services import admin as admin_services

logger = logging.getLogger(__name__)
_LOG_PREFIX = "[ADMIN]"


def _json_response(data: dict, status: int = 200) -> http.HttpResponse:
    return http.HttpResponse(
        headers={"Content-Type": "application/json"},
        content=simplejson.dumps(data),
        status=status,
    )


def _parse_body(request: http.HttpRequest, schema) -> typing.Tuple[typing.Optional[dict], typing.Optional[http.HttpResponse]]:
    """Shared JSON-body + marshmallow-schema parsing for the POST admin views below. Returns
    (validated_data, None) on success, or (None, error_response) to short-circuit the view."""
    try:
        body = json.loads(request.body) if request.body else {}
        validated = common_utils.validate_data_schema(data=body, schema=schema)
    except common_exceptions.ValidationSchemaException as e:
        return None, _json_response(
            {"message": "Invalid payload", "data": common_utils.get_exception_message(exception=e)}, status=400
        )
    except json.JSONDecodeError:
        return None, _json_response({"message": "Invalid JSON body"}, status=400)
    return validated, None


def _require_staff(request: http.HttpRequest) -> typing.Optional[http.HttpResponse]:
    """
    Shared guard for every staff-only admin view below. `request.user` is always a freshly
    fetched DB row (see JWTAuthenticationMiddleware) — `is_staff` is checked against the live
    value, never trusted from the JWT claim alone. Returns an error response to short-circuit the
    view, or None when the request may proceed.
    """
    if not request.user or not request.user.is_authenticated:
        logger.warning("{} User not authenticated for {}".format(_LOG_PREFIX, request.path))
        return http.HttpResponse(
            headers={"Content-Type": "application/json"},
            content=simplejson.dumps({"message": "User not authenticated"}),
            status=401,
        )
    if not request.user.is_staff:
        logger.warning(
            "{} User (id={}) is not staff, denying {}".format(_LOG_PREFIX, request.user.id, request.path)
        )
        return http.HttpResponse(
            headers={"Content-Type": "application/json"},
            content=simplejson.dumps({"message": "Staff access required"}),
            status=403,
        )
    return None


class AdminCompaniesView(views.View):
    """GET /admin/companies/ - Every company (excluding the internal admin company) with subscription state and provider-connection counts. Staff only."""

    def get(self, request: http.HttpRequest, *args: typing.Any, **kwargs: typing.Any) -> http.HttpResponse:
        denied = _require_staff(request)
        if denied:
            return denied

        try:
            data = admin_services.list_all_companies_for_admin()
        except Exception as e:
            logger.error("{} Error fetching companies for admin panel. Error: {}".format(_LOG_PREFIX, str(e)))
            return http.HttpResponse(
                headers={"Content-Type": "application/json"},
                content=simplejson.dumps({"message": "Error fetching companies"}),
                status=500,
            )

        return http.HttpResponse(
            headers={"Content-Type": "application/json"},
            content=simplejson.dumps({"data": data}),
            status=200,
        )


class AdminCompanyDetailView(views.View):
    """GET /admin/companies/<id>/ - One company's summary plus every CompanyProviders row for it. Staff only."""

    def get(self, request: http.HttpRequest, id: int, *args: typing.Any, **kwargs: typing.Any) -> http.HttpResponse:
        denied = _require_staff(request)
        if denied:
            return denied

        try:
            data = admin_services.get_admin_company_detail(company_id=id)
        except Exception as e:
            logger.error(
                "{} Error fetching company detail for company_id: {}. Error: {}".format(_LOG_PREFIX, id, str(e))
            )
            return http.HttpResponse(
                headers={"Content-Type": "application/json"},
                content=simplejson.dumps({"message": "Error fetching company"}),
                status=500,
            )

        if not data:
            return http.HttpResponse(
                headers={"Content-Type": "application/json"},
                content=simplejson.dumps({"message": "Company not found"}),
                status=404,
            )

        return http.HttpResponse(
            headers={"Content-Type": "application/json"},
            content=simplejson.dumps({"data": data}),
            status=200,
        )


@method_decorator(csrf_exempt, name="dispatch")
class AdminCreateCompanyView(views.View):
    """POST /admin/companies/create/ - Create a new company plus its first (admin) user. Staff only."""

    def post(self, request: http.HttpRequest, *args: typing.Any, **kwargs: typing.Any) -> http.HttpResponse:
        denied = _require_staff(request)
        if denied:
            return denied

        validated, err_response = _parse_body(request, admin_schema.AdminCreateCompanySchema())
        if err_response:
            return err_response

        try:
            data = admin_services.create_company_for_admin(
                company_name=validated["company_name"],
                email=validated["email"],
                first_name=validated["first_name"],
                last_name=validated["last_name"],
                password=validated.get("password"),
                trial_days=validated.get("trial_days"),
                subscription_plan=validated.get("subscription_plan"),
            )
        except ValueError as e:
            return _json_response({"message": str(e)}, status=400)
        except Exception as e:
            logger.error("{} Error creating company via admin panel. Error: {}".format(_LOG_PREFIX, str(e)))
            return _json_response({"message": "Error creating company"}, status=500)

        return _json_response({"data": data}, status=201)


@method_decorator(csrf_exempt, name="dispatch")
class AdminResetUserPasswordView(views.View):
    """POST /admin/companies/<company_id>/users/<user_id>/reset-password/ - Reset a user's
    password as platform staff (no same-company requirement on the acting user, unlike the
    company-admin-scoped /api/settings/company/team/<user_id>/reset-password/). Staff only."""

    def post(
        self, request: http.HttpRequest, company_id: int, user_id: int, *args: typing.Any, **kwargs: typing.Any
    ) -> http.HttpResponse:
        denied = _require_staff(request)
        if denied:
            return denied

        validated, err_response = _parse_body(request, admin_schema.AdminResetUserPasswordSchema())
        if err_response:
            return err_response

        data, err = admin_services.admin_reset_user_password(
            company_id=company_id, user_id=user_id, new_password=validated.get("new_password")
        )
        if err:
            return _json_response({"message": err}, status=404)

        return _json_response({"data": data}, status=200)


@method_decorator(csrf_exempt, name="dispatch")
class AdminExtendTrialView(views.View):
    """POST /admin/companies/<company_id>/extend-trial/ - Extend (or (re)grant) a company's
    comp'd trial by N days. Staff only."""

    def post(self, request: http.HttpRequest, company_id: int, *args: typing.Any, **kwargs: typing.Any) -> http.HttpResponse:
        denied = _require_staff(request)
        if denied:
            return denied

        validated, err_response = _parse_body(request, admin_schema.AdminExtendTrialSchema())
        if err_response:
            return err_response

        data, err = admin_services.extend_company_trial(
            company_id=company_id, days=validated["days"], plan=validated.get("plan")
        )
        if err:
            return _json_response({"message": err}, status=404)

        return _json_response({"data": data}, status=200)


class AdminProvidersView(views.View):
    """GET /admin/providers/ - Every provider with a count of companies connected to it. Staff only."""

    def get(self, request: http.HttpRequest, *args: typing.Any, **kwargs: typing.Any) -> http.HttpResponse:
        denied = _require_staff(request)
        if denied:
            return denied

        try:
            data = admin_services.list_all_providers_for_admin()
        except Exception as e:
            logger.error("{} Error fetching providers for admin panel. Error: {}".format(_LOG_PREFIX, str(e)))
            return http.HttpResponse(
                headers={"Content-Type": "application/json"},
                content=simplejson.dumps({"message": "Error fetching providers"}),
                status=500,
            )

        return http.HttpResponse(
            headers={"Content-Type": "application/json"},
            content=simplejson.dumps({"data": data}),
            status=200,
        )


class AdminProviderDetailView(views.View):
    """GET /admin/providers/<id>/ - One provider's summary plus every company connected to it. Staff only."""

    def get(self, request: http.HttpRequest, id: int, *args: typing.Any, **kwargs: typing.Any) -> http.HttpResponse:
        denied = _require_staff(request)
        if denied:
            return denied

        try:
            data = admin_services.get_admin_provider_detail(provider_id=id)
        except Exception as e:
            logger.error(
                "{} Error fetching provider detail for provider_id: {}. Error: {}".format(_LOG_PREFIX, id, str(e))
            )
            return http.HttpResponse(
                headers={"Content-Type": "application/json"},
                content=simplejson.dumps({"message": "Error fetching provider"}),
                status=500,
            )

        if not data:
            return http.HttpResponse(
                headers={"Content-Type": "application/json"},
                content=simplejson.dumps({"message": "Provider not found"}),
                status=404,
            )

        return http.HttpResponse(
            headers={"Content-Type": "application/json"},
            content=simplejson.dumps({"data": data}),
            status=200,
        )
