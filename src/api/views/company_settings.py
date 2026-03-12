"""
Company settings and team management views.
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
from src.api.schemas import company_settings as company_settings_schema
from src.api.services import company_settings as company_settings_services

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[COMPANY-SETTINGS-VIEW]"


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
class ProfileView(views.View):
    """
    GET  /api/settings/profile/ - Get current user's profile
    PUT  /api/settings/profile/ - Update current user's profile (first_name, last_name, email)
    """

    def get(self, request: http.HttpRequest, *args: typing.Any, **kwargs: typing.Any) -> http.HttpResponse:
        if not request.user or not request.user.is_authenticated:
            return _json_response({"message": "User not authenticated"}, status=401)

        data = company_settings_services.get_user_profile(request.user)
        if data is None:
            return _json_response({"message": "Profile not found"}, status=404)
        return _json_response({"data": data})

    def put(self, request: http.HttpRequest, *args: typing.Any, **kwargs: typing.Any) -> http.HttpResponse:
        if not request.user or not request.user.is_authenticated:
            return _json_response({"message": "User not authenticated"}, status=401)

        try:
            body = json.loads(request.body) if request.body else {}
            validated = common_utils.validate_data_schema(
                data=body,
                schema=company_settings_schema.UpdateProfileSchema(),
            )
        except common_exceptions.ValidationSchemaException as e:
            return _json_response(
                {"message": "Invalid payload", "data": common_utils.get_exception_message(exception=e)},
                status=400,
            )
        except json.JSONDecodeError:
            return _json_response({"message": "Invalid JSON body"}, status=400)

        if not any(k in validated for k in ("first_name", "last_name", "email")):
            return _json_response({"message": "No fields to update"}, status=400)

        result = company_settings_services.update_user_profile(
            user=request.user,
            first_name=validated.get("first_name"),
            last_name=validated.get("last_name"),
            email=validated.get("email"),
        )
        if isinstance(result, str):
            return _json_response({"message": result}, status=400)
        return _json_response({"message": "Profile updated", "data": result})


@method_decorator(csrf_exempt, name="dispatch")
class CompanySettingsView(views.View):
    """
    GET  /api/settings/company/ - Get company details (any company user)
    PUT  /api/settings/company/ - Update company (admin only)
    """

    def get(self, request: http.HttpRequest, *args: typing.Any, **kwargs: typing.Any) -> http.HttpResponse:
        company_id, err = _auth_and_company(request)
        if err:
            return _json_response({"message": err}, status=401 if "authenticated" in err else 400)

        data = company_settings_services.get_company_settings(company_id, request.user)
        if data is None:
            return _json_response({"message": "Company not found"}, status=404)
        return _json_response({"data": data})

    def put(self, request: http.HttpRequest, *args: typing.Any, **kwargs: typing.Any) -> http.HttpResponse:
        company_id, err = _auth_and_company(request)
        if err:
            return _json_response({"message": err}, status=401 if "authenticated" in err else 400)

        ok, admin_err = company_settings_services._is_company_admin(request.user, company_id)
        if not ok:
            return _json_response({"message": admin_err or "Admin access required"}, status=403)

        try:
            body = json.loads(request.body) if request.body else {}
            validated = common_utils.validate_data_schema(
                data=body,
                schema=company_settings_schema.UpdateCompanySettingsSchema(),
            )
        except common_exceptions.ValidationSchemaException as e:
            return _json_response(
                {"message": "Invalid payload", "data": common_utils.get_exception_message(exception=e)},
                status=400,
            )
        except json.JSONDecodeError:
            return _json_response({"message": "Invalid JSON body"}, status=400)

        data = company_settings_services.update_company_settings(
            company_id=company_id,
            user=request.user,
            name=validated.get("name"),
            business_type=validated.get("business_type"),
            country=validated.get("country"),
            state_province=validated.get("state_province"),
            tax_id=validated.get("tax_id"),
        )
        if data is None:
            return _json_response({"message": "Company not found"}, status=404)
        return _json_response({"message": "Company updated", "data": data})


@method_decorator(csrf_exempt, name="dispatch")
class CompanyTeamView(views.View):
    """
    GET  /api/settings/company/team/ - List company users (any company user)
    POST /api/settings/company/team/ - Add user to company (admin only)
    """

    def get(self, request: http.HttpRequest, *args: typing.Any, **kwargs: typing.Any) -> http.HttpResponse:
        company_id, err = _auth_and_company(request)
        if err:
            return _json_response({"message": err}, status=401 if "authenticated" in err else 400)

        data = company_settings_services.list_company_users(company_id, request.user)
        if data is None:
            return _json_response({"message": "Company not found"}, status=404)
        return _json_response({"data": data})

    def post(self, request: http.HttpRequest, *args: typing.Any, **kwargs: typing.Any) -> http.HttpResponse:
        company_id, err = _auth_and_company(request)
        if err:
            return _json_response({"message": err}, status=401 if "authenticated" in err else 400)

        ok, admin_err = company_settings_services._is_company_admin(request.user, company_id)
        if not ok:
            return _json_response({"message": admin_err or "Admin access required"}, status=403)

        try:
            body = json.loads(request.body) if request.body else {}
            validated = common_utils.validate_data_schema(
                data=body,
                schema=company_settings_schema.AddCompanyUserSchema(),
            )
        except common_exceptions.ValidationSchemaException as e:
            return _json_response(
                {"message": "Invalid payload", "data": common_utils.get_exception_message(exception=e)},
                status=400,
            )
        except json.JSONDecodeError:
            return _json_response({"message": "Invalid JSON body"}, status=400)

        result = company_settings_services.add_company_user(
            company_id=company_id,
            admin_user=request.user,
            email=validated["email"],
            first_name=validated["first_name"],
            last_name=validated["last_name"],
            password=validated["password"],
            is_company_admin=validated.get("is_company_admin", False),
        )
        if isinstance(result, str):
            return _json_response({"message": result}, status=400)
        return _json_response({"message": "User added", "data": result}, status=201)


@method_decorator(csrf_exempt, name="dispatch")
class CompanyTeamMemberView(views.View):
    """
    PATCH /api/settings/company/team/<user_id>/ - Update user role (admin only)
    DELETE /api/settings/company/team/<user_id>/ - Remove user from company (admin only)
    """

    def patch(self, request: http.HttpRequest, *args: typing.Any, **kwargs: typing.Any) -> http.HttpResponse:
        company_id, err = _auth_and_company(request)
        if err:
            return _json_response({"message": err}, status=401 if "authenticated" in err else 400)

        ok, admin_err = company_settings_services._is_company_admin(request.user, company_id)
        if not ok:
            return _json_response({"message": admin_err or "Admin access required"}, status=403)

        user_id = kwargs.get("user_id")
        if not user_id:
            return _json_response({"message": "User ID required"}, status=400)
        try:
            user_id = int(user_id)
        except (ValueError, TypeError):
            return _json_response({"message": "Invalid user ID"}, status=400)

        try:
            body = json.loads(request.body) if request.body else {}
            validated = common_utils.validate_data_schema(
                data=body,
                schema=company_settings_schema.UpdateCompanyUserRoleSchema(),
            )
        except common_exceptions.ValidationSchemaException as e:
            return _json_response(
                {"message": "Invalid payload", "data": common_utils.get_exception_message(exception=e)},
                status=400,
            )
        except json.JSONDecodeError:
            return _json_response({"message": "Invalid JSON body"}, status=400)

        result = company_settings_services.update_company_user_role(
            company_id=company_id,
            target_user_id=user_id,
            admin_user=request.user,
            is_company_admin=validated["is_company_admin"],
        )
        if isinstance(result, str):
            return _json_response({"message": result}, status=400)
        return _json_response({"message": "User role updated", "data": result})

    def delete(self, request: http.HttpRequest, *args: typing.Any, **kwargs: typing.Any) -> http.HttpResponse:
        company_id, err = _auth_and_company(request)
        if err:
            return _json_response({"message": err}, status=401 if "authenticated" in err else 400)

        ok, admin_err = company_settings_services._is_company_admin(request.user, company_id)
        if not ok:
            return _json_response({"message": admin_err or "Admin access required"}, status=403)

        user_id = kwargs.get("user_id")
        if not user_id:
            return _json_response({"message": "User ID required"}, status=400)
        try:
            user_id = int(user_id)
        except (ValueError, TypeError):
            return _json_response({"message": "Invalid user ID"}, status=400)

        err = company_settings_services.remove_company_user(
            company_id=company_id,
            target_user_id=user_id,
            admin_user=request.user,
        )
        if err:
            return _json_response({"message": err}, status=400)
        return _json_response({"message": "User removed from company"})
