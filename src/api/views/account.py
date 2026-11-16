import json
import logging
import typing

import simplejson
from django import http, views
from django.contrib.auth.models import User
from django.views.decorators.csrf import csrf_exempt
from django.utils.decorators import method_decorator

from common import exceptions as common_exceptions
from common import utils as common_utils

from src import models as src_models
from src.api.schemas import account as account_schema
from src.api.services import plan_lifecycle as plan_lifecycle_services

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[ACCOUNT-VIEW]"


@method_decorator(csrf_exempt, name="dispatch")
class UserAccountView(views.View):
    def get(self, request, *args, **kwargs):
        user_id = kwargs.get("user_id")

        if not request.user or not request.user.is_authenticated:
            return http.HttpResponse(
                content=simplejson.dumps({"message": "Permission denied"}),
                status=401
            )

        try:
            user = User.objects.get(id=user_id)
            profile = src_models.UserProfile.objects.get(user=user)
        except User.DoesNotExist:
            return http.HttpResponse(
                content=simplejson.dumps({"message": "User not found"}),
                status=404
            )

        return http.HttpResponse(
            content=simplejson.dumps({
                "message": "User fetched",
                "data": {
                    "id": user.id,
                    "email": user.email,
                    "first_name": user.first_name,
                    "last_name": user.last_name,
                    "company_id": profile.company_id
                }
            }),
            status=200
        )

    def put(self, request, *args, **kwargs):
        user_id = kwargs.get("user_id")

        if not request.user or not request.user.is_authenticated:
            return http.HttpResponse(
                content=simplejson.dumps({"message": "Permission denied"}),
                status=401
            )

        try:
            payload = json.loads(request.body)
            validated = common_utils.validate_data_schema(
                data=payload,
                schema=account_schema.UpdateUserSchema(),
            )
        except common_exceptions.ValidationSchemaException as e:
            return http.HttpResponse(
                content=simplejson.dumps({
                    "message": "Invalid payload",
                    "data": str(e),
                }),
                status=400
            )

        try:
            user = User.objects.get(id=user_id)
            profile = src_models.UserProfile.objects.get(user=user)

            user.email = validated["email"]
            user.first_name = validated["first_name"]
            user.last_name = validated["last_name"]
            user.save()

            profile.company_id = validated.get("company_id", profile.company_id)
            profile.save()

        except User.DoesNotExist:
            return http.HttpResponse(
                content=simplejson.dumps({"message": "User not found"}),
                status=404
            )

        return http.HttpResponse(
            content=simplejson.dumps({"message": "User updated"}),
            status=200
        )

    def delete(self, request, *args, **kwargs):
        user_id = kwargs.get("user_id")

        if not request.user or not request.user.is_authenticated:
            return http.HttpResponse(
                content=simplejson.dumps({"message": "Permission denied"}),
                status=401
            )

        try:
            user = User.objects.get(id=user_id)
            user.delete()
        except User.DoesNotExist:
            return http.HttpResponse(
                content=simplejson.dumps({"message": "User not found"}),
                status=404
            )

        return http.HttpResponse(
            content=simplejson.dumps({"message": "User deleted"}),
            status=200
        )



@method_decorator(csrf_exempt, name="dispatch")
class UserAccountsView(views.View):
    def get(self, request, *args, **kwargs):
        if not request.user or not request.user.is_authenticated:
            return http.HttpResponse(
                content=simplejson.dumps({"message": "Permission denied"}),
                status=401
            )

        all_users = User.objects.all()
        profiles = src_models.UserProfile.objects.all()
        profile_map = {p.user_id: p for p in profiles}

        data = []
        for u in all_users:
            profile = profile_map.get(u.id)
            data.append({
                "id": u.id,
                "email": u.email,
                "first_name": u.first_name,
                "last_name": u.last_name,
                "company_id": profile.company_id if profile else None,
            })

        return http.HttpResponse(
            content=simplejson.dumps({"message": "Users fetched", "data": data}),
            status=200
        )

    def post(self, request, *args, **kwargs):
        if not request.user or not request.user.is_authenticated:
            return http.HttpResponse(
                content=simplejson.dumps({"message": "Permission denied"}),
                status=401
            )

        try:
            payload = json.loads(request.body)
            validated = common_utils.validate_data_schema(
                data=payload,
                schema=account_schema.CreateUserSchema(),
            )
        except common_exceptions.ValidationSchemaException as e:
            return http.HttpResponse(
                content=simplejson.dumps({
                    "message": "Invalid payload",
                    "data": str(e),
                }),
                status=400
            )

        user = User.objects.create_user(
            username=validated["email"],
            email=validated["email"],
            password=validated["password"],
            first_name=validated["first_name"],
            last_name=validated["last_name"],
        )

        src_models.UserProfile.objects.create(
            user=user,
            company_id=validated.get("company_id")
        )

        return http.HttpResponse(
            content=simplejson.dumps({"message": "User created", "data": {"id": user.id}}),
            status=201
        )


def _plan_status_json(data: dict, status: int = 200) -> http.HttpResponse:
    return http.HttpResponse(
        headers={"Content-Type": "application/json"},
        content=simplejson.dumps({"data": data}),
        status=status,
    )


def _error_json(message: str, status: int, **extra: typing.Any) -> http.HttpResponse:
    return http.HttpResponse(
        headers={"Content-Type": "application/json"},
        content=simplejson.dumps({"message": message, **extra}),
        status=status,
    )


def _auth_and_company(
    request: http.HttpRequest,
) -> typing.Tuple[typing.Optional[int], typing.Optional[http.HttpResponse]]:
    """(company_id, None), or (None, error_response) -- 401 unauthenticated, 400 no company in the token."""
    if not request.user or not request.user.is_authenticated:
        return None, _error_json("User not authenticated", 401)
    company_id = getattr(request, "company_id", None)
    if not company_id:
        return None, _error_json("No company found in token", 400)
    return company_id, None


def _lifecycle_error_json(e: plan_lifecycle_services.PlanLifecycleError) -> http.HttpResponse:
    # Never 401 (the frontend logs the user out on it) and never 402 (it opens the global upgrade
    # dialog) -- PlanLifecycleError only carries 400/403/404/409, see its docstring.
    extra = {"error_code": e.error_code}
    if e.upgrade_required:
        extra["upgrade_required"] = True
    return _error_json(e.message, e.status_code, **extra)


class PlanStatusView(views.View):
    """GET /account/plan-status/ - Plan state, limits, usage, per-distributor picker data and the
    banner/modal flags the frontend renders from (see plan_lifecycle.get_plan_status). Any
    authenticated member, including a suspended one."""

    def get(self, request: http.HttpRequest, *args: typing.Any, **kwargs: typing.Any) -> http.HttpResponse:
        company_id, err = _auth_and_company(request)
        if err:
            return err
        data = plan_lifecycle_services.get_plan_status(company_id, request.user)
        if data is None:
            return _error_json("Company not found", 404)
        return _plan_status_json(data)


@method_decorator(csrf_exempt, name="dispatch")
class KeptIntegrationsView(views.View):
    """POST /account/kept-integrations/ - {"ids": [CompanyProviders.id, ...]}. Saves which
    distributors stay active on Scout: stored during the trial, applied (once) after an auto-pick
    expiry. Company admins only; see plan_lifecycle.save_kept_integrations for the error table."""

    def post(self, request: http.HttpRequest, *args: typing.Any, **kwargs: typing.Any) -> http.HttpResponse:
        company_id, err = _auth_and_company(request)
        if err:
            return err
        try:
            body = json.loads(request.body) if request.body else {}
            validated = common_utils.validate_data_schema(data=body, schema=account_schema.KeptIntegrationsSchema())
        except common_exceptions.ValidationSchemaException as e:
            return _error_json("Invalid payload", 400, data=common_utils.get_exception_message(exception=e))
        except json.JSONDecodeError:
            return _error_json("Invalid JSON body", 400)

        try:
            data = plan_lifecycle_services.save_kept_integrations(company_id, request.user, validated["ids"])
        except plan_lifecycle_services.PlanLifecycleError as e:
            return _lifecycle_error_json(e)
        return _plan_status_json(data)


@method_decorator(csrf_exempt, name="dispatch")
class DowngradeAcknowledgeView(views.View):
    """POST /account/downgrade-acknowledge/ - Dismisses the post-expiry modal (idempotent). Company
    admins only; 409 not_downgraded when the account has not been downgraded."""

    def post(self, request: http.HttpRequest, *args: typing.Any, **kwargs: typing.Any) -> http.HttpResponse:
        company_id, err = _auth_and_company(request)
        if err:
            return err
        try:
            data = plan_lifecycle_services.acknowledge_downgrade(company_id, request.user)
        except plan_lifecycle_services.PlanLifecycleError as e:
            return _lifecycle_error_json(e)
        return _plan_status_json(data)
