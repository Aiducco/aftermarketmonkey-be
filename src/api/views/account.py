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
