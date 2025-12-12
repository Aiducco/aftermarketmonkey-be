import json
import logging
import typing

import simplejson
from django import http, views
from django.views.decorators.csrf import csrf_exempt
from django.utils.decorators import method_decorator


from common import exceptions as common_exceptions
from common import utils as common_utils
from src.api.schemas import authentication as authentication_schema
from src.authentication import exceptions as authentication_exceptions
from src.authentication import services as authentication_services

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[AUTHENTICATION-VIEW]"


@method_decorator(csrf_exempt, name="dispatch")
class LoginView(views.View):
    def post(self, request: http.HttpRequest, *args: typing.Any, **kwargs: typing.Any) -> http.HttpResponse:
        logger.info(f"{_LOG_PREFIX} Received login request")
        logger.info(f"{_LOG_PREFIX} Headers: {dict(request.headers)}")
        logger.info(f"{_LOG_PREFIX} Body raw: {request.body.decode(errors='ignore')}")
        logger.info(f"{_LOG_PREFIX} Method: {request.method}")
        logger.info(f"{_LOG_PREFIX} Content-Type: {request.headers.get('Content-Type')}")

        try:
            # Validate the query parameters
            validated_data = common_utils.validate_data_schema(
                data=json.loads(request.body),
                schema=authentication_schema.LoginSchema(),
            )
        except common_exceptions.ValidationSchemaException as e:
            logger.exception(
                "{} Unable to validate login payload params. Error: {}.".format(
                    _LOG_PREFIX,
                    common_utils.get_exception_message(exception=e),
                )
            )
            return http.HttpResponse(
                headers={"Content-Type": "application/json"},
                content=simplejson.dumps(
                    {
                        "message": "Invalid payload parameters",
                        "data": common_utils.get_exception_message(exception=e),
                    }
                ),
                status=400,
            )

        logger.info(
            "{} Login payload data validated (email={}). Logging user in.".format(_LOG_PREFIX, validated_data["email"])
        )

        try:
            logged_in_response = authentication_services.login_user(
                email=validated_data["email"], password=validated_data["password"]
            )
        except authentication_exceptions.AuthenticationServiceException as e:
            return http.HttpResponse(
                headers={"Content-Type": "application/json"},
                content=simplejson.dumps({"message": common_utils.get_exception_message(exception=e)}),
                status=404,
            )
        except Exception as e:
            logger.exception(
                "{} Unexpected exception while logging user (email={}). Error: {}.".format(
                    _LOG_PREFIX,
                    validated_data["email"],
                    common_utils.get_exception_message(exception=e),
                )
            )
            return http.HttpResponse(
                headers={"Content-Type": "application/json"},
                content=simplejson.dumps({"message": "Internal server error"}),
                status=500,
            )

        logger.info("{} Logged user (email={}) in.".format(_LOG_PREFIX, validated_data["email"]))

        return http.HttpResponse(
            headers={"Content-Type": "application/json"},
            content=simplejson.dumps(
                {
                    "message": "User logged in",
                    "data": logged_in_response,
                }
            ),
            status=201,
        )


class ChangePasswordView(views.View):
    def post(self, request: http.HttpRequest, *args: typing.Any, **kwargs: typing.Any) -> http.HttpResponse:

        if not request.user or not request.user.is_authenticated:
            return http.HttpResponse(
                headers={"Content-Type": "application/json"},
                content=simplejson.dumps({"message": "User not authenticated"}),
                status=401,
            )

        logger.info("{} User (id={}) changing password.".format(_LOG_PREFIX, request.user.id))

        try:
            # Validate the query parameters
            validated_data = common_utils.validate_data_schema(
                data=json.loads(request.body),
                schema=authentication_schema.ChangePasswordSchema(),
            )
        except common_exceptions.ValidationSchemaException as e:
            logger.exception(
                "{} Unable to validate change password payload params. Error: {}.".format(
                    _LOG_PREFIX,
                    common_utils.get_exception_message(exception=e),
                )
            )
            return http.HttpResponse(
                headers={"Content-Type": "application/json"},
                content=simplejson.dumps(
                    {
                        "message": "Invalid payload parameters",
                        "data": common_utils.get_exception_message(exception=e),
                    }
                ),
                status=400,
            )

        try:
            authentication_services.user_change_password(
                user=request.user,
                current_password=validated_data["current_password"],
                new_password=validated_data["new_password"],
            )
        except authentication_exceptions.AuthenticationServiceException as e:
            return http.HttpResponse(
                headers={"Content-Type": "application/json"},
                content=simplejson.dumps({"message": common_utils.get_exception_message(exception=e)}),
                status=404,
            )
        except Exception as e:
            logger.exception(
                "{} Unexpected exception while changing user password (user_id={}). Error: {}.".format(
                    _LOG_PREFIX,
                    request.user.id,
                    common_utils.get_exception_message(exception=e),
                )
            )
            return http.HttpResponse(
                headers={"Content-Type": "application/json"},
                content=simplejson.dumps({"message": "Internal server error"}),
                status=500,
            )

        logger.info("{} Changed user password (user_id={}) in.".format(_LOG_PREFIX, request.user.id))

        return http.HttpResponse(
            headers={"Content-Type": "application/json"},
            content=simplejson.dumps({"message": "Password changed successfully"}),
            status=201,
        )


class LogoutView(views.View):
    def post(self, request: http.HttpRequest, *args: typing.Any, **kwargs: typing.Any) -> http.HttpResponse:
        if not request.user or not request.user.is_authenticated:
            return http.HttpResponse(
                headers={"Content-Type": "application/json"},
                content=simplejson.dumps({"message": "User not authenticated"}),
                status=401,
            )

        logger.info("{} User (id={}) logging out.".format(_LOG_PREFIX, request.user.id))

        try:
            authentication_services.logout_user(user_token=request.headers.get("Authorization").split()[1])
        except authentication_exceptions.AuthenticationServiceException as e:
            return http.HttpResponse(
                headers={"Content-Type": "application/json"},
                content=simplejson.dumps({"message": common_utils.get_exception_message(exception=e)}),
                status=404,
            )
        except Exception as e:
            logger.exception(
                "{} Unexpected exception while logging user out (user_id={}). Error: {}.".format(
                    _LOG_PREFIX,
                    request.user.id,
                    common_utils.get_exception_message(exception=e),
                )
            )
            return http.HttpResponse(
                headers={"Content-Type": "application/json"},
                content=simplejson.dumps({"message": "Internal server error"}),
                status=500,
            )

        logger.info("{} Logged out user (user_id={}) in.".format(_LOG_PREFIX, request.user.id))

        return http.HttpResponse(
            headers={"Content-Type": "application/json"},
            content=simplejson.dumps({"message": "User logged out"}),
            status=201,
        )
