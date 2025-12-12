import logging
import jwt
import datetime

from django import http
from django.conf import settings
from django.contrib.auth import models as auth_models
from django.contrib.auth import authenticate
from django.contrib.auth.hashers import check_password


from common import utils as common_utils
from src.authentication import exceptions as authentication_exceptions
from src import models as src_models
from src import enums as src_enums


_LOG_PREFIX = "[AUTHENTICATION-SERVICE]"

logger = logging.getLogger(__name__)

def decode_jwt_token(token: str) -> dict:
    try:
        return jwt.decode(
            jwt=token,
            key=settings.JWT_SECRET,
            algorithms=["HS256"],
        )
    except jwt.PyJWTError as e:
        msg = "Unable to decode JWT token (token={}). Error: {}".format(
            token, common_utils.get_exception_message(exception=e)
        )
        logger.exception("{} {}.".format(_LOG_PREFIX, msg))
        raise authentication_exceptions.InvalidJWTTokenError(msg)


def create_jwt_token(user: auth_models.User) -> dict:
    return jwt.encode(
        payload={
            "user_id": user.id,
            "email": user.email,
            "first_name": user.first_name,
            "last_name": user.last_name,
            "company_id": user.profile.company_id,
            "exp": datetime.datetime.utcnow() + datetime.timedelta(hours=12),
        },
        key=settings.JWT_SECRET,
        algorithm="HS256",
    )


def login_user(email: str, password: str) -> dict:
    try:
        user = authenticate(
            username=email,
            password=password,
        )
    except Exception as e:
        msg = "Unable to authenticate user (email={}). Error: {}".format(
            email, common_utils.get_exception_message(exception=e)
        )
        logger.exception("{} {}.".format(_LOG_PREFIX, msg))
        raise e
    return {"user_id": user.id, "access_token": create_jwt_token(user=user)}


def user_change_password(user: auth_models.User, current_password: str, new_password: str) -> None:
    if not check_password(current_password, user.password):
        raise authentication_exceptions.UserCurrentPasswordDoesNotMatchError(
            "User (id={}) current password does not match".format(user.id)
        )

    user.set_password(raw_password=new_password)
    user.save(update_fields=["password"])


def logout_user(user_token: str) -> None:
    return
