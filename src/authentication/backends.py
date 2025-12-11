from django.contrib.auth.backends import ModelBackend
from django.contrib.auth.models import User
from django.core.exceptions import ObjectDoesNotExist

from src.authentication import exceptions as authentication_exceptions


class EmailBackend(ModelBackend):
    def authenticate(self, request, username=None, password=None, **kwargs):
        try:
            user = User.objects.get(email=username)
        except ObjectDoesNotExist:
            raise authentication_exceptions.UserDoesNotExistError(
                f"User (email={username}) does not exist"
            )

        if user.check_password(raw_password=password):
            return user

        raise authentication_exceptions.UserIncorrectPasswordError(
            f"User (email={username}) incorrect password"
        )
