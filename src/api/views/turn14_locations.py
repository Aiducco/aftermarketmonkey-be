"""Turn14 locations API - list warehouse locations."""
import logging
import typing

import simplejson
from django import http, views

from src import models as src_models

logger = logging.getLogger(__name__)
_LOG_PREFIX = "[TURN14-LOCATIONS-API]"


def _auth_check(request: http.HttpRequest) -> typing.Tuple[typing.Optional[http.HttpResponse], bool]:
    """Returns (error_response, ok). If error_response is not None, return it."""
    if not request.user or not request.user.is_authenticated:
        return (
            http.HttpResponse(
                headers={"Content-Type": "application/json"},
                content=simplejson.dumps({"message": "User not authenticated"}),
                status=401,
            ),
            False,
        )
    return None, True


class Turn14LocationsView(views.View):
    """GET /turn14/locations/ - List Turn14 warehouse locations."""

    def get(self, request: http.HttpRequest, *args: typing.Any, **kwargs: typing.Any) -> http.HttpResponse:
        err, ok = _auth_check(request)
        if not ok:
            return err

        locations = list(
            src_models.Turn14Location.objects.all()
            .order_by("external_id")
            .values("id", "external_id", "name", "street", "city", "state", "country", "zip_code")
        )

        return http.HttpResponse(
            headers={"Content-Type": "application/json"},
            content=simplejson.dumps({"data": locations}),
            status=200,
        )
