"""
Spaces views (multi-spaces): the switcher's list of spaces.
"""
import json
import logging
import typing

import simplejson
from django import http, views
from django.utils.decorators import method_decorator
from django.views.decorators.csrf import csrf_exempt

from common import exceptions as common_exceptions
from common import utils as common_utils
from src.api.schemas import spaces as spaces_schema
from src.api.services import spaces as spaces_services

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[SPACES-VIEW]"


def _json_response(data: dict, status: int = 200) -> http.HttpResponse:
    return http.HttpResponse(
        headers={"Content-Type": "application/json"},
        content=simplejson.dumps(data),
        status=status,
    )


def _parse_body(
    request: http.HttpRequest, schema
) -> typing.Tuple[typing.Optional[dict], typing.Optional[http.HttpResponse]]:
    try:
        body = json.loads(request.body) if request.body else {}
        return common_utils.validate_data_schema(data=body, schema=schema), None
    except common_exceptions.ValidationSchemaException as e:
        return None, _json_response(
            {"message": "Invalid payload", "data": common_utils.get_exception_message(exception=e)}, status=400
        )
    except json.JSONDecodeError:
        return None, _json_response({"message": "Invalid JSON body"}, status=400)


def _auth_and_company(request) -> typing.Tuple[typing.Optional[int], typing.Optional[str]]:
    """Returns (company_id, error_message)."""
    if not request.user or not request.user.is_authenticated:
        return None, "User not authenticated"
    company_id = getattr(request, "company_id", None)
    if not company_id:
        return None, "No company found in token"
    return company_id, None


@method_decorator(csrf_exempt, name="dispatch")
class SpacesView(views.View):
    """
    GET  /spaces/ - The caller's space scope and the spaces they may see (reads X-Space-Id). Every company
    member may call it; a non-admin only receives their visible spaces.
    POST /spaces/ - Add a space (company admin of a flagged company): {kind, name, prefix, address?}.
    """

    def get(self, request: http.HttpRequest, *args: typing.Any, **kwargs: typing.Any) -> http.HttpResponse:
        company_id, err = _auth_and_company(request)
        if err:
            return _json_response({"message": err}, status=401 if "authenticated" in err else 400)

        scope = spaces_services.resolve_scope(request)
        return _json_response({"data": spaces_services.get_spaces_overview(scope)})

    def post(self, request: http.HttpRequest, *args: typing.Any, **kwargs: typing.Any) -> http.HttpResponse:
        company_id, err = _auth_and_company(request)
        if err:
            return _json_response({"message": err}, status=401 if "authenticated" in err else 400)

        scope = spaces_services.resolve_scope(request)
        try:
            spaces_services.require_admin(scope)
            validated, err_response = _parse_body(request, spaces_schema.CreateSpaceSchema())
            if err_response:
                return err_response
            data = spaces_services.create_space(scope, validated)
        except spaces_services.SpaceError as e:
            return _json_response(e.body(), status=e.status_code)
        return _json_response({"message": "Space created", "data": data}, status=201)


@method_decorator(csrf_exempt, name="dispatch")
class SpaceDetailView(views.View):
    """
    PATCH  /spaces/<id>/ - Rename a space, change its prefix or its one address (company admin, flagged).
    DELETE /spaces/<id>/ - Delete a space and its address (company admin, flagged).
    """

    def patch(self, request: http.HttpRequest, id: int, *args: typing.Any, **kwargs: typing.Any) -> http.HttpResponse:
        company_id, err = _auth_and_company(request)
        if err:
            return _json_response({"message": err}, status=401 if "authenticated" in err else 400)

        scope = spaces_services.resolve_scope(request)
        try:
            spaces_services.require_admin(scope)
            validated, err_response = _parse_body(request, spaces_schema.UpdateSpaceSchema())
            if err_response:
                return err_response
            data = spaces_services.update_space(scope, id, validated)
        except spaces_services.SpaceError as e:
            return _json_response(e.body(), status=e.status_code)
        return _json_response({"data": data})

    def delete(self, request: http.HttpRequest, id: int, *args: typing.Any, **kwargs: typing.Any) -> http.HttpResponse:
        company_id, err = _auth_and_company(request)
        if err:
            return _json_response({"message": err}, status=401 if "authenticated" in err else 400)

        scope = spaces_services.resolve_scope(request)
        try:
            spaces_services.delete_space(scope, id)
        except spaces_services.SpaceError as e:
            return _json_response(e.body(), status=e.status_code)
        return http.HttpResponse(status=204)


@method_decorator(csrf_exempt, name="dispatch")
class SpaceResortPreviewView(views.View):
    """GET /spaces/<id>/resort-preview/ - Past orders from distributors that start with the space's prefix and sit
    in another space: count, value, and the same per order account (company admin, flagged)."""

    def get(self, request: http.HttpRequest, id: int, *args: typing.Any, **kwargs: typing.Any) -> http.HttpResponse:
        company_id, err = _auth_and_company(request)
        if err:
            return _json_response({"message": err}, status=401 if "authenticated" in err else 400)

        scope = spaces_services.resolve_scope(request)
        try:
            data = spaces_services.resort_preview(scope, id)
        except spaces_services.SpaceError as e:
            return _json_response(e.body(), status=e.status_code)
        return _json_response({"data": data})


@method_decorator(csrf_exempt, name="dispatch")
class SpaceResortView(views.View):
    """POST /spaces/<id>/resort/ - Move those orders into the space (company admin, flagged). Orders an admin
    moved by hand stay where they are."""

    def post(self, request: http.HttpRequest, id: int, *args: typing.Any, **kwargs: typing.Any) -> http.HttpResponse:
        company_id, err = _auth_and_company(request)
        if err:
            return _json_response({"message": err}, status=401 if "authenticated" in err else 400)

        scope = spaces_services.resolve_scope(request)
        try:
            data = spaces_services.resort_apply(scope, id)
        except spaces_services.SpaceError as e:
            return _json_response(e.body(), status=e.status_code)
        return _json_response({"data": data})


@method_decorator(csrf_exempt, name="dispatch")
class SpaceFeedView(views.View):
    """DELETE /spaces/<id>/feeds/<provider_id>/ - The space stops using a feed another space owns (company admin,
    flagged). The feed, its owner and the other spaces that use it are untouched."""

    def delete(
        self, request: http.HttpRequest, id: int, provider_id: int, *args: typing.Any, **kwargs: typing.Any
    ) -> http.HttpResponse:
        company_id, err = _auth_and_company(request)
        if err:
            return _json_response({"message": err}, status=401 if "authenticated" in err else 400)

        scope = spaces_services.resolve_scope(request)
        try:
            spaces_services.stop_using_feed(scope, id, provider_id)
        except spaces_services.SpaceError as e:
            return _json_response(e.body(), status=e.status_code)
        return http.HttpResponse(status=204)
