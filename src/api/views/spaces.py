"""
Spaces views (multi-spaces): the switcher's list of spaces.
"""
import logging
import typing

import simplejson
from django import http, views
from django.utils.decorators import method_decorator
from django.views.decorators.csrf import csrf_exempt

from src.api.services import spaces as spaces_services

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[SPACES-VIEW]"


def _json_response(data: dict, status: int = 200) -> http.HttpResponse:
    return http.HttpResponse(
        headers={"Content-Type": "application/json"},
        content=simplejson.dumps(data),
        status=status,
    )


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
    GET /spaces/ - The caller's space scope and the spaces they may see (reads X-Space-Id). Every company
    member may call it; a non-admin only receives their visible spaces.
    """

    def get(self, request: http.HttpRequest, *args: typing.Any, **kwargs: typing.Any) -> http.HttpResponse:
        company_id, err = _auth_and_company(request)
        if err:
            return _json_response({"message": err}, status=401 if "authenticated" in err else 400)

        scope = spaces_services.resolve_scope(request)
        return _json_response({"data": spaces_services.get_spaces_overview(scope)})
