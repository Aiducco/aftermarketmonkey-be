import json
import logging
import typing

import simplejson
from django import http, views
from django.views.decorators.csrf import csrf_exempt
from django.utils.decorators import method_decorator

from common import exceptions as common_exceptions
from common import utils as common_utils

from src.api.schemas import support as support_schema
from src.api.services import support as support_services

logger = logging.getLogger(__name__)


def _json_response(data: dict, status: int = 200) -> http.HttpResponse:
    return http.HttpResponse(
        headers={"Content-Type": "application/json"},
        content=simplejson.dumps(data),
        status=status,
    )


def _auth_and_company(request) -> tuple[int | None, int | None, str | None]:
    """Returns (company_id, user_id, error_message)."""
    if not request.user or not request.user.is_authenticated:
        return None, None, "User not authenticated"
    company_id = getattr(request, "company_id", None)
    if not company_id:
        return None, None, "No company found in token"
    return company_id, request.user.id, None


@method_decorator(csrf_exempt, name="dispatch")
class SupportTicketsView(views.View):
    """
    POST /api/support/tickets/ — Submit a support ticket
    GET  /api/support/tickets/ — List current user's tickets
    """

    def post(self, request: http.HttpRequest, *args: typing.Any, **kwargs: typing.Any) -> http.HttpResponse:
        company_id, user_id, err = _auth_and_company(request)
        if err:
            return _json_response({"message": err}, status=401 if "authenticated" in err else 400)

        try:
            body = json.loads(request.body) if request.body else {}
            validated = common_utils.validate_data_schema(
                data=body,
                schema=support_schema.CreateTicketSchema(),
            )
        except common_exceptions.ValidationSchemaException as e:
            return _json_response(
                {"message": "Invalid payload", "data": common_utils.get_exception_message(exception=e)},
                status=400,
            )
        except json.JSONDecodeError:
            return _json_response({"message": "Invalid JSON body"}, status=400)

        ticket = support_services.create_ticket(
            company_id=company_id,
            user_id=user_id,
            subject=validated["subject"],
            message=validated["message"],
        )
        return _json_response({"message": "Ticket submitted", "data": ticket}, status=201)

    def get(self, request: http.HttpRequest, *args: typing.Any, **kwargs: typing.Any) -> http.HttpResponse:
        company_id, user_id, err = _auth_and_company(request)
        if err:
            return _json_response({"message": err}, status=401 if "authenticated" in err else 400)

        tickets = support_services.list_tickets(user_id=user_id)
        return _json_response({"data": tickets})
