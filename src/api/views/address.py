"""
Ship-to address autocomplete & validation endpoints.

The provider API key never leaves the backend -- these three endpoints are the only thing the
frontend talks to, so the provider can be swapped (Geoapify -> Google Places -> a paid
validator) without a frontend release.

All three require an authenticated session, and /suggest is additionally rate-limited per
user, because every suggest call spends a metered provider quota.
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

from src.api.schemas import address as address_schemas
from src.api.services import address as address_services

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[ADDRESS-VIEW]"


def _json_response(data: typing.Any, status: int = 200) -> http.HttpResponse:
    return http.HttpResponse(
        headers={"Content-Type": "application/json"},
        content=simplejson.dumps(data),
        status=status,
    )


def _error_response(message: str, status: int = 400) -> http.HttpResponse:
    return _json_response({"message": message}, status=status)


def _require_auth(request: http.HttpRequest) -> typing.Optional[http.HttpResponse]:
    """None on success. Company membership is not required -- the address book is useful
    during onboarding, before a company exists."""
    if not request.user or not request.user.is_authenticated:
        return _error_response("User not authenticated", status=401)
    return None


def _rate_limit_identity(request: http.HttpRequest) -> str:
    user_id = getattr(request.user, "id", None)
    if user_id:
        return "user:{}".format(user_id)
    # Unreachable while _require_auth runs first, but keeps the limiter correct if this view
    # is ever opened up.
    forwarded = (request.META.get("HTTP_X_FORWARDED_FOR") or "").split(",")[0].strip()
    return "ip:{}".format(forwarded or request.META.get("REMOTE_ADDR") or "unknown")


def _parse_json_body(
    request: http.HttpRequest,
) -> typing.Tuple[typing.Optional[dict], typing.Optional[http.HttpResponse]]:
    try:
        return (json.loads(request.body) if request.body else {}), None
    except json.JSONDecodeError:
        return None, _error_response("Invalid JSON body")


def _validated(
    data: dict, schema
) -> typing.Tuple[typing.Optional[dict], typing.Optional[http.HttpResponse]]:
    try:
        return common_utils.validate_data_schema(data=data, schema=schema), None
    except common_exceptions.ValidationSchemaException as e:
        return None, _json_response(
            {"message": "Invalid payload", "data": common_utils.get_exception_message(exception=e)},
            status=400,
        )


@method_decorator(csrf_exempt, name="dispatch")
class AddressSuggestView(views.View):
    """
    GET /api/address/suggest/?q=&country=&session= — type-ahead for the ADDRESS field.
    Returns {"suggestions": [{id, label}]}, at most ADDRESS_SUGGEST_LIMIT entries.

    ``id`` is opaque and only meaningful to /api/address/resolve/ within the same ``session``;
    it expires after ADDRESS_SUGGESTION_CACHE_TTL_SECONDS.

    An empty list is the answer for "nothing matched" AND for "the provider is down or
    unconfigured" — on purpose. The frontend shows no dropdown and no error either way, and
    the user carries on typing (see the failure rule in the spec).
    """

    def get(self, request: http.HttpRequest, *args, **kwargs) -> http.HttpResponse:
        err = _require_auth(request)
        if err:
            return err

        validated, err = _validated(
            data={
                "q": request.GET.get("q") or "",
                "country": request.GET.get("country") or None,
                "session": request.GET.get("session") or "",
            },
            schema=address_schemas.SuggestAddressSchema(),
        )
        if err:
            return err

        if not address_services.consume_suggest_quota(_rate_limit_identity(request)):
            return _error_response("Too many address lookups. Try again in a minute.", status=429)

        try:
            suggestions = address_services.suggest_addresses(
                q=validated["q"],
                country=validated.get("country"),
                session=str(validated["session"]),
            )
        except Exception:
            # The service already swallows provider errors; anything reaching here is a bug on
            # our side. Still answer 200 with nothing rather than breaking the user's typing.
            logger.exception("%s Unexpected error building address suggestions.", _LOG_PREFIX)
            suggestions = []

        return _json_response({"suggestions": suggestions})


@method_decorator(csrf_exempt, name="dispatch")
class AddressResolveView(views.View):
    """
    POST /api/address/resolve/ — {id, session}. Returns the full structured Address
    {line1, line2, city, state, postal_code, country} for a suggestion from /suggest/.

    404 when the id has expired or belongs to another session; the frontend keeps whatever
    the user typed.
    """

    def post(self, request: http.HttpRequest, *args, **kwargs) -> http.HttpResponse:
        err = _require_auth(request)
        if err:
            return err
        body, err = _parse_json_body(request)
        if err:
            return err

        validated, err = _validated(data=body, schema=address_schemas.ResolveAddressSchema())
        if err:
            return err

        try:
            address = address_services.resolve_address(
                suggestion_id=validated["id"], session=str(validated["session"])
            )
        except Exception:
            logger.exception("%s Unexpected error resolving an address suggestion.", _LOG_PREFIX)
            return _error_response("Suggestion is no longer available", status=404)

        if address is None:
            return _error_response("Suggestion is no longer available", status=404)
        return _json_response(address)


@method_decorator(csrf_exempt, name="dispatch")
class AddressValidateView(views.View):
    """
    POST /api/address/validate/ — the Address shape. Returns
    {status, suggested?: Address, messages: [str]} where status is valid | corrected |
    unverified.

    Advisory only: ``unverified`` is also what an unconfigured or unreachable provider
    produces, and the frontend lets the user continue regardless. Nothing here blocks a quote.
    """

    def post(self, request: http.HttpRequest, *args, **kwargs) -> http.HttpResponse:
        err = _require_auth(request)
        if err:
            return err
        body, err = _parse_json_body(request)
        if err:
            return err

        validated, err = _validated(data=body, schema=address_schemas.AddressSchema())
        if err:
            return err

        try:
            result = address_services.validate_address(validated)
        except Exception:
            logger.exception("%s Unexpected error validating an address.", _LOG_PREFIX)
            result = {
                "status": "unverified",
                "messages": ["We could not check this address right now."],
            }

        return _json_response(result)
