"""
Company locations (shop/warehouse address book) views.
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

from src.api.schemas import company_locations as company_locations_schema
from src.api.services import company_locations as company_locations_services

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[COMPANY-LOCATIONS-VIEW]"


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
class CompanyLocationsView(views.View):
    """
    GET  /company/locations/ - List this company's locations (address book), primary first.
    POST /company/locations/ - Add a location — {label, name, address1, city, state,
    postal_code, country, attention?, address2?, phone?, is_primary?}. The first location
    created for a company is always made primary regardless of is_primary.
    """

    def get(self, request: http.HttpRequest, *args: typing.Any, **kwargs: typing.Any) -> http.HttpResponse:
        company_id, err = _auth_and_company(request)
        if err:
            return _json_response({"message": err}, status=401 if "authenticated" in err else 400)

        data = company_locations_services.list_company_locations(company_id)
        return _json_response({"data": data})

    def post(self, request: http.HttpRequest, *args: typing.Any, **kwargs: typing.Any) -> http.HttpResponse:
        company_id, err = _auth_and_company(request)
        if err:
            return _json_response({"message": err}, status=401 if "authenticated" in err else 400)

        try:
            body = json.loads(request.body) if request.body else {}
            validated = common_utils.validate_data_schema(
                data=body,
                schema=company_locations_schema.CreateCompanyLocationSchema(),
            )
        except common_exceptions.ValidationSchemaException as e:
            return _json_response(
                {"message": "Invalid payload", "data": common_utils.get_exception_message(exception=e)},
                status=400,
            )
        except json.JSONDecodeError:
            return _json_response({"message": "Invalid JSON body"}, status=400)

        data = company_locations_services.create_company_location(company_id=company_id, **validated)
        return _json_response({"message": "Location created", "data": data}, status=201)


@method_decorator(csrf_exempt, name="dispatch")
class CompanyLocationDetailView(views.View):
    """
    PATCH  /company/locations/<id>/ - Update a location (including is_primary).
    DELETE /company/locations/<id>/ - Remove a location. If it was primary, the next one
    (alphabetically by label) becomes primary automatically.
    """

    def patch(self, request: http.HttpRequest, *args: typing.Any, **kwargs: typing.Any) -> http.HttpResponse:
        company_id, err = _auth_and_company(request)
        if err:
            return _json_response({"message": err}, status=401 if "authenticated" in err else 400)

        location_id = kwargs.get("id")
        try:
            body = json.loads(request.body) if request.body else {}
            validated = common_utils.validate_data_schema(
                data=body,
                schema=company_locations_schema.UpdateCompanyLocationSchema(),
            )
        except common_exceptions.ValidationSchemaException as e:
            return _json_response(
                {"message": "Invalid payload", "data": common_utils.get_exception_message(exception=e)},
                status=400,
            )
        except json.JSONDecodeError:
            return _json_response({"message": "Invalid JSON body"}, status=400)

        if not validated:
            return _json_response({"message": "No fields to update"}, status=400)

        result = company_locations_services.update_company_location(
            company_id=company_id, location_id=location_id, **validated
        )
        if isinstance(result, str):
            return _json_response({"message": result}, status=404)
        return _json_response({"message": "Location updated", "data": result})

    def delete(self, request: http.HttpRequest, *args: typing.Any, **kwargs: typing.Any) -> http.HttpResponse:
        company_id, err = _auth_and_company(request)
        if err:
            return _json_response({"message": err}, status=401 if "authenticated" in err else 400)

        location_id = kwargs.get("id")
        err = company_locations_services.delete_company_location(company_id=company_id, location_id=location_id)
        if err:
            return _json_response({"message": err}, status=404)
        return _json_response({"message": "Location removed"})
