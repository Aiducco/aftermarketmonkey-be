import logging
import typing

import simplejson
from django import http, views

from src.api.services import admin as admin_services

logger = logging.getLogger(__name__)
_LOG_PREFIX = "[ADMIN]"


def _require_staff(request: http.HttpRequest) -> typing.Optional[http.HttpResponse]:
    """
    Shared guard for every staff-only admin view below. `request.user` is always a freshly
    fetched DB row (see JWTAuthenticationMiddleware) — `is_staff` is checked against the live
    value, never trusted from the JWT claim alone. Returns an error response to short-circuit the
    view, or None when the request may proceed.
    """
    if not request.user or not request.user.is_authenticated:
        logger.warning("{} User not authenticated for {}".format(_LOG_PREFIX, request.path))
        return http.HttpResponse(
            headers={"Content-Type": "application/json"},
            content=simplejson.dumps({"message": "User not authenticated"}),
            status=401,
        )
    if not request.user.is_staff:
        logger.warning(
            "{} User (id={}) is not staff, denying {}".format(_LOG_PREFIX, request.user.id, request.path)
        )
        return http.HttpResponse(
            headers={"Content-Type": "application/json"},
            content=simplejson.dumps({"message": "Staff access required"}),
            status=403,
        )
    return None


class AdminCompaniesView(views.View):
    """GET /admin/companies/ - Every company (excluding the internal admin company) with subscription state and provider-connection counts. Staff only."""

    def get(self, request: http.HttpRequest, *args: typing.Any, **kwargs: typing.Any) -> http.HttpResponse:
        denied = _require_staff(request)
        if denied:
            return denied

        try:
            data = admin_services.list_all_companies_for_admin()
        except Exception as e:
            logger.error("{} Error fetching companies for admin panel. Error: {}".format(_LOG_PREFIX, str(e)))
            return http.HttpResponse(
                headers={"Content-Type": "application/json"},
                content=simplejson.dumps({"message": "Error fetching companies"}),
                status=500,
            )

        return http.HttpResponse(
            headers={"Content-Type": "application/json"},
            content=simplejson.dumps({"data": data}),
            status=200,
        )


class AdminCompanyDetailView(views.View):
    """GET /admin/companies/<id>/ - One company's summary plus every CompanyProviders row for it. Staff only."""

    def get(self, request: http.HttpRequest, id: int, *args: typing.Any, **kwargs: typing.Any) -> http.HttpResponse:
        denied = _require_staff(request)
        if denied:
            return denied

        try:
            data = admin_services.get_admin_company_detail(company_id=id)
        except Exception as e:
            logger.error(
                "{} Error fetching company detail for company_id: {}. Error: {}".format(_LOG_PREFIX, id, str(e))
            )
            return http.HttpResponse(
                headers={"Content-Type": "application/json"},
                content=simplejson.dumps({"message": "Error fetching company"}),
                status=500,
            )

        if not data:
            return http.HttpResponse(
                headers={"Content-Type": "application/json"},
                content=simplejson.dumps({"message": "Company not found"}),
                status=404,
            )

        return http.HttpResponse(
            headers={"Content-Type": "application/json"},
            content=simplejson.dumps({"data": data}),
            status=200,
        )


class AdminProvidersView(views.View):
    """GET /admin/providers/ - Every provider with a count of companies connected to it. Staff only."""

    def get(self, request: http.HttpRequest, *args: typing.Any, **kwargs: typing.Any) -> http.HttpResponse:
        denied = _require_staff(request)
        if denied:
            return denied

        try:
            data = admin_services.list_all_providers_for_admin()
        except Exception as e:
            logger.error("{} Error fetching providers for admin panel. Error: {}".format(_LOG_PREFIX, str(e)))
            return http.HttpResponse(
                headers={"Content-Type": "application/json"},
                content=simplejson.dumps({"message": "Error fetching providers"}),
                status=500,
            )

        return http.HttpResponse(
            headers={"Content-Type": "application/json"},
            content=simplejson.dumps({"data": data}),
            status=200,
        )


class AdminProviderDetailView(views.View):
    """GET /admin/providers/<id>/ - One provider's summary plus every company connected to it. Staff only."""

    def get(self, request: http.HttpRequest, id: int, *args: typing.Any, **kwargs: typing.Any) -> http.HttpResponse:
        denied = _require_staff(request)
        if denied:
            return denied

        try:
            data = admin_services.get_admin_provider_detail(provider_id=id)
        except Exception as e:
            logger.error(
                "{} Error fetching provider detail for provider_id: {}. Error: {}".format(_LOG_PREFIX, id, str(e))
            )
            return http.HttpResponse(
                headers={"Content-Type": "application/json"},
                content=simplejson.dumps({"message": "Error fetching provider"}),
                status=500,
            )

        if not data:
            return http.HttpResponse(
                headers={"Content-Type": "application/json"},
                content=simplejson.dumps({"message": "Provider not found"}),
                status=404,
            )

        return http.HttpResponse(
            headers={"Content-Type": "application/json"},
            content=simplejson.dumps({"data": data}),
            status=200,
        )
