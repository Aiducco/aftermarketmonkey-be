import logging
import typing

import simplejson
from django import http, views

from src.api.services import integrations as integrations_services

logger = logging.getLogger(__name__)
_LOG_PREFIX = "[INTEGRATIONS]"


class CompanyProvidersView(views.View):
    def get(self, request: http.HttpRequest, *args: typing.Any, **kwargs: typing.Any) -> http.HttpResponse:

        # AUTH CHECK
        logger.info(
            f"{_LOG_PREFIX} Auth check - user: {request.user}, is_authenticated: {getattr(request.user, 'is_authenticated', False) if request.user else False}"
        )
        if not request.user or not request.user.is_authenticated:
            logger.warning(
                f"{_LOG_PREFIX} User not authenticated for {request.path}"
            )
            return http.HttpResponse(
                headers={"Content-Type": "application/json"},
                content=simplejson.dumps({"message": "User not authenticated"}),
                status=401,
            )

        company_id = getattr(request, "company_id", None)

        if not company_id:
            return http.HttpResponse(
                headers={"Content-Type": "application/json"},
                content=simplejson.dumps({"message": "No company found in token"}),
                status=400,
            )

        try:
            data = integrations_services.get_company_providers(company_id=company_id)
        except Exception as e:
            logger.error(
                f"{_LOG_PREFIX} Error fetching company providers for company_id: {company_id}. Error: {str(e)}"
            )
            return http.HttpResponse(
                headers={"Content-Type": "application/json"},
                content=simplejson.dumps({"message": "Error fetching company providers"}),
                status=500,
            )

        return http.HttpResponse(
            headers={"Content-Type": "application/json"},
            content=simplejson.dumps({"data": data}),
            status=200,
        )


class CompanyProviderDetailView(views.View):
    def get(self, request: http.HttpRequest, *args: typing.Any, **kwargs: typing.Any) -> http.HttpResponse:

        # AUTH CHECK
        logger.info(
            f"{_LOG_PREFIX} Auth check - user: {request.user}, is_authenticated: {getattr(request.user, 'is_authenticated', False) if request.user else False}"
        )
        if not request.user or not request.user.is_authenticated:
            logger.warning(
                f"{_LOG_PREFIX} User not authenticated for {request.path}"
            )
            return http.HttpResponse(
                headers={"Content-Type": "application/json"},
                content=simplejson.dumps({"message": "User not authenticated"}),
                status=401,
            )

        company_id = getattr(request, "company_id", None)

        if not company_id:
            return http.HttpResponse(
                headers={"Content-Type": "application/json"},
                content=simplejson.dumps({"message": "No company found in token"}),
                status=400,
            )

        provider_id = kwargs.get('id')
        if not provider_id:
            return http.HttpResponse(
                headers={"Content-Type": "application/json"},
                content=simplejson.dumps({"message": "Provider ID is required"}),
                status=400,
            )

        try:
            provider_id_int = int(provider_id)
        except (ValueError, TypeError):
            return http.HttpResponse(
                headers={"Content-Type": "application/json"},
                content=simplejson.dumps({"message": "Invalid provider ID"}),
                status=400,
            )

        try:
            data = integrations_services.get_company_provider_by_id(
                company_id=company_id,
                provider_id=provider_id_int
            )
        except Exception as e:
            logger.error(
                f"{_LOG_PREFIX} Error fetching company provider with id: {provider_id_int} for company_id: {company_id}. Error: {str(e)}"
            )
            return http.HttpResponse(
                headers={"Content-Type": "application/json"},
                content=simplejson.dumps({"message": "Error fetching company provider"}),
                status=500,
            )

        if not data:
            return http.HttpResponse(
                headers={"Content-Type": "application/json"},
                content=simplejson.dumps({"message": "Company provider not found"}),
                status=404,
            )

        return http.HttpResponse(
            headers={"Content-Type": "application/json"},
            content=simplejson.dumps({"data": data}),
            status=200,
        )

