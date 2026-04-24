import json
import logging
import typing

import simplejson
from django import http, views

from src.api.services import integrations as integrations_services

logger = logging.getLogger(__name__)
_LOG_PREFIX = "[INTEGRATIONS]"


class ProvidersCatalogView(views.View):
    """GET /integrations/catalog/ - Returns all providers with connection status for the company."""

    def get(self, request: http.HttpRequest, *args: typing.Any, **kwargs: typing.Any) -> http.HttpResponse:
        if not request.user or not request.user.is_authenticated:
            logger.warning(f"{_LOG_PREFIX} User not authenticated for {request.path}")
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
            result = integrations_services.get_providers_catalog(company_id=company_id)
        except Exception as e:
            logger.error(
                f"{_LOG_PREFIX} Error fetching providers catalog for company_id: {company_id}. Error: {str(e)}"
            )
            return http.HttpResponse(
                headers={"Content-Type": "application/json"},
                content=simplejson.dumps({"message": "Error fetching providers catalog"}),
                status=500,
            )

        return http.HttpResponse(
            headers={"Content-Type": "application/json"},
            content=simplejson.dumps(result),
            status=200,
        )


class ProviderConnectView(views.View):
    """POST /integrations/catalog/<id>/connect/ - Create CompanyProviders with credentials."""

    def post(self, request: http.HttpRequest, *args: typing.Any, **kwargs: typing.Any) -> http.HttpResponse:
        if not request.user or not request.user.is_authenticated:
            logger.warning(f"{_LOG_PREFIX} User not authenticated for {request.path}")
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

        provider_id = kwargs.get("id")
        if not provider_id:
            return http.HttpResponse(
                headers={"Content-Type": "application/json"},
                content=simplejson.dumps({"message": "Provider ID is required"}),
                status=400,
            )

        try:
            body = json.loads(request.body) if request.body else {}
        except json.JSONDecodeError:
            return http.HttpResponse(
                headers={"Content-Type": "application/json"},
                content=simplejson.dumps({"message": "Invalid JSON body"}),
                status=400,
            )

        credentials = body if isinstance(body, dict) else {}
        data, err = integrations_services.connect_provider(
            company_id=company_id,
            provider_id=provider_id,
            credentials=credentials,
        )
        if err:
            return http.HttpResponse(
                headers={"Content-Type": "application/json"},
                content=simplejson.dumps({"message": err}),
                status=400,
            )

        return http.HttpResponse(
            headers={"Content-Type": "application/json"},
            content=simplejson.dumps({"data": data}),
            status=201,
        )


class ProviderDisconnectView(views.View):
    """DELETE /integrations/connections/<company_provider_id>/ - Remove CompanyProviders."""

    def delete(self, request: http.HttpRequest, *args: typing.Any, **kwargs: typing.Any) -> http.HttpResponse:
        if not request.user or not request.user.is_authenticated:
            logger.warning(f"{_LOG_PREFIX} User not authenticated for {request.path}")
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

        company_provider_id = kwargs.get("company_provider_id")
        if not company_provider_id:
            return http.HttpResponse(
                headers={"Content-Type": "application/json"},
                content=simplejson.dumps({"message": "Connection ID is required"}),
                status=400,
            )

        success, err = integrations_services.disconnect_provider(
            company_id=company_id,
            company_provider_id=company_provider_id,
        )
        if not success:
            return http.HttpResponse(
                headers={"Content-Type": "application/json"},
                content=simplejson.dumps({"message": err or "Connection not found"}),
                status=404,
            )

        return http.HttpResponse(status=204)


class ProviderConnectionDetailView(views.View):
    """
    GET /integrations/connections/<company_provider_id>/detail/
    Connection detail: catalog copy, required/optional field keys, redacted ``credentials`` and ``secrets_configured``.
    """

    def get(self, request: http.HttpRequest, *args: typing.Any, **kwargs: typing.Any) -> http.HttpResponse:
        if not request.user or not request.user.is_authenticated:
            logger.warning(f"{_LOG_PREFIX} User not authenticated for {request.path}")
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

        company_provider_id = kwargs.get("company_provider_id")
        if not company_provider_id:
            return http.HttpResponse(
                headers={"Content-Type": "application/json"},
                content=simplejson.dumps({"message": "Connection ID is required"}),
                status=400,
            )

        try:
            cpi = int(company_provider_id)
        except (TypeError, ValueError):
            return http.HttpResponse(
                headers={"Content-Type": "application/json"},
                content=simplejson.dumps({"message": "Invalid connection ID"}),
                status=400,
            )

        try:
            data = integrations_services.get_company_provider_connection_detail(
                company_id=company_id,
                company_provider_id=cpi,
            )
        except Exception as e:
            logger.error(
                f"{_LOG_PREFIX} Error fetching connection detail company_provider_id={cpi} company_id={company_id}: {e}"
            )
            return http.HttpResponse(
                headers={"Content-Type": "application/json"},
                content=simplejson.dumps({"message": "Error fetching connection detail"}),
                status=500,
            )

        if not data:
            return http.HttpResponse(
                headers={"Content-Type": "application/json"},
                content=simplejson.dumps({"message": "Connection not found"}),
                status=404,
            )

        return http.HttpResponse(
            headers={"Content-Type": "application/json"},
            content=simplejson.dumps({"data": data}),
            status=200,
        )


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


class BrandsWithProvidersView(views.View):
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

        try:
            data = integrations_services.get_all_brands_with_providers()
        except Exception as e:
            logger.error(
                f"{_LOG_PREFIX} Error fetching brands with providers. Error: {str(e)}"
            )
            return http.HttpResponse(
                headers={"Content-Type": "application/json"},
                content=simplejson.dumps({"message": "Error fetching brands with providers"}),
                status=500,
            )

        return http.HttpResponse(
            headers={"Content-Type": "application/json"},
            content=simplejson.dumps({"data": data}),
            status=200,
        )


class CompanyDestinationsWithBrandsView(views.View):
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
            data = integrations_services.get_company_destinations_with_brands(company_id=company_id)
        except Exception as e:
            logger.error(
                f"{_LOG_PREFIX} Error fetching company destinations with brands for company_id: {company_id}. Error: {str(e)}"
            )
            return http.HttpResponse(
                headers={"Content-Type": "application/json"},
                content=simplejson.dumps({"message": "Error fetching company destinations with brands"}),
                status=500,
            )

        return http.HttpResponse(
            headers={"Content-Type": "application/json"},
            content=simplejson.dumps({"data": data}),
            status=200,
        )


class CompanyDestinationDetailView(views.View):
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

        destination_id = kwargs.get('id')
        if not destination_id:
            return http.HttpResponse(
                headers={"Content-Type": "application/json"},
                content=simplejson.dumps({"message": "Destination ID is required"}),
                status=400,
            )

        try:
            destination_id_int = int(destination_id)
        except (ValueError, TypeError):
            return http.HttpResponse(
                headers={"Content-Type": "application/json"},
                content=simplejson.dumps({"message": "Invalid destination ID"}),
                status=400,
            )

        try:
            data = integrations_services.get_company_destination_by_id(
                company_id=company_id,
                destination_id=destination_id_int
            )
        except Exception as e:
            logger.error(
                f"{_LOG_PREFIX} Error fetching company destination with id: {destination_id_int} for company_id: {company_id}. Error: {str(e)}"
            )
            return http.HttpResponse(
                headers={"Content-Type": "application/json"},
                content=simplejson.dumps({"message": "Error fetching company destination"}),
                status=500,
            )

        if not data:
            return http.HttpResponse(
                headers={"Content-Type": "application/json"},
                content=simplejson.dumps({"message": "Company destination not found"}),
                status=404,
            )

        return http.HttpResponse(
            headers={"Content-Type": "application/json"},
            content=simplejson.dumps({"data": data}),
            status=200,
        )


class ExecutionRunsView(views.View):
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

        # Get destination_id from URL path (optional)
        destination_id = kwargs.get('destination_id')
        if destination_id:
            try:
                destination_id_int = int(destination_id)
            except (ValueError, TypeError):
                return http.HttpResponse(
                    headers={"Content-Type": "application/json"},
                    content=simplejson.dumps({"message": "Invalid destination ID"}),
                    status=400,
                )
        else:
            destination_id_int = None

        # Get pagination parameters from query string
        try:
            page = int(request.GET.get('page', 1))
        except (ValueError, TypeError):
            page = 1

        try:
            page_size = int(request.GET.get('page_size', 20))
            # Limit page_size to prevent abuse
            if page_size > 100:
                page_size = 100
            if page_size < 1:
                page_size = 20
        except (ValueError, TypeError):
            page_size = 20

        try:
            result = integrations_services.get_company_execution_runs(
                company_id=company_id,
                destination_id=destination_id_int,
                page=page,
                page_size=page_size
            )
        except Exception as e:
            logger.error(
                f"{_LOG_PREFIX} Error fetching execution runs for company_id: {company_id}. Error: {str(e)}"
            )
            return http.HttpResponse(
                headers={"Content-Type": "application/json"},
                content=simplejson.dumps({"message": "Error fetching execution runs"}),
                status=500,
            )

        return http.HttpResponse(
            headers={"Content-Type": "application/json"},
            content=simplejson.dumps(result),
            status=200,
        )


class ExecutionRunPartsHistoryView(views.View):
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

        execution_run_id = kwargs.get('execution_run_id')
        if not execution_run_id:
            return http.HttpResponse(
                headers={"Content-Type": "application/json"},
                content=simplejson.dumps({"message": "Execution run ID is required"}),
                status=400,
            )

        try:
            execution_run_id_int = int(execution_run_id)
        except (ValueError, TypeError):
            return http.HttpResponse(
                headers={"Content-Type": "application/json"},
                content=simplejson.dumps({"message": "Invalid execution run ID"}),
                status=400,
            )

        # Get pagination parameters from query string
        try:
            page = int(request.GET.get('page', 1))
        except (ValueError, TypeError):
            page = 1

        try:
            page_size = int(request.GET.get('page_size', 20))
            # Limit page_size to prevent abuse
            if page_size > 100:
                page_size = 100
            if page_size < 1:
                page_size = 20
        except (ValueError, TypeError):
            page_size = 20

        try:
            result = integrations_services.get_execution_run_parts_history(
                company_id=company_id,
                execution_run_id=execution_run_id_int,
                page=page,
                page_size=page_size
            )
        except Exception as e:
            logger.error(
                f"{_LOG_PREFIX} Error fetching parts history for execution_run_id: {execution_run_id_int}, company_id: {company_id}. Error: {str(e)}"
            )
            return http.HttpResponse(
                headers={"Content-Type": "application/json"},
                content=simplejson.dumps({"message": "Error fetching parts history"}),
                status=500,
            )

        if result is None:
            return http.HttpResponse(
                headers={"Content-Type": "application/json"},
                content=simplejson.dumps({"message": "Execution run not found"}),
                status=404,
            )

        return http.HttpResponse(
            headers={"Content-Type": "application/json"},
            content=simplejson.dumps(result),
            status=200,
        )

