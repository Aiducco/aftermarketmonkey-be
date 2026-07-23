import json
import logging
import typing

import simplejson
from django import http, views

from src.api.services import shop_management as shop_management_services

logger = logging.getLogger(__name__)
_LOG_PREFIX = "[SHOP-MANAGEMENT]"


class ShopManagementCatalogView(views.View):
    """GET /shop-management/catalog/ - Returns all shop-management providers with connection status."""

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
            result = shop_management_services.get_shop_management_catalog(company_id=company_id)
        except Exception as e:
            logger.error(f"{_LOG_PREFIX} Error fetching shop management catalog for company_id: {company_id}. Error: {str(e)}")
            return http.HttpResponse(
                headers={"Content-Type": "application/json"},
                content=simplejson.dumps({"message": "Error fetching shop management catalog"}),
                status=500,
            )

        return http.HttpResponse(
            headers={"Content-Type": "application/json"},
            content=simplejson.dumps(result),
            status=200,
        )


class ShopManagementProviderConnectView(views.View):
    """POST /shop-management/catalog/<id>/connect/ - Create CompanyShopManagementProviders with credentials."""

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
        data, err, error_code = shop_management_services.connect_provider(
            company_id=company_id,
            provider_id=provider_id,
            credentials=credentials,
        )
        if err:
            status = 404 if error_code == shop_management_services.CONNECTION_ERROR_NOT_FOUND else 400
            return http.HttpResponse(
                headers={"Content-Type": "application/json"},
                content=simplejson.dumps({"message": err, "error_code": error_code}),
                status=status,
            )

        return http.HttpResponse(
            headers={"Content-Type": "application/json"},
            content=simplejson.dumps({"data": data}),
            status=201,
        )


class ShopManagementConnectionView(views.View):
    """
    DELETE /shop-management/connections/<company_provider_id>/ — disconnect.
    PATCH /shop-management/connections/<company_provider_id>/ — partial credential update.
    """

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

        success, err = shop_management_services.disconnect(
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

    def patch(self, request: http.HttpRequest, *args: typing.Any, **kwargs: typing.Any) -> http.HttpResponse:
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
            body = json.loads(request.body) if request.body else {}
        except json.JSONDecodeError:
            return http.HttpResponse(
                headers={"Content-Type": "application/json"},
                content=simplejson.dumps({"message": "Invalid JSON body"}),
                status=400,
            )

        patch = body if isinstance(body, dict) else {}
        data, err, error_code = shop_management_services.update_connection(
            company_id=company_id,
            company_provider_id=cpi,
            credentials=patch,
        )
        if err:
            status = 404 if error_code == shop_management_services.CONNECTION_ERROR_NOT_FOUND else 400
            return http.HttpResponse(
                headers={"Content-Type": "application/json"},
                content=simplejson.dumps({"message": err, "error_code": error_code}),
                status=status,
            )

        return http.HttpResponse(
            headers={"Content-Type": "application/json"},
            content=simplejson.dumps({"data": data}),
            status=200,
        )


class ShopManagementConnectionDetailView(views.View):
    """GET /shop-management/connections/<company_provider_id>/detail/"""

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
            data = shop_management_services.get_connection_detail(
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


class CompanyShopManagementProvidersView(views.View):
    """GET /company-shop-management-providers/"""

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
            data = shop_management_services.list_company_connections(company_id=company_id)
        except Exception as e:
            logger.error(f"{_LOG_PREFIX} Error fetching shop management providers for company_id: {company_id}. Error: {str(e)}")
            return http.HttpResponse(
                headers={"Content-Type": "application/json"},
                content=simplejson.dumps({"message": "Error fetching shop management providers"}),
                status=500,
            )

        return http.HttpResponse(
            headers={"Content-Type": "application/json"},
            content=simplejson.dumps({"data": data}),
            status=200,
        )


class CompanyShopManagementProviderDetailView(views.View):
    """GET /company-shop-management-providers/<id>/"""

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

        company_provider_id = kwargs.get("id")
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
            data = shop_management_services.get_connection_detail(
                company_id=company_id,
                company_provider_id=cpi,
            )
        except Exception as e:
            logger.error(f"{_LOG_PREFIX} Error fetching shop management provider id={cpi} for company_id: {company_id}. Error: {str(e)}")
            return http.HttpResponse(
                headers={"Content-Type": "application/json"},
                content=simplejson.dumps({"message": "Error fetching shop management provider"}),
                status=500,
            )

        if not data:
            return http.HttpResponse(
                headers={"Content-Type": "application/json"},
                content=simplejson.dumps({"message": "Company shop management provider not found"}),
                status=404,
            )

        return http.HttpResponse(
            headers={"Content-Type": "application/json"},
            content=simplejson.dumps({"data": data}),
            status=200,
        )
