import json
import logging
import typing

import simplejson
from django import http, views

from src.api.services import shopmonkey_inventory as shopmonkey_inventory_services

logger = logging.getLogger(__name__)
_LOG_PREFIX = "[SHOPMONKEY-INVENTORY]"

_ERROR_STATUS = {
    shopmonkey_inventory_services.ERROR_FEATURE_NOT_AVAILABLE: 403,
    shopmonkey_inventory_services.ERROR_NOT_FOUND: 404,
    shopmonkey_inventory_services.ERROR_INVALID_INPUT: 400,
}


def _error_response(message: str, error_code: typing.Optional[str]) -> http.HttpResponse:
    return http.HttpResponse(
        headers={"Content-Type": "application/json"},
        content=simplejson.dumps({"message": message, "error_code": error_code}),
        status=_ERROR_STATUS.get(error_code, 400),
    )


class ShopMonkeyAvailableBrandsView(views.View):
    """GET /shopmonkey/inventory/available-brands/ -- brands this company could sync, and
    which of its connected providers carry each one."""

    def get(self, request: http.HttpRequest, *args: typing.Any, **kwargs: typing.Any) -> http.HttpResponse:
        if not request.user or not request.user.is_authenticated:
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

        data, error_code = shopmonkey_inventory_services.get_available_brands(company_id=company_id)
        if error_code:
            return _error_response("Shop management inventory sync is not available for this company.", error_code)
        return http.HttpResponse(
            headers={"Content-Type": "application/json"},
            content=simplejson.dumps({"data": data}),
            status=200,
        )


class ShopMonkeySyncBrandsView(views.View):
    """GET /shopmonkey/inventory/sync-brands/ -- this company's configured sync brands.
    POST /shopmonkey/inventory/sync-brands/ -- enable a brand (body: {brand_id, provider_id})."""

    def get(self, request: http.HttpRequest, *args: typing.Any, **kwargs: typing.Any) -> http.HttpResponse:
        if not request.user or not request.user.is_authenticated:
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

        data, error_code = shopmonkey_inventory_services.list_sync_brands(company_id=company_id)
        if error_code:
            return _error_response("Shop management inventory sync is not available for this company.", error_code)
        return http.HttpResponse(
            headers={"Content-Type": "application/json"},
            content=simplejson.dumps({"data": data}),
            status=200,
        )

    def post(self, request: http.HttpRequest, *args: typing.Any, **kwargs: typing.Any) -> http.HttpResponse:
        if not request.user or not request.user.is_authenticated:
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
            body = json.loads(request.body) if request.body else {}
        except json.JSONDecodeError:
            return http.HttpResponse(
                headers={"Content-Type": "application/json"},
                content=simplejson.dumps({"message": "Invalid JSON body"}),
                status=400,
            )

        try:
            brand_id = int(body.get("brand_id"))
            provider_id = int(body.get("provider_id"))
        except (TypeError, ValueError):
            return http.HttpResponse(
                headers={"Content-Type": "application/json"},
                content=simplejson.dumps({"message": "brand_id and provider_id are required integers"}),
                status=400,
            )

        data, error_code = shopmonkey_inventory_services.create_sync_brand(
            company_id=company_id, brand_id=brand_id, provider_id=provider_id
        )
        if error_code:
            return _error_response("Could not enable this brand for sync.", error_code)
        return http.HttpResponse(
            headers={"Content-Type": "application/json"},
            content=simplejson.dumps({"data": data}),
            status=201,
        )


class ShopMonkeySyncBrandDetailView(views.View):
    """PATCH /shopmonkey/inventory/sync-brands/<id>/ -- enable/disable (body: {enabled}).
    POST /shopmonkey/inventory/sync-brands/<id>/run/ is a separate view below."""

    def patch(self, request: http.HttpRequest, *args: typing.Any, **kwargs: typing.Any) -> http.HttpResponse:
        if not request.user or not request.user.is_authenticated:
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
            sync_brand_id = int(kwargs.get("id"))
        except (TypeError, ValueError):
            return http.HttpResponse(
                headers={"Content-Type": "application/json"},
                content=simplejson.dumps({"message": "Invalid sync brand id"}),
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

        enabled = body.get("enabled")
        if enabled is not None and not isinstance(enabled, bool):
            return http.HttpResponse(
                headers={"Content-Type": "application/json"},
                content=simplejson.dumps({"message": "enabled must be a boolean"}),
                status=400,
            )

        data, error_code = shopmonkey_inventory_services.update_sync_brand(
            company_id=company_id, sync_brand_id=sync_brand_id, enabled=enabled
        )
        if error_code:
            return _error_response("Could not update this sync brand.", error_code)
        return http.HttpResponse(
            headers={"Content-Type": "application/json"},
            content=simplejson.dumps({"data": data}),
            status=200,
        )


class ShopMonkeySyncBrandRunView(views.View):
    """POST /shopmonkey/inventory/sync-brands/<id>/run/ -- sync this brand now, synchronously."""

    def post(self, request: http.HttpRequest, *args: typing.Any, **kwargs: typing.Any) -> http.HttpResponse:
        if not request.user or not request.user.is_authenticated:
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
            sync_brand_id = int(kwargs.get("id"))
        except (TypeError, ValueError):
            return http.HttpResponse(
                headers={"Content-Type": "application/json"},
                content=simplejson.dumps({"message": "Invalid sync brand id"}),
                status=400,
            )

        try:
            data, error_code = shopmonkey_inventory_services.run_sync_brand_now(
                company_id=company_id, sync_brand_id=sync_brand_id
            )
        except Exception as e:
            logger.error(f"{_LOG_PREFIX} Error running sync (sync_brand_id={sync_brand_id}): {e}")
            return http.HttpResponse(
                headers={"Content-Type": "application/json"},
                content=simplejson.dumps({"message": "Error running sync"}),
                status=500,
            )
        if error_code:
            return _error_response("Could not run sync for this brand.", error_code)
        return http.HttpResponse(
            headers={"Content-Type": "application/json"},
            content=simplejson.dumps({"data": data}),
            status=200,
        )


class ShopMonkeySyncBrandPartsView(views.View):
    """GET /shopmonkey/inventory/sync-brands/<id>/parts/ -- drill-down parts list."""

    def get(self, request: http.HttpRequest, *args: typing.Any, **kwargs: typing.Any) -> http.HttpResponse:
        if not request.user or not request.user.is_authenticated:
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
            sync_brand_id = int(kwargs.get("id"))
        except (TypeError, ValueError):
            return http.HttpResponse(
                headers={"Content-Type": "application/json"},
                content=simplejson.dumps({"message": "Invalid sync brand id"}),
                status=400,
            )

        limit = 100
        try:
            limit = int(request.GET.get("limit", limit))
        except (TypeError, ValueError):
            pass

        data, error_code = shopmonkey_inventory_services.list_sync_brand_parts(
            company_id=company_id, sync_brand_id=sync_brand_id, limit=limit
        )
        if error_code:
            return _error_response("Could not fetch parts for this sync brand.", error_code)
        return http.HttpResponse(
            headers={"Content-Type": "application/json"},
            content=simplejson.dumps({"data": data}),
            status=200,
        )


class ShopMonkeySyncExecutionsView(views.View):
    """GET /shopmonkey/inventory/executions/?sync_brand_id=&status=&limit= -- the sync monitor
    feed."""

    def get(self, request: http.HttpRequest, *args: typing.Any, **kwargs: typing.Any) -> http.HttpResponse:
        if not request.user or not request.user.is_authenticated:
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

        sync_brand_id = None
        if request.GET.get("sync_brand_id"):
            try:
                sync_brand_id = int(request.GET["sync_brand_id"])
            except (TypeError, ValueError):
                sync_brand_id = None

        status_filter = request.GET.get("status") or None
        limit = 50
        try:
            limit = int(request.GET.get("limit", limit))
        except (TypeError, ValueError):
            pass

        data, error_code = shopmonkey_inventory_services.list_executions(
            company_id=company_id, sync_brand_id=sync_brand_id, status=status_filter, limit=limit
        )
        if error_code:
            return _error_response("Shop management inventory sync is not available for this company.", error_code)
        return http.HttpResponse(
            headers={"Content-Type": "application/json"},
            content=simplejson.dumps({"data": data}),
            status=200,
        )
