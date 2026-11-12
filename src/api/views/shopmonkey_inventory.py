import json
import logging
import typing

import simplejson
from django import http, views

from src import enums as src_enums
from src.api.services import shopmonkey_inventory as shopmonkey_inventory_services

logger = logging.getLogger(__name__)
_LOG_PREFIX = "[SHOPMONKEY-INVENTORY]"

_ERROR_STATUS = {
    shopmonkey_inventory_services.ERROR_FEATURE_NOT_AVAILABLE: 403,
    shopmonkey_inventory_services.ERROR_NOT_FOUND: 404,
    shopmonkey_inventory_services.ERROR_INVALID_INPUT: 400,
    shopmonkey_inventory_services.ERROR_CONFLICT: 409,
}


def _error_response(message: str, error_code: typing.Optional[str]) -> http.HttpResponse:
    return http.HttpResponse(
        headers={"Content-Type": "application/json"},
        content=simplejson.dumps({"message": message, "error_code": error_code}),
        status=_ERROR_STATUS.get(error_code, 400),
    )


def _parse_part_status(raw: typing.Optional[str]) -> typing.Tuple[typing.Optional[int], bool]:
    """
    The parts filter as a ShopMonkeySyncPartStatus value, from either form a caller sends: the integer
    (1/2/3) the contract documents, or the status name ("ACTIVE"/"INACTIVE"/"ERROR") the page actually
    sends. Returns (value, ok). Nothing sent is (None, True); a value that is neither is (None, False) and
    the caller must reject it -- silently treating an unreadable filter as "no filter" is what let the page's
    status chips return every part for as long as they did.
    """
    if raw is None or raw == "":
        return None, True
    raw = raw.strip()
    if raw.isdigit():
        value = int(raw)
        return (value, True) if value in {s.value for s in src_enums.ShopMonkeySyncPartStatus} else (None, False)
    member = src_enums.ShopMonkeySyncPartStatus.__members__.get(raw.upper())
    return (member.value, True) if member else (None, False)


class ShopMonkeyInventoryStateView(views.View):
    """GET /shopmonkey/inventory/state/ -- the shop-management systems this company could use for inventory sync, each
    with the state the page should show (not connected / connection failing / no distributors / ready)."""

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
        data, error_code = shopmonkey_inventory_services.get_inventory_state(company_id=company_id)
        if error_code:
            return _error_response("Inventory management is not available for this company.", error_code)
        return http.HttpResponse(
            headers={"Content-Type": "application/json"},
            content=simplejson.dumps({"data": data}),
            status=200,
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
            provider_ids = [int(pid) for pid in (body.get("provider_ids") or [])]
            primary_provider_id = int(body.get("primary_provider_id"))
        except (TypeError, ValueError):
            return http.HttpResponse(
                headers={"Content-Type": "application/json"},
                content=simplejson.dumps(
                    {"message": "brand_id, provider_ids (non-empty list of ints) and primary_provider_id are required"}
                ),
                status=400,
            )

        data, error_code = shopmonkey_inventory_services.create_sync_brand(
            company_id=company_id,
            brand_id=brand_id,
            provider_ids=provider_ids,
            primary_provider_id=primary_provider_id,
        )
        if error_code:
            return _error_response("Could not enable this brand for sync.", error_code)
        return http.HttpResponse(
            headers={"Content-Type": "application/json"},
            content=simplejson.dumps({"data": data}),
            status=201,
        )


class ShopMonkeySyncBrandDetailView(views.View):
    """GET /shopmonkey/inventory/sync-brands/<id>/ -- single-brand lookup (Phase 3), same row
    shape as the list view, so the detail page stops fetching every brand to find one.
    PATCH /shopmonkey/inventory/sync-brands/<id>/ -- reconfigure (body: any subset of
    {enabled, provider_ids, primary_provider_id}; omitted fields keep their current value).
    POST /shopmonkey/inventory/sync-brands/<id>/run/ is a separate view below."""

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

        data, error_code = shopmonkey_inventory_services.get_sync_brand(
            company_id=company_id, sync_brand_id=sync_brand_id
        )
        if error_code:
            return _error_response("Could not fetch this sync brand.", error_code)
        return http.HttpResponse(
            headers={"Content-Type": "application/json"},
            content=simplejson.dumps({"data": data}),
            status=200,
        )

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

        provider_ids = body.get("provider_ids")
        if provider_ids is not None:
            try:
                provider_ids = [int(pid) for pid in provider_ids]
            except (TypeError, ValueError):
                return http.HttpResponse(
                    headers={"Content-Type": "application/json"},
                    content=simplejson.dumps({"message": "provider_ids must be a list of integers"}),
                    status=400,
                )

        primary_provider_id = body.get("primary_provider_id")
        if primary_provider_id is not None:
            try:
                primary_provider_id = int(primary_provider_id)
            except (TypeError, ValueError):
                return http.HttpResponse(
                    headers={"Content-Type": "application/json"},
                    content=simplejson.dumps({"message": "primary_provider_id must be an integer"}),
                    status=400,
                )

        data, error_code = shopmonkey_inventory_services.update_sync_brand(
            company_id=company_id,
            sync_brand_id=sync_brand_id,
            enabled=enabled,
            provider_ids=provider_ids,
            primary_provider_id=primary_provider_id,
        )
        if error_code:
            return _error_response("Could not update this sync brand.", error_code)
        return http.HttpResponse(
            headers={"Content-Type": "application/json"},
            content=simplejson.dumps({"data": data}),
            status=200,
        )

    def delete(self, request: http.HttpRequest, *args: typing.Any, **kwargs: typing.Any) -> http.HttpResponse:
        """DELETE /shopmonkey/inventory/sync-brands/<id>/ -- body {"confirm_brand_name": "<the brand's name>"}.
        Queues the removal of the brand's parts from ShopMonkey and then of the brand's sync config; returns 202
        with the QUEUED execution (kind DELETE_BRAND). 400 without the right confirmation, 409 if the brand has a
        sync or a delete queued/running."""
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
        confirm = body.get("confirm_brand_name") if isinstance(body, dict) else None

        data, error_code = shopmonkey_inventory_services.delete_sync_brand(
            company_id=company_id,
            sync_brand_id=sync_brand_id,
            confirm_brand_name=confirm if isinstance(confirm, str) else None,
        )
        if error_code == shopmonkey_inventory_services.ERROR_CONFLICT:
            return _error_response(
                "This brand has a sync or delete in progress. Try again when it finishes.", error_code
            )
        if error_code == shopmonkey_inventory_services.ERROR_INVALID_INPUT:
            return _error_response("Send confirm_brand_name equal to the brand's name to delete it.", error_code)
        if error_code:
            return _error_response("Could not delete this brand.", error_code)
        return http.HttpResponse(
            headers={"Content-Type": "application/json"},
            content=simplejson.dumps({"data": data}),
            status=202,
        )


class ShopMonkeySyncBrandRunView(views.View):
    """POST /shopmonkey/inventory/sync-brands/<id>/run/ -- queue a sync for this brand and return
    immediately (Phase 3). Nothing has run when this returns 202; process_shopmonkey_sync_jobs
    is what actually calls ShopMonkey. The caller polls GET .../executions/ until the row leaves
    QUEUED/RUNNING."""

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
            status=202,
        )


class ShopMonkeySyncBrandPartsView(views.View):
    """GET /shopmonkey/inventory/sync-brands/<id>/parts/?limit=&offset=&status= -- drill-down
    parts list, server-side paged (Phase 3) with an optional ShopMonkeySyncPartStatus filter."""

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

        offset = 0
        try:
            offset = int(request.GET.get("offset", offset))
        except (TypeError, ValueError):
            pass

        status_filter, status_ok = _parse_part_status(request.GET.get("status"))
        if not status_ok:
            return _error_response(
                "status must be 1, 2, 3 or ACTIVE, INACTIVE, ERROR.", shopmonkey_inventory_services.ERROR_INVALID_INPUT
            )

        result, error_code = shopmonkey_inventory_services.list_sync_brand_parts(
            company_id=company_id, sync_brand_id=sync_brand_id, limit=limit, offset=offset, status=status_filter
        )
        if error_code:
            return _error_response("Could not fetch parts for this sync brand.", error_code)
        return http.HttpResponse(
            headers={"Content-Type": "application/json"},
            content=simplejson.dumps(result),
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

        offset = 0
        try:
            offset = int(request.GET.get("offset", offset))
        except (TypeError, ValueError):
            pass

        result, error_code = shopmonkey_inventory_services.list_executions(
            company_id=company_id, sync_brand_id=sync_brand_id, status=status_filter, limit=limit, offset=offset
        )
        if error_code:
            return _error_response("Shop management inventory sync is not available for this company.", error_code)
        return http.HttpResponse(
            headers={"Content-Type": "application/json"},
            content=simplejson.dumps(result),
            status=200,
        )
