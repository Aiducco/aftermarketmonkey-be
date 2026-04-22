import logging
import typing

import simplejson
from django import http, views

from src.api.services import company_settings as company_settings_services
from src.api.services import parts as parts_services
from src.audit import parts as audit_parts

logger = logging.getLogger(__name__)
_LOG_PREFIX = "[PARTS-API]"


def _auth_check(request: http.HttpRequest) -> typing.Tuple[typing.Optional[http.HttpResponse], typing.Optional[int]]:
    """Returns (error_response, company_id) or (None, company_id)."""
    if not request.user or not request.user.is_authenticated:
        return (
            http.HttpResponse(
                headers={"Content-Type": "application/json"},
                content=simplejson.dumps({"message": "User not authenticated"}),
                status=401,
            ),
            None,
        )
    company_id = getattr(request, "company_id", None)
    return None, company_id


class PartsSearchView(views.View):
    """GET /parts/search/?sku=xxx - Search parts by part_number."""

    def get(self, request: http.HttpRequest, *args: typing.Any, **kwargs: typing.Any) -> http.HttpResponse:
        err, company_id = _auth_check(request)
        if err:
            return err

        sku = request.GET.get("sku", "").strip()
        if not sku:
            return http.HttpResponse(
                headers={"Content-Type": "application/json"},
                content=simplejson.dumps({"message": "Missing required parameter: sku", "data": []}),
                status=400,
            )

        try:
            limit = min(int(request.GET.get("limit", 50)), 100)
        except ValueError:
            limit = 50

        try:
            result = parts_services.get_parts_search(sku=sku, limit=limit)
        except Exception as e:
            logger.exception("{} Search error: {}".format(_LOG_PREFIX, str(e)))
            return http.HttpResponse(
                headers={"Content-Type": "application/json"},
                content=simplejson.dumps({"message": "Error searching parts"}),
                status=500,
            )

        if company_id is not None:
            audit_parts.record_part_request(
                company_id=company_id,
                user_id=request.user.id if request.user else None,
                action="search",
                search_query=sku[:512] if sku else None,
            )

        return http.HttpResponse(
            headers={"Content-Type": "application/json"},
            content=simplejson.dumps({
                "data": result["data"],
                "provider_image_urls": result["provider_image_urls"],
            }),
            status=200,
        )


class MasterPartBrandsForFilterView(views.View):
    """
    GET /parts/search/brands/ — brands with at least one master part, for search filter comboboxes.
    Optional query params: q (substring match on name), limit (default 100, max 2000).
    """

    def get(self, request: http.HttpRequest, *args: typing.Any, **kwargs: typing.Any) -> http.HttpResponse:
        err, _company_id = _auth_check(request)
        if err:
            return err

        try:
            limit = min(int(request.GET.get("limit", 100)), 2000)
        except ValueError:
            limit = 100

        q = (request.GET.get("q") or "").strip()
        try:
            data = parts_services.get_master_part_brand_filter_options(q=q, limit=limit)
        except Exception as e:
            logger.exception("{} Master part brands for filter error: {}".format(_LOG_PREFIX, str(e)))
            return http.HttpResponse(
                headers={"Content-Type": "application/json"},
                content=simplejson.dumps({"message": "Error listing brands"}),
                status=500,
            )

        return http.HttpResponse(
            headers={"Content-Type": "application/json"},
            content=simplejson.dumps({"data": data}),
            status=200,
        )


class MasterPartCategoryFiltersView(views.View):
    """
    GET /parts/search/categories/ — distinct category and overview_category from category_mappings, for filters.
    Optional query params: q (substring match on each), limit (default 200, max 2000).
    """

    def get(self, request: http.HttpRequest, *args: typing.Any, **kwargs: typing.Any) -> http.HttpResponse:
        err, _company_id = _auth_check(request)
        if err:
            return err

        try:
            limit = min(int(request.GET.get("limit", 200)), 2000)
        except ValueError:
            limit = 200

        q = (request.GET.get("q") or "").strip()
        try:
            data = parts_services.get_master_part_category_filter_options(q=q, limit=limit)
        except Exception as e:
            logger.exception("{} Master part category filters error: {}".format(_LOG_PREFIX, str(e)))
            return http.HttpResponse(
                headers={"Content-Type": "application/json"},
                content=simplejson.dumps({"message": "Error listing categories"}),
                status=500,
            )

        return http.HttpResponse(
            headers={"Content-Type": "application/json"},
            content=simplejson.dumps({"data": data}),
            status=200,
        )


class PartAuditMyHistoryView(views.View):
    """GET /parts/audit/me/ — current user's part detail audit rows with Meilisearch-shaped part payloads."""

    def get(self, request: http.HttpRequest, *args: typing.Any, **kwargs: typing.Any) -> http.HttpResponse:
        err, company_id = _auth_check(request)
        if err:
            return err
        if company_id is None:
            return http.HttpResponse(
                headers={"Content-Type": "application/json"},
                content=simplejson.dumps({"message": "Company context required"}),
                status=400,
            )

        try:
            limit = min(int(request.GET.get("limit", 50)), 100)
        except ValueError:
            limit = 50
        try:
            offset = max(int(request.GET.get("offset", 0)), 0)
        except ValueError:
            offset = 0

        try:
            result = parts_services.list_part_detail_audit_history(
                company_id=company_id,
                user_id=request.user.id,
                limit=limit,
                offset=offset,
            )
        except Exception as e:
            logger.exception("{} Part audit history error: {}".format(_LOG_PREFIX, str(e)))
            return http.HttpResponse(
                headers={"Content-Type": "application/json"},
                content=simplejson.dumps({"message": "Error fetching part audit history"}),
                status=500,
            )

        return http.HttpResponse(
            headers={"Content-Type": "application/json"},
            content=simplejson.dumps(result),
            status=200,
        )


class PartAuditCompanyHistoryView(views.View):
    """GET /parts/audit/company/ — company-wide part detail audit (company admin only)."""

    def get(self, request: http.HttpRequest, *args: typing.Any, **kwargs: typing.Any) -> http.HttpResponse:
        err, company_id = _auth_check(request)
        if err:
            return err
        if company_id is None:
            return http.HttpResponse(
                headers={"Content-Type": "application/json"},
                content=simplejson.dumps({"message": "Company context required"}),
                status=400,
            )

        ok, admin_err = company_settings_services._is_company_admin(request.user, company_id)
        if not ok:
            return http.HttpResponse(
                headers={"Content-Type": "application/json"},
                content=simplejson.dumps({"message": admin_err or "Company admin access required"}),
                status=403,
            )

        try:
            limit = min(int(request.GET.get("limit", 50)), 100)
        except ValueError:
            limit = 50
        try:
            offset = max(int(request.GET.get("offset", 0)), 0)
        except ValueError:
            offset = 0

        try:
            result = parts_services.list_part_detail_audit_history(
                company_id=company_id,
                user_id=None,
                limit=limit,
                offset=offset,
            )
        except Exception as e:
            logger.exception("{} Company part audit history error: {}".format(_LOG_PREFIX, str(e)))
            return http.HttpResponse(
                headers={"Content-Type": "application/json"},
                content=simplejson.dumps({"message": "Error fetching part audit history"}),
                status=500,
            )

        return http.HttpResponse(
            headers={"Content-Type": "application/json"},
            content=simplejson.dumps(result),
            status=200,
        )


class PartDetailView(views.View):
    """GET /parts/<id>/ - MasterPart plus per-provider rows; inventory/pricing only if company integrates that provider."""

    def get(self, request: http.HttpRequest, *args: typing.Any, **kwargs: typing.Any) -> http.HttpResponse:
        err, company_id = _auth_check(request)
        if err:
            return err

        part_id = kwargs.get("id")
        if part_id is None:
            return http.HttpResponse(
                headers={"Content-Type": "application/json"},
                content=simplejson.dumps({"message": "Part ID required"}),
                status=400,
            )

        try:
            data = parts_services.get_part_detail(master_part_id=part_id, company_id=company_id)
        except Exception as e:
            logger.exception("{} Part detail error: {}".format(_LOG_PREFIX, str(e)))
            return http.HttpResponse(
                headers={"Content-Type": "application/json"},
                content=simplejson.dumps({"message": "Error fetching part detail"}),
                status=500,
            )

        if data is None:
            return http.HttpResponse(
                headers={"Content-Type": "application/json"},
                content=simplejson.dumps({"message": "Part not found"}),
                status=404,
            )

        if company_id is not None:
            audit_parts.record_part_request(
                company_id=company_id,
                user_id=request.user.id if request.user else None,
                action="detail",
                master_part_id=part_id,
            )

        return http.HttpResponse(
            headers={"Content-Type": "application/json"},
            content=simplejson.dumps({"data": data}),
            status=200,
        )
