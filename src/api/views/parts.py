import logging
import typing

import simplejson
from django import http, views

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
