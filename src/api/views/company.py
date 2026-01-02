import json
import logging
import typing

import simplejson
from django import http, views

from src.models import CompanyDestinations

logger = logging.getLogger(__name__)
_LOG_PREFIX = "[COMPANY-DESTINATIONS]"


class CompanyDestinationsView(views.View):
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


        destinations = CompanyDestinations.objects.filter(company_id=company_id)

        data = [
            {
                "id": d.id,
                "status": d.status,
                "status_name": d.status_name,
                "destination_type": d.destination_type,
                "destination_type_name": d.destination_type_name,
                "credentials": d.credentials,
            }
            for d in destinations
        ]

        return http.HttpResponse(
            headers={"Content-Type": "application/json"},
            content=simplejson.dumps({"data": data}),
            status=200,
        )
