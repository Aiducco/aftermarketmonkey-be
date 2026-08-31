"""
``POST /api/search`` -- unified search, tire mode.

Additive: ``GET /parts/search/`` (``src.api.views.parts.PartsSearchView``) is untouched and
continues to serve the existing parts flow. This endpoint reads the parts index only for a tab
count, at ``limit 1``, and never writes to it.
"""
import logging
import typing

import simplejson
from django import http, views
from django.utils.decorators import method_decorator
from django.views.decorators.csrf import csrf_exempt

from src.api.services import tire_search as tire_search_services
from src.api.services import wheel_search as wheel_search_services
from src.audit import parts as audit_parts
from src.domain import tire_filters

logger = logging.getLogger(__name__)
_LOG_PREFIX = "[SEARCH-API]"


def _json(payload: typing.Dict[str, typing.Any], status: int = 200) -> http.HttpResponse:
    return http.HttpResponse(
        headers={"Content-Type": "application/json"},
        content=simplejson.dumps(payload),
        status=status,
    )


def _auth_check(request: http.HttpRequest) -> typing.Tuple[typing.Optional[http.HttpResponse], typing.Optional[int]]:
    """Mirrors src.api.views.parts._auth_check -- same contract, kept local so the parts module
    is not imported for one helper."""
    if not request.user or not request.user.is_authenticated:
        return _json({"message": "User not authenticated"}, status=401), None
    return None, getattr(request, "company_id", None)


@method_decorator(csrf_exempt, name="dispatch")
class SearchView(views.View):
    """
    POST /api/search/

    Body:
        {
          "mode":    "tires" | "wheels" | "parts",   optional -- omit to let the server route
          "q":       "275/70R18 mud terrain",
          "filters": {"tread_category": "MT", "rim_diameter_in": 18},
          "sort":    "diameter_asc",
          "limit":   24,
          "offset":  0
        }

    ``mode`` is optional. Omitted, the server routes by what the query parsed into: a size or a
    tread category means tires, anything else falls through to the parts index so a part number
    behaves exactly as it does today. Sent explicitly, it pins the tab the user clicked.

    **No pricing is returned.** Results are the search index documents, shaped. The client
    hydrates a page by posting the returned ids to ``POST /parts/bulk-pricing/``, which is the
    existing per-company pricing endpoint built for exactly that.

    ``q`` is parsed into filters **only when ``filters`` is absent or empty**. Once the client
    sends filters they are used verbatim -- otherwise removing a facet chip would not stick,
    because the server would re-derive it from query text the user never edited.
    """

    def post(self, request: http.HttpRequest, *args: typing.Any, **kwargs: typing.Any) -> http.HttpResponse:
        err, company_id = _auth_check(request)
        if err:
            return err

        try:
            body = simplejson.loads(request.body or b"{}")
        except (ValueError, TypeError):
            return _json({"message": "Request body must be valid JSON."}, status=400)
        if not isinstance(body, dict):
            return _json({"message": "Request body must be a JSON object."}, status=400)

        # mode is OPTIONAL and must stay optional. Defaulting it to "tires" pins the router shut:
        # the service can no longer fall through to the parts index, so a query with no tire
        # structure ("bed mat tacoma", a part number) returns an empty tire page instead of the
        # parts results it returns today. Send it only to pin the tab the user clicked.
        mode = body.get("mode")
        if mode is not None:
            mode = str(mode).strip().lower() or None

        filters = body.get("filters") or {}
        if not isinstance(filters, dict):
            return _json({"message": "'filters' must be an object."}, status=400)

        try:
            if mode == wheel_search_services.MODE_WHEELS:
                # Wheels are their own service, not a branch inside tire search: the two are
                # driven by different things (a typed size versus picked fitment) and tire search
                # is live. They return the same envelope, so the client renders both the same way.
                payload = wheel_search_services.search(
                    q=body.get("q") or "",
                    filters=filters,
                    sort=body.get("sort"),
                    limit=body.get("limit") or wheel_search_services.DEFAULT_LIMIT,
                    offset=body.get("offset") or 0,
                )
            else:
                payload = tire_search_services.search(
                    q=body.get("q") or "",
                    filters=filters,
                    mode=mode,
                    sort=body.get("sort"),
                    limit=body.get("limit") or tire_search_services.DEFAULT_LIMIT,
                    offset=body.get("offset") or 0,
                )
        except wheel_search_services.SearchError as exc:
            return _json({"message": str(exc)}, status=400)
        except (tire_filters.UnknownFilterField, tire_filters.InvalidFilterValue) as exc:
            # An unrecognised filter key is a 400, never a silent drop: quietly ignoring it shows
            # the user more results than they asked for with no way to tell that happened.
            return _json({"message": str(exc)}, status=400)
        except tire_search_services.SearchError as exc:
            return _json({"message": str(exc)}, status=400)
        except Exception as exc:
            logger.exception("%s search failed: %s", _LOG_PREFIX, exc)
            return _json({"message": "Search failed."}, status=500)

        if company_id is not None:
            audit_parts.record_part_request(
                company_id=company_id,
                user_id=request.user.id if request.user else None,
                action="search",
                search_query=(body.get("q") or "")[:512] or None,
            )

        return _json(payload)


@method_decorator(csrf_exempt, name="dispatch")
class SearchFacetsView(views.View):
    """
    GET /api/search/facets/?mode=tire

    The facet rail definition on its own, so the client can render the shell before any search
    has run. Server-owned, so order and labels change without a client deploy.
    """

    def get(self, request: http.HttpRequest, *args: typing.Any, **kwargs: typing.Any) -> http.HttpResponse:
        err, _company_id = _auth_check(request)
        if err:
            return err

        mode = (request.GET.get("mode") or tire_search_services.MODE_TIRES).strip().lower()
        if mode == wheel_search_services.MODE_WHEELS:
            return _json({"mode": mode, "facets": wheel_search_services.facets_config()})
        if mode != tire_search_services.MODE_TIRES:
            return _json({"message": "Unsupported mode {!r}.".format(mode)}, status=400)
        return _json({"mode": mode, "facets": tire_search_services.facets_config()})
