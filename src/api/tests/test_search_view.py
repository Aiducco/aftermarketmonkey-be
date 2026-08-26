"""
Tests for ``src.api.views.search`` -- the HTTP layer.

These exist because of a real production bug. The service layer supported routing a
no-structure query to the parts index, and was tested doing so, but the *view* defaulted
``mode`` to "tires" and rejected anything else -- so the fallback was unreachable over HTTP and
a part-number search returned an empty tire page. Every test in this file went missing at the
same moment: the suite only ever called ``tire_search.search()`` directly.

The service is stubbed; what is under test is the request/response contract.
"""
import typing
import unittest.mock as mock

import simplejson
from django.test import RequestFactory, SimpleTestCase

from src.api.views import search as search_views
from src.domain import tire_filters


class _User:
    is_authenticated = True
    id = 1


def _post(body: typing.Optional[dict] = None, *, authenticated: bool = True, company_id: int = 16):
    request = RequestFactory().post("/api/search/", data=simplejson.dumps(body or {}), content_type="application/json")
    request.user = _User() if authenticated else None
    request.company_id = company_id
    return request


def _payload(**overrides):
    base = {
        "mode": "tires",
        "query": "",
        "total": 0,
        "limit": 24,
        "offset": 0,
        "applied_filters": {},
        "interpretation": {"parsed": False, "matched": {}, "chips": [], "relaxed": None},
        "results": [],
        "facets": [],
        "tabs": [],
        "timing": {},
    }
    base.update(overrides)
    return base


class SearchViewTests(SimpleTestCase):
    databases = []

    def _call(self, body, service=None, **kwargs):
        service = service or mock.Mock(return_value=_payload())
        with mock.patch.object(search_views.tire_search_services, "search", service), mock.patch.object(
            search_views.audit_parts, "record_part_request"
        ):
            response = search_views.SearchView.as_view()(_post(body, **kwargs))
        return response, service

    def test_mode_is_not_forced_when_the_client_omits_it(self):
        """
        The regression. If the view substitutes a default here, the service can never route to
        parts and "bed mat tacoma" dead-ends on an empty tire page.
        """
        _response, service = self._call({"q": "bed mat tacoma"})
        self.assertIsNone(service.call_args.kwargs["mode"])

    def test_an_explicit_mode_is_passed_through(self):
        for mode in ("tires", "parts"):
            _response, service = self._call({"q": "x", "mode": mode})
            self.assertEqual(service.call_args.kwargs["mode"], mode)

    def test_mode_is_normalised(self):
        _response, service = self._call({"q": "x", "mode": "  TIRES "})
        self.assertEqual(service.call_args.kwargs["mode"], "tires")

    def test_a_blank_mode_is_treated_as_absent(self):
        _response, service = self._call({"q": "x", "mode": "   "})
        self.assertIsNone(service.call_args.kwargs["mode"])

    def test_parts_mode_is_not_rejected(self):
        # It was, which is what made the fallback unreachable.
        response, _service = self._call({"q": "bed mat", "mode": "parts"})
        self.assertEqual(response.status_code, 200)

    def test_a_parts_response_is_returned_verbatim(self):
        service = mock.Mock(return_value=_payload(mode="parts", total=40))
        response, _ = self._call({"q": "bed mat tacoma"}, service=service)
        body = simplejson.loads(response.content)
        self.assertEqual(body["mode"], "parts")
        self.assertEqual(body["total"], 40)

    def test_filters_are_forwarded_untouched(self):
        _response, service = self._call({"q": "mud terrain", "filters": {"rim_diameter_in": 18}})
        self.assertEqual(service.call_args.kwargs["filters"], {"rim_diameter_in": 18})

    def test_unauthenticated(self):
        response, _ = self._call({"q": "x"}, authenticated=False)
        self.assertEqual(response.status_code, 401)

    def test_malformed_json(self):
        request = RequestFactory().post("/api/search/", data="{not json", content_type="application/json")
        request.user = _User()
        request.company_id = 1
        response = search_views.SearchView.as_view()(request)
        self.assertEqual(response.status_code, 400)

    def test_non_object_body(self):
        request = RequestFactory().post("/api/search/", data="[1,2]", content_type="application/json")
        request.user = _User()
        request.company_id = 1
        response = search_views.SearchView.as_view()(request)
        self.assertEqual(response.status_code, 400)

    def test_non_object_filters(self):
        response, _ = self._call({"filters": ["a"]})
        self.assertEqual(response.status_code, 400)

    def test_unknown_filter_key_is_a_400(self):
        service = mock.Mock(side_effect=tire_filters.UnknownFilterField("Unknown filter field(s): colour"))
        response, _ = self._call({"filters": {"colour": "red"}}, service=service)
        self.assertEqual(response.status_code, 400)
        self.assertIn("colour", simplejson.loads(response.content)["message"])

    def test_search_error_is_a_400(self):
        service = mock.Mock(side_effect=search_views.tire_search_services.SearchError("Unknown sort"))
        response, _ = self._call({"sort": "price_asc"}, service=service)
        self.assertEqual(response.status_code, 400)

    def test_unexpected_failure_is_a_500_not_a_leak(self):
        service = mock.Mock(side_effect=RuntimeError("connection reset by peer"))
        response, _ = self._call({"q": "x"}, service=service)
        self.assertEqual(response.status_code, 500)
        # The internal message must not reach the client.
        self.assertEqual(simplejson.loads(response.content)["message"], "Search failed.")

    def test_no_pricing_is_hydrated_by_this_endpoint(self):
        # Prices come from POST /parts/bulk-pricing/; company_id is only for the audit trail.
        _response, service = self._call({"q": "275/70R18"})
        self.assertNotIn("company_id", service.call_args.kwargs)


class SearchFacetsViewTests(SimpleTestCase):
    databases = []

    def _get(self, query="", authenticated=True):
        request = RequestFactory().get("/api/search/facets/" + query)
        request.user = _User() if authenticated else None
        request.company_id = 1
        with mock.patch.object(
            search_views.tire_search_services,
            "facets_config",
            return_value=[
                {
                    "field": "tread_category",
                    "label": "Tread type",
                    "widget": "multiselect",
                    "collapse_after": 8,
                    "unit": None,
                    "value_labels": {},
                }
            ],
        ):
            return search_views.SearchFacetsView.as_view()(request)

    def test_defaults_to_tires(self):
        response = self._get()
        body = simplejson.loads(response.content)
        self.assertEqual(body["mode"], "tires")
        self.assertEqual(body["facets"][0]["field"], "tread_category")

    def test_explicit_tires(self):
        self.assertEqual(self._get("?mode=tires").status_code, 200)

    def test_unsupported_mode(self):
        self.assertEqual(self._get("?mode=wheels").status_code, 400)

    def test_unauthenticated(self):
        self.assertEqual(self._get(authenticated=False).status_code, 401)
