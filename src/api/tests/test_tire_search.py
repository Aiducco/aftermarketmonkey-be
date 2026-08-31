"""
Tests for ``src.api.services.tire_search``.

Meilisearch and Postgres are patched out: what is under test is the *handler order*, not the
search engine. The first test in this file is the one the handoff document warns a suite will
miss unless it is written explicitly.
"""
import threading
import time
import typing
import unittest.mock as mock

from django.test import SimpleTestCase

from src.api.services import tire_search
from src.domain import tire_filters


def _response(hits=(), total=0, facets=None):
    return {"hits": list(hits), "estimatedTotalHits": total, "facetDistribution": facets or {}}


def _fake_multi_search(tires=None, parts=None, calls=None):
    """Stand in for the one multi-search round trip. ``calls`` records each invocation."""

    def run(**kwargs):
        if calls is not None:
            calls.append(kwargs)
        return (tires if tires is not None else _response()), parts

    return run


class _ExplodingParser:
    """A parser that fails the test if it is ever called."""

    def __init__(self):
        self.called = False

    def __call__(self, *args: typing.Any, **kwargs: typing.Any):
        self.called = True
        raise AssertionError("parse_query must not be called when the request carries filters")


class RefinementTests(SimpleTestCase):
    databases = []

    def test_parser_is_not_invoked_when_filters_are_present(self):
        """
        The refinement bug, asserted directly.

        With filters present the query text must be used verbatim and never re-parsed. Otherwise
        a user who removes the "Mud terrain" chip gets it straight back, because the server
        re-derives it from query text they never edited -- and no amount of clicking removes it.
        """
        parser = _ExplodingParser()
        with mock.patch.object(tire_search, "_multi_search", _fake_multi_search()), mock.patch.object(
            tire_search, "facets_config", return_value=[]
        ), mock.patch.object(tire_search.tires_index, "is_configured", return_value=True):
            result = tire_search.search(
                q="mud terrain 275/70R18",
                filters={"rim_diameter_in": 18},
                parse_query_fn=parser,
            )

        self.assertFalse(parser.called)
        # The client's filters are used exactly as sent -- nothing inferred is added.
        self.assertEqual(result["applied_filters"], {"rim_diameter_in": 18})
        self.assertFalse(result["interpretation"]["parsed"])

    def test_parser_is_invoked_when_filters_are_absent(self):
        calls = []

        def parser(text, **kwargs):
            calls.append(text)
            from src.domain import tire_query

            return tire_query.ParsedQuery(filters={"rim_diameter_in": 18.0}, residue="", matched={"size": "275/70R18"})

        with mock.patch.object(tire_search, "_multi_search", _fake_multi_search()), mock.patch.object(
            tire_search, "facets_config", return_value=[]
        ), mock.patch.object(tire_search, "brand_names", return_value=frozenset()), mock.patch.object(
            tire_search, "tread_category_codes", return_value=frozenset()
        ), mock.patch.object(
            tire_search.tires_index, "is_configured", return_value=True
        ):
            result = tire_search.search(q="275/70R18", filters=None, parse_query_fn=parser)

        self.assertEqual(calls, ["275/70R18"])
        self.assertTrue(result["interpretation"]["parsed"])
        self.assertEqual(result["interpretation"]["matched"], {"size": "275/70R18"})

    def test_empty_filters_dict_counts_as_absent(self):
        """``{"filters": {}}`` is what a client sends on a first search, not a refinement."""
        calls = []

        def parser(text, **kwargs):
            calls.append(text)
            from src.domain import tire_query

            return tire_query.ParsedQuery()

        with mock.patch.object(tire_search, "_multi_search", _fake_multi_search()), mock.patch.object(
            tire_search, "facets_config", return_value=[]
        ), mock.patch.object(tire_search, "brand_names", return_value=frozenset()), mock.patch.object(
            tire_search, "tread_category_codes", return_value=frozenset()
        ), mock.patch.object(
            tire_search.tires_index, "is_configured", return_value=True
        ):
            tire_search.search(q="ridge grappler", filters={}, parse_query_fn=parser)

        self.assertEqual(calls, ["ridge grappler"])


class RelaxationTests(SimpleTestCase):
    databases = []

    def _search_with(self, *, parsed_filters, totals):
        """Run a search whose tires index returns ``totals`` on successive calls."""
        from src.domain import tire_query

        sequence = list(totals)
        seen_filters = []

        def multi_search(*, text_query, filter_expression, sort_spec, tires_limit, parts_limit, offset):
            seen_filters.append(filter_expression)
            return _response(total=sequence.pop(0)), None

        def parser(text, **kwargs):
            return tire_query.ParsedQuery(filters=dict(parsed_filters), residue="", matched={})

        with mock.patch.object(tire_search, "_multi_search", multi_search), mock.patch.object(
            tire_search, "facets_config", return_value=[]
        ), mock.patch.object(tire_search, "brand_names", return_value=frozenset()), mock.patch.object(
            tire_search, "tread_category_codes", return_value=frozenset()
        ), mock.patch.object(
            tire_search.tires_index, "is_configured", return_value=True
        ):
            result = tire_search.search(q="anything", filters=None, parse_query_fn=parser)
        return result, seen_filters

    def test_zero_hits_on_a_parsed_query_relaxes_once(self):
        result, seen = self._search_with(
            parsed_filters={"rim_diameter_in": 18.0, "tread_category": "MT"}, totals=[0, 5]
        )
        self.assertEqual(len(seen), 2)
        self.assertEqual(result["interpretation"]["relaxed"]["dropped"], "tread_category")
        self.assertNotIn("tread_category", result["applied_filters"])
        # The dimension survives: a different size is not a near miss.
        self.assertIn("rim_diameter_in", result["applied_filters"])

    def test_it_retries_only_once(self):
        # totals: initial parse (0), the one relaxation retry (0), then the zero-result facets
        # fallback's own extra _multi_search call (0) -- see FacetFallbackTests for that path in
        # isolation.
        result, seen = self._search_with(
            parsed_filters={"rim_diameter_in": 18.0, "tread_category": "MT", "brand_name": "NITTO"},
            totals=[0, 0, 0],
        )
        self.assertEqual(len(seen), 3)
        self.assertEqual(result["total"], 0)

    def test_no_relaxation_when_only_dimensions_remain(self):
        # totals: initial parse (0), then the zero-result facets fallback's own extra call (0) --
        # no relaxation call in between since only a NEVER_RELAX field remains.
        result, seen = self._search_with(parsed_filters={"rim_diameter_in": 18.0}, totals=[0, 0])
        self.assertEqual(len(seen), 2)
        self.assertIsNone(result["interpretation"]["relaxed"])

    def test_user_set_filters_are_never_relaxed(self):
        # Relaxation applies to what the server inferred, never to what the user chose.
        with mock.patch.object(tire_search, "_multi_search", _fake_multi_search()), mock.patch.object(
            tire_search, "facets_config", return_value=[]
        ), mock.patch.object(tire_search.tires_index, "is_configured", return_value=True):
            result = tire_search.search(q="", filters={"tread_category": "MT"}, parse_query_fn=_ExplodingParser())

        self.assertIsNone(result["interpretation"]["relaxed"])
        self.assertEqual(result["applied_filters"], {"tread_category": "MT"})


class ValidationTests(SimpleTestCase):
    databases = []

    def test_unknown_filter_key_raises_rather_than_being_dropped(self):
        with mock.patch.object(tire_search.tires_index, "is_configured", return_value=True):
            with self.assertRaises(tire_filters.UnknownFilterField):
                tire_search.search(q="", filters={"colour": "red"}, parse_query_fn=_ExplodingParser())

    def test_unknown_sort_is_rejected(self):
        with mock.patch.object(tire_search.tires_index, "is_configured", return_value=True):
            with self.assertRaises(tire_search.SearchError):
                tire_search.search(
                    q="", filters={"rim_diameter_in": 18}, sort="price_asc", parse_query_fn=_ExplodingParser()
                )

    def test_limit_is_clamped(self):
        with mock.patch.object(tire_search, "_multi_search", _fake_multi_search()) as _, mock.patch.object(
            tire_search, "facets_config", return_value=[]
        ), mock.patch.object(tire_search.tires_index, "is_configured", return_value=True):
            result = tire_search.search(
                q="", filters={"rim_diameter_in": 18}, limit=10_000, parse_query_fn=_ExplodingParser()
            )
        self.assertEqual(result["limit"], tire_search.MAX_LIMIT)


class ReferenceCacheTests(SimpleTestCase):
    databases = []

    def test_loader_runs_once_until_invalidated(self):
        calls = []

        cached = tire_search._Cached(lambda: calls.append(1) or frozenset(["A"]))
        cached.get()
        cached.get()
        self.assertEqual(len(calls), 1)

        cached.invalidate()
        cached.get()
        self.assertEqual(len(calls), 2)

    def test_expired_read_returns_stale_value_without_blocking(self):
        """
        A request landing right after TTL expiry must never pay the reload cost itself -- that
        was exactly the production bug (2026-08-26): a slow reload under host pressure blocked
        the request long enough to trip gunicorn's worker timeout. An expired get() should hand
        back the stale value immediately and refresh on a background thread instead.
        """
        loader_may_return = threading.Event()
        loader_started = threading.Event()
        second_call_done = threading.Event()
        calls = []

        def slow_loader():
            calls.append(1)
            if len(calls) == 1:
                return "value-1"
            loader_started.set()
            loader_may_return.wait(timeout=5)
            second_call_done.set()
            return "value-2"

        # A long ttl: once expired, this test drives the refresh manually via the events above
        # rather than by waiting out a real TTL again, so a slow test runner can't make a second
        # background refresh race the assertions below.
        cached = tire_search._Cached(slow_loader, ttl=1000)
        first = cached.get()
        self.assertEqual(first, "value-1")

        cached._loaded_at -= 2000  # force the next get() to see it as expired
        second = cached.get()  # must return instantly, not block on the reload
        self.assertEqual(second, "value-1", "expired get() must return the stale value, not block")

        self.assertTrue(loader_started.wait(timeout=1), "background refresh never started")
        loader_may_return.set()
        self.assertTrue(second_call_done.wait(timeout=1), "background refresh never landed")
        self.assertEqual(cached._value, "value-2")


class ModeRoutingTests(SimpleTestCase):
    """
    The router. A query that parses into structure is a tire query; anything else falls through
    to the parts index so a part number behaves exactly as it does today instead of dead-ending
    on an empty tire page.
    """

    databases = []

    def _run(self, *, parsed_filters, tires_total, parts_total, mode=None, q="anything"):
        from src.domain import tire_query

        seen = []

        def multi_search(**kwargs):
            seen.append(kwargs)
            return (
                _response(hits=[{"id": 1}] if tires_total else [], total=tires_total),
                _response(hits=[{"id": 2}] if parts_total else [], total=parts_total),
            )

        def parser(text, **kwargs):
            return tire_query.ParsedQuery(filters=dict(parsed_filters), residue=text, matched={})

        with mock.patch.object(tire_search, "_multi_search", multi_search), mock.patch.object(
            tire_search, "facets_config", return_value=[]
        ), mock.patch.object(tire_search, "brand_names", return_value=frozenset()), mock.patch.object(
            tire_search, "tread_category_codes", return_value=frozenset()
        ), mock.patch.object(
            tire_search.tires_index, "is_configured", return_value=True
        ):
            return tire_search.search(q=q, filters=None, mode=mode, parse_query_fn=parser), seen

    def test_a_structural_parse_routes_to_tires(self):
        result, _ = self._run(parsed_filters={"rim_diameter_in": 18.0}, tires_total=12, parts_total=900)
        # Even with far more parts hits, a parsed size is unambiguously a tire query.
        self.assertEqual(result["mode"], tire_search.MODE_TIRES)

    def test_nothing_parsed_and_no_tire_hits_falls_back_to_parts(self):
        result, _ = self._run(parsed_filters={}, tires_total=0, parts_total=40, q="bed mat tacoma")
        self.assertEqual(result["mode"], tire_search.MODE_PARTS)
        self.assertEqual(result["total"], 40)

    def test_nothing_parsed_but_tires_wins_on_hits(self):
        # "ridge grappler" is a model name: no structure, but the tire index knows it.
        result, _ = self._run(parsed_filters={}, tires_total=211, parts_total=3, q="ridge grappler")
        self.assertEqual(result["mode"], tire_search.MODE_TIRES)

    def test_tires_wins_a_tie(self):
        # Tires wins whenever it has any real hits at all, ties included -- a coincidental
        # digit-substring match count in the much larger parts index is not a reason to prefer
        # it over a genuine tire hit (confirmed live: "275/55" text-matched 8 tires vs 4381
        # parts purely by substring overlap, and used to lose on count alone).
        result, _ = self._run(parsed_filters={}, tires_total=5, parts_total=5)
        self.assertEqual(result["mode"], tire_search.MODE_TIRES)

    def test_parts_wins_when_tires_has_far_fewer_hits(self):
        # Even a large gap in parts' favor does not flip it back -- tires only loses when it is
        # genuinely empty.
        result, _ = self._run(parsed_filters={}, tires_total=8, parts_total=4381, q="275/55")
        self.assertEqual(result["mode"], tire_search.MODE_TIRES)

    def test_explicit_mode_overrides_the_router(self):
        result, _ = self._run(parsed_filters={}, tires_total=0, parts_total=40, mode=tire_search.MODE_TIRES)
        self.assertEqual(result["mode"], tire_search.MODE_TIRES)

    def test_unknown_mode_is_rejected(self):
        with mock.patch.object(tire_search.tires_index, "is_configured", return_value=True):
            with self.assertRaises(tire_search.SearchError):
                tire_search.search(q="x", mode="wheels", parse_query_fn=_ExplodingParser())

    def test_exactly_one_round_trip(self):
        # Both indexes in a single multi-search, not two sequential calls.
        _, seen = self._run(parsed_filters={"rim_diameter_in": 18.0}, tires_total=12, parts_total=3)
        self.assertEqual(len(seen), 1)

    def test_no_pricing_is_returned(self):
        # Prices come from POST /parts/bulk-pricing/, not from here.
        result, _ = self._run(parsed_filters={"rim_diameter_in": 18.0}, tires_total=1, parts_total=0)
        for row in result["results"]:
            self.assertNotIn("offers", row)
        self.assertNotIn("hydrate_ms", result["timing"])


class TabsAndChipsTests(SimpleTestCase):
    databases = []

    def test_tabs_are_hidden_when_only_one_index_has_hits(self):
        # A part-number search must render no tab strip, so the page looks like it does today.
        self.assertEqual(tire_search._build_tabs(tires_total=0, parts_total=40, active="parts"), [])
        self.assertEqual(tire_search._build_tabs(tires_total=12, parts_total=0, active="tires"), [])

    def test_tabs_appear_when_both_have_hits_and_mark_the_active_one(self):
        tabs = tire_search._build_tabs(tires_total=12, parts_total=40, active="tires")
        self.assertEqual([t["mode"] for t in tabs], ["tires", "parts"])
        self.assertEqual([t["count"] for t in tabs], [12, 40])
        self.assertEqual([t["active"] for t in tabs], [True, False])

    def test_chips_are_built_from_applied_filters(self):
        facets = [
            {
                "field": "tread_category",
                "label": "Tread type",
                "widget": "multiselect",
                "unit": None,
                "collapse_after": 8,
                "value_labels": {"MT": "Mud terrain"},
            }
        ]
        with mock.patch.object(tire_search, "facets_config", return_value=facets):
            chips = tire_search._build_chips({"tread_category": "MT", "rim_diameter_in": 18.0})
        by_field = {chip["field"]: chip for chip in chips}
        # Label and value label come from facet_config, so a chip and its facet cannot disagree.
        self.assertEqual(by_field["tread_category"]["label"], "Tread type")
        self.assertEqual(by_field["tread_category"]["display"], "Mud terrain")
        # A field with no facet row still gets a readable fallback label.
        self.assertEqual(by_field["rim_diameter_in"]["label"], "Rim Diameter In")
        self.assertTrue(all(chip["removable"] for chip in chips))

    def test_chip_display_for_lists_ranges_and_booleans(self):
        with mock.patch.object(tire_search, "facets_config", return_value=[]):
            chips = {
                c["field"]: c["display"]
                for c in tire_search._build_chips(
                    {"load_range": ["D", "E"], "overall_diameter_in": {"min": 32, "max": 34}, "is_3pmsf": True}
                )
            }
        self.assertEqual(chips["load_range"], "D, E")
        self.assertEqual(chips["overall_diameter_in"], "32-34")
        self.assertEqual(chips["is_3pmsf"], "Yes")


def _facet_row(field, **overrides):
    """A ``facet_config`` row as the loader reads it. Every column set explicitly: a Mock that
    auto-creates ``min_distinct_values`` hands the loader a Mock where it expects an int."""
    values = {
        "field": field,
        "label": field.replace("_", " ").title(),
        "widget": "multiselect",
        "collapse_after": 8,
        "unit": None,
        "value_labels": None,
        "value_order": "count",
        "min_distinct_values": 1,
        "requires_filter_on": None,
        "requires_true_value": False,
    }
    values.update(overrides)
    return mock.Mock(**values)


def _shaped_facet(field, **overrides):
    """A loaded facet config entry -- what ``facets_config()`` returns, not a DB row."""
    entry = {
        "field": field,
        "label": field.replace("_", " ").title(),
        "widget": "multiselect",
        "collapse_after": 8,
        "unit": None,
        "value_labels": {},
        "value_order": "count",
        "value_sequence": [],
        "min_distinct_values": 1,
        "requires_filter_on": None,
        "requires_true_value": False,
    }
    entry.update(overrides)
    return entry


class FacetLabelTests(SimpleTestCase):
    databases = []

    def _load(self, rows, vocabularies):
        with mock.patch.object(tire_search, "_facet_vocabularies", return_value=vocabularies), mock.patch(
            "src.models.FacetConfig"
        ) as facet_model:
            facet_model.objects.filter.return_value.order_by.return_value = rows
            return tire_search._load_facets_config()

    def test_tread_category_labels_come_from_the_tread_category_table(self):
        """
        The labels live in ``tread_category`` -- it is the FK target and the LLM's constraint --
        so facet_config must not carry a second copy that can drift from it.
        """
        facets = self._load(
            [_facet_row("tread_category", label="Tread type")],
            {"tread_category": {"labels": {"AT": "All Terrain"}, "sequence": ["AT"]}},
        )
        self.assertEqual(facets[0]["value_labels"]["AT"], "All Terrain")
        self.assertEqual(facets[0]["value_sequence"], ["AT"])

    def test_an_explicit_value_label_overrides_the_table(self):
        facets = self._load(
            [_facet_row("tread_category", value_labels={"AT": "All-Terrain (custom)"})],
            {"tread_category": {"labels": {"AT": "All Terrain"}, "sequence": []}},
        )
        self.assertEqual(facets[0]["value_labels"]["AT"], "All-Terrain (custom)")

    def test_visibility_rules_travel_with_the_facet(self):
        facets = self._load(
            [_facet_row("overall_diameter_in", widget="range", requires_filter_on="rim_diameter_in")],
            {},
        )
        self.assertEqual(facets[0]["requires_filter_on"], "rim_diameter_in")


class LoadRangeVocabularyTests(SimpleTestCase):
    """``E`` and ``XL`` are two vocabularies in one column and are labelled differently."""

    databases = []

    def test_letter_ranges_carry_their_ply_equivalence_and_passenger_ranges_do_not(self):
        rows = [
            mock.Mock(load_range="XL", ply_rating=None),
            mock.Mock(load_range="E", ply_rating=10),
            mock.Mock(load_range="ZZ", ply_rating=None),
        ]
        with mock.patch("src.models.TireLoadRange") as model:
            model.objects.order_by.return_value = rows
            labels, sequence = tire_search._load_range_vocabulary()
        self.assertEqual(labels["E"], "E \u2014 10 ply")
        self.assertEqual(labels["XL"], "XL \u2014 Extra load")
        # No ply rating and no passenger expansion: the bare code, never an invented "4 ply".
        self.assertEqual(labels["ZZ"], "ZZ")
        self.assertEqual(sequence, ["XL", "E", "ZZ"])


class SpeedRatingVocabularyTests(SimpleTestCase):
    databases = []

    def test_the_code_carries_its_speed_and_the_table_carries_the_order(self):
        rows = [
            mock.Mock(code="U", max_speed_mph=124),
            mock.Mock(code="H", max_speed_mph=130),
            mock.Mock(code="(Y)", max_speed_mph=None),
        ]
        with mock.patch("src.models.TireSpeedRating") as model:
            model.objects.order_by.return_value = rows
            labels, sequence = tire_search._speed_rating_vocabulary()
        self.assertEqual(labels["H"], "H \u2014 130 mph")
        # H is 130 mph and belongs after U, which is why the table orders this and not the client.
        self.assertEqual(sequence, ["U", "H", "(Y)"])
        self.assertEqual(labels["(Y)"], "(Y)")


class FacetVisibilityTests(SimpleTestCase):
    """A control that cannot change the result set is worse than an absent one."""

    databases = []

    def _shape(self, config, distribution, **kwargs):
        with mock.patch.object(tire_search, "facets_config", return_value=config):
            return tire_search._shape_facets(distribution, **kwargs)

    def test_the_unknown_bucket_never_reaches_the_client(self):
        """The index writes "" for a NULL service_type on 38k of 47k tires."""
        shaped = self._shape(
            [_shaped_facet("service_type")],
            {"service_type": {"": 38199, "LT": 4200, "P": 3100}},
        )
        self.assertEqual([value["value"] for value in shaped[0]["values"]], ["LT", "P"])

    def test_a_facet_with_one_distinct_value_is_hidden_when_it_must_discriminate(self):
        config = [_shaped_facet("service_type", min_distinct_values=2)]
        self.assertEqual(self._shape(config, {"service_type": {"LT": 900}}), [])
        self.assertEqual(len(self._shape(config, {"service_type": {"LT": 900, "P": 5}})), 1)

    def test_overall_diameter_appears_only_once_a_wheel_size_scopes_it(self):
        config = [_shaped_facet("overall_diameter_in", widget="range", requires_filter_on="rim_diameter_in")]
        distribution = {"overall_diameter_in": {"31.6": 12, "33.0": 4}}
        self.assertEqual(self._shape(config, distribution), [])
        shaped = self._shape(config, distribution, applied={"rim_diameter_in": 17})
        self.assertEqual(shaped[0]["field"], "overall_diameter_in")

    def test_a_range_facet_carries_the_min_and_max_a_slider_needs(self):
        config = [_shaped_facet("overall_diameter_in", widget="range")]
        distribution = {"overall_diameter_in": {"31.6": 12, "33.0": 4}}
        shaped = self._shape(config, distribution, stats={"overall_diameter_in": {"min": 31.6, "max": 33.0}})
        self.assertEqual(shaped[0]["stats"], {"min": 31.6, "max": 33.0})
        # ...and still does when the engine sends no stats at all.
        shaped = self._shape(config, distribution)
        self.assertEqual(shaped[0]["stats"], {"min": 31.6, "max": 33.0})

    def test_a_toggle_that_needs_a_true_row_is_hidden_when_nothing_is_rated(self):
        config = [_shaped_facet("is_3pmsf", widget="toggle", requires_true_value=True)]
        self.assertEqual(self._shape(config, {"is_3pmsf": {"false": 1200}}), [])
        self.assertEqual(len(self._shape(config, {"is_3pmsf": {"false": 1200, "true": 8}})), 1)

    def test_in_stock_is_not_hidden_by_the_same_rule(self):
        """It is an always-shown facet: the user may be looking at what is out of stock."""
        config = [_shaped_facet("in_stock", widget="toggle")]
        self.assertEqual(len(self._shape(config, {"in_stock": {"false": 40}})), 1)


class FacetFieldGuardTests(SimpleTestCase):
    """
    Meilisearch rejects the whole multi-search when one requested facet is not filterable, so the
    window between a deploy and ``index_tires_meilisearch --setup`` must cost one control, not the
    search endpoint.
    """

    databases = []

    def test_a_facet_the_live_index_cannot_face_on_is_skipped(self):
        config = [_shaped_facet("brand_name"), _shaped_facet("oe_marking")]
        with mock.patch.object(tire_search, "facets_config", return_value=config), mock.patch.object(
            tire_search, "index_filterable_fields", return_value=frozenset(["brand_name"])
        ):
            self.assertEqual(tire_search._facet_fields(), ["brand_name"])

    def test_an_unreachable_index_falls_back_to_this_codes_own_list(self):
        with mock.patch.object(tire_search.tires_index, "live_filterable_attributes", return_value=None):
            fields = tire_search._load_index_filterable_fields()
        self.assertEqual(fields, frozenset(tire_search.tires_index.FILTERABLE_ATTRIBUTES))


class FacetValueTypeTests(SimpleTestCase):
    """
    Meilisearch hands back every facet key as a string and the client sends it straight back as a
    filter, so an untyped value renders fine and filters nothing.
    """

    databases = []

    def _shape(self, config, distribution):
        with mock.patch.object(tire_search, "facets_config", return_value=config):
            return tire_search._shape_facets(distribution)

    def test_a_numeric_facet_returns_numbers(self):
        shaped = self._shape(
            [_shaped_facet("rim_diameter_in"), _shaped_facet("distributor_ids")],
            {"rim_diameter_in": {"18.0": 4}, "distributor_ids": {"1": 9}},
        )
        self.assertEqual(shaped[0]["values"][0]["value"], 18.0)
        self.assertEqual(shaped[1]["values"][0]["value"], 1)
        self.assertIsInstance(shaped[1]["values"][0]["value"], int)

    def test_a_boolean_facet_returns_booleans(self):
        shaped = self._shape([_shaped_facet("in_stock", widget="toggle")], {"in_stock": {"true": 12}})
        self.assertIs(shaped[0]["values"][0]["value"], True)

    def test_a_text_facet_is_left_alone_even_when_it_looks_numeric(self):
        """A brand called "911" is a string in the index and must stay one."""
        shaped = self._shape([_shaped_facet("brand_name")], {"brand_name": {"911": 3}})
        self.assertEqual(shaped[0]["values"][0]["value"], "911")

    def test_the_label_still_resolves_from_the_string_key(self):
        shaped = self._shape(
            [_shaped_facet("distributor_ids", value_labels={"1": "Turn 14"})],
            {"distributor_ids": {"1": 9}},
        )
        self.assertEqual(shaped[0]["values"][0]["label"], "Turn 14")


class FacetOrderTests(SimpleTestCase):
    databases = []

    def _shape(self, config, distribution):
        with mock.patch.object(tire_search, "facets_config", return_value=config):
            return tire_search._shape_facets(distribution)

    def test_wheel_sizes_read_as_sizes_not_as_a_popularity_chart(self):
        shaped = self._shape(
            [_shaped_facet("rim_diameter_in", value_order="numeric")],
            {"rim_diameter_in": {"20.0": 900, "16.0": 120, "17.0": 400, "8.0": 3}},
        )
        self.assertEqual([value["value"] for value in shaped[0]["values"]], [8.0, 16.0, 17.0, 20.0])

    def test_speed_ratings_follow_the_table_never_the_alphabet(self):
        """H is 130 mph and belongs between U and V."""
        shaped = self._shape(
            [_shaped_facet("speed_rating", value_order="vocabulary", value_sequence=["S", "T", "U", "H", "V"])],
            {"speed_rating": {"V": 5, "H": 90, "T": 300, "S": 12}},
        )
        self.assertEqual([value["value"] for value in shaped[0]["values"]], ["S", "T", "H", "V"])

    def test_an_unknown_value_sorts_last_rather_than_breaking_the_facet(self):
        shaped = self._shape(
            [_shaped_facet("speed_rating", value_order="vocabulary", value_sequence=["S", "T"])],
            {"speed_rating": {"ZZ": 1, "T": 300}},
        )
        self.assertEqual([value["value"] for value in shaped[0]["values"]], ["T", "ZZ"])

    def test_brands_still_lead_with_the_most_results(self):
        shaped = self._shape(
            [_shaped_facet("brand_name")],
            {"brand_name": {"NITTO": 40, "TOYO": 900}},
        )
        self.assertEqual([value["value"] for value in shaped[0]["values"]], ["TOYO", "NITTO"])


class FacetFallbackTests(SimpleTestCase):
    """
    A zero-hit tires search still needs a populated filter panel -- Meilisearch's own
    facetDistribution is empty whenever a query matches nothing, which is exactly when a user
    most needs to see what to relax. See tire_search.search, step 5.
    """

    databases = []

    _FACET_CONFIG = [_shaped_facet("brand_name", label="Brand")]

    def test_zero_hits_refetches_facets_with_the_filter_dropped(self):
        seen = []

        def multi_search(*, text_query, filter_expression, sort_spec, tires_limit, parts_limit, offset):
            seen.append({"text_query": text_query, "filter_expression": filter_expression})
            if len(seen) == 1:
                return _response(total=0), None  # the real, filtered search: nothing matches
            return _response(total=0, facets={"brand_name": {"NITTO": 42}}), None  # the fallback

        with mock.patch.object(tire_search, "_multi_search", multi_search), mock.patch.object(
            tire_search, "facets_config", return_value=self._FACET_CONFIG
        ), mock.patch.object(tire_search.tires_index, "is_configured", return_value=True):
            result = tire_search.search(q="nitto", filters={"rim_diameter_in": 999}, parse_query_fn=_ExplodingParser())

        self.assertEqual(len(seen), 2)
        # The fallback call drops the filter but keeps searching the same text.
        self.assertEqual(seen[1]["filter_expression"], "")
        self.assertEqual(seen[1]["text_query"], seen[0]["text_query"])
        # And the response actually carries the fallback's facet values, not an empty panel.
        self.assertEqual(result["total"], 0)
        self.assertEqual(result["facets"][0]["field"], "brand_name")
        self.assertEqual(result["facets"][0]["values"][0]["count"], 42)

    def test_nonzero_hits_do_not_trigger_a_second_call(self):
        seen = []

        def multi_search(*, text_query, filter_expression, sort_spec, tires_limit, parts_limit, offset):
            seen.append(1)
            return _response(hits=[{"id": 1}], total=5, facets={"brand_name": {"NITTO": 5}}), None

        with mock.patch.object(tire_search, "_multi_search", multi_search), mock.patch.object(
            tire_search, "facets_config", return_value=self._FACET_CONFIG
        ), mock.patch.object(tire_search.tires_index, "is_configured", return_value=True):
            result = tire_search.search(q="nitto", filters={"rim_diameter_in": 18}, parse_query_fn=_ExplodingParser())

        self.assertEqual(len(seen), 1)
        self.assertEqual(result["total"], 5)
