"""
``POST /api/search``: parse, compile a filter, route to an index, search, respond.

The handler order is load-bearing and is the thing most likely to be broken by a later change:

  1. **``q`` is parsed only when the request carries no filters.** Once the client has sent
     filters, the filters are the truth. Re-parsing ``q`` on a refinement is the classic bug in
     this kind of search -- the user removes the "Mud terrain" chip, the server re-reads "mud
     terrain" out of query text they never edited, and the chip comes straight back. There is an
     explicit test asserting the parser is not called when filters are present.
  2. Build the Meilisearch expression, whitelisted against the index's own filterable attributes.
  3. One multi-search: tires in full, parts at ``limit 1`` purely for a tab count.
  4. If a *parsed* query returned nothing, relax one filter and retry once. Never relax filters
     the user set themselves, and never relax a dimension.
  5. Shape the index documents into the response. **No database read on the hit path.**

**No pricing anywhere in this module.** Cost is negotiated per company, so there is no number
that is correct for every viewer, and it is neither indexed nor fetched here. The client posts
the ids from a page of results to ``POST /parts/bulk-pricing/`` -- the existing endpoint built
for exactly that, and exempt from the detail-view billing limit. Keeping it out of this path is
also what makes the endpoint fast: hydration was the most expensive step of the request when it
was here, and it was duplicating a call the client already makes.

The only database reads left are the cached reference values (tire brand names, the tread
vocabulary, the facet rail), refreshed at most once every few minutes.
"""
import logging
import threading
import time
import typing

from django.db import connection

from src.api.services import search_tabs
from src.domain import spec_line as spec_line_domain
from src.domain import tire_filters, tire_query, tire_spec_display, wheel_size
from src.search import meilisearch_client as parts_index
from src.search import tires_index, wheels_index

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[TIRE-SEARCH]"

DEFAULT_LIMIT = 50
MAX_LIMIT = 100

# API-facing mode names. These are what the client switches on, and they match the index each
# one is served from.
MODE_TIRES = "tires"
MODE_PARTS = "parts"

# facet_config keys tire rows under the singular. Kept as a named mapping rather than silently
# reusing the API string, so the two can differ without a mystery.
FACET_MODE_BY_SEARCH_MODE = {MODE_TIRES: "tire"}

# Sorts the API accepts, mapped to the index's sortable attributes. A whitelist rather than a
# pass-through: an unsortable attribute is a Meilisearch error, not a graceful degradation.
SORT_OPTIONS = {
    "diameter_asc": ["overall_diameter_in:asc"],
    "diameter_desc": ["overall_diameter_in:desc"],
    "rim_asc": ["rim_diameter_in:asc"],
    "load_desc": ["load_index:desc"],
    "speed_desc": ["speed_sort:desc"],
    "brand_asc": ["brand_name:asc"],
}


class SearchError(Exception):
    """Bad input from the client. The view turns this into a 400."""


# Reference data read on every request: the tire brand list, the tread vocabulary and the facet
# rail. All three change on the timescale of a catalog sync, not a request. Measured live
# 2026-08-26: normally ~1.6s of a 2.2s search, but under host memory pressure (concurrent Turn 14
# sweeps evicting Postgres's buffer cache) the same query took 21s -- long enough to trip
# gunicorn's worker timeout, kill the worker, and hand the next request to a fresh worker with an
# empty cache, repeating. Stale-while-revalidate (see _Cached.get) exists specifically so a slow
# refresh is never on a live request's critical path after the first one.
REFERENCE_CACHE_TTL_SECONDS = 1800


class _Cached:
    """
    One lazily-loaded, TTL-expiring reference value -- stale-while-revalidate.

    Only the very first load (no value yet) blocks the caller; that cost is normally absorbed by
    warm_reference_caches() firing in the background at process start (see wsgi.py) well before
    real traffic arrives; a request that beats it there simply pays the cost itself, once. Every
    later expiry serves the stale value immediately and refreshes on a background thread, so a
    slow reload -- whatever the reason -- never blocks a request again.
    """

    def __init__(self, loader: typing.Callable[[], typing.Any], ttl: float = REFERENCE_CACHE_TTL_SECONDS):
        self._loader = loader
        self._ttl = ttl
        self._value: typing.Any = None
        self._loaded_at = 0.0
        self._lock = threading.Lock()
        self._refreshing = False

    def get(self) -> typing.Any:
        now = time.monotonic()
        if self._value is not None and now - self._loaded_at < self._ttl:
            return self._value
        if self._value is None:
            with self._lock:
                # Re-check: another thread may have loaded while this one waited.
                if self._value is None:
                    self._value = self._loader()
                    self._loaded_at = time.monotonic()
            return self._value
        self._refresh_in_background()
        return self._value

    def _refresh_in_background(self) -> None:
        with self._lock:
            if self._refreshing:
                return
            self._refreshing = True

        def _run() -> None:
            try:
                value = self._loader()
                self._value = value
                self._loaded_at = time.monotonic()
            except Exception:
                logger.exception("%s reference cache refresh failed; keeping the stale value.", _LOG_PREFIX)
            finally:
                with self._lock:
                    self._refreshing = False

        threading.Thread(target=_run, daemon=True).start()

    def invalidate(self) -> None:
        """Drop the cached value. Call after a reindex or a facet_config edit."""
        with self._lock:
            self._value = None
            self._loaded_at = 0.0
            self._refreshing = False


def allowed_filter_fields() -> typing.AbstractSet[str]:
    return frozenset(tires_index.FILTERABLE_ATTRIBUTES)


def _load_brand_names() -> typing.AbstractSet[str]:
    """
    Brand names that actually have an indexable tire.

    Scoped to tire brands rather than all ~3,400 brands so that a query like "titan" is not
    matched against a brand that sells no tires and then filtered to zero results.
    """
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT DISTINCT b.name FROM brands b "
            "JOIN master_parts mp ON mp.brand_id = b.id "
            "JOIN tire_specs ts ON ts.master_part_id = mp.id "
            "WHERE mp.product_type = 'tire' AND b.name IS NOT NULL"
        )
        return frozenset(row[0] for row in cursor.fetchall())


def _load_tread_category_codes() -> typing.AbstractSet[str]:
    from src import models as src_models

    return frozenset(src_models.TreadCategory.objects.values_list("code", flat=True))


def _tread_category_vocabulary() -> typing.Tuple[typing.Dict[str, str], typing.List[str]]:
    """
    Code -> display label, plus the codes in ``sort_order`` (terrain first, because that is what
    truck buyers filter on).

    One query for both: the labels and the order come off the same 27 rows, and this runs on a
    cache refresh where a second round trip buys nothing.
    """
    from src import models as src_models

    labels, sequence = {}, []
    for row in src_models.TreadCategory.objects.order_by("sort_order"):
        labels[row.code] = row.label
        sequence.append(row.code)
    return labels, sequence


def _load_range_vocabulary() -> typing.Tuple[typing.Dict[str, str], typing.List[str]]:
    """
    ``E`` -> ``E - 10 ply``, ``XL`` -> ``XL - Extra load``, in the table's own order.

    Two vocabularies share one column and they are labelled differently on purpose: an LT letter
    range means a ply equivalence and a passenger range does not have one at all. Writing
    "XL (4 ply)" would be inventing a number -- see ``TireLoadRange``.
    """
    from src import models as src_models

    labels: typing.Dict[str, str] = {}
    sequence: typing.List[str] = []
    for row in src_models.TireLoadRange.objects.order_by("sort_order"):
        code = row.load_range
        sequence.append(code)
        if row.ply_rating:
            labels[code] = "{} \u2014 {} ply".format(code, row.ply_rating)
        else:
            passenger = tire_spec_display.LOAD_RANGE_LABELS.get(code)
            labels[code] = "{} \u2014 {}".format(code, passenger) if passenger else code
    return labels, sequence


def _speed_rating_vocabulary() -> typing.Tuple[typing.Dict[str, str], typing.List[str]]:
    """
    ``H`` -> ``H - 130 mph``, in speed order.

    Both halves matter and for the same reason: the code alone is unreadable, and speed order is
    the only order that reads as anything but random, since H is 130 mph and belongs between U and
    V. Alphabetical is a wrong answer here, not a neutral one -- and a rail sorted correctly but
    labelled "H, Q, R" still leaves the user to know that off the top of their head.
    """
    from src import models as src_models

    labels, sequence = {}, []
    for row in src_models.TireSpeedRating.objects.order_by("sort_order"):
        sequence.append(row.code)
        labels[row.code] = "{} \u2014 {} mph".format(row.code, row.max_speed_mph) if row.max_speed_mph else row.code
    return labels, sequence


def _distributor_labels() -> typing.Dict[str, str]:
    """Provider id -> name. The facet filters on ids (stable) and shows names (not)."""
    from src import models as src_models

    return {str(row.id): row.name for row in src_models.Providers.objects.all() if row.name}


def _facet_vocabularies() -> typing.Dict[str, typing.Dict[str, typing.Any]]:
    """
    Per-field ``{"labels": ..., "sequence": ...}`` for every facet whose vocabulary already lives
    somewhere else.

    Read at facet-config load time (cached for 30 minutes with everything else), never per
    request. The point is single-sourcing: ``tread_category.label`` is the FK target and the LLM's
    constraint, ``TireLoadRange`` is what resolves ply ratings, ``providers`` is what names a
    distributor, and ``VEHICLE_CLASS_LABELS`` is what the part detail panel renders. A second copy
    of any of them inside ``facet_config.value_labels`` would drift the day one is edited.
    """
    from src import models as src_models

    load_range_labels, load_range_sequence = _load_range_vocabulary()
    tread_labels, tread_sequence = _tread_category_vocabulary()
    speed_labels, speed_sequence = _speed_rating_vocabulary()
    return {
        "tread_category": {"labels": tread_labels, "sequence": tread_sequence},
        "load_range": {"labels": load_range_labels, "sequence": load_range_sequence},
        "speed_rating": {"labels": speed_labels, "sequence": speed_sequence},
        "vehicle_class": {"labels": dict(tire_spec_display.VEHICLE_CLASS_LABELS), "sequence": []},
        "tier": {"labels": dict(src_models.TireSpec.TIER_CHOICES), "sequence": []},
        "distributor_ids": {"labels": _distributor_labels(), "sequence": []},
    }


def _load_facets_config() -> typing.List[typing.Dict[str, typing.Any]]:
    """
    The facet rail definition, server-owned so it changes without a client deploy.

    Labels and value order are merged in from the reference tables here (see
    ``_facet_vocabularies``); a ``value_labels`` entry on the row still wins, so a facet can
    always override. The visibility rules travel with the facet rather than being applied here --
    they depend on the result set, which this cached function has never seen.
    """
    from src import models as src_models

    vocabularies = _facet_vocabularies()

    facets = []
    for row in src_models.FacetConfig.objects.filter(mode=FACET_MODE_BY_SEARCH_MODE[MODE_TIRES]).order_by("sort_order"):
        vocabulary = vocabularies.get(row.field) or {}
        value_labels = {**(vocabulary.get("labels") or {}), **dict(row.value_labels or {})}
        facets.append(
            {
                "field": row.field,
                "label": row.label,
                "widget": row.widget,
                "collapse_after": row.collapse_after,
                "unit": row.unit,
                "value_labels": value_labels,
                "value_order": getattr(row, "value_order", None) or "count",
                "value_sequence": list(vocabulary.get("sequence") or []),
                "min_distinct_values": getattr(row, "min_distinct_values", None) or 1,
                "requires_filter_on": getattr(row, "requires_filter_on", None) or None,
                "requires_true_value": bool(getattr(row, "requires_true_value", False)),
            }
        )
    return facets


def _load_index_filterable_fields() -> typing.AbstractSet[str]:
    """
    What the live index accepts as a facet. Falls back to this code's own list when the index
    cannot be asked -- if Meilisearch is unreachable the search is failing on its own merits, and
    a fallback that hides every facet would just make that harder to diagnose.
    """
    live = tires_index.live_filterable_attributes()
    return live if live is not None else frozenset(tires_index.FILTERABLE_ATTRIBUTES)


_BRAND_NAMES = _Cached(_load_brand_names)
_TREAD_CATEGORY_CODES = _Cached(_load_tread_category_codes)
_FACETS_CONFIG = _Cached(_load_facets_config)
_INDEX_FILTERABLE = _Cached(_load_index_filterable_fields)


def brand_names() -> typing.AbstractSet[str]:
    return _BRAND_NAMES.get()


def tread_category_codes() -> typing.AbstractSet[str]:
    return _TREAD_CATEGORY_CODES.get()


def facets_config() -> typing.List[typing.Dict[str, typing.Any]]:
    return _FACETS_CONFIG.get()


def index_filterable_fields() -> typing.AbstractSet[str]:
    return _INDEX_FILTERABLE.get()


def warm_reference_caches() -> None:
    """
    Populate all three reference caches now, in the calling thread.

    Called from wsgi.py on a background thread right after each gunicorn worker forks, so the
    one unavoidable blocking load per process happens before that worker takes real traffic
    instead of during a user's first request. Safe to call more than once (a concurrent request
    racing this just does its own first-load under the same lock, see _Cached.get).
    """
    for cache in (_BRAND_NAMES, _TREAD_CATEGORY_CODES, _FACETS_CONFIG, _INDEX_FILTERABLE):
        try:
            cache.get()
        except Exception:
            logger.exception("%s warm_reference_caches: failed to warm %s.", _LOG_PREFIX, cache._loader.__name__)


def invalidate_reference_cache() -> None:
    """Drop every cached reference value. Call after a reindex or a facet_config change."""
    for cache in (_BRAND_NAMES, _TREAD_CATEGORY_CODES, _FACETS_CONFIG, _INDEX_FILTERABLE):
        cache.invalidate()


def _facet_fields() -> typing.List[str]:
    """
    The fields to ask Meilisearch for counts on.

    Intersected with what the **live** index accepts, not with what this code believes it
    configured. Meilisearch rejects the *entire* multi-search when one requested facet is not
    filterable, so the gap between a deploy and ``index_tires_meilisearch --setup`` would take
    tire search down completely rather than hide one control -- and the same is true of any
    ``facet_config`` row naming a field the index has never had. Either way the facet is simply
    absent until the index catches up, which is the behaviour the FE spec promises.

    The live attribute set is cached with the other reference data (30 minutes,
    stale-while-revalidate), so this costs no HTTP call on the request path.
    """
    allowed = index_filterable_fields()
    fields, skipped = [], []
    for facet in facets_config():
        (fields if facet["field"] in allowed else skipped).append(facet["field"])
    if skipped:
        logger.warning(
            "%s not facetable on the live index yet, skipped: %s (run index_tires_meilisearch --setup)",
            _LOG_PREFIX,
            skipped,
        )
    return fields


def _hit_to_result(hit: typing.Mapping[str, typing.Any]) -> typing.Dict[str, typing.Any]:
    return {
        "id": hit["id"],
        "brand_id": hit.get("brand_id"),
        "brand_name": hit.get("brand_name") or "",
        "model_name": hit.get("model_name") or "",
        "sub_model": hit.get("sub_model") or "",
        "part_number": hit.get("part_number") or "",
        "gtin": hit.get("gtin") or "",
        "image_url": hit.get("image_url") or "",
        "size_display": hit.get("size_display") or "",
        # Composed from resolved structured fields, never from a distributor title.
        "spec_line": spec_line_domain.build_spec_line(hit),
        "tread_category": hit.get("tread_category") or "",
        "tread_category_label": hit.get("tread_category_label") or "",
        "vehicle_class": hit.get("vehicle_class") or "",
        "overall_diameter_in": hit.get("overall_diameter_in"),
        "rim_diameter_in": hit.get("rim_diameter_in"),
        "load_index": hit.get("load_index"),
        "max_load_lb": hit.get("max_load_lb"),
        "speed_rating": hit.get("speed_rating") or "",
        "max_speed_mph": hit.get("max_speed_mph"),
        "load_range": hit.get("load_range") or "",
        "ply_rating": hit.get("ply_rating"),
        "use_case_tags": hit.get("use_case_tags") or [],
        "in_stock": hit.get("in_stock", False),
        "available_qty": hit.get("available_qty", 0),
        "distributor_count": hit.get("distributor_count", 0),
        "distributor_names": hit.get("distributor_names") or [],
        # Tri-state: present only when known. The client must treat an absent key as "unknown",
        # not as false -- see tires_index._TRISTATE_FLAGS.
        **{flag: hit[flag] for flag in ("is_3pmsf", "is_ms", "is_run_flat", "is_studdable") if flag in hit},
    }


def search(
    *,
    q: str = "",
    filters: typing.Optional[typing.Mapping[str, typing.Any]] = None,
    mode: typing.Optional[str] = None,
    sort: typing.Optional[str] = None,
    limit: int = DEFAULT_LIMIT,
    offset: int = 0,
    parse_query_fn: typing.Callable[..., tire_query.ParsedQuery] = tire_query.parse_query,
) -> typing.Dict[str, typing.Any]:
    """
    One search request: parse, compile a filter, route, search, respond.

    **The endpoint is a router plus a filter compiler.** It does not reimplement search --
    Meilisearch still does that. What it adds is deciding *which index* the query belongs to and
    turning free text into numeric filters, which is the whole reason "275/70R18" stops returning
    thousands of fuzzy text matches.

    Mode is chosen, not assumed:

      * an explicit ``mode`` from the client always wins
      * ``filters`` present means the client is refining a tire result set -> tires
      * a query that parsed into structural filters -> tires
      * otherwise both indexes are searched and tires wins whenever it has any hits at all;
        parts is the fallback only when tires comes back genuinely empty. This is what keeps
        "bed mat tacoma" and "WET40451" behaving as parts searches (tires has zero hits for
        either) while a query like "275/55" -- real tire shape, but missing enough of the size
        for the parser to build a filter -- surfaces as tires instead of losing to a much larger
        coincidental digit-substring match count in the parts index.

    ``parse_query_fn`` is injected so a test can assert it is **not** called when ``filters`` is
    present -- the refinement bug, which a suite misses unless the assertion is explicit.
    """
    started = time.monotonic()
    if not tires_index.is_configured():
        raise SearchError("Search is not configured.")

    limit = max(1, min(int(limit or DEFAULT_LIMIT), MAX_LIMIT))
    offset = max(0, int(offset or 0))
    q = (q or "").strip()

    if mode is not None and mode not in (MODE_TIRES, MODE_PARTS):
        raise SearchError("Unknown mode {!r}. Allowed: {}, {}".format(mode, MODE_TIRES, MODE_PARTS))

    sort_spec = None
    if sort:
        if sort not in SORT_OPTIONS:
            raise SearchError("Unknown sort {!r}. Allowed: {}".format(sort, ", ".join(sorted(SORT_OPTIONS))))
        sort_spec = SORT_OPTIONS[sort]

    # ---- 1. parse ---------------------------------------------------------------------------
    parse_t0 = time.monotonic()
    interpretation: typing.Dict[str, typing.Any] = {"parsed": False, "matched": {}, "chips": [], "relaxed": None}
    parsed: typing.Optional[tire_query.ParsedQuery] = None

    if filters:
        # Refinement: the client owns the filters, and q is used verbatim as free text.
        tire_filters_dict = dict(filters)
        text_query = q
    else:
        parsed = parse_query_fn(q, brand_names=brand_names(), valid_categories=tread_category_codes())
        tire_filters_dict = dict(parsed.filters)
        # When a size parsed the tire search is a pure filter query: leaving the size in q would
        # let Meilisearch text-match it too and reintroduce exactly the noise the filter removed.
        text_query = parsed.residue
        interpretation["parsed"] = parsed.parsed_anything
        interpretation["matched"] = parsed.matched
    parse_ms = (time.monotonic() - parse_t0) * 1000

    # ---- 2. compile ------------------------------------------------------------------------
    filter_expression = tire_filters.build_filter(tire_filters_dict, allowed_filter_fields())

    # A query that produced structure, or a client that sent filters, is unambiguously about
    # tires; anything else has to be settled by asking both indexes.
    tires_is_certain = bool(filters) or bool(tire_filters_dict) or mode == MODE_TIRES

    # A wheel-shaped query routes itself, the same way a tire-shaped one does: "18x8 5x114.3" is a
    # wheel and nothing else. Handled by handing the whole request to the wheel service -- the rest
    # of this function shapes tire documents, so merely setting a mode here would return tire
    # results under a wheels label.
    #
    # Reached only once the tire parser has found nothing, and that order is load-bearing rather
    # than incidental. ATV tire sizes are written exactly like wheel sizes: "20x10-9" is a tire on
    # a 9-inch rim, and src.domain.wheel_size reads it as a 20x10 wheel just as readily. Letting
    # tires answer first keeps those with tires, and only a query tires cannot explain at all
    # reaches the wheel parser.
    if not tires_is_certain and mode is None and q and wheel_size.parse(q) is not None:
        from src.api.services import (
            wheel_search,  # local: keeps the module import graph acyclic
        )

        if wheels_index.is_configured():
            return wheel_search.search(q=q, filters={}, sort=None, limit=limit, offset=offset)

    resolved_mode = mode or (MODE_TIRES if tires_is_certain else None)

    # ---- 3. one multi-search ----------------------------------------------------------------
    search_t0 = time.monotonic()
    tires_limit = limit if resolved_mode != MODE_PARTS else 1
    parts_limit = 1 if resolved_mode == MODE_TIRES else limit
    tires_response, parts_response, wheels_response = _multi_search(
        text_query=text_query,
        filter_expression=filter_expression,
        sort_spec=sort_spec,
        tires_limit=tires_limit,
        parts_limit=parts_limit,
        offset=offset,
    )

    tires_total = tires_response.get("estimatedTotalHits", 0)
    parts_total = parts_response.get("estimatedTotalHits", 0) if parts_response is not None else 0
    wheels_total = wheels_response.get("estimatedTotalHits", 0) if wheels_response is not None else 0

    if resolved_mode is None:
        # Nothing structural parsed. Tires wins whenever it has any real hits at all -- a
        # coincidental digit-substring match in the (much larger) parts index outscoring a real
        # tire hit is worse than the reverse (confirmed live: "275/55" text-matched 8 tires but
        # 4381 parts purely by loose substring overlap, and used to lose to parts on count alone).
        # Parts is the fallback only when tires is genuinely empty.
        resolved_mode = MODE_TIRES if tires_total > 0 else MODE_PARTS

    # ---- 4. relaxation (parsed tire queries only) --------------------------------------------
    if resolved_mode == MODE_TIRES and tires_total == 0 and parsed is not None and parsed.parsed_anything:
        relaxation = tire_query.relax(tire_filters_dict)
        if relaxation is not None:
            dropped, reduced = relaxation
            tire_filters_dict = reduced
            filter_expression = tire_filters.build_filter(tire_filters_dict, allowed_filter_fields())
            tires_response, _, _ = _multi_search(
                text_query=text_query,
                filter_expression=filter_expression,
                sort_spec=sort_spec,
                tires_limit=limit,
                parts_limit=0,
                offset=offset,
            )
            tires_total = tires_response.get("estimatedTotalHits", 0)
            interpretation["relaxed"] = {
                "dropped": dropped,
                "message": "No exact matches, so the {} filter was dropped.".format(dropped.replace("_", " ")),
            }
    search_ms = (time.monotonic() - search_t0) * 1000

    # ---- 5. shape the response ---------------------------------------------------------------
    #
    # No pricing is fetched here. Cost is per company and already served by
    # ``POST /parts/bulk-pricing/``, which the client calls with the ids from this page -- that
    # endpoint exists precisely to hydrate a page of Meilisearch hits and is explicitly exempt
    # from the detail-view billing limit. Doing it here as well would duplicate that call and put
    # a Postgres round trip on the critical path of every search; measured against production it
    # was the single most expensive step of the request.
    if resolved_mode == MODE_TIRES:
        results = [_hit_to_result(hit) for hit in tires_response.get("hits", [])]
        total = tires_total
        applied = tire_filters_dict
        facet_distribution = tires_response.get("facetDistribution") or {}
        facet_stats = tires_response.get("facetStats") or {}
        if total == 0:
            # Meilisearch only computes facet counts from the documents a query actually
            # matches -- zero hits means an empty facetDistribution, which leaves the filter
            # panel with nothing to render right when the user most needs it to see what to
            # relax. Re-fetch facets for the same text query with the filter dropped (not the
            # whole unfiltered catalog) so counts still reflect "everything this text matches."
            facet_only, _, _ = _multi_search(
                text_query=text_query,
                filter_expression="",
                sort_spec=None,
                tires_limit=0,
                parts_limit=0,
                offset=0,
            )
            facet_distribution = facet_only.get("facetDistribution") or {}
            facet_stats = facet_only.get("facetStats") or {}
        facets = _shape_facets(facet_distribution, applied=applied, stats=facet_stats)
    else:
        results = [_parts_hit_to_result(hit) for hit in (parts_response or {}).get("hits", [])]
        total = parts_total
        # Parts facets are a later deliverable; the panel renders from whatever the server sends,
        # so an empty list is a valid answer rather than a broken client.
        facets = []
        applied = {}

    interpretation["chips"] = _build_chips(applied) if resolved_mode == MODE_TIRES else []

    timing = {
        "parse_ms": round(parse_ms, 1),
        "search_ms": round(search_ms, 1),
        "total_ms": round((time.monotonic() - started) * 1000, 1),
    }

    payload = {
        "mode": resolved_mode,
        "query": q,
        "total": total,
        "limit": limit,
        "offset": offset,
        "applied_filters": applied,
        "interpretation": interpretation,
        "results": results,
        "facets": facets,
        "tabs": search_tabs.build_tabs(
            {
                search_tabs.MODE_TIRES: tires_total,
                search_tabs.MODE_WHEELS: wheels_total,
                search_tabs.MODE_PARTS: parts_total,
            },
            active=resolved_mode,
        ),
        "timing": timing,
    }

    # ---- 6. telemetry -------------------------------------------------------------------------
    _log_search(q=q, mode=resolved_mode, interpretation=interpretation, total=total, timing=timing)
    return payload


def _multi_search(
    *,
    text_query: str,
    filter_expression: str,
    sort_spec: typing.Optional[typing.List[str]],
    tires_limit: int,
    parts_limit: int,
    offset: int,
) -> typing.Tuple[typing.Dict[str, typing.Any], typing.Optional[typing.Dict[str, typing.Any]]]:
    """
    Both indexes in **one** HTTP round trip.

    The parts query is read-only and exists for the tab count and the no-parse fallback; nothing
    here ever writes to the parts index. A parts failure degrades to "no parts results" rather
    than failing the whole search -- a tab badge is not worth a 500.
    """
    queries: typing.List[typing.Dict[str, typing.Any]] = [
        {
            "indexUid": tires_index.INDEX_NAME_TIRES,
            "q": text_query,
            "limit": max(tires_limit, 0),
            "offset": offset,
            "facets": _facet_fields(),
        }
    ]
    if filter_expression:
        queries[0]["filter"] = filter_expression
    if sort_spec:
        queries[0]["sort"] = sort_spec

    include_parts = parts_limit > 0
    if include_parts:
        queries.append(
            {
                "indexUid": parts_index.INDEX_NAME,
                "q": text_query,
                "limit": parts_limit,
                "offset": offset if parts_limit > 1 else 0,
            }
        )

    # Wheels, for the tab badge only: limit 0 fetches a count and no documents. Added to the same
    # multi-search rather than a second call, so the strip costs nothing in round trips. No filter
    # is applied -- a tire filter expression names attributes the wheels index does not have, and
    # Meilisearch rejects the whole multi-search when one query references an unknown field.
    include_wheels = wheels_index.is_configured()
    if include_wheels:
        queries.append({"indexUid": wheels_index.INDEX_NAME_WHEELS, "q": text_query, "limit": 0})

    client = tires_index._client()
    try:
        responses = client.multi_search(queries)["results"]
    except Exception as exc:
        if not include_parts:
            raise
        # Most often the parts index is simply absent (a fresh environment). Retry tires alone so
        # tire search still works.
        logger.warning("%s multi-search failed, retrying tires alone: %s", _LOG_PREFIX, exc)
        responses = client.multi_search(queries[:1])["results"]
        include_parts = False
        include_wheels = False

    by_index = {response.get("indexUid"): response for response in responses}
    tires_response = by_index.get(tires_index.INDEX_NAME_TIRES, {"hits": [], "estimatedTotalHits": 0})
    parts_response = by_index.get(parts_index.INDEX_NAME) if include_parts else None
    wheels_response = by_index.get(wheels_index.INDEX_NAME_WHEELS) if include_wheels else None
    return tires_response, parts_response, wheels_response


def _parts_hit_to_result(hit: typing.Mapping[str, typing.Any]) -> typing.Dict[str, typing.Any]:
    """
    A parts-index document in the response shape.

    Deliberately the same envelope as a tire result -- ``id``, ``brand_name``, ``spec_line``,
    ``offers`` -- so one row component renders either mode. ``spec_line`` falls back to the
    description here because a part has no structured specs to compose from yet; that is what
    parts v2 changes.
    """
    return {
        "id": hit["id"],
        "brand_id": hit.get("brand_id"),
        "brand_name": hit.get("brand_name") or "",
        "model_name": "",
        "part_number": hit.get("part_number") or "",
        "gtin": hit.get("gtin") or "",
        "image_url": hit.get("image_url") or "",
        "size_display": "",
        "spec_line": (hit.get("description") or "")[:200],
        "category": hit.get("category") or "",
        "overview_category": hit.get("overview_category") or "",
        "distributor_count": hit.get("distributor_count", 0),
        "distributor_names": hit.get("distributor_names") or [],
    }


def _build_chips(applied: typing.Mapping[str, typing.Any]) -> typing.List[typing.Dict[str, typing.Any]]:
    """
    Removable chips describing how the query was read.

    Built from the **applied filters**, not from the raw query text, so removing a chip and
    re-submitting the remaining filters reproduces exactly the state the chip described. Labels
    reuse ``facet_config`` so a chip and its facet never disagree.
    """
    by_field = {facet["field"]: facet for facet in facets_config()}
    chips = []
    for field in sorted(applied):
        value = applied[field]
        facet = by_field.get(field)
        label = facet["label"] if facet else field.replace("_", " ").title()
        value_labels = (facet or {}).get("value_labels") or {}
        if isinstance(value, dict):
            display = "{}-{}".format(value.get("min", ""), value.get("max", ""))
        elif isinstance(value, (list, tuple, set)):
            display = ", ".join(value_labels.get(str(v), str(v)) for v in value)
        elif isinstance(value, bool):
            display = "Yes"
        else:
            display = value_labels.get(str(value), str(value))
        chips.append(
            {
                "field": field,
                "value": value,
                "label": label,
                "display": display,
                "unit": (facet or {}).get("unit"),
                "removable": True,
            }
        )
    return chips


def _log_search(
    *,
    q: str,
    mode: str,
    interpretation: typing.Mapping[str, typing.Any],
    total: int,
    timing: typing.Mapping[str, typing.Any],
) -> None:
    """
    One structured line per search: the instrumentation the plan asks for from day one.

    Emitted through ``logging`` rather than written to a table -- a synchronous insert on the
    request path would cost more than the search it is measuring. Zero-result rate, parse-failure
    rate and latency are all derivable from these fields.
    """
    logger.info(
        "%s query=%r mode=%s parsed=%s matched=%s relaxed=%s total=%s zero_result=%s timing=%s",
        _LOG_PREFIX,
        q[:200],
        mode,
        interpretation.get("parsed"),
        sorted(interpretation.get("matched") or {}),
        (interpretation.get("relaxed") or {}).get("dropped"),
        total,
        total == 0,
        timing,
    )


def _facet_values(distribution_values: typing.Mapping[str, typing.Any]) -> typing.Dict[str, int]:
    """
    The facet's real values: everything but the empty bucket.

    The index writes ``""`` for a NULL ``service_type`` / ``vehicle_class`` / ``tier`` /
    ``load_range``, and 38,199 of 47,655 tires have no service type at all -- so the unfiltered
    counts are dominated by a blank checkbox that means "we do not know", not "none of the above".
    Selecting it would be meaningless and rendering it looks like a bug, so it never reaches the
    client. Dropping it here rather than in the projection keeps the filter semantics of the index
    unchanged and needs no reindex.
    """
    return {str(value): count for value, count in distribution_values.items() if str(value).strip() != "" and count}


def _has_true_value(values: typing.Mapping[str, int]) -> bool:
    """A boolean facet's ``true`` bucket, whatever case the engine spelled it in."""
    return any(str(value).lower() == "true" and count for value, count in values.items())


def _typed_facet_value(field: str, value: str) -> typing.Any:
    """
    A facet value in the type the index stores, not the string JSON handed us.

    Meilisearch returns every ``facetDistribution`` key as a string, and the client sends the value
    it was given straight back as a filter. ``rim_diameter_in = "18"`` is a string comparison
    against a numeric field and matches nothing, so a facet can render perfectly and still be
    unclickable. The numeric/boolean field lists live on the index module next to the projection
    that produces them (with a test that they do not drift), because that is the only place that
    knows what type each attribute really holds -- guessing from the value would type a brand
    called "911" as a number.
    """
    if field in tires_index.BOOLEAN_FILTERABLE:
        return str(value).lower() == "true"
    if field not in tires_index.NUMERIC_FILTERABLE:
        return value
    number = _as_float(value)
    if number is None:
        return value
    return int(number) if number.is_integer() and "." not in str(value) else number


def _as_float(value: typing.Any) -> typing.Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _is_visible(
    facet: typing.Mapping[str, typing.Any],
    values: typing.Mapping[str, int],
    applied: typing.Mapping[str, typing.Any],
) -> bool:
    """
    Whether this facet earns its place on **this** result set.

    Every rule here is data on the ``facet_config`` row, not a special case in code -- see the
    FacetConfig docstring. The principle behind all three: a control that cannot change the
    result set is worse than an absent one, because the user spends attention on it first.
    """
    if len(values) < max(1, facet.get("min_distinct_values") or 1):
        return False
    required = facet.get("requires_filter_on")
    if required and required not in applied:
        return False
    if facet.get("requires_true_value") and not _has_true_value(values):
        return False
    return True


def _ordered_values(
    facet: typing.Mapping[str, typing.Any],
    values: typing.Mapping[str, int],
) -> typing.List[typing.Tuple[str, int]]:
    """Order the values inside one facet -- popularity, numeric, or the reference table's order."""
    items = list(values.items())
    order = facet.get("value_order") or "count"
    if order == "numeric":
        # Unparseable values sort last rather than exploding: a facet is not worth a 500.
        return sorted(items, key=lambda item: (_as_float(item[0]) is None, _as_float(item[0]) or 0.0))
    if order == "vocabulary":
        sequence = {value: position for position, value in enumerate(facet.get("value_sequence") or [])}
        return sorted(items, key=lambda item: (sequence.get(item[0], len(sequence)), item[0]))
    return sorted(items, key=lambda item: (-item[1], item[0]))


def _range_stats(
    field: str,
    values: typing.Mapping[str, int],
    stats: typing.Mapping[str, typing.Any],
) -> typing.Optional[typing.Dict[str, float]]:
    """
    ``{"min": .., "max": ..}`` for a range widget -- the only thing a slider actually needs.

    Meilisearch returns ``facetStats`` for numeric facets, so that is used when present; the
    fallback derives the same two numbers from the distribution's own keys, which keeps the widget
    working against an engine (or a test double) that does not send stats.
    """
    engine_stats = stats.get(field) or {}
    low, high = engine_stats.get("min"), engine_stats.get("max")
    if low is None or high is None:
        numbers = [number for number in (_as_float(value) for value in values) if number is not None]
        if not numbers:
            return None
        low, high = min(numbers), max(numbers)
    return {"min": float(low), "max": float(high)}


def _shape_facets(
    distribution: typing.Mapping[str, typing.Any],
    *,
    applied: typing.Optional[typing.Mapping[str, typing.Any]] = None,
    stats: typing.Optional[typing.Mapping[str, typing.Any]] = None,
) -> typing.List[typing.Dict[str, typing.Any]]:
    """
    Join the index's facet counts to the server-owned facet config.

    Order and labels come from ``facet_config``; a facet the index returned nothing for, or one
    whose visibility rule this result set does not satisfy, is omitted rather than shown as a dead
    control. ``applied`` is what makes a conditional facet conditional -- "Overall diameter"
    appears only once a wheel size is chosen.

    A ``range`` facet also carries ``stats`` (min/max), because a slider cannot be built from a
    list of counts.
    """
    applied = applied or {}
    stats = stats or {}
    shaped = []
    for facet in facets_config():
        values = _facet_values(distribution.get(facet["field"]) or {})
        if not values or not _is_visible(facet, values, applied):
            continue
        labels = facet["value_labels"]
        entry = {
            "field": facet["field"],
            "label": facet["label"],
            "widget": facet["widget"],
            "unit": facet["unit"],
            "collapse_after": facet["collapse_after"],
            "values": [
                {
                    "value": _typed_facet_value(facet["field"], value),
                    "label": labels.get(str(value), str(value)),
                    "count": count,
                }
                for value, count in _ordered_values(facet, values)
            ],
        }
        if facet["widget"] == "range":
            entry["stats"] = _range_stats(facet["field"], values, stats)
        shaped.append(entry)
    return shaped
