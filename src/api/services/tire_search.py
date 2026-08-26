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

from src.domain import spec_line as spec_line_domain
from src.domain import tire_filters, tire_query
from src.search import meilisearch_client as parts_index
from src.search import tires_index

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
# rail. All three change on the timescale of a catalog sync, not a request, and querying them per
# request cost 1.6s of a 2.2s search -- measured, not assumed.
#
# Built into a new object and swapped in by reference rather than mutated in place, so a request
# reading the cache while another thread refreshes it sees either the old value or the new one,
# never a half-populated dict. A plain assignment is atomic under the GIL; the lock only stops
# several threads doing the same expensive query at once after an expiry.
REFERENCE_CACHE_TTL_SECONDS = 300


class _Cached:
    """One lazily-loaded, TTL-expiring reference value."""

    def __init__(self, loader: typing.Callable[[], typing.Any], ttl: float = REFERENCE_CACHE_TTL_SECONDS):
        self._loader = loader
        self._ttl = ttl
        self._value: typing.Any = None
        self._loaded_at = 0.0
        self._lock = threading.Lock()

    def get(self) -> typing.Any:
        now = time.monotonic()
        if self._value is not None and now - self._loaded_at < self._ttl:
            return self._value
        with self._lock:
            # Re-check: another thread may have refreshed while this one waited.
            if self._value is not None and time.monotonic() - self._loaded_at < self._ttl:
                return self._value
            self._value = self._loader()
            self._loaded_at = time.monotonic()
            return self._value

    def invalidate(self) -> None:
        """Drop the cached value. Call after a reindex or a facet_config edit."""
        with self._lock:
            self._value = None
            self._loaded_at = 0.0


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


def _tread_category_labels() -> typing.Dict[str, str]:
    """Code -> display label, straight from ``tread_category``."""
    from src import models as src_models

    return {row.code: row.label for row in src_models.TreadCategory.objects.all()}


def _load_facets_config() -> typing.List[typing.Dict[str, typing.Any]]:
    """The facet rail definition, server-owned so it changes without a client deploy."""
    from src import models as src_models

    # tread_category labels come from the tread_category table rather than being duplicated into
    # facet_config.value_labels: the table is already the source of truth for them (it is the FK
    # target and the LLM's constraint), and a second copy would drift the moment a label is
    # edited. A value_labels entry on the row still wins, so a facet can override if it needs to.
    category_labels = _tread_category_labels()

    facets = []
    for row in src_models.FacetConfig.objects.filter(mode=FACET_MODE_BY_SEARCH_MODE[MODE_TIRES]).order_by("sort_order"):
        value_labels = dict(row.value_labels or {})
        if row.field == "tread_category":
            value_labels = {**category_labels, **value_labels}
        facets.append(
            {
                "field": row.field,
                "label": row.label,
                "widget": row.widget,
                "collapse_after": row.collapse_after,
                "unit": row.unit,
                "value_labels": value_labels,
            }
        )
    return facets


_BRAND_NAMES = _Cached(_load_brand_names)
_TREAD_CATEGORY_CODES = _Cached(_load_tread_category_codes)
_FACETS_CONFIG = _Cached(_load_facets_config)


def brand_names() -> typing.AbstractSet[str]:
    return _BRAND_NAMES.get()


def tread_category_codes() -> typing.AbstractSet[str]:
    return _TREAD_CATEGORY_CODES.get()


def facets_config() -> typing.List[typing.Dict[str, typing.Any]]:
    return _FACETS_CONFIG.get()


def invalidate_reference_cache() -> None:
    """Drop every cached reference value. Call after a reindex or a facet_config change."""
    for cache in (_BRAND_NAMES, _TREAD_CATEGORY_CODES, _FACETS_CONFIG):
        cache.invalidate()


def _facet_fields() -> typing.List[str]:
    return [facet["field"] for facet in facets_config()]


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
    resolved_mode = mode or (MODE_TIRES if tires_is_certain else None)

    # ---- 3. one multi-search ----------------------------------------------------------------
    search_t0 = time.monotonic()
    tires_limit = limit if resolved_mode != MODE_PARTS else 1
    parts_limit = 1 if resolved_mode == MODE_TIRES else limit
    tires_response, parts_response = _multi_search(
        text_query=text_query,
        filter_expression=filter_expression,
        sort_spec=sort_spec,
        tires_limit=tires_limit,
        parts_limit=parts_limit,
        offset=offset,
    )

    tires_total = tires_response.get("estimatedTotalHits", 0)
    parts_total = parts_response.get("estimatedTotalHits", 0) if parts_response is not None else 0

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
            tires_response, _ = _multi_search(
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
        facets = _shape_facets(tires_response.get("facetDistribution") or {})
        applied = tire_filters_dict
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
        "tabs": _build_tabs(tires_total=tires_total, parts_total=parts_total, active=resolved_mode),
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

    by_index = {response.get("indexUid"): response for response in responses}
    tires_response = by_index.get(tires_index.INDEX_NAME_TIRES, {"hits": [], "estimatedTotalHits": 0})
    parts_response = by_index.get(parts_index.INDEX_NAME) if include_parts else None
    return tires_response, parts_response


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


def _build_tabs(*, tires_total: int, parts_total: int, active: str) -> typing.List[typing.Dict[str, typing.Any]]:
    """
    Tab strip. **Only modes with hits appear**, so a part-number search renders no tab strip at
    all and the page looks exactly like it does today.
    """
    candidates = [
        {"mode": MODE_TIRES, "label": "Tires", "count": tires_total},
        {"mode": MODE_PARTS, "label": "Parts", "count": parts_total},
    ]
    tabs = [dict(tab, active=tab["mode"] == active) for tab in candidates if tab["count"] > 0]
    return tabs if len(tabs) > 1 else []


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


def _shape_facets(distribution: typing.Mapping[str, typing.Any]) -> typing.List[typing.Dict[str, typing.Any]]:
    """
    Join the index's facet counts to the server-owned facet config.

    Order and labels come from ``facet_config``; only fields the index actually returned counts
    for are included, so a facet that would render empty is omitted rather than shown as a dead
    control.
    """
    shaped = []
    for facet in facets_config():
        values = distribution.get(facet["field"]) or {}
        if not values:
            continue
        labels = facet["value_labels"]
        shaped.append(
            {
                "field": facet["field"],
                "label": facet["label"],
                "widget": facet["widget"],
                "unit": facet["unit"],
                "collapse_after": facet["collapse_after"],
                "values": [
                    {"value": value, "label": labels.get(str(value), str(value)), "count": count}
                    for value, count in sorted(values.items(), key=lambda item: (-item[1], str(item[0])))
                ],
            }
        )
    return shaped
