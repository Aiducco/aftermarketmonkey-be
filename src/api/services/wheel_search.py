"""
Wheel search behind ``POST /api/search`` with ``mode="wheels"``.

A sibling of ``src.api.services.tire_search`` rather than a branch inside it. The two answer
different questions -- a tire search is driven by a size string a customer types, a wheel search is
driven by fitment they pick -- and tire search is live. Threading a second product through its
router, chip builder and facet rail would put that at risk for no gain, so this module owns wheels
end to end and returns the **same response envelope** so the client renders both with one component.

The filter vocabulary is a closed whitelist. An unknown key is a 400, never a silent drop: ignoring
it shows the customer more wheels than they asked for with no way to tell that happened -- and for
wheels "more than you asked for" means parts that do not bolt to the car.

Bolt pattern filtering goes through the **array** fields (``bolt_circles_mm``, ``bolt_lug_counts``,
``bolt_patterns``). A multi-fit wheel is drilled twice and either drilling is a real fit; filtering
the scalar column would hide wheels that physically fit the customer's hub.
"""
import logging
import time
import typing

from src.api.services import search_tabs
from src.search import meilisearch_client as parts_index
from src.search import tires_index, wheels_index

logger = logging.getLogger(__name__)
_LOG_PREFIX = "[WHEEL-SEARCH]"

MODE_WHEELS = "wheels"
DEFAULT_LIMIT = 50
MAX_LIMIT = 200

# Rendered labels for coded facet values. The client never shows a code.
VEHICLE_CLASS_LABELS = {
    "passenger": "Passenger",
    "light_truck": "Light truck",
    "trailer": "Trailer",
    "commercial": "Commercial",
    "motorcycle": "Motorcycle",
    "atv_utv": "ATV / UTV",
}
CONSTRUCTION_LABELS = {
    "cast": "Cast",
    "flow_formed": "Flow formed",
    "forged": "Forged",
    "steel": "Steel",
    "multi_piece": "Multi piece",
}
FINISH_LABELS = {
    "black": "Black",
    "silver": "Silver",
    "grey": "Grey",
    "gunmetal": "Gunmetal",
    "anthracite": "Anthracite",
    "bronze": "Bronze",
    "gold": "Gold",
    "white": "White",
    "red": "Red",
    "blue": "Blue",
    "green": "Green",
    "orange": "Orange",
    "chrome": "Chrome",
    "polished": "Polished",
    "machined": "Machined",
    "raw": "Raw",
}


class SearchError(Exception):
    """A bad request, surfaced to the client as a 400."""


# name -> (index attribute, python type). The type matters: Meilisearch reads a quoted "18" as a
# string and matches nothing against a numeric 18, so a facet that renders can still be unclickable.
FILTER_FIELDS: typing.Dict[str, typing.Tuple[str, str]] = {
    "diameter_in": ("diameter_in", "number"),
    "width_in": ("width_in", "number"),
    "size_display": ("size_display", "string"),
    "bolt_circle_mm": ("bolt_circles_mm", "number"),
    "bolt_lug_count": ("bolt_lug_counts", "number"),
    "bolt_pattern": ("bolt_patterns", "string"),
    "offset_mm": ("offset_mm", "number"),
    "center_bore_mm": ("center_bore_mm", "number"),
    "load_rating_lb": ("load_rating_lb", "number"),
    "finish_family": ("finish_family", "string"),
    "construction": ("construction", "string"),
    "material": ("material", "string"),
    "vehicle_class": ("vehicle_class", "string"),
    "brand_id": ("brand_id", "number"),
    "brand_name": ("brand_name", "string"),
    "in_stock": ("in_stock", "boolean"),
    "is_beadlock": ("is_beadlock", "boolean"),
    "is_dually": ("is_dually", "boolean"),
    "is_blank_drilled": ("is_blank_drilled", "boolean"),
    "tpms_compatible": ("tpms_compatible", "boolean"),
    "distributor_ids": ("distributor_ids", "number"),
}

# Range filters arrive as {"gte": x, "lte": y}. Only numeric fields accept them.
_RANGE_KEYS = ("gte", "lte", "gt", "lt")
_RANGE_OPS = {"gte": ">=", "lte": "<=", "gt": ">", "lt": "<"}

SORT_OPTIONS: typing.Dict[str, typing.List[str]] = {
    "diameter_asc": ["diameter_in:asc"],
    "diameter_desc": ["diameter_in:desc"],
    "width_asc": ["width_in:asc"],
    "width_desc": ["width_in:desc"],
    "offset_asc": ["offset_mm:asc"],
    "offset_desc": ["offset_mm:desc"],
    "brand_asc": ["brand_name:asc"],
}

# The facet rail, server-owned so order, labels and widgets change without a client deploy.
#
# The shape is deliberately identical to the tire rail (``src.api.services.tire_search``) key for
# key, so the client renders both from one component. ``widget`` is what tells it whether to draw
# checkboxes, a slider or a switch -- ``type`` alone could not, which is what blocked the UI.
#
# Widget choice follows the distinct-value count, measured against the 43,272 wheels in the index:
#
#   diameter 23 values, width 28    multiselect -- a buyer picks "20 inch", they do not drag to it,
#                                   and this matches how the tire rail treats rim_diameter_in
#   offset 200, hub bore 156,       range -- a list of 200 offsets is not a control
#   load rating 95
#
# ``stats`` (min/max) is emitted for **every** numeric facet regardless of widget, not just ranges,
# so switching diameter or width to a slider is a one-line change here and needs no backend work.
#
# ``min_distinct_values`` hides a facet that would render as a dead control: vehicle_class and
# construction currently hold one value each across the whole index, because the Wheel Pros feed
# publishes neither and only keyword-derived rows have them. They appear on their own once the
# other feeds land.
FACETS_CONFIG: typing.List[typing.Dict[str, typing.Any]] = [
    {
        "field": "diameter_in",
        "label": "Diameter",
        "widget": "multiselect",
        "unit": "in",
        "collapse_after": 10,
        "value_labels": {},
        "value_order": "numeric",
        "value_sequence": [],
        "min_distinct_values": 1,
        "requires_filter_on": None,
        "requires_true_value": False,
    },
    {
        "field": "width_in",
        "label": "Width",
        "widget": "multiselect",
        "unit": "in",
        "collapse_after": 10,
        "value_labels": {},
        "value_order": "numeric",
        "value_sequence": [],
        "min_distinct_values": 1,
        "requires_filter_on": None,
        "requires_true_value": False,
    },
    {
        "field": "bolt_pattern",
        "label": "Bolt pattern",
        "widget": "multiselect",
        "unit": None,
        "collapse_after": 10,
        "value_labels": {},
        "value_order": "count",
        "value_sequence": [],
        "min_distinct_values": 1,
        "requires_filter_on": None,
        "requires_true_value": False,
    },
    {
        "field": "offset_mm",
        "label": "Offset",
        "widget": "range",
        "unit": "mm",
        "collapse_after": None,
        "value_labels": {},
        "value_order": "numeric",
        "value_sequence": [],
        "min_distinct_values": 2,
        "requires_filter_on": None,
        "requires_true_value": False,
    },
    {
        "field": "center_bore_mm",
        "label": "Hub bore",
        "widget": "range",
        "unit": "mm",
        "collapse_after": None,
        "value_labels": {},
        "value_order": "numeric",
        "value_sequence": [],
        "min_distinct_values": 2,
        "requires_filter_on": None,
        "requires_true_value": False,
    },
    {
        "field": "load_rating_lb",
        "label": "Load rating",
        "widget": "range",
        "unit": "lb",
        "collapse_after": None,
        "value_labels": {},
        "value_order": "numeric",
        "value_sequence": [],
        "min_distinct_values": 2,
        "requires_filter_on": None,
        "requires_true_value": False,
    },
    {
        "field": "finish_family",
        "label": "Finish",
        "widget": "multiselect",
        "unit": None,
        "collapse_after": 8,
        "value_labels": FINISH_LABELS,
        "value_order": "count",
        "value_sequence": [],
        "min_distinct_values": 1,
        "requires_filter_on": None,
        "requires_true_value": False,
    },
    {
        "field": "brand_name",
        "label": "Brand",
        "widget": "multiselect",
        "unit": None,
        "collapse_after": 8,
        "value_labels": {},
        "value_order": "count",
        "value_sequence": [],
        "min_distinct_values": 1,
        "requires_filter_on": None,
        "requires_true_value": False,
    },
    {
        "field": "vehicle_class",
        "label": "Vehicle",
        "widget": "multiselect",
        "unit": None,
        "collapse_after": 8,
        "value_labels": VEHICLE_CLASS_LABELS,
        "value_order": "count",
        "value_sequence": [],
        "min_distinct_values": 2,
        "requires_filter_on": None,
        "requires_true_value": False,
    },
    {
        "field": "construction",
        "label": "Construction",
        "widget": "multiselect",
        "unit": None,
        "collapse_after": 8,
        "value_labels": CONSTRUCTION_LABELS,
        "value_order": "count",
        "value_sequence": [],
        "min_distinct_values": 2,
        "requires_filter_on": None,
        "requires_true_value": False,
    },
    {
        "field": "in_stock",
        "label": "In stock",
        "widget": "toggle",
        "unit": None,
        "collapse_after": None,
        "value_labels": {"true": "In stock", "false": "Out of stock"},
        "value_order": "count",
        "value_sequence": [],
        "min_distinct_values": 1,
        "requires_filter_on": None,
        "requires_true_value": False,
    },
    {
        "field": "is_beadlock",
        "label": "Beadlock",
        "widget": "toggle",
        "unit": None,
        "collapse_after": None,
        "value_labels": {"true": "Beadlock", "false": "Not beadlock"},
        "value_order": "count",
        "value_sequence": [],
        "min_distinct_values": 1,
        "requires_filter_on": None,
        "requires_true_value": False,
    },
]


def facets_config() -> typing.List[typing.Dict[str, typing.Any]]:
    """The rail definition, so the client can render the shell before any search has run."""
    return [dict(facet) for facet in FACETS_CONFIG]


def _quote(value: typing.Any, kind: str) -> str:
    if kind == "number":
        try:
            number = float(value)
        except (TypeError, ValueError):
            raise SearchError("Expected a number, got {!r}.".format(value))
        return str(int(number)) if number.is_integer() else str(number)
    if kind == "boolean":
        if isinstance(value, str):
            value = value.strip().lower() in ("1", "true", "yes")
        return "true" if value else "false"
    text = str(value).replace('"', '\\"')
    return '"{}"'.format(text)


def compile_filters(filters: typing.Mapping[str, typing.Any]) -> typing.List[str]:
    """
    Turn the client's filter object into Meilisearch filter expressions.

    A list of values is an OR within the field; separate fields are ANDed, which is what a facet
    rail means by "Black or Bronze, 20 inch".
    """
    clauses: typing.List[str] = []
    for key, value in filters.items():
        if key not in FILTER_FIELDS:
            raise SearchError("Unknown filter field {!r}. Allowed: {}".format(key, ", ".join(sorted(FILTER_FIELDS))))
        attribute, kind = FILTER_FIELDS[key]
        if value is None or value == "" or value == []:
            continue

        if isinstance(value, dict) and any(k in value for k in _RANGE_KEYS):
            if kind != "number":
                raise SearchError("Filter {!r} does not accept a range.".format(key))
            for bound, op in _RANGE_OPS.items():
                if bound in value and value[bound] is not None:
                    clauses.append("{} {} {}".format(attribute, op, _quote(value[bound], kind)))
            continue

        values = value if isinstance(value, (list, tuple)) else [value]
        ors = ["{} = {}".format(attribute, _quote(item, kind)) for item in values]
        clauses.append(ors[0] if len(ors) == 1 else "({})".format(" OR ".join(ors)))
    return clauses


def _facet_fields() -> typing.List[str]:
    """
    Fields to request counts on, intersected with what the **live** index accepts.

    Meilisearch rejects the entire search when one requested facet is not filterable, so between a
    deploy and ``index_wheels_meilisearch --setup`` an un-intersected list would take wheel search
    down completely rather than hide one control.
    """
    wanted = [FILTER_FIELDS[f["field"]][0] for f in FACETS_CONFIG if f["field"] in FILTER_FIELDS]
    live = wheels_index.live_filterable_attributes()
    if live is None:
        return wanted
    return [name for name in wanted if name in live]


def _shape_hit(hit: typing.Mapping[str, typing.Any]) -> typing.Dict[str, typing.Any]:
    """One index document -> one result card. No pricing: the client hydrates a page by posting
    the ids to ``POST /parts/bulk-pricing/``, exactly as the tire and parts flows do."""
    return {
        "id": hit["id"],
        "brand_id": hit.get("brand_id"),
        "brand_name": hit.get("brand_name") or "",
        "model_name": hit.get("model_name") or "",
        "style_number": hit.get("style_number") or "",
        "part_number": hit.get("part_number") or "",
        "gtin": hit.get("gtin") or "",
        "image_url": hit.get("image_url") or "",
        "size_display": hit.get("size_display") or "",
        "spec_line": _spec_line(hit),
        "bolt_patterns": hit.get("bolt_patterns") or [],
        "offset_mm": hit.get("offset_mm"),
        "center_bore_mm": hit.get("center_bore_mm"),
        "finish": hit.get("finish") or "",
        "finish_family": hit.get("finish_family") or "",
        "load_rating_lb": hit.get("load_rating_lb"),
        "is_blank_drilled": hit.get("is_blank_drilled", False),
        "distributor_count": hit.get("distributor_count", 0),
        "distributor_names": hit.get("distributor_names") or [],
        "in_stock": hit.get("in_stock", False),
    }


def _spec_line(hit: typing.Mapping[str, typing.Any]) -> str:
    """The one line under the title: ``20x9  6x135 / 6x5.5  +18mm  106.1mm``."""
    parts = [hit.get("size_display") or ""]
    patterns = hit.get("bolt_patterns") or []
    if patterns:
        parts.append(" / ".join(patterns))
    elif hit.get("is_blank_drilled"):
        parts.append("undrilled")
    offset = hit.get("offset_mm")
    if offset is not None:
        parts.append("{:+d}mm".format(int(offset)))
    bore = hit.get("center_bore_mm")
    if bore is not None:
        parts.append("{}mm bore".format(bore))
    return "  ".join(p for p in parts if p)


def search(
    *,
    q: str = "",
    filters: typing.Optional[typing.Mapping[str, typing.Any]] = None,
    sort: typing.Optional[str] = None,
    limit: int = DEFAULT_LIMIT,
    offset: int = 0,
) -> typing.Dict[str, typing.Any]:
    """One wheel search: compile filters, query, shape. Same envelope as tire search."""
    started = time.monotonic()
    if not wheels_index.is_configured():
        raise SearchError("Search is not configured.")

    limit = max(1, min(int(limit or DEFAULT_LIMIT), MAX_LIMIT))
    offset = max(0, int(offset or 0))
    q = (q or "").strip()
    filters = filters or {}

    sort_spec = None
    if sort:
        if sort not in SORT_OPTIONS:
            raise SearchError("Unknown sort {!r}. Allowed: {}".format(sort, ", ".join(sorted(SORT_OPTIONS))))
        sort_spec = SORT_OPTIONS[sort]

    clauses = compile_filters(filters)
    request: typing.Dict[str, typing.Any] = {
        "limit": limit,
        "offset": offset,
        "facets": _facet_fields(),
    }
    if clauses:
        request["filter"] = clauses
    if sort_spec:
        request["sort"] = sort_spec

    search_t0 = time.monotonic()
    response, tab_counts = _search_with_tab_counts(q, request)
    search_ms = (time.monotonic() - search_t0) * 1000

    hits = response.get("hits", [])
    total = response.get("estimatedTotalHits", response.get("totalHits", len(hits)))
    payload = {
        "mode": MODE_WHEELS,
        "query": q,
        "total": total,
        "limit": limit,
        "offset": offset,
        "applied_filters": dict(filters),
        "interpretation": {"parsed": False, "matched": {}, "chips": [], "relaxed": None},
        "results": [_shape_hit(hit) for hit in hits],
        "facets": _shape_facets(response.get("facetDistribution") or {}, stats=response.get("facetStats") or {}),
        "tabs": search_tabs.build_tabs(dict(tab_counts, **{search_tabs.MODE_WHEELS: total}), active=MODE_WHEELS),
        "timing": {
            "parse_ms": 0.0,
            "search_ms": round(search_ms, 1),
            "total_ms": round((time.monotonic() - started) * 1000, 1),
        },
    }
    logger.info("%s q=%r filters=%s total=%s in %.1fms", _LOG_PREFIX, q, list(filters), total, search_ms)
    return payload


def _shape_facets(
    distribution: typing.Mapping[str, typing.Mapping[str, int]],
    *,
    stats: typing.Optional[typing.Mapping[str, typing.Any]] = None,
) -> typing.List[typing.Dict[str, typing.Any]]:
    """
    Join the index's facet counts to the server-owned rail. Same output shape as the tire rail.

    A facet the index returned nothing for, or one holding fewer distinct values than
    ``min_distinct_values``, is omitted rather than shown as a control that cannot narrow
    anything -- a "Construction: Forged (459)" checkbox with no alternative is a dead click.

    ``stats`` is emitted for every numeric facet, not only ranges. A slider needs it and a
    multiselect does not, but carrying it either way means the rail can switch a widget without a
    backend change.
    """
    stats = stats or {}
    by_attribute = {FILTER_FIELDS[f["field"]][0]: f for f in FACETS_CONFIG if f["field"] in FILTER_FIELDS}
    shaped = []
    for attribute, counts in distribution.items():
        config = by_attribute.get(attribute)
        if config is None or not counts:
            continue
        if len(counts) < config["min_distinct_values"]:
            continue
        kind = FILTER_FIELDS[config["field"]][1]
        labels = config["value_labels"]
        values = [
            {
                "value": _typed_facet_value(value, kind),
                "label": labels.get(str(value), str(value)),
                "count": count,
            }
            for value, count in _ordered_values(config, counts, kind)
        ]
        entry = {
            "field": config["field"],
            "label": config["label"],
            "widget": config["widget"],
            "unit": config["unit"],
            "collapse_after": config["collapse_after"],
            "values": values,
        }
        if kind == "number":
            entry["stats"] = _range_stats(attribute, counts, stats)
        shaped.append(entry)

    order = [f["field"] for f in FACETS_CONFIG]
    shaped.sort(key=lambda facet: order.index(facet["field"]) if facet["field"] in order else len(order))
    return shaped


def _ordered_values(
    config: typing.Mapping[str, typing.Any], counts: typing.Mapping[str, int], kind: str
) -> typing.List[typing.Tuple[str, int]]:
    """Rail order: numerically for a measurement, by frequency for a vocabulary."""
    items = list(counts.items())
    if config["value_order"] == "numeric" and kind == "number":
        return sorted(items, key=lambda kv: _as_float(kv[0]) if _as_float(kv[0]) is not None else 0.0)
    if config["value_order"] == "vocabulary" and config["value_sequence"]:
        sequence = {value: index for index, value in enumerate(config["value_sequence"])}
        return sorted(items, key=lambda kv: (sequence.get(kv[0], len(sequence)), -kv[1]))
    return sorted(items, key=lambda kv: (-kv[1], kv[0]))


def _as_float(value: typing.Any) -> typing.Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _range_stats(
    attribute: str, counts: typing.Mapping[str, int], stats: typing.Mapping[str, typing.Any]
) -> typing.Optional[typing.Dict[str, float]]:
    """
    ``{"min": .., "max": ..}`` -- the only thing a slider needs.

    Meilisearch sends ``facetStats`` for numeric facets, so that is preferred; deriving the same
    two numbers from the distribution keys is the fallback, which keeps the widget working against
    an engine or a test double that sends no stats.
    """
    engine = stats.get(attribute) or {}
    low, high = engine.get("min"), engine.get("max")
    if low is None or high is None:
        numbers = [n for n in (_as_float(v) for v in counts) if n is not None]
        if not numbers:
            return None
        low, high = min(numbers), max(numbers)
    return {"min": float(low), "max": float(high)}


def _typed_facet_value(value: str, kind: str) -> typing.Any:
    """Meilisearch returns every facet key as a string. Send numbers back as numbers, or the
    client posts a quoted value the index will not match."""
    if kind == "number":
        try:
            number = float(value)
            return int(number) if number.is_integer() else number
        except (TypeError, ValueError):
            return value
    if kind == "boolean":
        return value == "true"
    return value


def _search_with_tab_counts(
    q: str, request: typing.Dict[str, typing.Any]
) -> typing.Tuple[typing.Mapping[str, typing.Any], typing.Dict[str, int]]:
    """
    The wheels query plus a count from each sibling index, in one round trip.

    The tires and parts queries are ``limit: 0`` -- a total and no documents -- and carry no
    filter, because a wheel filter names attributes those indexes do not have and Meilisearch
    rejects an entire multi-search when one query references an unknown field.

    A failure in the sibling counts must not fail the search: the strip is a nicety and the
    results are not. On error this retries wheels alone and returns no counts, which renders no
    tab strip rather than a 500.
    """
    client = wheels_index._client()
    wheels_query = dict(request, indexUid=wheels_index.SPEC.name, q=q)
    queries = [
        wheels_query,
        {"indexUid": tires_index.INDEX_NAME_TIRES, "q": q, "limit": 0},
        {"indexUid": parts_index.INDEX_NAME, "q": q, "limit": 0},
    ]
    try:
        results = client.multi_search(queries)["results"]
    except Exception as exc:
        logger.warning("%s multi-search failed, retrying wheels alone: %s", _LOG_PREFIX, exc)
        try:
            return client.index(wheels_index.SPEC.name).search(q, request), {}
        except Exception as inner:
            logger.exception("%s query failed: %s", _LOG_PREFIX, inner)
            raise SearchError("Search failed.")

    by_index = {r.get("indexUid"): r for r in results}
    wheels = by_index.get(wheels_index.SPEC.name) or {"hits": [], "estimatedTotalHits": 0}
    counts = {
        search_tabs.MODE_TIRES: (by_index.get(tires_index.INDEX_NAME_TIRES) or {}).get("estimatedTotalHits", 0),
        search_tabs.MODE_PARTS: (by_index.get(parts_index.INDEX_NAME) or {}).get("estimatedTotalHits", 0),
    }
    return wheels, counts
