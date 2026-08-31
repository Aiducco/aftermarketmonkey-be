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

from src.search import wheels_index

logger = logging.getLogger(__name__)
_LOG_PREFIX = "[WHEEL-SEARCH]"

MODE_WHEELS = "wheels"
DEFAULT_LIMIT = 50
MAX_LIMIT = 200


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

# The facet rail, server-owned so order and labels change without a client deploy.
FACETS_CONFIG: typing.List[typing.Dict[str, typing.Any]] = [
    {"field": "diameter_in", "label": "Diameter", "type": "number", "unit": "in"},
    {"field": "width_in", "label": "Width", "type": "number", "unit": "in"},
    {"field": "bolt_pattern", "label": "Bolt pattern", "type": "string"},
    {"field": "offset_mm", "label": "Offset", "type": "number", "unit": "mm"},
    {"field": "finish_family", "label": "Finish", "type": "string"},
    {"field": "brand_name", "label": "Brand", "type": "string"},
    {"field": "vehicle_class", "label": "Vehicle", "type": "string"},
    {"field": "construction", "label": "Construction", "type": "string"},
    {"field": "center_bore_mm", "label": "Hub bore", "type": "number", "unit": "mm"},
    {"field": "load_rating_lb", "label": "Load rating", "type": "number", "unit": "lb"},
    {"field": "in_stock", "label": "In stock", "type": "boolean"},
    {"field": "is_beadlock", "label": "Beadlock", "type": "boolean"},
]


def facets_config() -> typing.List[typing.Dict[str, typing.Any]]:
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
    try:
        response = wheels_index._client().index(wheels_index.SPEC.name).search(q, request)
    except Exception as exc:
        logger.exception("%s query failed: %s", _LOG_PREFIX, exc)
        raise SearchError("Search failed.")
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
        "facets": _shape_facets(response.get("facetDistribution") or {}),
        "tabs": [],
        "timing": {
            "parse_ms": 0.0,
            "search_ms": round(search_ms, 1),
            "total_ms": round((time.monotonic() - started) * 1000, 1),
        },
    }
    logger.info("%s q=%r filters=%s total=%s in %.1fms", _LOG_PREFIX, q, list(filters), total, search_ms)
    return payload


def _shape_facets(distribution: typing.Mapping[str, typing.Mapping[str, int]]) -> typing.List[dict]:
    """Facet counts in rail order, with the client-facing field name restored."""
    by_attribute = {FILTER_FIELDS[f["field"]][0]: f for f in FACETS_CONFIG if f["field"] in FILTER_FIELDS}
    shaped = []
    for attribute, counts in distribution.items():
        config = by_attribute.get(attribute)
        if config is None:
            continue
        kind = FILTER_FIELDS[config["field"]][1]
        values = [
            {"value": _typed_facet_value(value, kind), "count": count}
            for value, count in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))
        ]
        shaped.append({"field": config["field"], "label": config["label"], "type": kind, "values": values})
    order = [f["field"] for f in FACETS_CONFIG]
    shaped.sort(key=lambda facet: order.index(facet["field"]) if facet["field"] in order else len(order))
    return shaped


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
