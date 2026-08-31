"""
The Meilisearch index behind wheel search.

A separate index from parts and from tires, for the same reason tires got their own: the questions
are different. Nobody text-searches a bolt pattern -- they pick one, and everything else narrows
around it. So the fitment attributes are filterable and **not** searchable, and the searchable
lane carries brand, model and style number only.

**Bolt patterns are indexed as arrays, and that is the central design decision.** A multi-fit wheel
is drilled twice: ``5x112`` *and* ``5x120``. Held as two scalar fields, a filter on
``bolt_circle_mm = 120`` would miss every wheel whose second pattern is the match -- the customer's
car would not find a wheel that physically bolts to it. As an array, one filter matches either
drilling. ``bolt_pattern_display`` stays scalar alongside for display and sort.

``is_blank_drilled`` is a real boolean and is indexed as one. An undrilled wheel fits nothing as
shipped, so it must be excludable; it is emphatically not "bolt pattern unknown".

The tri-state flags (``is_beadlock``, ``is_dually``, ``tpms_compatible``) follow the tire rule:
NULL means unknown, so the key is **omitted from the document** rather than written as false. A
false would make "beadlock" quietly exclude every wheel whose feed never mentioned it.
"""
import logging
import typing

from django.conf import settings
from django.db import connection

from src.search import index_builder
from src.search import meilisearch_client as parts_index

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[WHEELS-INDEX]"

INDEX_NAME_WHEELS = getattr(settings, "MEILISEARCH_INDEX_WHEELS", "wheels_v1")
REINDEX_BATCH_SIZE = getattr(settings, "MEILISEARCH_WHEELS_BATCH_SIZE", 5000)

SEARCHABLE_ATTRIBUTES = [
    "part_number",
    "gtin",
    "brand_name",
    "model_name",
    "style_number",
    "search_text",
    # The safety net under the query parser. A size or a bolt pattern normally becomes a filter and
    # never reaches the text engine at all -- but when the parser does not recognise a shape, the
    # raw string is still sent as ``q``, and without this there is nothing in the document for it
    # to hit, so the search returns nothing rather than something imperfect.
    #
    # Its own attribute rather than folded into ``search_text`` because typo tolerance has to be
    # off here and on there. "6x135" is five characters, which Meilisearch will happily match to
    # "6x139" with one typo allowed -- two different bolt patterns that do not interchange, on a
    # part that bolts to a car.
    "fitment_text",
]

FILTERABLE_ATTRIBUTES = [
    # fitment -- the entire reason this index exists
    "diameter_in",
    "width_in",
    "size_display",
    "bolt_lug_counts",
    "bolt_circles_mm",
    "bolt_patterns",
    "offset_mm",
    "center_bore_mm",
    "is_blank_drilled",
    # the wheel itself
    "finish_family",
    "construction",
    "material",
    "vehicle_class",
    "load_rating_lb",
    "is_beadlock",
    "is_dually",
    "tpms_compatible",
    # commerce
    "brand_id",
    "brand_name",
    "in_stock",
    "distributor_ids",
    "distributor_names",
    "distributor_count",
]

SORTABLE_ATTRIBUTES = ["diameter_in", "width_in", "offset_mm", "load_rating_lb", "brand_name"]

# Filterable attributes the document holds as numbers or booleans rather than strings (for a list
# field, the element type). A facet value has to travel back to the client as the type the index
# stores: Meilisearch reads a quoted ``"18"`` as a string and matches nothing against a numeric
# 18, so a facet that renders correctly can still be unclickable.
NUMERIC_FILTERABLE = frozenset(
    [
        "diameter_in",
        "width_in",
        "bolt_lug_counts",
        "bolt_circles_mm",
        "offset_mm",
        "center_bore_mm",
        "load_rating_lb",
        "brand_id",
        "distributor_ids",
        "distributor_count",
    ]
)
BOOLEAN_FILTERABLE = frozenset(["in_stock", "is_blank_drilled", "is_beadlock", "is_dually", "tpms_compatible"])

# NULL means unknown. Omitted from the document rather than written false -- see the module
# docstring. ``is_blank_drilled`` is deliberately absent: it is non-null with a real default.
_TRISTATE_FLAGS = ("is_beadlock", "is_dually", "tpms_compatible")

SPEC = index_builder.IndexSpec(
    name=INDEX_NAME_WHEELS,
    log_prefix=_LOG_PREFIX,
    searchable=SEARCHABLE_ATTRIBUTES,
    filterable=FILTERABLE_ATTRIBUTES,
    sortable=SORTABLE_ATTRIBUTES,
    typo_disabled=["part_number", "gtin", "style_number", "fitment_text"],
)


def _assert_not_a_shared_index(name: str) -> None:
    """
    Refuse to aim a wheels operation at another index.

    ``swap_indexes`` and ``delete_index`` take a name, so a bad ``MEILISEARCH_INDEX_WHEELS`` is the
    one configuration mistake that could destroy the parts or tires index.
    """
    protected = {
        parts_index.INDEX_NAME,
        parts_index.INDEX_NAME_VEHICLES,
        "{}_staging".format(parts_index.INDEX_NAME),
        getattr(settings, "MEILISEARCH_INDEX_TIRES", "tires_v1"),
        "{}_staging".format(getattr(settings, "MEILISEARCH_INDEX_TIRES", "tires_v1")),
    }
    if name in protected:
        raise RuntimeError(
            "Refusing to operate: the wheels index name {!r} collides with a protected index "
            "({}). Check MEILISEARCH_INDEX_WHEELS.".format(name, ", ".join(sorted(protected)))
        )


def is_configured() -> bool:
    return parts_index.is_configured()


def _client():
    _assert_not_a_shared_index(INDEX_NAME_WHEELS)
    return parts_index._get_client()


# ==========================================================================================
# Projection
# ==========================================================================================
_PROJECTION_SQL = """
SELECT
    mp.id                       AS id,
    mp.brand_id                 AS brand_id,
    b.name                      AS brand_name,
    mp.part_number              AS part_number,
    mp.sku                      AS sku,
    mp.gtin                     AS gtin,
    COALESCE(NULLIF(mp.image_url, ''), feed.image_url) AS image_url,
    ws.model_name               AS model_name,
    ws.sub_model                AS sub_model,
    ws.style_number             AS style_number,
    ws.size_display             AS size_display,
    ws.diameter_in              AS diameter_in,
    ws.width_in                 AS width_in,
    ws.bolt_lug_count           AS bolt_lug_count,
    ws.bolt_circle_mm           AS bolt_circle_mm,
    ws.bolt_pattern_display     AS bolt_pattern_display,
    ws.bolt_lug_count_2         AS bolt_lug_count_2,
    ws.bolt_circle_mm_2         AS bolt_circle_mm_2,
    ws.bolt_pattern_2_display   AS bolt_pattern_2_display,
    ws.is_blank_drilled         AS is_blank_drilled,
    ws.offset_mm                AS offset_mm,
    ws.backspacing_in           AS backspacing_in,
    ws.center_bore_mm           AS center_bore_mm,
    ws.load_rating_lb           AS load_rating_lb,
    ws.weight_lb                AS weight_lb,
    ws.finish                   AS finish,
    ws.finish_family            AS finish_family,
    ws.construction             AS construction,
    ws.material                 AS material,
    ws.vehicle_class            AS vehicle_class,
    ws.is_beadlock              AS is_beadlock,
    ws.is_dually                AS is_dually,
    ws.tpms_compatible          AS tpms_compatible,
    ws.lug_seat                 AS lug_seat,
    ws.search_aliases           AS search_aliases,
    ws.size_disputed            AS size_disputed,
    COALESCE(o.in_stock, FALSE)                      AS in_stock,
    COALESCE(o.available_qty, 0)                     AS available_qty,
    COALESCE(o.distributor_ids, ARRAY[]::int[])      AS distributor_ids,
    COALESCE(o.distributor_names, ARRAY[]::text[])   AS distributor_names,
    ws.updated_at               AS updated_at
FROM master_parts mp
JOIN wheel_specs ws ON ws.master_part_id = mp.id
JOIN brands b ON b.id = mp.brand_id
-- The feed's own image, used only where the master part has none. 17,084 wheels have no image
-- on master_parts and Wheel Pros publishes one for 10,046 of them, so this is the difference
-- between a populated grid and a wall of placeholders. Read at projection time rather than
-- written back: master_parts is the shared parts table and this index has no business writing to
-- it. LIMIT 1 because a part number can appear twice in the feed (see wheel_enrichment), and the
-- freshest row is the one still being maintained.
LEFT JOIN LATERAL (
    SELECT wp.image_url
    FROM wheelpros_parts wp
    WHERE wp.part_number = ws.source_external_id
      AND ws.source_feed = 'wheelpros'
      AND wp.image_url IS NOT NULL
      AND wp.image_url <> ''
    ORDER BY wp.run_date DESC NULLS LAST, wp.id DESC
    LIMIT 1
) feed ON TRUE
LEFT JOIN LATERAL (
    SELECT
        bool_or(COALESCE(ppi.warehouse_total_qty, 0) > 0) AS in_stock,
        COALESCE(SUM(COALESCE(ppi.warehouse_total_qty, 0)), 0) AS available_qty,
        array_agg(DISTINCT pp.provider_id) AS distributor_ids,
        array_agg(DISTINCT pr.name)        AS distributor_names
    FROM provider_parts pp
    JOIN providers pr ON pr.id = pp.provider_id
    LEFT JOIN provider_part_inventory ppi ON ppi.provider_part_id = pp.id
    WHERE pp.master_part_id = mp.id
      AND pp.is_discontinued = FALSE
) o ON TRUE
WHERE mp.product_type = 'wheel'
  AND mp.id > %s
  {brand_clause}
ORDER BY mp.id
LIMIT %s
"""

_PLACEHOLDER_VALUES = frozenset(["", "NA", "N/A", "NONE", "NULL", "-"])


def _text_or_empty(value: typing.Any) -> str:
    text = ("" if value is None else str(value)).strip()
    return "" if text.upper() in _PLACEHOLDER_VALUES else text


def _normalized_gtin(value: typing.Any) -> str:
    text = _text_or_empty(value)
    return text if text.isdigit() else ""


def _trim(value: typing.Any) -> str:
    """139.70 -> "139.7", 135.00 -> "135". The way a customer types it."""
    text = format(float(value), "f").rstrip("0").rstrip(".")
    return text or "0"


def _number(value: typing.Any) -> typing.Any:
    """Decimals must reach the index as floats: a filter on ``diameter_in = 20`` matches nothing
    against a string, and Meilisearch will not coerce."""
    return None if value is None else float(value)


def project_wheel(row: typing.Mapping[str, typing.Any]) -> typing.Dict[str, typing.Any]:
    """One projection row -> one document. **Pure**: no IO, so the shape is testable without a
    database or a search server."""
    lug_counts = [c for c in (row["bolt_lug_count"], row["bolt_lug_count_2"]) if c is not None]
    circles = [_number(c) for c in (row["bolt_circle_mm"], row["bolt_circle_mm_2"]) if c is not None]
    patterns = [p for p in (row["bolt_pattern_display"], row["bolt_pattern_2_display"]) if p]

    aliases = list(row["search_aliases"] or [])
    # Every way a customer might write this wheel's fitment, so an unparsed query still lands.
    # Both spellings of each pattern: the feed published one of "6x5.5" and "6x139.7" and a
    # customer may type either.
    fitment_parts = [row["size_display"], *patterns]
    for circle, lugs in (
        (row["bolt_circle_mm"], row["bolt_lug_count"]),
        (row["bolt_circle_mm_2"], row["bolt_lug_count_2"]),
    ):
        if circle is not None and lugs is not None:
            fitment_parts.append("{}x{}".format(lugs, _trim(circle)))
    if row["offset_mm"] is not None:
        fitment_parts.append("{:+d}mm".format(int(row["offset_mm"])))

    search_text = " ".join(
        part for part in [row["brand_name"], row["model_name"], row["style_number"], row["finish"], *aliases] if part
    )

    document: typing.Dict[str, typing.Any] = {
        "id": row["id"],
        "brand_id": row["brand_id"],
        "brand_name": row["brand_name"] or "",
        "part_number": row["part_number"] or "",
        "sku": row["sku"] or "",
        "gtin": _normalized_gtin(row["gtin"]),
        "image_url": _text_or_empty(row["image_url"]),
        "model_name": row["model_name"] or "",
        "sub_model": row["sub_model"] or "",
        "style_number": row["style_number"] or "",
        "search_text": search_text,
        "fitment_text": " ".join(dict.fromkeys(p for p in fitment_parts if p)),
        # fitment
        "size_display": row["size_display"],
        "diameter_in": _number(row["diameter_in"]),
        "width_in": _number(row["width_in"]),
        "bolt_lug_counts": lug_counts,
        "bolt_circles_mm": circles,
        "bolt_patterns": patterns,
        "bolt_pattern_display": row["bolt_pattern_display"] or "",
        "is_blank_drilled": bool(row["is_blank_drilled"]),
        "offset_mm": row["offset_mm"],
        "backspacing_in": _number(row["backspacing_in"]),
        "center_bore_mm": _number(row["center_bore_mm"]),
        "load_rating_lb": row["load_rating_lb"],
        "weight_lb": _number(row["weight_lb"]),
        # the wheel
        "finish": row["finish"] or "",
        "finish_family": row["finish_family"] or "",
        "construction": row["construction"] or "",
        "material": row["material"] or "",
        "vehicle_class": row["vehicle_class"] or "",
        "lug_seat": row["lug_seat"] or "",
        "size_disputed": bool(row["size_disputed"]),
        # commerce
        "in_stock": bool(row["in_stock"]),
        "available_qty": int(row["available_qty"] or 0),
        "distributor_ids": list(row["distributor_ids"] or []),
        "distributor_names": list(row["distributor_names"] or []),
        "distributor_count": len(row["distributor_ids"] or []),
        "updated_at": row["updated_at"].isoformat() if row["updated_at"] else None,
    }

    # NULL is unknown: leave the key out rather than asserting false.
    for flag in _TRISTATE_FLAGS:
        if row[flag] is not None:
            document[flag] = bool(row[flag])
    return document


def iter_documents(
    *,
    brand_ids: typing.Optional[typing.Sequence[int]] = None,
    batch_size: int = REINDEX_BATCH_SIZE,
) -> typing.Iterator[typing.List[typing.Dict[str, typing.Any]]]:
    """Batches of projected documents, keyset-paginated by ``master_parts.id``."""
    brand_clause = ""
    extra: typing.List[typing.Any] = []
    if brand_ids:
        brand_clause = "AND mp.brand_id = ANY(%s)"
        extra = [list(brand_ids)]
    sql = _PROJECTION_SQL.format(brand_clause=brand_clause)

    last_id = 0
    while True:
        with connection.cursor() as cursor:
            cursor.execute(sql, [last_id] + extra + [batch_size])
            columns = [column[0] for column in cursor.description]
            rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
        if not rows:
            return
        last_id = rows[-1]["id"]
        yield [project_wheel(row) for row in rows]


def indexable_count(brand_ids: typing.Optional[typing.Sequence[int]] = None) -> int:
    sql = (
        "SELECT count(*) FROM master_parts mp JOIN wheel_specs ws ON ws.master_part_id = mp.id "
        "WHERE mp.product_type = 'wheel'"
    )
    params: typing.List[typing.Any] = []
    if brand_ids:
        sql += " AND mp.brand_id = ANY(%s)"
        params.append(list(brand_ids))
    with connection.cursor() as cursor:
        cursor.execute(sql, params)
        return cursor.fetchone()[0]


# ==========================================================================================
# Index management -- the machinery lives in src.search.index_builder
# ==========================================================================================
def live_filterable_attributes() -> typing.Optional[typing.FrozenSet[str]]:
    if not is_configured():
        return None
    return index_builder.live_filterable_attributes(_client(), SPEC)


def setup_index() -> bool:
    if not is_configured():
        logger.warning("%s Meilisearch not configured; skipping setup.", _LOG_PREFIX)
        return False
    return index_builder.setup(_client(), SPEC)


def upsert_documents(documents: typing.Sequence[typing.Dict[str, typing.Any]]) -> int:
    if not is_configured() or not documents:
        return 0
    client = _client()
    task = client.index(SPEC.name).add_documents(list(documents), primary_key=SPEC.primary_key)
    index_builder.wait(client, task.task_uid)
    return len(documents)


def delete_documents(master_part_ids: typing.Sequence[int]) -> int:
    """Drop documents whose master parts are no longer wheels (reclassified, or de-enriched)."""
    if not is_configured() or not master_part_ids:
        return 0
    client = _client()
    task = client.index(SPEC.name).delete_documents([int(i) for i in master_part_ids])
    index_builder.wait(client, task.task_uid)
    return len(master_part_ids)


def reindex(
    *,
    brand_ids: typing.Optional[typing.Sequence[int]] = None,
    batch_size: int = REINDEX_BATCH_SIZE,
) -> typing.Tuple[int, int]:
    """Full rebuild into staging, verified, then swapped in atomically. ``(live, expected)``."""
    if not is_configured():
        logger.warning("%s Meilisearch not configured; skipping reindex.", _LOG_PREFIX)
        return 0, 0
    _assert_not_a_shared_index(SPEC.name)
    _assert_not_a_shared_index(SPEC.staging_name)
    return index_builder.rebuild(
        _client(),
        SPEC,
        iter_documents=lambda: iter_documents(brand_ids=brand_ids, batch_size=batch_size),
        expected=indexable_count(brand_ids),
    )
