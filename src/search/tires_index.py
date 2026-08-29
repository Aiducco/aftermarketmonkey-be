"""
The ``tires_v1`` Meilisearch index: projection, settings, and the jobs that build it.

**This module never touches the parts or vehicles indexes.** It deliberately shares no mutable
state with ``src.search.meilisearch_client`` -- it imports only that module's client factory and
its ``is_configured`` check, both read-only -- and ``_assert_not_a_shared_index`` refuses to run
if the tires index name ever resolves to the parts or vehicles name. That guard exists because
the destructive operations here (``swap_indexes``, ``delete_index``) are aimed by a *string*, and
a mistyped environment variable would otherwise point a swap at the live parts index.

Why a separate index rather than a ``product_type`` filter on ``parts``: the whole point of tire
mode is that size fields are **filterable and not searchable**. Searching "275/70R18" against a
text index is what returns 4,463 results; filtering ``section_width_mm = 275 AND aspect_ratio = 70
AND rim_diameter_in = 18`` returns the tires that actually are that size. Those two behaviours
cannot coexist in one index's ``searchableAttributes``.

**Price is deliberately absent from the document.** Cost in this system is per-company
(``ProviderPartCompanyPricing``), so there is no single number that is correct for every viewer,
and indexing one company's negotiated cost where another company can filter on it would be wrong
in a way that is invisible. Stock *is* global, so ``in_stock`` / ``available_qty`` /
``distributor_ids`` are indexed and are what the FE filters on; prices are resolved per company
from Postgres for the visible page only.
"""
import decimal
import logging
import time
import typing

from django.conf import settings
from django.db import connection

from src.search import meilisearch_client as parts_index

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[TIRES-INDEX]"

INDEX_NAME_TIRES = getattr(settings, "MEILISEARCH_INDEX_TIRES", "tires_v1")

REINDEX_BATCH_SIZE = getattr(settings, "MEILISEARCH_TIRES_BATCH_SIZE", 5000)

# Nothing here is text-searched by size. ``part_number`` and ``gtin`` are the exact-match lanes
# (typo tolerance is disabled on them below -- a one-character typo on a part number is a
# different part, not a near miss), and ``search_text`` carries brand + model + aliases, which is
# what a shopper actually types when they are not typing a size.
SEARCHABLE_ATTRIBUTES = ["part_number", "gtin", "brand_name", "model_name", "search_text"]

FILTERABLE_ATTRIBUTES = [
    # size -- the entire reason this index exists
    "service_type",
    "section_width_mm",
    "aspect_ratio",
    "rim_diameter_in",
    "section_width_in",
    "overall_diameter_in",
    "construction",
    "notation",
    # service description
    "load_index",
    "speed_rating",
    "speed_sort",
    "load_range",
    "ply_rating",
    # classification
    # model_name is both searchable and filterable: text search is how a shopper finds it, but
    # "every other size of this same model" on a product page is an exact filter, not a search.
    "model_name",
    "tread_category",
    "vehicle_class",
    "use_case_tags",
    "tier",
    "noise_level",
    "is_3pmsf",
    "is_ms",
    "is_run_flat",
    "is_studdable",
    # fitment to a wheel
    "rim_width_min_in",
    "rim_width_max_in",
    # commerce (no price -- see the module docstring)
    "brand_id",
    "brand_name",
    "in_stock",
    "distributor_ids",
    "distributor_names",
    "distributor_count",
]

# ``speed_sort`` exists because you cannot range-filter on the letter: H is 210 km/h and sits
# between U and V, so "T or above" is ``speed_sort >= 23``, never ``speed_rating >= 'T'``.
SORTABLE_ATTRIBUTES = [
    "overall_diameter_in",
    "rim_diameter_in",
    "section_width_mm",
    "load_index",
    "speed_sort",
    "brand_name",
]

MAX_VALUES_PER_FACET = 200
MAX_TOTAL_HITS = 5000

# Booleans that mean "unknown" when NULL. These keys are omitted from the document entirely
# rather than serialised as false -- a false would make the severe-snow facet silently exclude
# every tire whose certification we simply do not know, which looks like missing inventory and
# gets reported as "search is broken", not as a data gap.
_TRISTATE_FLAGS = ("is_3pmsf", "is_ms", "is_run_flat", "is_studdable")


def _assert_not_a_shared_index(name: str) -> None:
    """
    Refuse to aim a tires operation at another index.

    ``swap_indexes`` and ``delete_index`` take a name, so a bad ``MEILISEARCH_INDEX_TIRES`` is the
    one configuration mistake that could destroy the parts index. Cheap to check, catastrophic to
    miss.
    """
    protected = {
        parts_index.INDEX_NAME,
        parts_index.INDEX_NAME_VEHICLES,
        "{}_staging".format(parts_index.INDEX_NAME),
    }
    if name in protected:
        raise RuntimeError(
            "Refusing to operate: the tires index name {!r} collides with a protected index "
            "({}). Check MEILISEARCH_INDEX_TIRES.".format(name, ", ".join(sorted(protected)))
        )


def is_configured() -> bool:
    return parts_index.is_configured()


def _client():
    _assert_not_a_shared_index(INDEX_NAME_TIRES)
    return parts_index._get_client()


# ==========================================================================================
# Projection
# ==========================================================================================

# One row per indexable tire. Joins are all to things that are true for everyone: stock is global,
# pricing is not and is absent by design. ``speed_sort`` is the only value still read from a
# lookup at projection time -- max_load_lb / max_speed_mph / ply_rating are already denormalised
# onto tire_specs by the enrichment job.
_PROJECTION_SQL = """
SELECT
    mp.id                       AS id,
    mp.brand_id                 AS brand_id,
    b.name                      AS brand_name,
    mp.part_number              AS part_number,
    mp.sku                      AS sku,
    mp.gtin                     AS gtin,
    mp.image_url                AS image_url,
    ts.model_name               AS model_name,
    ts.sub_model                AS sub_model,
    ts.size_display             AS size_display,
    ts.notation                 AS notation,
    ts.service_type             AS service_type,
    ts.section_width_mm         AS section_width_mm,
    ts.aspect_ratio             AS aspect_ratio,
    ts.section_width_in         AS section_width_in,
    ts.overall_diameter_in      AS overall_diameter_in,
    ts.rim_diameter_in          AS rim_diameter_in,
    ts.construction             AS construction,
    ts.load_index               AS load_index,
    ts.load_index_dual          AS load_index_dual,
    ts.max_load_lb              AS max_load_lb,
    ts.speed_rating             AS speed_rating,
    ts.max_speed_mph            AS max_speed_mph,
    sr.sort_order               AS speed_sort,
    ts.load_range               AS load_range,
    ts.ply_rating               AS ply_rating,
    ts.tread_category           AS tread_category,
    tc.label                    AS tread_category_label,
    ts.vehicle_class            AS vehicle_class,
    ts.use_case_tags            AS use_case_tags,
    ts.search_aliases           AS search_aliases,
    ts.tier                     AS tier,
    ts.noise_level              AS noise_level,
    ts.is_3pmsf                 AS is_3pmsf,
    ts.is_ms                    AS is_ms,
    ts.is_run_flat              AS is_run_flat,
    ts.is_studdable             AS is_studdable,
    ts.tread_depth_32nds        AS tread_depth_32nds,
    ts.max_psi                  AS max_psi,
    ts.rim_width_min_in         AS rim_width_min_in,
    ts.rim_width_max_in         AS rim_width_max_in,
    ts.utqg_treadwear           AS utqg_treadwear,
    ts.utqg_traction            AS utqg_traction,
    ts.utqg_temperature         AS utqg_temperature,
    COALESCE(o.in_stock, FALSE) AS in_stock,
    COALESCE(o.available_qty, 0) AS available_qty,
    COALESCE(o.distributor_ids, ARRAY[]::int[])      AS distributor_ids,
    COALESCE(o.distributor_names, ARRAY[]::text[])   AS distributor_names,
    ts.updated_at               AS updated_at
FROM master_parts mp
JOIN tire_specs ts ON ts.master_part_id = mp.id
JOIN brands b ON b.id = mp.brand_id
LEFT JOIN speed_rating sr ON sr.code = ts.speed_rating
LEFT JOIN tread_category tc ON tc.code = ts.tread_category
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
WHERE mp.product_type = 'tire'
  AND mp.id > %s
  {brand_clause}
ORDER BY mp.id
LIMIT %s
"""


# Placeholder strings distributors ship in place of a null. 536,041 master parts carry a literal
# "NA" in image_url (599 of them tires), which is truthy in Python and would be indexed as a URL.
_PLACEHOLDER_VALUES = frozenset(["", "NA", "N/A", "NONE", "NULL", "-"])


def _text_or_empty(value: typing.Any) -> str:
    """Trim, and treat a distributor's placeholder-for-null as empty."""
    text = ("" if value is None else str(value)).strip()
    return "" if text.upper() in _PLACEHOLDER_VALUES else text


def _normalized_gtin(value: typing.Any) -> str:
    """
    Strip the ``.0`` that a float round-trip leaves on a GTIN.

    2,674 master parts hold "840269932199.0" -- some upstream feed parse read the barcode as a
    number. A GTIN is an exact-match search lane with typo tolerance disabled, so the suffix does
    not degrade the match, it prevents it entirely. Normalised here rather than in the source
    table because fixing 2,674 rows is a data migration, not an indexing decision.
    """
    text = _text_or_empty(value)
    if text.endswith(".0") and text[:-2].isdigit():
        return text[:-2]
    return text


def _decimal_to_float(value: typing.Any) -> typing.Any:
    """Meilisearch cannot filter or sort on a string, and psycopg2 hands back NUMERIC as Decimal,
    which serialises as a string. Every dimensional field goes through here."""
    return float(value) if isinstance(value, decimal.Decimal) else value


def project_tire(row: typing.Mapping[str, typing.Any]) -> typing.Dict[str, typing.Any]:
    """
    One projection row -> one Meilisearch document. **Pure**: no IO, no globals, so the shape can
    be tested without a database or a search server.

    Two rules do the real work here:
      * every dimensional value is a float, never a Decimal or a string -- otherwise
        ``rim_diameter_in = 18`` silently matches nothing;
      * a tri-state flag that is NULL is **left out of the document**, never written as false.
    """
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
        # size
        "size_display": row["size_display"],
        "notation": row["notation"],
        "service_type": row["service_type"] or "",
        "section_width_mm": row["section_width_mm"],
        "aspect_ratio": row["aspect_ratio"],
        "section_width_in": _decimal_to_float(row["section_width_in"]),
        "overall_diameter_in": _decimal_to_float(row["overall_diameter_in"]),
        "rim_diameter_in": _decimal_to_float(row["rim_diameter_in"]),
        "construction": row["construction"] or "",
        # service description, with the resolved numbers a buyer can actually read
        "load_index": row["load_index"],
        "load_index_dual": row["load_index_dual"],
        "max_load_lb": row["max_load_lb"],
        "speed_rating": row["speed_rating"] or "",
        "max_speed_mph": row["max_speed_mph"],
        "speed_sort": row["speed_sort"],
        "load_range": row["load_range"] or "",
        "ply_rating": row["ply_rating"],
        # classification
        "tread_category": row["tread_category"] or "",
        "tread_category_label": row["tread_category_label"] or "",
        "vehicle_class": row["vehicle_class"] or "",
        "use_case_tags": list(row["use_case_tags"] or []),
        "tier": row["tier"] or "",
        "noise_level": row["noise_level"] or "",
        # distributor specs
        "tread_depth_32nds": _decimal_to_float(row["tread_depth_32nds"]),
        "max_psi": row["max_psi"],
        "rim_width_min_in": _decimal_to_float(row["rim_width_min_in"]),
        "rim_width_max_in": _decimal_to_float(row["rim_width_max_in"]),
        "utqg_treadwear": row["utqg_treadwear"],
        "utqg_traction": row["utqg_traction"] or "",
        "utqg_temperature": row["utqg_temperature"] or "",
        # commerce (stock only -- price is per company, see the module docstring)
        "in_stock": bool(row["in_stock"]),
        "available_qty": int(row["available_qty"] or 0),
        "distributor_ids": [i for i in (row["distributor_ids"] or []) if i is not None],
        "distributor_names": [n for n in (row["distributor_names"] or []) if n],
        "updated_at": row["updated_at"].isoformat() if row.get("updated_at") else None,
    }
    document["distributor_count"] = len(document["distributor_ids"])

    # The one free-text lane. Aliases are in here rather than in their own searchable attribute so
    # that "duratrac" and "Wrangler DuraTrac" score against the same field.
    document["search_text"] = " ".join(
        part
        for part in [
            row["brand_name"] or "",
            row["model_name"] or "",
            row["sub_model"] or "",
            " ".join(row["search_aliases"] or []),
        ]
        if part
    ).lower()

    for flag in _TRISTATE_FLAGS:
        value = row[flag]
        if value is not None:
            document[flag] = bool(value)

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
        yield [project_tire(row) for row in rows]


def indexable_count(brand_ids: typing.Optional[typing.Sequence[int]] = None) -> int:
    """How many documents a full build should produce. Used to verify a build before swapping --
    a silently short index is worse than a stale one."""
    sql = (
        "SELECT count(*) FROM master_parts mp JOIN tire_specs ts ON ts.master_part_id = mp.id "
        "WHERE mp.product_type = 'tire'"
    )
    params: typing.List[typing.Any] = []
    if brand_ids:
        sql += " AND mp.brand_id = ANY(%s)"
        params.append(list(brand_ids))
    with connection.cursor() as cursor:
        cursor.execute(sql, params)
        return cursor.fetchone()[0]


# ==========================================================================================
# Index management
# ==========================================================================================


def _apply_settings(index) -> None:
    index.update_searchable_attributes(SEARCHABLE_ATTRIBUTES)
    index.update_filterable_attributes(FILTERABLE_ATTRIBUTES)
    index.update_sortable_attributes(SORTABLE_ATTRIBUTES)
    index.update_faceting_settings({"maxValuesPerFacet": MAX_VALUES_PER_FACET})
    index.update_pagination_settings({"maxTotalHits": MAX_TOTAL_HITS})
    index.update_distinct_attribute("id")
    # A typo in a part number or a GTIN is a different product, not a near miss.
    index.update_typo_tolerance({"disableOnAttributes": ["part_number", "gtin"]})


def setup_index(index_name: str = INDEX_NAME_TIRES) -> bool:
    """Create and configure the tires index. Idempotent."""
    if not is_configured():
        logger.warning("%s Meilisearch not configured; skipping setup.", _LOG_PREFIX)
        return False
    _assert_not_a_shared_index(index_name)
    try:
        client = _client()
        try:
            client.create_index(index_name, {"primaryKey": "id"})
        except Exception:
            pass  # already exists -- configuring below is the idempotent part
        _apply_settings(client.index(index_name))
        logger.info("%s index '%s' configured", _LOG_PREFIX, index_name)
        return True
    except Exception as exc:
        logger.exception("%s setup failed: %s", _LOG_PREFIX, exc)
        return False


def _wait(client, task_uid: int, timeout_ms: int = 600_000) -> None:
    client.wait_for_task(task_uid, timeout_in_ms=timeout_ms)


def upsert_documents(
    documents: typing.Sequence[typing.Dict[str, typing.Any]],
    index_name: str = INDEX_NAME_TIRES,
) -> int:
    """Add or replace documents by id. This is the incremental path."""
    if not is_configured() or not documents:
        return 0
    _assert_not_a_shared_index(index_name)
    client = _client()
    task = client.index(index_name).add_documents(list(documents), primary_key="id")
    _wait(client, task.task_uid)
    return len(documents)


def delete_documents(master_part_ids: typing.Sequence[int], index_name: str = INDEX_NAME_TIRES) -> int:
    """Drop documents whose master parts are no longer tires (reclassified, or de-enriched)."""
    if not is_configured() or not master_part_ids:
        return 0
    _assert_not_a_shared_index(index_name)
    client = _client()
    task = client.index(index_name).delete_documents([int(i) for i in master_part_ids])
    _wait(client, task.task_uid)
    return len(master_part_ids)


def reindex(
    *,
    brand_ids: typing.Optional[typing.Sequence[int]] = None,
    batch_size: int = REINDEX_BATCH_SIZE,
) -> typing.Tuple[int, int]:
    """
    Full rebuild into a staging index, verified, then swapped in atomically.

    Returns ``(indexed, expected)``. **The swap is refused unless the two match**: a partially
    built index that quietly replaces a good one is the failure mode worth engineering against,
    because it looks like inventory disappearing rather than like a broken job.

    ``brand_ids`` is for testing a single brand end-to-end. It scopes the *staging build*, so it
    would swap in an index containing only that brand -- the command refuses to combine it with a
    swap for exactly that reason.
    """
    if not is_configured():
        logger.warning("%s Meilisearch not configured; skipping reindex.", _LOG_PREFIX)
        return 0, 0

    staging_name = "{}_staging".format(INDEX_NAME_TIRES)
    _assert_not_a_shared_index(INDEX_NAME_TIRES)
    _assert_not_a_shared_index(staging_name)

    expected = indexable_count(brand_ids)
    started = time.monotonic()
    logger.info(
        "%s rebuild start | live=%s staging=%s expected_docs=%s",
        _LOG_PREFIX,
        INDEX_NAME_TIRES,
        staging_name,
        expected,
    )

    client = _client()
    # Start from empty: a leftover staging index from a failed run would otherwise contribute
    # stale documents to the count check and to the swapped-in result.
    try:
        task = client.delete_index(staging_name)
        _wait(client, task.task_uid, timeout_ms=120_000)
    except Exception:
        pass
    client.create_index(staging_name, {"primaryKey": "id"})
    _apply_settings(client.index(staging_name))

    indexed = 0
    task_uids: typing.List[int] = []
    for batch in iter_documents(brand_ids=brand_ids, batch_size=batch_size):
        task = client.index(staging_name).add_documents(batch, primary_key="id")
        task_uids.append(task.task_uid)
        indexed += len(batch)
        logger.info("%s staged %s/%s", _LOG_PREFIX, indexed, expected)

    for task_uid in task_uids:
        _wait(client, task_uid)

    actual = client.index(staging_name).get_stats().number_of_documents
    if actual != expected:
        logger.error(
            "%s REFUSING TO SWAP: staging has %s documents, expected %s. Live index '%s' left "
            "untouched; staging kept for inspection.",
            _LOG_PREFIX,
            actual,
            expected,
            INDEX_NAME_TIRES,
        )
        return actual, expected

    # Meilisearch refuses to swap against an index that does not exist, which is the state on a
    # first-ever build. Create it empty so the swap has something to trade places with.
    try:
        client.create_index(INDEX_NAME_TIRES, {"primaryKey": "id"})
        _apply_settings(client.index(INDEX_NAME_TIRES))
    except Exception:
        pass  # already exists, which is the normal case after the first build

    # swap_indexes is ASYNCHRONOUS. Without waiting on the task and checking that it succeeded,
    # a failed swap is indistinguishable from a successful one -- and the staging delete below
    # would then destroy the only good copy of the data while this function reported success.
    swap_task = client.swap_indexes([{"indexes": [INDEX_NAME_TIRES, staging_name]}])
    _wait(client, swap_task.task_uid, timeout_ms=120_000)
    swap_status = client.get_task(swap_task.task_uid).status
    if swap_status != "succeeded":
        logger.error(
            "%s SWAP FAILED (status=%s). Live index '%s' is unchanged and staging '%s' is kept "
            "with the freshly built documents -- do not delete it before investigating.",
            _LOG_PREFIX,
            swap_status,
            INDEX_NAME_TIRES,
            staging_name,
        )
        return 0, expected

    # Only now is staging safe to drop: it holds the *previous* documents.
    live = client.index(INDEX_NAME_TIRES).get_stats().number_of_documents
    if live != expected:
        logger.error(
            "%s post-swap count is %s, expected %s. Staging '%s' kept for inspection.",
            _LOG_PREFIX,
            live,
            expected,
            staging_name,
        )
        return live, expected

    try:
        task = client.delete_index(staging_name)
        _wait(client, task.task_uid, timeout_ms=120_000)
    except Exception as exc:
        logger.warning("%s post-swap staging cleanup failed (non-fatal): %s", _LOG_PREFIX, exc)

    logger.info(
        "%s rebuild done | %s documents live in '%s' in %.1fs",
        _LOG_PREFIX,
        live,
        INDEX_NAME_TIRES,
        time.monotonic() - started,
    )
    return live, expected
