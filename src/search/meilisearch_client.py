"""
Meilisearch client for parts search index.
Backend uses master key for indexing; frontend will use a public read-only key.
"""
import logging
import time
import typing
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait

from django.conf import settings

logger = logging.getLogger(__name__)

INDEX_NAME = getattr(settings, "MEILISEARCH_INDEX_PARTS", "parts")

# Full reindex: tune via MEILISEARCH_REINDEX_BATCH_SIZE and MEILISEARCH_REINDEX_UPLOAD_WORKERS.
# Larger batches + multiple parallel upload threads are faster; retries still cover transient HTTP errors.
REINDEX_DEFAULT_BATCH_SIZE = getattr(settings, "MEILISEARCH_REINDEX_BATCH_SIZE", 20000)
REINDEX_DEFAULT_UPLOAD_WORKERS = getattr(settings, "MEILISEARCH_REINDEX_UPLOAD_WORKERS", 8)

_REINDEX_ADD_RETRIES = 4
_REINDEX_TRANSIENT_SUBSTRINGS = (
    "connection reset",
    "connection aborted",
    "remote end closed",
    "timed out",
    "timeout",
    "temporarily unavailable",
    "bad gateway",
    "gateway time-out",
    "eof occurred",
)


def _transient_meilisearch_error(exc: BaseException) -> bool:
    msg = str(exc).lower()
    return any(s in msg for s in _REINDEX_TRANSIENT_SUBSTRINGS)

# Searchable: what the user types to find results (brand_name is on the document for filtering only)
SEARCHABLE_ATTRIBUTES = ["part_number", "sku", "description", "aaia_code"]

# Filterable: for sidebar / API filters; categories come from first ProviderPart with non-empty category
FILTERABLE_ATTRIBUTES = ["brand_name", "category", "overview_category"]


def _get_client():
    """Lazy import to avoid import errors when meilisearch is not installed."""
    import meilisearch

    host = getattr(settings, "MEILISEARCH_HOST", "http://localhost:7700")
    api_key = getattr(settings, "MEILISEARCH_MASTER_KEY", None) or ""
    timeout = getattr(settings, "MEILISEARCH_HTTP_TIMEOUT", 600)
    return meilisearch.Client(host, api_key, timeout=timeout)


def is_configured() -> bool:
    """Return True if Meilisearch is configured (host set)."""
    host = getattr(settings, "MEILISEARCH_HOST", "").strip()
    return bool(host)


def setup_index() -> bool:
    """
    Create index and configure searchable/filterable attributes.
    Idempotent: safe to run multiple times.
    Returns True on success.
    """
    if not is_configured():
        logger.warning("Meilisearch not configured (MEILISEARCH_HOST empty). Skipping setup.")
        return False

    try:
        client = _get_client()
        index = client.index(INDEX_NAME)

        index.update_searchable_attributes(SEARCHABLE_ATTRIBUTES)
        index.update_filterable_attributes(FILTERABLE_ATTRIBUTES)

        logger.info("Meilisearch index '%s' configured: searchable=%s, filterable=%s",
                    INDEX_NAME, SEARCHABLE_ATTRIBUTES, FILTERABLE_ATTRIBUTES)
        return True
    except Exception as e:
        logger.exception("Meilisearch setup failed: %s", str(e))
        return False


def _get_brand_name(part) -> str:
    """Get brand name from MasterPart (part.brand -> Brands.name)."""
    if not part.brand_id:
        return ""
    try:
        brand = part.brand
        return (brand.name or "").strip() if brand else ""
    except Exception:
        return ""


def _index_categories_for_master_part(part) -> typing.Tuple[str, str]:
    """
    Map / index fields: first ProviderPart (by id) with non-empty ``category`` ->
    (category, overview_category). Empty strings when none.
    Used for single-part indexing (e.g. post-save signals).
    For bulk reindex use _bulk_category_map_for_ids() instead.
    """
    if not part or not getattr(part, "id", None):
        return "", ""
    cache = getattr(part, "_prefetched_objects_cache", None)
    if cache and "provider_parts" in cache:
        for pp in part.provider_parts.all():
            c = (pp.category or "").strip()
            if c:
                return c, (pp.overview_category or "").strip()
    from src.models import ProviderPart

    for pp in ProviderPart.objects.filter(master_part_id=part.id).order_by("id"):
        c = (pp.category or "").strip()
        if c:
            return c, (pp.overview_category or "").strip()
    return "", ""


def _bulk_category_map_for_ids(
    master_part_ids: typing.List[int],
) -> typing.Dict[int, typing.Tuple[str, str]]:
    """
    Single query: returns {master_part_id: (category, overview_category)} for the first
    ProviderPart (lowest id) with a non-empty category for each id in the list.
    Replaces the per-part N+1 lookup during bulk reindex.
    """
    from src.models import ProviderPart

    result: typing.Dict[int, typing.Tuple[str, str]] = {}
    if not master_part_ids:
        return result
    seen: typing.Set[int] = set()
    qs = (
        ProviderPart.objects
        .filter(master_part_id__in=master_part_ids)
        .exclude(category__isnull=True)
        .exclude(category="")
        .order_by("master_part_id", "id")
        .values("master_part_id", "category", "overview_category")
    )
    for row in qs:
        mpid = row["master_part_id"]
        if mpid not in seen:
            result[mpid] = (row["category"] or "", row["overview_category"] or "")
            seen.add(mpid)
    return result


def _build_docs_for_batch(
    parts: typing.List,
    cat_map: typing.Dict[int, typing.Tuple[str, str]],
) -> typing.List[typing.Dict]:
    """
    Build Meilisearch document dicts for a batch of MasterPart instances.
    ``cat_map`` comes from _bulk_category_map_for_ids() — pre-computed in the main thread
    so worker threads only do the HTTP upload, not DB queries.
    """
    docs = []
    for part in parts:
        brand_name = _get_brand_name(part)
        category, overview_category = cat_map.get(part.id, ("", ""))
        docs.append({
            "id": part.id,
            "brand_id": part.brand_id,
            "brand_name": brand_name or "",
            "part_number": part.part_number or "",
            "sku": part.sku or "",
            "description": (part.description or "")[:10000],
            "aaia_code": part.aaia_code or "",
            "image_url": part.image_url or "",
            "category": category,
            "overview_category": overview_category,
            "created_at": part.created_at.isoformat() if part.created_at else None,
            "updated_at": part.updated_at.isoformat() if part.updated_at else None,
        })
    return docs


def _upload_raw_docs(docs: typing.List[typing.Dict]) -> bool:
    """
    Upload pre-built document dicts to Meilisearch with retries.
    Returns True on success. Workers and single-threaded paths both use this.
    """
    if not docs:
        return True
    last_err: typing.Optional[BaseException] = None
    for attempt in range(_REINDEX_ADD_RETRIES):
        try:
            client = _get_client()
            index = client.index(INDEX_NAME)
            index.add_documents(docs, primary_key="id")
            return True
        except Exception as e:
            last_err = e
            if attempt < _REINDEX_ADD_RETRIES - 1 and _transient_meilisearch_error(e):
                wait_s = min(30.0, 2.0 ** attempt)
                logger.warning(
                    "Meilisearch _upload_raw_docs: transient error (retry) | len=%s attempt=%s/%s "
                    "wait_s=%.1f err_type=%s err=%s",
                    len(docs), attempt + 1, _REINDEX_ADD_RETRIES,
                    wait_s, type(e).__name__, e,
                )
                time.sleep(wait_s)
                continue
            logger.exception(
                "Meilisearch _upload_raw_docs: failed | len=%s attempt=%s err_type=%s",
                len(docs), attempt + 1, type(e).__name__,
            )
            return False
    if last_err:
        logger.exception("Meilisearch _upload_raw_docs: exhausted retries | err=%s", last_err)
    return False


def _part_to_document(part) -> typing.Dict:
    """Convert MasterPart instance to Meilisearch document."""
    return master_part_to_index_shape(part)


def master_part_to_index_shape(part) -> typing.Dict:
    """
    Public shape of a MasterPart as stored in the Meilisearch parts index.
    Use for API responses that should match indexed search hits (e.g. audit/history tables).

    ``category`` and ``overview_category`` are filter-only in Meilisearch; values come from the
    first ``ProviderPart`` (by id) with a non-empty ``category`` for that master part.
    """
    brand_name = _get_brand_name(part)
    category, overview_category = _index_categories_for_master_part(part)
    return {
        "id": part.id,
        "brand_id": part.brand_id,
        "brand_name": brand_name or "",  # Never None for Meilisearch
        "part_number": part.part_number or "",
        "sku": part.sku or "",
        "description": (part.description or "")[:10000],  # Meilisearch has limits
        "aaia_code": part.aaia_code or "",
        "image_url": part.image_url or "",
        "category": category or "",
        "overview_category": overview_category or "",
        "created_at": part.created_at.isoformat() if part.created_at else None,
        "updated_at": part.updated_at.isoformat() if part.updated_at else None,
    }


def add_documents(parts: typing.List) -> bool:
    """
    Index a list of MasterPart instances.
    Returns True on success.
    Retries on transient network / proxy errors (connection reset, timeouts).
    """
    if not is_configured() or not parts:
        return bool(not parts)

    docs = [_part_to_document(p) for p in parts]
    last_err: typing.Optional[BaseException] = None
    for attempt in range(_REINDEX_ADD_RETRIES):
        try:
            client = _get_client()
            index = client.index(INDEX_NAME)
            index.add_documents(docs, primary_key="id")
            if attempt > 0:
                logger.info(
                    "Meilisearch add_documents: success after retries | len=%s attempts=%s",
                    len(docs),
                    attempt + 1,
                )
            else:
                logger.debug("Meilisearch add_documents: ok | len=%s", len(docs))
            return True
        except Exception as e:
            last_err = e
            if attempt < _REINDEX_ADD_RETRIES - 1 and _transient_meilisearch_error(e):
                wait_s = min(30.0, 2.0 ** attempt)
                logger.warning(
                    "Meilisearch add_documents: transient error (will retry) | len=%s attempt=%s/%s "
                    "wait_s=%.1f err_type=%s err=%s",
                    len(docs),
                    attempt + 1,
                    _REINDEX_ADD_RETRIES,
                    wait_s,
                    type(e).__name__,
                    e,
                )
                time.sleep(wait_s)
                continue
            logger.exception(
                "Meilisearch add_documents: non-retry or final failure | len=%s attempt=%s err_type=%s",
                len(docs),
                attempt + 1,
                type(e).__name__,
            )
            return False
    if last_err:
        logger.exception("Meilisearch add_documents exhausted retries: %s", str(last_err))
    return False


def add_documents_in_batches(
    queryset,
    batch_size: int = 10000,
    max_upload_workers: int = 1,
) -> typing.Tuple[int, int]:
    """
    Index all parts from a MasterPart queryset in batches (cursor-based by id).
    When ``max_upload_workers`` > 1, upload batches concurrently (each worker uses its own
    Meilisearch client). Main thread reads from Django; workers only serialize and POST.
    Returns (total_indexed, total_failed).
    """
    if not is_configured():
        return 0, 0

    # Ensure brand is loaded (MasterPart.brand -> Brands).
    # Category data is fetched per-batch via _bulk_category_map_for_ids — no prefetch_related needed.
    from src.models import MasterPart

    if queryset.model == MasterPart:
        queryset = queryset.select_related("brand")

    if max_upload_workers <= 1:
        total_ok = 0
        total_fail = 0
        last_id = 0
        batch_num = 0
        loop_t0 = time.monotonic()
        last_progress_t = loop_t0

        logger.info(
            "Meilisearch bulk index: started | index=%s batch_size=%s workers=1",
            INDEX_NAME,
            batch_size,
        )

        while True:
            batch = list(queryset.filter(id__gt=last_id).order_by("id")[:batch_size])
            if not batch:
                break
            batch_num += 1
            id_first, id_last = batch[0].id, batch[-1].id
            batch_t0 = time.monotonic()
            cat_map = _bulk_category_map_for_ids([p.id for p in batch])
            docs = _build_docs_for_batch(batch, cat_map)
            ok = _upload_raw_docs(docs)
            batch_dt = time.monotonic() - batch_t0
            if ok:
                total_ok += len(batch)
            else:
                total_fail += len(batch)
            last_id = id_last
            dps = len(batch) / batch_dt if batch_dt > 0 else 0.0
            logger.info(
                "Meilisearch bulk index: batch | num=%s id_range=%s..%s size=%s ok=%s "
                "batch_s=%.3f dps=%.0f total_ok=%s total_fail=%s",
                batch_num,
                id_first,
                id_last,
                len(batch),
                ok,
                batch_dt,
                dps,
                total_ok,
                total_fail,
            )
            now = time.monotonic()
            if now - last_progress_t >= 60.0:
                elapsed = now - loop_t0
                rate = total_ok / elapsed if elapsed > 0 else 0.0
                logger.info(
                    "Meilisearch bulk index: heartbeat | elapsed_s=%.0f total_ok=%s total_fail=%s "
                    "overall_dps=%.0f",
                    elapsed,
                    total_ok,
                    total_fail,
                    rate,
                )
                last_progress_t = now

        elapsed = time.monotonic() - loop_t0
        rate = total_ok / elapsed if elapsed > 0 else 0.0
        logger.info(
            "Meilisearch bulk index: finished | batches=%s total_ok=%s total_fail=%s elapsed_s=%.1f "
            "overall_dps=%.0f",
            batch_num,
            total_ok,
            total_fail,
            elapsed,
            rate,
        )
        return total_ok, total_fail

    def _upload_batch(
        docs: typing.List[typing.Dict],
        batch_index: int,
        id_first: int,
        id_last: int,
    ) -> typing.Tuple[int, int]:
        """Upload pre-built doc dicts. DB queries are done in the main thread; worker only does HTTP."""
        if not docs:
            return 0, 0
        last_err: typing.Optional[BaseException] = None
        upload_t0 = time.monotonic()
        for attempt in range(_REINDEX_ADD_RETRIES):
            try:
                client = _get_client()
                index = client.index(INDEX_NAME)
                index.add_documents(docs, primary_key="id")
                upload_dt = time.monotonic() - upload_t0
                dps = len(docs) / upload_dt if upload_dt > 0 else 0.0
                if attempt > 0:
                    logger.info(
                        "Meilisearch parallel batch: success after retries | batch=%s id_range=%s..%s "
                        "size=%s attempts=%s batch_s=%.3f dps=%.0f",
                        batch_index,
                        id_first,
                        id_last,
                        len(docs),
                        attempt + 1,
                        upload_dt,
                        dps,
                    )
                else:
                    logger.info(
                        "Meilisearch parallel batch: ok | batch=%s id_range=%s..%s size=%s batch_s=%.3f dps=%.0f",
                        batch_index,
                        id_first,
                        id_last,
                        len(docs),
                        upload_dt,
                        dps,
                    )
                return len(docs), 0
            except Exception as e:
                last_err = e
                if attempt < _REINDEX_ADD_RETRIES - 1 and _transient_meilisearch_error(e):
                    wait_s = min(30.0, 2.0 ** attempt)
                    logger.warning(
                        "Meilisearch parallel batch: transient (will retry) | batch=%s id_range=%s..%s "
                        "size=%s attempt=%s/%s wait_s=%.1f err_type=%s err=%s",
                        batch_index,
                        id_first,
                        id_last,
                        len(docs),
                        attempt + 1,
                        _REINDEX_ADD_RETRIES,
                        wait_s,
                        type(e).__name__,
                        e,
                    )
                    time.sleep(wait_s)
                    continue
                logger.exception(
                    "Meilisearch parallel batch: failed | batch=%s id_range=%s..%s size=%s err_type=%s",
                    batch_index,
                    id_first,
                    id_last,
                    len(docs),
                    type(e).__name__,
                )
                return 0, len(docs)
        if last_err:
            logger.exception(
                "Meilisearch parallel batch: exhausted retries | batch=%s id_range=%s..%s err=%s",
                batch_index,
                id_first,
                id_last,
                last_err,
            )
        return 0, len(docs)

    total_ok = 0
    total_fail = 0
    last_id = 0
    max_in_flight = max(2, max_upload_workers * 2)
    in_flight = []
    batch_index = 0
    par_t0 = time.monotonic()
    last_par_heartbeat = par_t0

    logger.info(
        "Meilisearch bulk index: started (parallel) | index=%s batch_size=%s max_upload_workers=%s "
        "max_in_flight=%s",
        INDEX_NAME,
        batch_size,
        max_upload_workers,
        max_in_flight,
    )

    with ThreadPoolExecutor(max_workers=max_upload_workers) as executor:
        while True:
            batch = list(queryset.filter(id__gt=last_id).order_by("id")[:batch_size])
            if not batch:
                break
            last_id = batch[-1].id
            batch_index += 1
            id_first, id_last = batch[0].id, batch[-1].id
            # Build docs in the main thread (DB queries) so workers only handle HTTP.
            cat_map = _bulk_category_map_for_ids([p.id for p in batch])
            docs = _build_docs_for_batch(batch, cat_map)
            in_flight.append(executor.submit(_upload_batch, docs, batch_index, id_first, id_last))
            while len(in_flight) >= max_in_flight:
                done, _ = wait(in_flight, return_when=FIRST_COMPLETED)
                for fut in done:
                    in_flight.remove(fut)
                    ok, fail = fut.result()
                    total_ok += ok
                    total_fail += fail
                    now = time.monotonic()
                    if now - last_par_heartbeat >= 60.0:
                        elapsed = now - par_t0
                        rate = total_ok / elapsed if elapsed > 0 else 0.0
                        logger.info(
                            "Meilisearch bulk index: heartbeat (parallel) | elapsed_s=%.0f total_ok=%s "
                            "total_fail=%s overall_dps=%.0f in_flight=%s",
                            elapsed,
                            total_ok,
                            total_fail,
                            rate,
                            len(in_flight),
                        )
                        last_par_heartbeat = now

        for fut in in_flight:
            ok, fail = fut.result()
            total_ok += ok
            total_fail += fail

    par_elapsed = time.monotonic() - par_t0
    par_rate = total_ok / par_elapsed if par_elapsed > 0 else 0.0
    logger.info(
        "Meilisearch bulk index: finished (parallel) | batches_submitted=%s total_ok=%s total_fail=%s "
        "elapsed_s=%.1f overall_dps=%.0f",
        batch_index,
        total_ok,
        total_fail,
        par_elapsed,
        par_rate,
    )
    return total_ok, total_fail


def reindex_all_master_parts(
    batch_size: int = REINDEX_DEFAULT_BATCH_SIZE,
    max_upload_workers: int = REINDEX_DEFAULT_UPLOAD_WORKERS,
) -> typing.Tuple[int, int]:
    """
    Configure index, delete all documents, then bulk-index every ``MasterPart``.
    Uses parallel HTTP uploads when ``max_upload_workers`` > 1.
    Returns (total_indexed, total_failed). No-op (0, 0) if Meilisearch is not configured.
    """
    if not is_configured():
        logger.warning("Meilisearch not configured; skipping reindex.")
        return 0, 0

    from src.models import MasterPart

    total_parts = MasterPart.objects.count()
    host = getattr(settings, "MEILISEARCH_HOST", "")
    http_timeout = getattr(settings, "MEILISEARCH_HTTP_TIMEOUT", 600)
    pipeline_t0 = time.monotonic()
    logger.info(
        "Meilisearch reindex: pipeline start | index=%s host=%s total_master_parts=%s "
        "batch_size=%s max_upload_workers=%s http_timeout_s=%s",
        INDEX_NAME,
        host,
        total_parts,
        batch_size,
        max_upload_workers,
        http_timeout,
    )

    t_setup = time.monotonic()
    if not setup_index():
        logger.error("Meilisearch setup_index failed; skipping reindex.")
        return 0, 0
    logger.info("Meilisearch reindex: setup_index ok in %.2fs", time.monotonic() - t_setup)

    t_delete = time.monotonic()
    if not delete_all_documents():
        logger.error("Meilisearch delete_all_documents failed; skipping indexing.")
        return 0, 0
    logger.info("Meilisearch reindex: delete_all_documents ok in %.2fs", time.monotonic() - t_delete)

    queryset = MasterPart.objects.select_related("brand").order_by("id")
    t_index = time.monotonic()
    ok, fail = add_documents_in_batches(
        queryset,
        batch_size=batch_size,
        max_upload_workers=max_upload_workers,
    )
    index_elapsed = time.monotonic() - t_index
    total_elapsed = time.monotonic() - pipeline_t0
    logger.info(
        "Meilisearch reindex: pipeline done | indexed=%s failed=%s index_phase_s=%.1f total_elapsed_s=%.1f",
        ok,
        fail,
        index_elapsed,
        total_elapsed,
    )
    return ok, fail


def delete_document(part_id: int) -> bool:
    """Remove a part from the index by id."""
    if not is_configured():
        return False

    try:
        client = _get_client()
        index = client.index(INDEX_NAME)
        index.delete_document(part_id)
        return True
    except Exception as e:
        logger.exception("Meilisearch delete_document failed: %s", str(e))
        return False


def delete_all_documents() -> bool:
    """
    Clear all documents from the parts index and wait for the async task to complete.
    Meilisearch delete_all_documents() is asynchronous — without waiting, new documents
    can be indexed into the index while the delete is still in progress.
    """
    if not is_configured():
        return False

    try:
        client = _get_client()
        index = client.index(INDEX_NAME)
        task = index.delete_all_documents()
        logger.info(
            "Meilisearch: delete_all_documents enqueued | index=%s task_uid=%s; waiting for completion…",
            INDEX_NAME,
            task.task_uid,
        )
        client.wait_for_task(task.task_uid, timeout_in_ms=300_000)
        logger.info("Meilisearch: all documents deleted from '%s'", INDEX_NAME)
        return True
    except Exception as e:
        logger.exception("Meilisearch delete_all_documents failed: %s", str(e))
        return False
