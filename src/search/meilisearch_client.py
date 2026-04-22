"""
Meilisearch client for parts search index.
Backend uses master key for indexing; frontend will use a public read-only key.
"""
import logging
import typing
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait

from django.conf import settings

logger = logging.getLogger(__name__)

INDEX_NAME = getattr(settings, "MEILISEARCH_INDEX_PARTS", "parts")

# Defaults for full reindex after distributor master-parts sync (tuned for typical Meilisearch throughput).
REINDEX_DEFAULT_BATCH_SIZE = 10000
REINDEX_DEFAULT_UPLOAD_WORKERS = 4

# Searchable: what the user types to find results (brand_name is on the document for filtering only)
SEARCHABLE_ATTRIBUTES = ["part_number", "sku", "description", "aaia_code"]

# Filterable: for sidebar / API filters; categories come from first ProviderPart with non-empty category
FILTERABLE_ATTRIBUTES = ["brand_name", "category", "overview_category"]


def _get_client():
    """Lazy import to avoid import errors when meilisearch is not installed."""
    import meilisearch

    host = getattr(settings, "MEILISEARCH_HOST", "http://localhost:7700")
    api_key = getattr(settings, "MEILISEARCH_MASTER_KEY", None) or ""
    return meilisearch.Client(host, api_key)


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
    """
    if not is_configured() or not parts:
        return bool(not parts)

    try:
        client = _get_client()
        index = client.index(INDEX_NAME)
        docs = [_part_to_document(p) for p in parts]
        index.add_documents(docs, primary_key="id")
        logger.info("Meilisearch: indexed %d parts", len(docs))
        return True
    except Exception as e:
        logger.exception("Meilisearch add_documents failed: %s", str(e))
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

    # Ensure brand is loaded (MasterPart.brand -> Brands)
    from django.db.models import Prefetch

    from src.models import MasterPart, ProviderPart

    if queryset.model == MasterPart:
        queryset = queryset.select_related("brand").prefetch_related(
            Prefetch("provider_parts", queryset=ProviderPart.objects.order_by("id"))
        )

    if max_upload_workers <= 1:
        total_ok = 0
        total_fail = 0
        last_id = 0

        while True:
            batch = list(queryset.filter(id__gt=last_id).order_by("id")[:batch_size])
            if not batch:
                break
            if add_documents(batch):
                total_ok += len(batch)
            else:
                total_fail += len(batch)
            last_id = batch[-1].id

        return total_ok, total_fail

    def _upload_batch(parts: typing.List) -> typing.Tuple[int, int]:
        if not parts:
            return 0, 0
        try:
            client = _get_client()
            index = client.index(INDEX_NAME)
            docs = [_part_to_document(p) for p in parts]
            index.add_documents(docs, primary_key="id")
            return len(docs), 0
        except Exception as e:
            logger.exception("Meilisearch parallel batch upload failed: %s", str(e))
            return 0, len(parts)

    total_ok = 0
    total_fail = 0
    last_id = 0
    max_in_flight = max(2, max_upload_workers * 2)
    in_flight = []

    with ThreadPoolExecutor(max_workers=max_upload_workers) as executor:
        while True:
            batch = list(queryset.filter(id__gt=last_id).order_by("id")[:batch_size])
            if not batch:
                break
            last_id = batch[-1].id
            in_flight.append(executor.submit(_upload_batch, batch))
            while len(in_flight) >= max_in_flight:
                done, _ = wait(in_flight, return_when=FIRST_COMPLETED)
                for fut in done:
                    in_flight.remove(fut)
                    ok, fail = fut.result()
                    total_ok += ok
                    total_fail += fail

        for fut in in_flight:
            ok, fail = fut.result()
            total_ok += ok
            total_fail += fail

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

    if not setup_index():
        logger.error("Meilisearch setup_index failed; skipping reindex.")
        return 0, 0

    if not delete_all_documents():
        logger.error("Meilisearch delete_all_documents failed; skipping indexing.")
        return 0, 0

    from src.models import MasterPart

    queryset = MasterPart.objects.select_related("brand").order_by("id")
    ok, fail = add_documents_in_batches(
        queryset,
        batch_size=batch_size,
        max_upload_workers=max_upload_workers,
    )
    logger.info("Meilisearch full reindex finished: indexed=%s failed=%s", ok, fail)
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
    """Clear all documents from the parts index."""
    if not is_configured():
        return False

    try:
        client = _get_client()
        index = client.index(INDEX_NAME)
        index.delete_all_documents()
        logger.info("Meilisearch: deleted all documents from '%s'", INDEX_NAME)
        return True
    except Exception as e:
        logger.exception("Meilisearch delete_all_documents failed: %s", str(e))
        return False
