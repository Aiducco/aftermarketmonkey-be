"""
Meilisearch client for parts search index.
Backend uses master key for indexing; frontend will use a public read-only key.
"""
import logging
import typing

from django.conf import settings

logger = logging.getLogger(__name__)

INDEX_NAME = getattr(settings, "MEILISEARCH_INDEX_PARTS", "parts")

# Searchable: what the user types to find results
SEARCHABLE_ATTRIBUTES = ["part_number", "sku", "description", "aaia_code", "brand_name"]

# Filterable: for sidebar filters (e.g. brand_id)
FILTERABLE_ATTRIBUTES = ["brand_id"]


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


def _part_to_document(part) -> typing.Dict:
    """Convert MasterPart instance to Meilisearch document."""
    brand_name = _get_brand_name(part)
    return {
        "id": part.id,
        "brand_id": part.brand_id,
        "brand_name": brand_name or "",  # Never None for Meilisearch
        "part_number": part.part_number or "",
        "sku": part.sku or "",
        "description": (part.description or "")[:10000],  # Meilisearch has limits
        "aaia_code": part.aaia_code or "",
        "image_url": part.image_url or "",
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
) -> typing.Tuple[int, int]:
    """
    Index all parts from a MasterPart queryset in batches (cursor-based by id).
    Returns (total_indexed, total_failed).
    """
    if not is_configured():
        return 0, 0

    # Ensure brand is loaded (MasterPart.brand -> Brands)
    from src.models import MasterPart

    if queryset.model == MasterPart:
        queryset = queryset.select_related("brand")

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
