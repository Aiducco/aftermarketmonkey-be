import csv
import logging
import re
import time
import typing
from concurrent.futures import ThreadPoolExecutor, as_completed
from decimal import Decimal

import pandas as pd
import pgbulk
from django.conf import settings
from django.db import close_old_connections, connection
from django.db.models.functions import Upper
from django.utils import timezone

from src import enums as src_enums
from src import models as src_models
from src.integrations.clients.atech import client as atech_client
from src.integrations.clients.atech import exceptions as atech_exceptions
from src.integrations.services.meyer import normalize_brand_match_key
from src.integrations.utils.brand_matching import (
    best_fuzzy_brand_match,
    brands_by_first_token_upper,
    normalize_upper_words,
)

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[ATECH-SERVICES]"

_EXCEL_FORMULA_PATTERN = re.compile(r'^="?([^"]*)"?$|^=(\d+(?:\.\d+)?)$')

# Larger batches = fewer DB round-trips on multi-million-row feeds (tune down if OOM on small workers).
ATECH_PARTS_UPSERT_BATCH = 50000
ATECH_PARTS_UPSERT_DELAY = 0.05

ATECH_PRICING_UPSERT_BATCH = 15000

# ``feed_part_number__in`` lookups when building AtechCompanyPricing (max params per query).
ATECH_FEED_KEY_LOOKUP_CHUNK = 15000

# Parallel ORM chunk lookups (``ATECH_PRICING_LOOKUP_MAX_WORKERS``) and parallel per-company
# pricing pull+ingest in ``fetch_and_save_atech_catalog`` (``ATECH_COMPANY_PRICING_SYNC_MAX_WORKERS``).
def _atech_setting_int(name: str, default: int) -> int:
    try:
        raw = getattr(settings, name, default)
        v = int(str(raw).strip())
        return v if v >= 1 else default
    except (TypeError, ValueError):
        return default


ATECH_PRICING_LOOKUP_MAX_WORKERS = _atech_setting_int("ATECH_PRICING_LOOKUP_MAX_WORKERS", 4)
ATECH_COMPANY_PRICING_SYNC_MAX_WORKERS = _atech_setting_int("ATECH_COMPANY_PRICING_SYNC_MAX_WORKERS", 1)
ATECH_PARTS_UPSERT_MAX_WORKERS = _atech_setting_int("ATECH_PARTS_UPSERT_MAX_WORKERS", 1)
ATECH_COMPANY_PRICING_UPSERT_MAX_WORKERS = _atech_setting_int("ATECH_COMPANY_PRICING_UPSERT_MAX_WORKERS", 1)

# Up to 8MB used only to pick an encoding; streaming uses one pass over the file.
_ATECH_ENCODING_SNIFF_BYTES = 8 * 1024 * 1024
ATECH_PARSE_PROGRESS_EVERY = _atech_setting_int("ATECH_PARSE_PROGRESS_EVERY", 250000)

ATECH_COMPANY_PRICING_UPDATE_FIELDS = [
    "cost",
    "retail_price",
    "jobber_price",
    "core_charge",
    "fee_hazmat",
    "fee_truck_us",
    "fee_handling_ground",
    "fee_handling_air",
    "updated_at",
]

ATECH_PARTS_UPDATE_FIELDS = [
    "brand",
    "brand_prefix",
    "part_number",
    "mfr_part_number",
    "description",
    "cost",
    "retail_price",
    "jobber_price",
    "qty_tallmadge",
    "qty_sparks",
    "qty_mcdonough",
    "qty_arlington",
    "core_charge",
    "fee_hazmat",
    "fee_truck_us",
    "fee_handling_ground",
    "fee_handling_air",
    "gtin",
    "image_url",
    "raw_data",
    "updated_at",
]


def _clean_csv_value(value: typing.Any) -> typing.Optional[str]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    s = str(value).strip()
    if not s:
        return None
    match = _EXCEL_FORMULA_PATTERN.match(s)
    if match:
        return (match.group(1) or match.group(2) or "").strip() or None
    return s


def _safe_decimal(value: typing.Any) -> typing.Optional[Decimal]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    try:
        s = _clean_csv_value(value)
        if s is None or s == "":
            return None
        return Decimal(str(s))
    except Exception:
        return None


def _safe_int(value: typing.Any) -> typing.Optional[int]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    try:
        s = _clean_csv_value(value)
        if s is None or s == "":
            return None
        return int(float(s))
    except Exception:
        return None


def _normalize_row_keys(row: typing.Dict) -> typing.Dict:
    return {
        (k.strip() if isinstance(k, str) else k): v for k, v in row.items()
    }


# Feeds are often UTF-8; some Windows exports use cp1252. latin-1 always decodes bytes (last resort).
_ATECH_FEED_CSV_ENCODINGS = ("utf-8-sig", "utf-8", "cp1252", "latin-1")


def _normalize_atech_row_cell(v: typing.Any) -> typing.Any:
    if v is None or v == "" or (isinstance(v, float) and pd.isna(v)):
        return None
    return v


def _normalize_atech_row_dict(row: typing.Dict) -> typing.Dict:
    norm = _normalize_row_keys(row)
    return {k: _normalize_atech_row_cell(v) for k, v in norm.items()}


def _sniff_atech_csv_encoding(path: str) -> str:
    """
    Match legacy :func:`pandas.read_csv` behavior: first encoding in
    :data:`_ATECH_FEED_CSV_ENCODINGS` that decodes a leading file slice. Avoids
    materializing the whole feed; rare mis-detects are unchanged from previous risk profile.
    """
    try:
        with open(path, "rb") as bf:
            sample = bf.read(_ATECH_ENCODING_SNIFF_BYTES)
    except OSError as e:
        raise ValueError("Could not read A-Tech feed at {!r}: {}".format(path, e)) from e
    last_error: typing.Optional[Exception] = None
    for enc in _ATECH_FEED_CSV_ENCODINGS:
        try:
            sample.decode(enc)
        except UnicodeDecodeError as e:
            last_error = e
            continue
        return enc
    if last_error:
        raise last_error
    return "utf-8"


def _select_atech_csv_encoding(path: str) -> str:
    """
    Pick a likely encoding from a leading sample.
    We avoid a full-file pre-validation pass to keep disk reads low on large feeds.
    """
    return _sniff_atech_csv_encoding(path)


def _iter_atech_csv_rows(path: str) -> typing.Iterator[typing.Dict]:
    """
    Stream A-Tech feed rows with the same per-cell semantics as the legacy pandas path.
    """
    enc = _select_atech_csv_encoding(path)
    if enc != "utf-8-sig":
        logger.info(
            "{} Streaming feed {} with encoding {!r}.".format(_LOG_PREFIX, path, enc)
        )
    with open(path, "r", newline="", encoding=enc, errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            yield _normalize_atech_row_dict(row)


def _merge_atech_pricing_row_into_map(
    row: typing.Dict, pmap: typing.Dict[str, typing.Dict[str, typing.Any]]
) -> None:
    """In-place: feed ``part_number`` (full line) -> price payload; last row per key wins."""
    fn = _clean_csv_value(row.get("part_number"))
    if not fn:
        return
    key = fn.strip()
    pmap[key] = {
        "cost": _safe_decimal(row.get("price_atech_current")),
        "retail_price": _safe_decimal(row.get("price_current_month")),
        "jobber_price": _safe_decimal(row.get("cost_current_sheet")),
        "core_charge": _safe_decimal(row.get("cost_core")),
        "fee_hazmat": _safe_decimal(row.get("fee_hazmat")),
        "fee_truck_us": _safe_decimal(row.get("fee_truck_us")),
        "fee_handling_ground": _safe_decimal(row.get("fee_handling_ground")),
        "fee_handling_air": _safe_decimal(row.get("fee_handling_air")),
    }


def _atech_prefix_from_part_number(part_number: typing.Any) -> str:
    """SKU prefix: segment before first '-' (uppercase), else full token uppercase."""
    raw = _clean_csv_value(part_number)
    if not raw:
        return ""
    s = raw.strip().upper()
    if "-" in s:
        return s.split("-", 1)[0].strip()
    return s


def _atech_pricing_map_from_iterable(
    records: typing.Iterable[typing.Dict],
    *,
    progress_every: int = 0,
    progress_label: str = "",
) -> typing.Dict[str, typing.Dict[str, typing.Any]]:
    """Map feed ``part_number`` (full line) -> price payload; last row per key wins."""
    out: typing.Dict[str, typing.Dict[str, typing.Any]] = {}
    rows_seen = 0
    started = time.monotonic()
    for row in records:
        rows_seen += 1
        _merge_atech_pricing_row_into_map(row, out)
        if progress_every > 0 and (rows_seen % progress_every) == 0:
            logger.info(
                "{} {} pricing parse progress: {} row(s) scanned, {} unique keys in {:.1f}s.".format(
                    _LOG_PREFIX,
                    progress_label or "A-Tech",
                    rows_seen,
                    len(out),
                    time.monotonic() - started,
                )
            )
    if rows_seen:
        logger.info(
            "{} {} pricing parse done: {} row(s), {} unique keys in {:.1f}s.".format(
                _LOG_PREFIX,
                progress_label or "A-Tech",
                rows_seen,
                len(out),
                time.monotonic() - started,
            )
        )
    return out


def _id_by_feed_keys_parallel(
    feed_keys: typing.List[str],
    log_prefix: str,
    company_provider_id: int,
) -> typing.Dict[str, int]:
    """
    Resolve ``feed_part_number`` -> ``AtechParts.id`` in chunks; optional thread parallelism.
    """
    if not feed_keys:
        return {}
    chunk = ATECH_FEED_KEY_LOOKUP_CHUNK
    chunks: typing.List[typing.List[str]] = [
        feed_keys[i : i + chunk] for i in range(0, len(feed_keys), chunk)
    ]
    n_chunks = len(chunks)
    id_by_feed: typing.Dict[str, int] = {}
    max_w = ATECH_PRICING_LOOKUP_MAX_WORKERS
    use_parallel = n_chunks > 1 and max_w > 1
    workers = min(max_w, n_chunks, 32) if use_parallel else 1

    def _load_one(ck: typing.List[str]) -> typing.Dict[str, int]:
        close_old_connections()
        try:
            out: typing.Dict[str, int] = {}
            for row in src_models.AtechParts.objects.filter(feed_part_number__in=ck).values_list(
                "feed_part_number", "id"
            ):
                fn = (row[0] or "").strip()
                if fn:
                    out[fn] = int(row[1])
            return out
        finally:
            close_old_connections()

    if workers == 1:
        started = time.monotonic()
        chunk_idx = 0
        for ck in chunks:
            chunk_idx += 1
            id_by_feed.update(_load_one(ck))
            if n_chunks and (chunk_idx % 25 == 0 or chunk_idx == n_chunks):
                logger.info(
                    "{} AtechCompanyPricing part lookup: chunk {}/{} (company_provider id={}).".format(
                        log_prefix, chunk_idx, n_chunks, company_provider_id
                    )
                )
        logger.info(
            "{} AtechCompanyPricing part lookup done: {} key(s), {} matched part id(s) in {:.1f}s "
            "(company_provider id={}).".format(
                log_prefix,
                len(feed_keys),
                len(id_by_feed),
                time.monotonic() - started,
                company_provider_id,
            )
        )
        return id_by_feed

    logger.info(
        "{} AtechCompanyPricing part lookup: {} chunk(s), {} worker(s) (company_provider id={}).".format(
            log_prefix, n_chunks, workers, company_provider_id
        )
    )
    started = time.monotonic()
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = [ex.submit(_load_one, ck) for ck in chunks]
        for fut in as_completed(futs):
            id_by_feed.update(fut.result())
    logger.info(
        "{} AtechCompanyPricing part lookup done: {} key(s), {} matched part id(s) in {:.1f}s "
        "(company_provider id={}).".format(
            log_prefix,
            len(feed_keys),
            len(id_by_feed),
            time.monotonic() - started,
            company_provider_id,
        )
    )
    return id_by_feed


def _upsert_atech_parts_batch(batch: typing.List[src_models.AtechParts]) -> int:
    """
    Upsert one AtechParts batch with thread-safe DB connection handling.
    """
    close_old_connections()
    try:
        _now = timezone.now()
        for _p in batch:
            _p.updated_at = _now
        pgbulk.upsert(
            src_models.AtechParts,
            batch,
            unique_fields=["feed_part_number"],
            update_fields=ATECH_PARTS_UPDATE_FIELDS,
            returning=False,
        )
        connection.close()
        return len(batch)
    finally:
        close_old_connections()


def _upsert_atech_company_pricing_batch(
    batch: typing.List[src_models.AtechCompanyPricing],
) -> int:
    """
    Upsert one AtechCompanyPricing batch with thread-safe DB connection handling.
    """
    close_old_connections()
    try:
        pgbulk.upsert(
            src_models.AtechCompanyPricing,
            batch,
            unique_fields=["part", "company"],
            update_fields=ATECH_COMPANY_PRICING_UPDATE_FIELDS,
            returning=False,
        )
        connection.close()
        return len(batch)
    finally:
        close_old_connections()


def _strip_known_prefix_suffix(full_part: str, prefix_upper: str) -> str:
    """
    Part after ``PREFIX-`` when the feed line starts with that prefix (case-insensitive).
    Otherwise fallback: segment after first hyphen, or full string.
    """
    full = (full_part or "").strip()
    if not full:
        return ""
    p = (prefix_upper or "").strip().upper()
    prefix_dash = "{}-".format(p) if p else ""
    if p and full.upper().startswith(prefix_dash):
        return full[len(prefix_dash) :].strip()
    if "-" in full:
        return full.split("-", 1)[1].strip()
    return full.strip()


def _active_atech_company_providers_queryset():
    return src_models.CompanyProviders.objects.filter(
        provider__kind=src_enums.BrandProviderKind.ATECH.value,
        provider__status=src_enums.BrandProviderStatus.ACTIVE.value,
    ).select_related("company", "provider")


def _primary_atech_company_provider() -> typing.Optional[src_models.CompanyProviders]:
    base = _active_atech_company_providers_queryset()
    cp = base.filter(primary=True).first()
    if cp:
        return cp
    return base.first()


def _load_prefix_to_brand_id() -> typing.Dict[str, int]:
    """
    Map uppercase SKU prefix -> ``AtechBrand`` id from ``AtechPrefixBrand`` (authoritative for ingest).

    Ingest matches the prefix parsed from each feed line (segment before the first ``-``, else the
    whole token), then sets ``AtechParts.brand`` when this map contains that prefix.
    """
    out: typing.Dict[str, int] = {}
    for row in (
        src_models.AtechPrefixBrand.objects.values_list("prefix", "atech_brand_id")
    ):
        prefix = str(row[0] or "").strip().upper()
        bid = row[1]
        if prefix and bid:
            out[prefix] = int(bid)
    return out


def _json_safe_row(row: typing.Dict) -> typing.Dict:
    """Coerce feed row values for JSONField (pandas may yield non-JSON scalars)."""
    out: typing.Dict[str, typing.Any] = {}
    for k, v in row.items():
        key = k if isinstance(k, str) else str(k)
        if v is None or (isinstance(v, float) and pd.isna(v)):
            out[key] = None
            continue
        if isinstance(v, (str, int, bool)):
            out[key] = v
            continue
        if isinstance(v, Decimal):
            out[key] = str(v)
            continue
        try:
            if float(v) == int(float(v)):
                out[key] = int(float(v))
            else:
                out[key] = float(v)
        except (TypeError, ValueError):
            out[key] = str(v)
    return out


def _part_from_feed_row(
    row: typing.Dict,
    brand_id: typing.Optional[int],
    prefix_upper: str,
) -> typing.Optional[src_models.AtechParts]:
    feed_pn = _clean_csv_value(row.get("part_number"))
    if not feed_pn:
        return None
    feed_pn = feed_pn.strip()
    prefix_u = (prefix_upper or "").strip().upper()
    stripped = _strip_known_prefix_suffix(feed_pn, prefix_u)
    if not stripped:
        stripped = feed_pn
    brand = src_models.AtechBrand(pk=brand_id) if brand_id else None
    return src_models.AtechParts(
        brand=brand,
        brand_prefix=prefix_u,
        feed_part_number=feed_pn,
        part_number=stripped,
        mfr_part_number=stripped,
        description=_clean_csv_value(row.get("description")),
        cost=_safe_decimal(row.get("price_atech_current")),
        retail_price=_safe_decimal(row.get("price_current_month")),
        jobber_price=_safe_decimal(row.get("cost_current_sheet")),
        qty_tallmadge=_safe_int(row.get("tallmadge_qty")),
        qty_sparks=_safe_int(row.get("sparks_qty")),
        qty_mcdonough=_safe_int(row.get("mcdonough_qty")),
        qty_arlington=_safe_int(row.get("arlington_qty")),
        core_charge=_safe_decimal(row.get("cost_core")),
        fee_hazmat=_safe_decimal(row.get("fee_hazmat")),
        fee_truck_us=_safe_decimal(row.get("fee_truck_us")),
        fee_handling_ground=_safe_decimal(row.get("fee_handling_ground")),
        fee_handling_air=_safe_decimal(row.get("fee_handling_air")),
        gtin=_clean_csv_value(row.get("gtin")),
        image_url=_clean_csv_value(row.get("image_url")),
        raw_data=_json_safe_row(row) if row else None,
    )


def backfill_atech_parts_brands_from_prefix_mapping(
    batch_size: int = 5000,
) -> typing.Tuple[int, int, int]:
    """
    Set ``AtechParts.brand_id`` from ``AtechPrefixBrand`` using the same rules as feed ingest:

    - Prefer stored ``brand_prefix`` (uppercase); if blank, derive from ``feed_part_number`` via
      :func:`_atech_prefix_from_part_number`.
    - Look up ``AtechBrand`` id in :func:`_load_prefix_to_brand_id`; upsert only when it differs.

    Returns ``(updated_count, already_correct_count, unmapped_count)``.
    """
    prefix_to_brand = _load_prefix_to_brand_id()
    if not prefix_to_brand:
        logger.warning(
            "{} No AtechPrefixBrand rows; cannot backfill part brands.".format(_LOG_PREFIX),
        )
        return 0, 0, 0

    qs = src_models.AtechParts.objects.order_by("id").values_list(
        "id",
        "brand_id",
        "brand_prefix",
        "feed_part_number",
    )
    batch: typing.List[src_models.AtechParts] = []
    updated = 0
    already_correct = 0
    unmapped = 0

    def _flush() -> None:
        nonlocal batch, updated
        if not batch:
            return
        # PK-only ``brand_id`` fixups: use bulk_update (avoids pgbulk INSERT paths with null NOT NULLs).
        src_models.AtechParts.objects.bulk_update(batch, ["brand_id"], batch_size=1000)
        updated += len(batch)
        batch = []

    for row in qs.iterator(chunk_size=batch_size):
        pid, existing_bid, bp, feed_pn = (
            int(row[0]),
            row[1],
            row[2],
            row[3],
        )
        prefix = (bp or "").strip().upper()
        if not prefix:
            prefix = _atech_prefix_from_part_number(feed_pn)
        brand_id = prefix_to_brand.get(prefix) if prefix else None
        if brand_id is None:
            unmapped += 1
            continue
        if existing_bid == brand_id:
            already_correct += 1
            continue
        if not (feed_pn or "").strip():
            unmapped += 1
            continue
        batch.append(src_models.AtechParts(id=pid, brand_id=brand_id))
        if len(batch) >= batch_size:
            _flush()

    _flush()
    logger.info(
        "{} Backfill part brands: updated={}, already_correct={}, unmapped_or_no_prefix={}.".format(
            _LOG_PREFIX,
            updated,
            already_correct,
            unmapped,
        )
    )
    return updated, already_correct, unmapped


# ``AtechBrand`` external_id or name (after :func:`normalize_brand_match_key`) -> exact catalog ``Brands.name``.
# Extend when fuzzy matching would pick the wrong row (same pattern as Meyer canonical overrides).
_ATECH_UNMAPPED_SYNC_CANONICAL_BRAND: typing.Dict[str, str] = {}


def _atech_brand_name_upper_for_sync(atech_brand: src_models.AtechBrand) -> str:
    name_upper = (atech_brand.name or "").strip().upper()
    if not name_upper:
        name_upper = "BRAND_{}".format(atech_brand.external_id)
    return name_upper


def _atech_unmapped_sync_canonical_brand_name(
    ab: src_models.AtechBrand,
) -> typing.Optional[str]:
    for raw in (ab.external_id, ab.name):
        k = normalize_brand_match_key(raw)
        if not k:
            continue
        target = _ATECH_UNMAPPED_SYNC_CANONICAL_BRAND.get(k)
        if target:
            return target
    return None


def sync_unmapped_atech_brands_to_brands(dry_run: bool = False) -> typing.List[src_models.AtechBrand]:
    """
    For each ``AtechBrand`` without ``BrandAtechBrandMapping``: resolve ``Brands`` by exact name
    (uppercase), then fuzzy word-prefix match (shared util with Meyer / WheelPros / DLG); otherwise
    create a catalog brand. Upserts mapping, ``BrandProviders``, ``CompanyBrands`` for
    TICK_PERFORMANCE.

    If ``dry_run`` is True, no database writes; logs only A-Tech brands that matched an existing
    ``Brands`` row (exact, canonical override, or fuzzy).
    """
    logger.info(
        "{} Syncing unmapped A-Tech brands to Brands{}.".format(
            _LOG_PREFIX,
            " (dry run)" if dry_run else "",
        )
    )

    atech_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.ATECH.value,
    ).first()
    if not atech_provider:
        logger.warning("{} A-Tech provider not found.".format(_LOG_PREFIX))
        return []

    tick_company = src_models.Company.objects.filter(name="TICK_PERFORMANCE").first()
    if not tick_company:
        logger.warning("{} Company TICK_PERFORMANCE not found. Skipping.".format(_LOG_PREFIX))
        return []

    if not dry_run:
        _, cp_created = src_models.CompanyProviders.objects.get_or_create(
            company=tick_company,
            provider=atech_provider,
            defaults={"credentials": {}, "primary": False},
        )
        if cp_created:
            logger.info("{} Created CompanyProviders for TICK_PERFORMANCE + A-Tech.".format(_LOG_PREFIX))

    logger.info("{} Loading BrandAtechBrandMapping ids (existing links)...".format(_LOG_PREFIX))
    mapped_ids = set(
        src_models.BrandAtechBrandMapping.objects.values_list("atech_brand_id", flat=True).distinct()
    )
    logger.info(
        "{} Loading unmapped AtechBrand rows (exclude {} linked id(s))...".format(
            _LOG_PREFIX,
            len(mapped_ids),
        )
    )
    unmapped = list(
        src_models.AtechBrand.objects.exclude(id__in=mapped_ids).order_by("id")
    )
    logger.info("{} Loaded {} unmapped A-Tech brand row(s).".format(_LOG_PREFIX, len(unmapped)))
    if not unmapped:
        logger.info("{} No unmapped A-Tech brands.".format(_LOG_PREFIX))
        return []

    name_upper_keys: typing.Set[str] = set()
    for ab in unmapped:
        if (ab.name or "").strip():
            name_upper_keys.add((ab.name or "").strip().upper())

    brands_by_upper_name: typing.Dict[str, src_models.Brands] = {}
    if name_upper_keys:
        for b in (
            src_models.Brands.objects.annotate(_name_u=Upper("name"))
            .filter(_name_u__in=name_upper_keys)
            .order_by("id")
        ):
            key = (b.name or "").strip().upper()
            if key not in brands_by_upper_name:
                brands_by_upper_name[key] = b

    canonical_targets_upper: typing.Set[str] = set()
    for ab in unmapped:
        t = _atech_unmapped_sync_canonical_brand_name(ab)
        if t:
            canonical_targets_upper.add(t.strip().upper())

    brands_by_upper_for_canonical: typing.Dict[str, src_models.Brands] = {}
    if canonical_targets_upper:
        for b in (
            src_models.Brands.objects.annotate(_name_u=Upper("name"))
            .filter(_name_u__in=canonical_targets_upper)
            .order_by("id")
        ):
            ku = (b.name or "").strip().upper()
            if ku not in brands_by_upper_for_canonical:
                brands_by_upper_for_canonical[ku] = b

    resolved_by_atech_id: typing.Dict[int, src_models.Brands] = {}
    canonical_matched_ids: typing.Set[int] = set()
    exact_matched_ids: typing.Set[int] = set()
    for ab in sorted(unmapped, key=lambda x: x.id):
        canon_name = _atech_unmapped_sync_canonical_brand_name(ab)
        if canon_name:
            b = brands_by_upper_for_canonical.get(canon_name.strip().upper())
            if b:
                resolved_by_atech_id[ab.id] = b
                canonical_matched_ids.add(ab.id)
            else:
                logger.warning(
                    "{} A-Tech canonical brand override {!r} -> {!r} but no Brands row with that name.".format(
                        _LOG_PREFIX,
                        ab.name or ab.external_id,
                        canon_name,
                    )
                )

    for ab in sorted(unmapped, key=lambda x: x.id):
        if ab.id in resolved_by_atech_id:
            continue
        nm = (ab.name or "").strip().upper()
        if nm:
            brand = brands_by_upper_name.get(nm)
            if brand:
                resolved_by_atech_id[ab.id] = brand
                exact_matched_ids.add(ab.id)

    unresolved_after_exact = [ab for ab in unmapped if ab.id not in resolved_by_atech_id]
    brands_first_index: typing.Dict[str, typing.List[src_models.Brands]] = {}
    if unresolved_after_exact:
        logger.info(
            "{} Fuzzy phase: {} A-Tech brand(s) still unmatched; building first-token index "
            "(full ``Brands`` table scan — can take minutes on large catalogs)...".format(
                _LOG_PREFIX,
                len(unresolved_after_exact),
            )
        )
        _t0 = time.monotonic()
        brands_first_index = brands_by_first_token_upper()
        logger.info(
            "{} First-token index built in {:.1f}s ({} first-token bucket(s)).".format(
                _LOG_PREFIX,
                time.monotonic() - _t0,
                len(brands_first_index),
            )
        )
    all_brands_fallback: typing.Optional[typing.List[src_models.Brands]] = None
    fuzzy_matches = 0
    fuzzy_matched_ids: typing.Set[int] = set()
    _fuzzy_total = len(unresolved_after_exact)
    for _fuzzy_i, ab in enumerate(unresolved_after_exact):
        if _fuzzy_i > 0 and _fuzzy_i % 200 == 0:
            logger.info(
                "{} Fuzzy matching progress: {}/{} AtechBrand(s)...".format(
                    _LOG_PREFIX,
                    _fuzzy_i,
                    _fuzzy_total,
                )
            )
        parts = normalize_upper_words(ab.name or "").split()
        candidates: typing.List[src_models.Brands] = []
        if parts:
            candidates = list(brands_first_index.get(parts[0], ()))
        if not candidates:
            if all_brands_fallback is None:
                logger.info(
                    "{} Loading full ``Brands`` list as fuzzy fallback (one-time; heavy)...".format(
                        _LOG_PREFIX,
                    )
                )
                _t1 = time.monotonic()
                all_brands_fallback = list(
                    src_models.Brands.objects.only("id", "name", "aaia_code").order_by("id")
                )
                logger.info(
                    "{} Fallback list: {} brand row(s) in {:.1f}s.".format(
                        _LOG_PREFIX,
                        len(all_brands_fallback),
                        time.monotonic() - _t1,
                    )
                )
            candidates = all_brands_fallback
        brand = best_fuzzy_brand_match(ab.name or "", candidates)
        if brand:
            resolved_by_atech_id[ab.id] = brand
            fuzzy_matched_ids.add(ab.id)
            fuzzy_matches += 1
            if not dry_run:
                logger.debug(
                    "{} Fuzzy-matched A-Tech brand {!r} to Brand id={} name={!r}.".format(
                        _LOG_PREFIX,
                        ab.name,
                        brand.id,
                        brand.name,
                    )
                )

    if dry_run:
        for ab in sorted(unmapped, key=lambda x: x.id):
            if ab.id not in resolved_by_atech_id:
                continue
            brand = resolved_by_atech_id[ab.id]
            if ab.id in canonical_matched_ids:
                how = "canonical_override"
            elif ab.id in exact_matched_ids:
                how = "exact"
            else:
                how = "fuzzy"
            logger.info(
                "{} [dry-run] match ({}) AtechBrand id={} external_id={!r} name={!r} "
                "-> Brand id={} name={!r}".format(
                    _LOG_PREFIX,
                    how,
                    ab.id,
                    ab.external_id,
                    ab.name,
                    brand.id,
                    brand.name,
                )
            )
        would_create = [ab for ab in unmapped if ab.id not in resolved_by_atech_id]
        logger.info(
            "{} [dry-run] Summary: {} canonical overrides, {} exact matches, {} fuzzy matches, "
            "{} unmatched (would create Brand). No writes performed.".format(
                _LOG_PREFIX,
                len(canonical_matched_ids),
                len(exact_matched_ids),
                len(fuzzy_matched_ids),
                len(would_create),
            )
        )
        return unmapped

    new_brand_specs: typing.Set[str] = set()
    for ab in sorted(unmapped, key=lambda x: x.id):
        if ab.id in resolved_by_atech_id:
            continue
        new_brand_specs.add(_atech_brand_name_upper_for_sync(ab))

    created_brands = 0
    if new_brand_specs:
        existing_names = set(
            src_models.Brands.objects.filter(name__in=list(new_brand_specs)).values_list(
                "name", flat=True
            )
        )
        new_brand_rows = [
            src_models.Brands(
                name=name,
                status=src_enums.BrandProviderStatus.ACTIVE.value,
                status_name=src_enums.BrandProviderStatus.ACTIVE.name,
                aaia_code=None,
            )
            for name in new_brand_specs
            if name not in existing_names
        ]
        if new_brand_rows:
            src_models.Brands.objects.bulk_create(new_brand_rows, ignore_conflicts=True)
            created_brands = len(new_brand_rows)
        by_name = {
            b.name: b
            for b in src_models.Brands.objects.filter(name__in=list(new_brand_specs))
        }
        for ab in unmapped:
            if ab.id not in resolved_by_atech_id:
                nu = _atech_brand_name_upper_for_sync(ab)
                resolved_by_atech_id[ab.id] = by_name[nu]

    mapping_models = [
        src_models.BrandAtechBrandMapping(
            brand_id=resolved_by_atech_id[ab.id].id,
            atech_brand_id=ab.id,
        )
        for ab in unmapped
    ]
    try:
        pgbulk.upsert(
            src_models.BrandAtechBrandMapping,
            mapping_models,
            unique_fields=["brand", "atech_brand"],
            update_fields=[],
            returning=False,
        )
    except Exception as e:
        logger.error("{} Error upserting BrandAtechBrandMapping: {}.".format(_LOG_PREFIX, str(e)))
        raise

    created_bp = 0
    created_cb = 0
    for ab in unmapped:
        brand = resolved_by_atech_id[ab.id]
        _, bpc = src_models.BrandProviders.objects.get_or_create(
            brand=brand,
            provider=atech_provider,
        )
        if bpc:
            created_bp += 1
        _, cbc = src_models.CompanyBrands.objects.get_or_create(
            company=tick_company,
            brand=brand,
            defaults={
                "status": src_enums.CompanyBrandStatus.ACTIVE.value,
                "status_name": src_enums.CompanyBrandStatus.ACTIVE.name,
            },
        )
        if cbc:
            created_cb += 1

    logger.info(
        "{} A-Tech brand sync done. Canonical overrides: {}, brands created: {}, fuzzy name matches: {}, "
        "BrandAtechBrandMapping upserted: {}, BrandProviders: {}, CompanyBrands: {}.".format(
            _LOG_PREFIX,
            len(canonical_matched_ids),
            created_brands,
            fuzzy_matches,
            len(mapping_models),
            created_bp,
            created_cb,
        )
    )
    return unmapped


def fetch_and_save_atech_catalog(force_download: bool = False) -> None:
    """
    Download atechfile.txt from the primary A-Tech CompanyProvider SFTP and upsert AtechParts.

    Rows without a matching ``AtechPrefixBrand`` still ingest with ``brand`` null; ``brand_prefix``
    is always set from the feed line. Add prefix mappings to attach ``brand`` on later runs.
    """
    logger.info("{} Starting A-Tech feed ingest.".format(_LOG_PREFIX))

    catalog_cp = _primary_atech_company_provider()
    if not catalog_cp:
        logger.info("{} No active A-Tech CompanyProviders. Skipping.".format(_LOG_PREFIX))
        return

    prefix_to_brand = _load_prefix_to_brand_id()

    credentials = catalog_cp.credentials
    try:
        sftp = atech_client.AtechSFTPClient(credentials=credentials)
    except ValueError as e:
        logger.error("{} {}".format(_LOG_PREFIX, str(e)))
        raise

    logger.info(
        "{} Downloading primary A-Tech feed (force_download={}).".format(
            _LOG_PREFIX,
            force_download,
        )
    )
    try:
        local_path = sftp.download_feed_file(force_download=force_download)
    except atech_exceptions.AtechException as e:
        logger.error("{} {}".format(_LOG_PREFIX, str(e)))
        raise

    logger.info(
        "{} Parsing A-Tech feed rows from {} (progress every {} rows).".format(
            _LOG_PREFIX,
            local_path,
            ATECH_PARSE_PROGRESS_EVERY,
        )
    )
    parse_started = time.monotonic()
    parts_by_feed_pn: typing.Dict[str, src_models.AtechParts] = {}
    unmapped_prefix_rows = 0
    feed_row_count = 0
    parts_with_model = 0
    for row in _iter_atech_csv_rows(local_path):
        feed_row_count += 1
        # Same prefix rule as ``brand_prefix`` on ``AtechParts`` (see ``_atech_prefix_from_part_number``).
        prefix = _atech_prefix_from_part_number(row.get("part_number"))
        brand_id = prefix_to_brand.get(prefix) if prefix else None
        if prefix and brand_id is None:
            unmapped_prefix_rows += 1
        p = _part_from_feed_row(row, brand_id, prefix)
        if p:
            parts_with_model += 1
            sku = (p.feed_part_number or "").strip()
            if sku:
                parts_by_feed_pn[sku] = p
        if (feed_row_count % ATECH_PARSE_PROGRESS_EVERY) == 0:
            logger.info(
                "{} Feed parse progress: {} row(s) scanned, {} model rows, {} dedup keys in {:.1f}s.".format(
                    _LOG_PREFIX,
                    feed_row_count,
                    parts_with_model,
                    len(parts_by_feed_pn),
                    time.monotonic() - parse_started,
                )
            )

    if feed_row_count == 0:
        logger.warning("{} Feed file empty or unreadable: {}.".format(_LOG_PREFIX, local_path))
        return
    logger.info(
        "{} Feed parse done: {} row(s), {} model rows, {} dedup keys in {:.1f}s.".format(
            _LOG_PREFIX,
            feed_row_count,
            parts_with_model,
            len(parts_by_feed_pn),
            time.monotonic() - parse_started,
        )
    )

    if unmapped_prefix_rows:
        logger.info(
            "{} Ingested {} feed rows with no AtechPrefixBrand mapping (brand left null).".format(
                _LOG_PREFIX,
                unmapped_prefix_rows,
            )
        )

    if parts_with_model > len(parts_by_feed_pn):
        logger.info(
            "{} Deduped {} -> {} rows on feed_part_number.".format(
                _LOG_PREFIX,
                parts_with_model,
                len(parts_by_feed_pn),
            )
        )

    parts = list(parts_by_feed_pn.values())

    if not parts:
        logger.warning("{} No AtechParts to upsert from primary feed.".format(_LOG_PREFIX))
    else:
        total_batches = (len(parts) + ATECH_PARTS_UPSERT_BATCH - 1) // ATECH_PARTS_UPSERT_BATCH
        upsert_workers = max(1, min(ATECH_PARTS_UPSERT_MAX_WORKERS, total_batches))
        logger.info(
            "{} Upserting {} AtechParts in {} batches (batch_size={}, workers={}); this can take many minutes.".format(
                _LOG_PREFIX,
                len(parts),
                total_batches,
                ATECH_PARTS_UPSERT_BATCH,
                upsert_workers,
            )
        )
        batches = [
            parts[i : i + ATECH_PARTS_UPSERT_BATCH]
            for i in range(0, len(parts), ATECH_PARTS_UPSERT_BATCH)
        ]
        if upsert_workers == 1:
            for batch_num, batch in enumerate(batches, start=1):
                _upsert_atech_parts_batch(batch)
                logger.info(
                    "{} AtechParts upsert progress: batch {}/{} (through row ~{}/{})".format(
                        _LOG_PREFIX,
                        batch_num,
                        total_batches,
                        min(batch_num * ATECH_PARTS_UPSERT_BATCH, len(parts)),
                        len(parts),
                    )
                )
                if batch_num < total_batches:
                    time.sleep(ATECH_PARTS_UPSERT_DELAY)
        else:
            # Parallel mode is optional/tunable; use low worker counts to avoid overloading DB/WAL.
            completed = 0
            rows_done = 0
            with ThreadPoolExecutor(max_workers=upsert_workers) as ex:
                futs = [ex.submit(_upsert_atech_parts_batch, batch) for batch in batches]
                for fut in as_completed(futs):
                    rows_done += fut.result()
                    completed += 1
                    logger.info(
                        "{} AtechParts parallel upsert progress: batch {}/{} (through row ~{}/{})".format(
                            _LOG_PREFIX,
                            completed,
                            total_batches,
                            rows_done,
                            len(parts),
                        )
                    )

        logger.info("{} Finished upserting {} AtechParts from primary feed.".format(_LOG_PREFIX, len(parts)))

    atech_kind = src_enums.BrandProviderKind.ATECH.value
    atech_provider_row = src_models.Providers.objects.filter(kind=atech_kind).first()
    pricing_cps: typing.List[src_models.CompanyProviders] = []
    if atech_provider_row:
        pricing_cps = list(
            _active_atech_company_providers_queryset().filter(provider=atech_provider_row)
        )
    if pricing_cps:
        n_cp = len(pricing_cps)
        max_cw = ATECH_COMPANY_PRICING_SYNC_MAX_WORKERS
        workers = max(1, min(max_cw, n_cp))
        if workers == 1:
            for cp in pricing_cps:
                logger.info(
                    "{} A-Tech company pricing SFTP pull: company_id={} company_provider_id={} primary={}.".format(
                        _LOG_PREFIX,
                        cp.company_id,
                        cp.id,
                        cp.primary,
                    )
                )
                sync_atech_company_pricing_for_company_provider(cp.id, force_download=force_download)
        else:
            logger.info(
                "{} A-Tech company pricing: {} company provider(s), {} parallel worker(s) (per-company files differ).".format(
                    _LOG_PREFIX,
                    n_cp,
                    workers,
                )
            )

            def _run_company_pricing_sync(cp: src_models.CompanyProviders) -> None:
                close_old_connections()
                try:
                    logger.info(
                        "{} A-Tech company pricing SFTP pull: company_id={} company_provider_id={} primary={}.".format(
                            _LOG_PREFIX,
                            cp.company_id,
                            cp.id,
                            cp.primary,
                        )
                    )
                    sync_atech_company_pricing_for_company_provider(cp.id, force_download=force_download)
                finally:
                    close_old_connections()

            with ThreadPoolExecutor(max_workers=workers) as ex:
                list(ex.map(_run_company_pricing_sync, pricing_cps))
        logger.info(
            "{} A-Tech per-company pricing pulls completed for {} company provider(s).".format(
                _LOG_PREFIX,
                n_cp,
            )
        )


def sync_atech_company_pricing_for_company_provider(
    company_provider_id: int,
    force_download: bool = True,
) -> None:
    """
    Download A-Tech feed file for one ``CompanyProviders`` row and upsert ``AtechCompanyPricing``.
    Resolves each file line to ``AtechParts`` by ``feed_part_number`` (full distributor line).
    ``MasterPart`` uses ``AtechParts.part_number``; ``ProviderPart.provider_external_id`` is composite
    (``atech_brand_id`` + part number); see ``sync_master_parts_from_atech``.
    """
    cp = (
        src_models.CompanyProviders.objects.filter(
            id=company_provider_id,
            provider__kind=src_enums.BrandProviderKind.ATECH.value,
            provider__status=src_enums.BrandProviderStatus.ACTIVE.value,
        )
        .select_related("company", "provider")
        .first()
    )
    if not cp:
        logger.warning(
            "{} No active A-Tech CompanyProviders id={}. Skipping.".format(_LOG_PREFIX, company_provider_id)
        )
        return

    creds = dict(cp.credentials or {})
    if not str(creds.get("local_feed_path") or "").strip():
        creds["local_feed_path"] = "/tmp/atech_feed_company_{}.txt".format(cp.company_id)
    try:
        client = atech_client.AtechSFTPClient(credentials=creds)
    except ValueError as e:
        logger.error("{} company_id={}: {}".format(_LOG_PREFIX, cp.company_id, str(e)))
        raise
    logger.info(
        "{} Downloading company feed: company_id={} company_provider_id={} force_download={}.".format(
            _LOG_PREFIX,
            cp.company_id,
            cp.id,
            force_download,
        )
    )
    try:
        company_feed_path = client.download_feed_file(force_download=force_download)
    except atech_exceptions.AtechException as e:
        logger.error(
            "{} A-Tech feed download error company_id={}: {}.".format(_LOG_PREFIX, cp.company_id, str(e)),
        )
        raise

    logger.info(
        "{} Building per-company pricing map: company_id={} company_provider_id={} path={} "
        "(progress every {} rows).".format(
            _LOG_PREFIX,
            cp.company_id,
            cp.id,
            company_feed_path,
            ATECH_PARSE_PROGRESS_EVERY,
        )
    )
    pmap = _atech_pricing_map_from_iterable(
        _iter_atech_csv_rows(company_feed_path),
        progress_every=ATECH_PARSE_PROGRESS_EVERY,
        progress_label="company_id={}".format(cp.company_id),
    )
    if not pmap:
        logger.warning(
            "{} No pricing rows parsed for A-Tech company_provider id={}.".format(_LOG_PREFIX, company_provider_id)
        )
        return

    feed_keys = list(pmap.keys())
    id_by_feed = _id_by_feed_keys_parallel(feed_keys, _LOG_PREFIX, company_provider_id)

    pricing_to_upsert: typing.List[src_models.AtechCompanyPricing] = []
    for feed_fn, pdata in pmap.items():
        k = (feed_fn or "").strip()
        part_id = id_by_feed.get(k)
        if not part_id:
            continue
        pricing_to_upsert.append(
            src_models.AtechCompanyPricing(
                part_id=part_id,
                company=cp.company,
                cost=pdata.get("cost"),
                retail_price=pdata.get("retail_price"),
                jobber_price=pdata.get("jobber_price"),
                core_charge=pdata.get("core_charge"),
                fee_hazmat=pdata.get("fee_hazmat"),
                fee_truck_us=pdata.get("fee_truck_us"),
                fee_handling_ground=pdata.get("fee_handling_ground"),
                fee_handling_air=pdata.get("fee_handling_air"),
            )
        )
    batch_total = 0
    n_price_batches = (
        (len(pricing_to_upsert) + ATECH_PRICING_UPSERT_BATCH - 1) // ATECH_PRICING_UPSERT_BATCH
        if pricing_to_upsert
        else 0
    )
    upsert_workers = (
        max(1, min(ATECH_COMPANY_PRICING_UPSERT_MAX_WORKERS, n_price_batches))
        if n_price_batches
        else 1
    )
    if n_price_batches:
        logger.info(
            "{} AtechCompanyPricing upsert start: {} batch(es), workers={} (company_provider id={}).".format(
                _LOG_PREFIX, n_price_batches, upsert_workers, company_provider_id
            )
        )
    batches = [
        pricing_to_upsert[j : j + ATECH_PRICING_UPSERT_BATCH]
        for j in range(0, len(pricing_to_upsert), ATECH_PRICING_UPSERT_BATCH)
    ]
    if upsert_workers == 1:
        price_batch_num = 0
        for batch in batches:
            price_batch_num += 1
            batch_total += _upsert_atech_company_pricing_batch(batch)
            if n_price_batches and (price_batch_num % 10 == 0 or price_batch_num == n_price_batches):
                logger.info(
                    "{} AtechCompanyPricing upsert progress: {}/{} batches (company_provider id={}).".format(
                        _LOG_PREFIX, price_batch_num, n_price_batches, company_provider_id
                    )
                )
    else:
        completed = 0
        with ThreadPoolExecutor(max_workers=upsert_workers) as ex:
            futs = [ex.submit(_upsert_atech_company_pricing_batch, batch) for batch in batches]
            for fut in as_completed(futs):
                batch_total += fut.result()
                completed += 1
                if n_price_batches and (completed % 10 == 0 or completed == n_price_batches):
                    logger.info(
                        "{} AtechCompanyPricing parallel upsert progress: {}/{} batches (company_provider id={}).".format(
                            _LOG_PREFIX, completed, n_price_batches, company_provider_id
                        )
                    )
    logger.info(
        "{} AtechCompanyPricing upserted {} rows for company_provider id={}.".format(
            _LOG_PREFIX, batch_total, company_provider_id,
        )
    )
