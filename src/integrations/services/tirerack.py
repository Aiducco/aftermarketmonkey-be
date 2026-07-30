import csv
import logging
import os
import sys
import tempfile
import time
import typing
from decimal import Decimal

import pgbulk
from django.db import connection
from django.db.models.functions import Upper
from django.utils import timezone

from src import enums as src_enums
from src import models as src_models
from src.integrations import credentials as credentials_helper
from src.integrations.clients.tirerack import client as tirerack_client
from src.integrations.clients.tirerack import exceptions as tirerack_exceptions
from src.integrations.utils.brand_matching import (
    best_fuzzy_brand_match,
    brands_by_compact_key,
    brands_by_first_token_upper,
    normalize_compact_key,
    normalize_upper_words,
)

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[TIRERACK-SERVICES]"

_TIRERACK_PARTS_UPSERT_BATCH = 25000
_TIRERACK_PARTS_UPSERT_DELAY = 0.05
_TIRERACK_CSV_ENCODINGS = ("utf-8-sig", "utf-8", "cp1252", "latin-1")
_TIRERACK_ENCODING_SNIFF_BYTES = 8 * 1024 * 1024

# Some feed lines can exceed the csv module's default 131072-byte field limit; raise it as high
# as the platform allows (same guard DLG's service uses).
_maxint = sys.maxsize
while True:
    try:
        csv.field_size_limit(_maxint)
        break
    except OverflowError:
        _maxint = int(_maxint / 10)


def _clean(value: typing.Any) -> typing.Optional[str]:
    if value is None:
        return None
    s = str(value).strip()
    if not s or s.lower() == "none":
        # The feed itself uses the literal text "None" as its placeholder for "no warranty"
        # (confirmed on real rows, e.g. Road Hazard Warranty) -- treat it the same as blank.
        return None
    return s


def _safe_decimal(value: typing.Any) -> typing.Optional[Decimal]:
    s = _clean(value)
    if s is None:
        return None
    try:
        return Decimal(s)
    except Exception:
        return None


def _safe_int(value: typing.Any) -> typing.Optional[int]:
    s = _clean(value)
    if s is None:
        return None
    try:
        return int(float(s))
    except Exception:
        return None


def _json_safe_row(row: typing.Dict) -> typing.Dict[str, typing.Any]:
    return {
        (k if isinstance(k, str) else str(k)): (None if v is None else str(v))
        for k, v in row.items()
    }


def _write_temp_catalog_file(filename: str, content: bytes) -> str:
    path = os.path.join(tempfile.gettempdir(), "tirerack_{}".format(filename))
    with open(path, "wb") as f:
        f.write(content)
    return path


def _sniff_csv_encoding(path: str) -> str:
    try:
        with open(path, "rb") as bf:
            sample = bf.read(_TIRERACK_ENCODING_SNIFF_BYTES)
    except OSError as e:
        raise ValueError("Could not read TireRack CSV at {!r}: {}".format(path, e)) from e
    for enc in _TIRERACK_CSV_ENCODINGS:
        try:
            sample.decode(enc)
            return enc
        except UnicodeDecodeError:
            continue
    return "utf-8"


def _iter_csv_rows(path: str) -> typing.Iterator[typing.Dict]:
    """Stream TireRack's catalog CSV rows without materializing the raw file twice."""
    enc = _sniff_csv_encoding(path)
    if enc != "utf-8-sig":
        logger.info("{} Streaming TireRack CSV {} with encoding {!r}.".format(_LOG_PREFIX, path, enc))
    with open(path, "r", newline="", encoding=enc, errors="replace") as f:
        reader = csv.DictReader(f)
        for raw_row in reader:
            norm = {(k.strip() if isinstance(k, str) else k): v for k, v in raw_row.items()}
            yield {k: (None if v is None or v == "" else v) for k, v in norm.items()}


def _records_from_csv(path: str) -> typing.List[typing.Dict]:
    return list(_iter_csv_rows(path))


def _tirerack_brand_key(value: typing.Any) -> typing.Optional[str]:
    raw = _clean(value)
    return raw.upper() if raw else None


# TireRackBrand name (uppercased) -> exact catalog Brands.name. Empty today; extend here if a
# real feed brand name needs a manual override (same purpose as DLG's canonical-override dict,
# e.g. the TOYO/TOYOTA collision it guards against -- none observed yet in TireRack's data).
_TIRERACK_UNMAPPED_SYNC_CANONICAL_BRAND: typing.Dict[str, str] = {}


def _active_tirerack_company_providers_queryset():
    return src_models.CompanyProviders.objects.filter(
        provider__kind=src_enums.BrandProviderKind.TIRERACK.value,
        provider__status=src_enums.BrandProviderStatus.ACTIVE.value,
    ).select_related("company", "provider")


def _primary_tirerack_company_provider() -> typing.Optional[src_models.CompanyProviders]:
    base = _active_tirerack_company_providers_queryset()
    cp = base.filter(primary=True).first()
    if cp:
        return cp
    return base.first()


def sync_unmapped_tirerack_brands_to_brands(dry_run: bool = False) -> typing.List[src_models.TireRackBrand]:
    """
    For each TireRackBrand without BrandTireRackBrandMapping: match catalog Brands by canonical
    override, then exact match on upper(Brands.name), then compact-key match, then fuzzy
    word-prefix match -- same cascade order as DLG's sync_unmapped_dlg_brands_to_brands, reusing
    the shared src.integrations.utils.brand_matching helpers. Unlike DLG, this does not tie new
    brands to any specific company (no CompanyBrands step) -- TireRackBrand -> Brands only needs
    BrandProviders (mirrors VossenBrand's single-brand link, generalized here to many brands)
    since TireRack is a platform-wide catalog any company can browse once connected.
    """
    logger.info(
        "{} Syncing unmapped TireRack brands to Brands{}.".format(_LOG_PREFIX, " (dry run)" if dry_run else "")
    )

    tirerack_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.TIRERACK.value,
    ).first()
    if not tirerack_provider:
        logger.warning("{} TireRack provider not found.".format(_LOG_PREFIX))
        return []

    mapped_ids = set(
        src_models.BrandTireRackBrandMapping.objects.values_list("tirerack_brand_id", flat=True).distinct()
    )
    unmapped = list(src_models.TireRackBrand.objects.exclude(id__in=mapped_ids).order_by("id"))
    if not unmapped:
        logger.info("{} No unmapped TireRack brands.".format(_LOG_PREFIX))
        return []

    name_upper_keys = {(trb.name or "").strip().upper() for trb in unmapped if (trb.name or "").strip()}
    brands_by_upper_name: typing.Dict[str, src_models.Brands] = {}
    if name_upper_keys:
        for b in (
            src_models.Brands.objects.annotate(_name_u=Upper("name"))
            .filter(_name_u__in=name_upper_keys)
            .order_by("id")
        ):
            key = (b.name or "").strip().upper()
            brands_by_upper_name.setdefault(key, b)

    resolved_by_id: typing.Dict[int, src_models.Brands] = {}
    canonical_matched_ids: typing.Set[int] = set()
    exact_matched_ids: typing.Set[int] = set()

    for trb in sorted(unmapped, key=lambda x: x.id):
        canon = _TIRERACK_UNMAPPED_SYNC_CANONICAL_BRAND.get((trb.name or "").strip().upper())
        if canon:
            b = brands_by_upper_name.get(canon.strip().upper())
            if b:
                resolved_by_id[trb.id] = b
                canonical_matched_ids.add(trb.id)

    for trb in sorted(unmapped, key=lambda x: x.id):
        if trb.id in resolved_by_id:
            continue
        b = brands_by_upper_name.get((trb.name or "").strip().upper())
        if b:
            resolved_by_id[trb.id] = b
            exact_matched_ids.add(trb.id)

    # Compact-key phase: catches punctuation/spacing-only differences the word-prefix fuzzy
    # phase below can't (it compares whole tokens, never merges/splits words).
    compact_matched_ids: typing.Set[int] = set()
    still_unresolved = [trb for trb in unmapped if trb.id not in resolved_by_id]
    if still_unresolved:
        compact_index = brands_by_compact_key()
        for trb in sorted(still_unresolved, key=lambda x: x.id):
            key = normalize_compact_key(trb.name or "")
            if not key:
                continue
            b = compact_index.get(key)
            if b:
                resolved_by_id[trb.id] = b
                compact_matched_ids.add(trb.id)

    unresolved_after_compact = [trb for trb in unmapped if trb.id not in resolved_by_id]
    brands_first_index = brands_by_first_token_upper() if unresolved_after_compact else {}
    all_brands_fallback: typing.Optional[typing.List[src_models.Brands]] = None
    fuzzy_matched_ids: typing.Set[int] = set()
    for trb in unresolved_after_compact:
        source = trb.name or ""
        parts = normalize_upper_words(source).split()
        candidates: typing.List[src_models.Brands] = []
        if parts:
            candidates = list(brands_first_index.get(parts[0], ()))
        if not candidates:
            if all_brands_fallback is None:
                all_brands_fallback = list(
                    src_models.Brands.objects.only("id", "name", "aaia_code").order_by("id")
                )
            candidates = all_brands_fallback
        brand = best_fuzzy_brand_match(source, candidates) if candidates else None
        if brand:
            resolved_by_id[trb.id] = brand
            fuzzy_matched_ids.add(trb.id)

    if dry_run:
        would_create = [trb for trb in unmapped if trb.id not in resolved_by_id]
        logger.info(
            "{} [dry-run] Summary: {} canonical, {} exact, {} compact, {} fuzzy, {} unmatched "
            "(would create Brand). No writes performed.".format(
                _LOG_PREFIX, len(canonical_matched_ids), len(exact_matched_ids),
                len(compact_matched_ids), len(fuzzy_matched_ids), len(would_create),
            )
        )
        return unmapped

    new_brand_names: typing.Set[str] = set()
    for trb in unmapped:
        if trb.id not in resolved_by_id:
            new_brand_names.add((trb.name or "").strip().upper() or "BRAND_{}".format(trb.id))

    created_brands = 0
    if new_brand_names:
        existing_names = set(
            src_models.Brands.objects.filter(name__in=list(new_brand_names)).values_list("name", flat=True)
        )
        new_rows = [
            src_models.Brands(
                name=name,
                status=src_enums.BrandProviderStatus.ACTIVE.value,
                status_name=src_enums.BrandProviderStatus.ACTIVE.name,
                aaia_code=None,
            )
            for name in new_brand_names
            if name not in existing_names
        ]
        if new_rows:
            src_models.Brands.objects.bulk_create(new_rows, ignore_conflicts=True)
            created_brands = len(new_rows)
        by_name = {b.name: b for b in src_models.Brands.objects.filter(name__in=list(new_brand_names))}
        for trb in unmapped:
            if trb.id not in resolved_by_id:
                nu = (trb.name or "").strip().upper() or "BRAND_{}".format(trb.id)
                resolved_by_id[trb.id] = by_name[nu]

    mapping_models = [
        src_models.BrandTireRackBrandMapping(brand_id=resolved_by_id[trb.id].id, tirerack_brand_id=trb.id)
        for trb in unmapped
    ]
    pgbulk.upsert(
        src_models.BrandTireRackBrandMapping,
        mapping_models,
        unique_fields=["brand", "tirerack_brand"],
        update_fields=[],
        returning=False,
    )

    created_bp = 0
    for trb in unmapped:
        brand = resolved_by_id[trb.id]
        _, bpc = src_models.BrandProviders.objects.get_or_create(brand=brand, provider=tirerack_provider)
        if bpc:
            created_bp += 1

    logger.info(
        "{} TireRack brand sync done. canonical={}, exact={}, compact={}, fuzzy={}, created={}, "
        "mappings upserted={}, BrandProviders created={}.".format(
            _LOG_PREFIX, len(canonical_matched_ids), len(exact_matched_ids), len(compact_matched_ids),
            len(fuzzy_matched_ids), created_brands, len(mapping_models), created_bp,
        )
    )
    return unmapped


def _part_from_row(
    row: typing.Dict,
    brand_by_key: typing.Dict[str, src_models.TireRackBrand],
) -> typing.Optional[src_models.TireRackParts]:
    bkey = _tirerack_brand_key(row.get("Manufacturer Name"))
    pn = _clean(row.get("Manufacturer Part#"))
    if not bkey or not pn:
        return None
    brand = brand_by_key.get(bkey)
    if not brand:
        return None
    return src_models.TireRackParts(
        brand=brand,
        part_number=pn,
        description=_clean(row.get("Description")),
        quantity=_safe_int(row.get("Quantity")),
        country_of_origin=_clean(row.get("Country of Origin")),
        fet=_safe_decimal(row.get("FET")),
        base_price=_safe_decimal(row.get("Base Price")),
        total_price=_safe_decimal(row.get("Total Price")),
        road_hazard_warranty=_clean(row.get("Road Hazard Warranty")),
        treadlife_warranty_1=_clean(row.get("Treadlife Warranty 1")),
        treadlife_warranty_2=_clean(row.get("Treadlife Warranty 2")),
        treadlife_warranty_3=_clean(row.get("Treadlife Warranty 3")),
        raw_data=_json_safe_row(row) if row else None,
    )


def _dedupe_tirerack_parts_for_upsert(
    parts: typing.List[src_models.TireRackParts],
) -> typing.List[src_models.TireRackParts]:
    by_key: typing.Dict[typing.Tuple[int, str], src_models.TireRackParts] = {}
    for p in parts:
        bid = p.brand_id if p.brand_id is not None else (p.brand.pk if p.brand else None)
        pn = (p.part_number or "").strip()
        if not bid or not pn:
            continue
        by_key[(int(bid), pn)] = p
    return list(by_key.values())


_TIRERACK_PARTS_UPDATE_FIELDS = [
    "description", "quantity", "country_of_origin", "fet", "base_price", "total_price",
    "road_hazard_warranty", "treadlife_warranty_1", "treadlife_warranty_2", "treadlife_warranty_3",
    "raw_data", "updated_at",
]


def fetch_and_save_tirerack_catalog() -> None:
    """
    Download the latest daily CSV (one new ``CustomTruck_<date>.csv`` dropped each morning --
    see TireRackSFTPClient) from the PRIMARY TireRack connection and upsert the shared
    TireRackBrand / TireRackParts catalog. Each company has its own separate TireRack SFTP
    account/feed (same pattern as DLG) -- this shared catalog step only uses the primary
    connection (see _primary_tirerack_company_provider), matching DLG's own split: catalog is
    shared, but per-company pricing (Total Price) is pulled separately per company via
    sync_tirerack_company_pricing_for_company_provider into TireRackCompanyPricing.
    """
    logger.info("{} Starting TireRack catalog ingest.".format(_LOG_PREFIX))

    catalog_cp = _primary_tirerack_company_provider()
    if not catalog_cp:
        logger.info("{} No active TireRack CompanyProviders. Skipping.".format(_LOG_PREFIX))
        return

    try:
        sftp = tirerack_client.TireRackSFTPClient(credentials=credentials_helper.get_feed_credentials(catalog_cp))
    except ValueError as e:
        logger.error("{} {}".format(_LOG_PREFIX, str(e)))
        raise

    try:
        filename, content = sftp.download_latest_catalog()
    except tirerack_exceptions.TireRackException as e:
        logger.error("{} {}".format(_LOG_PREFIX, str(e)))
        raise

    local_path = _write_temp_catalog_file(filename, content)
    records = _records_from_csv(local_path)
    if not records:
        logger.warning("{} Catalog file empty or unreadable: {}.".format(_LOG_PREFIX, local_path))
        return

    brand_keys: typing.Set[str] = set()
    brand_name_by_key: typing.Dict[str, str] = {}
    for row in records:
        k = _tirerack_brand_key(row.get("Manufacturer Name"))
        if not k:
            continue
        brand_keys.add(k)
        raw = row.get("Manufacturer Name")
        if raw is not None and str(raw).strip():
            brand_name_by_key.setdefault(k, str(raw).strip())

    brand_objs = [
        src_models.TireRackBrand(name=brand_name_by_key.get(k, k))
        for k in sorted(brand_keys)
    ]
    if brand_objs:
        pgbulk.upsert(
            src_models.TireRackBrand,
            brand_objs,
            unique_fields=["name"],
            update_fields=[],
        )
        connection.close()

    brand_by_key: typing.Dict[str, src_models.TireRackBrand] = {}
    if brand_objs:
        for b in src_models.TireRackBrand.objects.filter(name__in=[o.name for o in brand_objs]):
            brand_by_key[b.name.upper()] = b
    if not brand_by_key:
        logger.warning("{} No TireRack brands after upsert.".format(_LOG_PREFIX))
        return

    parts_raw: typing.List[src_models.TireRackParts] = []
    for row in records:
        p = _part_from_row(row, brand_by_key)
        if p:
            parts_raw.append(p)

    parts = _dedupe_tirerack_parts_for_upsert(parts_raw)
    if len(parts) < len(parts_raw):
        logger.info(
            "{} Deduped {} -> {} TireRackParts on (brand, part_number).".format(
                _LOG_PREFIX, len(parts_raw), len(parts),
            )
        )

    if not parts:
        logger.warning("{} No TireRackParts to upsert.".format(_LOG_PREFIX))
        return

    total_batches = (len(parts) + _TIRERACK_PARTS_UPSERT_BATCH - 1) // _TIRERACK_PARTS_UPSERT_BATCH
    logger.info(
        "{} Upserting {} TireRackParts in {} batches (batch_size={}).".format(
            _LOG_PREFIX, len(parts), total_batches, _TIRERACK_PARTS_UPSERT_BATCH,
        )
    )
    batch_num = 0
    for i in range(0, len(parts), _TIRERACK_PARTS_UPSERT_BATCH):
        batch_num += 1
        batch = parts[i : i + _TIRERACK_PARTS_UPSERT_BATCH]
        now = timezone.now()
        for p in batch:
            p.updated_at = now
        pgbulk.upsert(
            src_models.TireRackParts,
            batch,
            unique_fields=["part_number", "brand"],
            update_fields=_TIRERACK_PARTS_UPDATE_FIELDS,
        )
        connection.close()
        logger.info(
            "{} TireRackParts upsert progress: batch {}/{} (through ~{}/{})".format(
                _LOG_PREFIX, batch_num, total_batches, min(i + len(batch), len(parts)), len(parts),
            )
        )
        if i + _TIRERACK_PARTS_UPSERT_BATCH < len(parts):
            time.sleep(_TIRERACK_PARTS_UPSERT_DELAY)

    logger.info("{} Finished TireRack catalog ingest ({} parts).".format(_LOG_PREFIX, len(parts)))


_TIRERACK_PRICING_UPSERT_BATCH = 2000
_TIRERACK_PART_ID_LOOKUP_CHUNK = 3000


def _upsert_tirerack_company_pricing_for_company(
    company: src_models.Company,
    records: typing.List[typing.Dict],
) -> int:
    """Map this company's own feed rows to the shared TireRackParts catalog and upsert TireRackCompanyPricing (Total Price column)."""
    brand_by_upper_name: typing.Dict[str, src_models.TireRackBrand] = {
        b.name.upper(): b for b in src_models.TireRackBrand.objects.all()
    }
    if not brand_by_upper_name:
        return 0

    pairs: typing.List[typing.Tuple[int, str]] = []
    seen_pairs: typing.Set[typing.Tuple[int, str]] = set()
    for row in records:
        bkey = _tirerack_brand_key(row.get("Manufacturer Name"))
        pn = _clean(row.get("Manufacturer Part#"))
        if not bkey or not pn:
            continue
        brand = brand_by_upper_name.get(bkey)
        if not brand:
            continue
        t = (brand.id, pn)
        if t not in seen_pairs:
            seen_pairs.add(t)
            pairs.append(t)
    if not pairs:
        return 0

    part_id_by_pair: typing.Dict[typing.Tuple[int, str], int] = {}
    for i in range(0, len(pairs), _TIRERACK_PART_ID_LOOKUP_CHUNK):
        chunk = pairs[i : i + _TIRERACK_PART_ID_LOOKUP_CHUNK]
        with connection.cursor() as cur:
            cur.execute(
                "SELECT id, brand_id, part_number FROM tirerack_parts WHERE (brand_id, part_number) IN %s",
                (tuple(chunk),),
            )
            for pid, bid, pn in cur.fetchall():
                pn_s = (pn or "").strip() if isinstance(pn, str) else str(pn or "").strip()
                part_id_by_pair[(int(bid), pn_s)] = int(pid)
    if not part_id_by_pair:
        return 0

    by_conflict: typing.Dict[typing.Tuple[int, int], src_models.TireRackCompanyPricing] = {}
    for row in records:
        bkey = _tirerack_brand_key(row.get("Manufacturer Name"))
        pn = _clean(row.get("Manufacturer Part#"))
        if not bkey or not pn:
            continue
        brand = brand_by_upper_name.get(bkey)
        if not brand:
            continue
        pid = part_id_by_pair.get((brand.id, pn))
        if not pid:
            continue
        by_conflict[(pid, company.id)] = src_models.TireRackCompanyPricing(
            part_id=pid,
            company=company,
            total_price=_safe_decimal(row.get("Total Price")),
        )

    pricing_rows = list(by_conflict.values())
    total = 0
    for j in range(0, len(pricing_rows), _TIRERACK_PRICING_UPSERT_BATCH):
        batch = pricing_rows[j : j + _TIRERACK_PRICING_UPSERT_BATCH]
        pgbulk.upsert(
            src_models.TireRackCompanyPricing,
            batch,
            unique_fields=["part", "company"],
            update_fields=["total_price", "updated_at"],
            returning=False,
        )
        total += len(batch)
        connection.close()
    return total


def sync_tirerack_company_pricing_for_company_provider(company_provider_id: int) -> None:
    """
    Download this company's own TireRack SFTP feed and upsert TireRackCompanyPricing. Each
    company has its own TireRack dealer account/credentials (same pattern as DLG's
    sync_dlg_company_pricing_for_company_provider) -- separate from the shared catalog ingest
    (fetch_and_save_tirerack_catalog), which uses only the primary connection. Requires matching
    TireRackParts/TireRackBrand (run the primary's catalog + brand sync first).
    """
    cp = (
        src_models.CompanyProviders.objects.filter(
            id=company_provider_id,
            provider__kind=src_enums.BrandProviderKind.TIRERACK.value,
            provider__status=src_enums.BrandProviderStatus.ACTIVE.value,
        )
        .select_related("company", "provider")
        .first()
    )
    if not cp:
        logger.warning(
            "{} No active TireRack CompanyProviders id={}. Skipping pricing.".format(
                _LOG_PREFIX, company_provider_id,
            )
        )
        return

    try:
        sftp = tirerack_client.TireRackSFTPClient(credentials=credentials_helper.get_feed_credentials(cp))
    except ValueError as e:
        logger.error("{} company_id={}: {}.".format(_LOG_PREFIX, cp.company_id, str(e)))
        raise

    try:
        filename, content = sftp.download_latest_catalog()
    except tirerack_exceptions.TireRackException as e:
        logger.error(
            "{} TireRack pricing download error company_id={}: {}.".format(_LOG_PREFIX, cp.company_id, str(e))
        )
        raise

    local_path = _write_temp_catalog_file("company_{}_{}".format(cp.company_id, filename), content)
    records = _records_from_csv(local_path)
    if not records:
        logger.warning("{} TireRack pricing file empty: {}.".format(_LOG_PREFIX, local_path))
        return
    n = _upsert_tirerack_company_pricing_for_company(cp.company, records)
    logger.info(
        "{} TireRackCompanyPricing upserted {} rows for company_id={} (company_provider id={}).".format(
            _LOG_PREFIX, n, cp.company_id, company_provider_id,
        )
    )
