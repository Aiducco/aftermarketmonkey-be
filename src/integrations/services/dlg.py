import logging
import re
import time
import typing
from decimal import Decimal

import pandas as pd
import pgbulk
from django.db import connection
from django.db.models.functions import Upper
from django.utils import timezone

from src import enums as src_enums
from src import models as src_models
from src.integrations.clients.dlg import client as dlg_client
from src.integrations.clients.dlg import exceptions as dlg_exceptions
from src.integrations.utils.brand_matching import (
    best_fuzzy_brand_match,
    brands_by_first_token_upper,
    normalize_upper_words,
)

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[DLG-SERVICES]"

_EXCEL_FORMULA_PATTERN = re.compile(r'^="?([^"]*)"?$|^=(\d+(?:\.\d+)?)$')

DLG_PARTS_UPSERT_BATCH = 25000
DLG_PARTS_UPSERT_DELAY = 0.05
DLG_PRICING_UPSERT_BATCH = 2000
DLG_PART_ID_LOOKUP_CHUNK = 3000

DLG_PARTS_UPDATE_FIELDS = [
    "display_name",
    "available_on_hand",
    "units",
    "base_price",
    "raw_data",
    "updated_at",
]

_DLG_CSV_ENCODINGS = ("utf-8-sig", "utf-8", "cp1252", "latin-1")


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
    return {(k.strip() if isinstance(k, str) else k): v for k, v in row.items()}


def _read_dlg_dataframe(path: str) -> pd.DataFrame:
    last_error: typing.Optional[Exception] = None
    for encoding in _DLG_CSV_ENCODINGS:
        try:
            df = pd.read_csv(path, dtype=object, encoding=encoding, keep_default_na=False)
            if encoding != "utf-8-sig":
                logger.info(
                    "{} Read DLG inventory {} with encoding {!r}.".format(_LOG_PREFIX, path, encoding)
                )
            return df
        except UnicodeDecodeError as e:
            last_error = e
            continue
    if last_error:
        raise last_error
    raise ValueError("Could not read DLG inventory at {!r}".format(path))


def _records_from_csv(path: str) -> typing.List[typing.Dict]:
    df = _read_dlg_dataframe(path)
    out: typing.List[typing.Dict] = []
    for row in df.to_dict(orient="records"):
        norm = _normalize_row_keys(row)
        out.append({
            k: (None if v is None or v == "" or (isinstance(v, float) and pd.isna(v)) else v)
            for k, v in norm.items()
        })
    return out


def _dlg_brand_key(value: typing.Any) -> typing.Optional[str]:
    raw = _clean_csv_value(value)
    if not raw:
        return None
    return raw.upper()


def normalize_brand_match_key(value: typing.Optional[str]) -> str:
    """Normalize for comparison: strip, collapse whitespace, uppercase (same as Meyer CSV / unmapped sync)."""
    if not value:
        return ""
    s = str(value).strip().upper()
    s = re.sub(r"\s+", " ", s)
    return s


def _dlg_brand_name_upper_for_sync(dlg_brand: src_models.DlgBrand) -> str:
    name_upper = (dlg_brand.name or "").strip().upper()
    if not name_upper:
        name_upper = "BRAND_{}".format(dlg_brand.external_id)
    return name_upper


# DlgBrand name or external_id (after normalize_brand_match_key) -> exact catalog ``Brands.name``.
_DLG_UNMAPPED_SYNC_CANONICAL_BRAND: typing.Dict[str, str] = {}


def _dlg_unmapped_sync_canonical_brand_name(dlg_brand: src_models.DlgBrand) -> typing.Optional[str]:
    for raw in (dlg_brand.external_id, dlg_brand.name):
        k = normalize_brand_match_key(raw)
        if not k:
            continue
        target = _DLG_UNMAPPED_SYNC_CANONICAL_BRAND.get(k)
        if target:
            return target
    return None


def _dlg_sync_source_label(dlg_brand: src_models.DlgBrand) -> str:
    """Prefer feed display name, then external_id — used for fuzzy match + collision checks."""
    return ((dlg_brand.name or dlg_brand.external_id or "").strip())


def _dlg_first_token_upper(label: str) -> str:
    parts = normalize_upper_words(label or "").split()
    return parts[0] if parts else ""


def _dlg_is_toyo_toyota_collision(dlg_label: str, catalog_brand_name: str) -> bool:
    """
    Fuzzy prefix logic would map TOYO ↔ TOYOTA; block that pair (either direction).
    Compared on first uppercase word of each side.
    """
    a = _dlg_first_token_upper(dlg_label)
    b = _dlg_first_token_upper(catalog_brand_name)
    if not a or not b:
        return False
    return frozenset((a, b)) == frozenset(("TOYO", "TOYOTA"))


def _dlg_filter_fuzzy_candidates(
    dlg_brand: src_models.DlgBrand,
    candidates: typing.Iterable[src_models.Brands],
) -> typing.List[src_models.Brands]:
    src = _dlg_sync_source_label(dlg_brand)
    return [
        b
        for b in candidates
        if not _dlg_is_toyo_toyota_collision(src, b.name or "")
    ]


def _json_safe_row(row: typing.Dict) -> typing.Dict[str, typing.Any]:
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


def _brand_map_for_names(names: typing.Collection[str]) -> typing.Dict[str, src_models.DlgBrand]:
    brands = src_models.DlgBrand.objects.filter(external_id__in=list(names))
    return {b.external_id: b for b in brands}


def _dedupe_dlg_parts_for_upsert(parts: typing.List[src_models.DlgParts]) -> typing.List[src_models.DlgParts]:
    by_key: typing.Dict[typing.Tuple[int, str], src_models.DlgParts] = {}
    for p in parts:
        bid = p.brand_id
        if bid is None and p.brand is not None:
            bid = p.brand.pk
        pn = (p.part_number or "").strip()
        if not bid or not pn:
            continue
        by_key[(int(bid), pn)] = p
    return list(by_key.values())


def _active_dlg_company_providers_queryset():
    return src_models.CompanyProviders.objects.filter(
        provider__kind=src_enums.BrandProviderKind.DLG.value,
        provider__status=src_enums.BrandProviderStatus.ACTIVE.value,
    ).select_related("company", "provider")


def _primary_dlg_company_provider() -> typing.Optional[src_models.CompanyProviders]:
    base = _active_dlg_company_providers_queryset()
    cp = base.filter(primary=True).first()
    if cp:
        return cp
    return base.first()


def sync_unmapped_dlg_brands_to_brands(dry_run: bool = False) -> typing.List[src_models.DlgBrand]:
    """
    For each DlgBrand without BrandDlgBrandMapping: match catalog ``Brands`` by canonical overrides,
    then exact match on ``upper(Brands.name)`` vs uppercased Dlg ``name`` and ``external_id``,
    then fuzzy word-prefix match (same uppercase normalization as other syncs). TOYO and TOYOTA are
    never matched to each other. Otherwise create ``Brands``. Upserts mapping / BrandProviders /
    CompanyBrands for TICK_PERFORMANCE.
    """
    logger.info(
        "{} Syncing unmapped DLG brands to Brands{}.".format(
            _LOG_PREFIX,
            " (dry run)" if dry_run else "",
        )
    )

    dlg_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.DLG.value,
    ).first()
    if not dlg_provider:
        logger.warning("{} DLG provider not found.".format(_LOG_PREFIX))
        return []

    tick_company = src_models.Company.objects.filter(name="TICK_PERFORMANCE").first()
    if not tick_company:
        logger.warning("{} Company TICK_PERFORMANCE not found. Skipping.".format(_LOG_PREFIX))
        return []

    if not dry_run:
        _, cp_created = src_models.CompanyProviders.objects.get_or_create(
            company=tick_company,
            provider=dlg_provider,
            defaults={"credentials": {}, "primary": False},
        )
        if cp_created:
            logger.info("{} Created CompanyProviders for TICK_PERFORMANCE + DLG.".format(_LOG_PREFIX))

    mapped_ids = set(
        src_models.BrandDlgBrandMapping.objects.values_list("dlg_brand_id", flat=True).distinct()
    )
    unmapped = list(src_models.DlgBrand.objects.exclude(id__in=mapped_ids).order_by("id"))
    if not unmapped:
        logger.info("{} No unmapped DLG brands.".format(_LOG_PREFIX))
        return []

    name_upper_keys: typing.Set[str] = set()
    for db in unmapped:
        if (db.name or "").strip():
            name_upper_keys.add((db.name or "").strip().upper())
        if (db.external_id or "").strip():
            name_upper_keys.add((db.external_id or "").strip().upper())

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
    for db in unmapped:
        t = _dlg_unmapped_sync_canonical_brand_name(db)
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

    resolved_by_dlg_id: typing.Dict[int, src_models.Brands] = {}
    canonical_matched_ids: typing.Set[int] = set()
    exact_matched_ids: typing.Set[int] = set()
    for db in sorted(unmapped, key=lambda x: x.id):
        canon_name = _dlg_unmapped_sync_canonical_brand_name(db)
        if canon_name:
            b = brands_by_upper_for_canonical.get(canon_name.strip().upper())
            if b:
                if _dlg_is_toyo_toyota_collision(_dlg_sync_source_label(db), b.name or ""):
                    logger.info(
                        "{} Skipping canonical match (TOYO/TOYOTA): DlgBrand id={} -> Brand id={}.".format(
                            _LOG_PREFIX, db.id, b.id,
                        )
                    )
                else:
                    resolved_by_dlg_id[db.id] = b
                    canonical_matched_ids.add(db.id)
            else:
                logger.warning(
                    "{} DLG canonical brand override {!r} -> {!r} but no Brands row with that name.".format(
                        _LOG_PREFIX,
                        db.name or db.external_id,
                        canon_name,
                    )
                )

    for db in sorted(unmapped, key=lambda x: x.id):
        if db.id in resolved_by_dlg_id:
            continue
        exact_keys: typing.List[str] = []
        if (db.name or "").strip():
            exact_keys.append((db.name or "").strip().upper())
        if (db.external_id or "").strip():
            k = (db.external_id or "").strip().upper()
            if k not in exact_keys:
                exact_keys.append(k)
        for nm in exact_keys:
            brand = brands_by_upper_name.get(nm)
            if not brand:
                continue
            if _dlg_is_toyo_toyota_collision(_dlg_sync_source_label(db), brand.name or ""):
                continue
            resolved_by_dlg_id[db.id] = brand
            exact_matched_ids.add(db.id)
            break

    unresolved_after_exact = [db for db in unmapped if db.id not in resolved_by_dlg_id]
    brands_first_index = brands_by_first_token_upper() if unresolved_after_exact else {}
    all_brands_fallback: typing.Optional[typing.List[src_models.Brands]] = None
    fuzzy_matches = 0
    fuzzy_matched_ids: typing.Set[int] = set()
    for db in unresolved_after_exact:
        source = _dlg_sync_source_label(db)
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
        candidates = _dlg_filter_fuzzy_candidates(db, candidates)
        brand = best_fuzzy_brand_match(source, candidates) if candidates else None
        if brand:
            resolved_by_dlg_id[db.id] = brand
            fuzzy_matched_ids.add(db.id)
            fuzzy_matches += 1
            if not dry_run:
                logger.debug(
                    "{} Fuzzy-matched DLG brand {!r} to Brand id={} name={!r}.".format(
                        _LOG_PREFIX,
                        source,
                        brand.id,
                        brand.name,
                    )
                )

    if dry_run:
        for db in sorted(unmapped, key=lambda x: x.id):
            if db.id not in resolved_by_dlg_id:
                continue
            brand = resolved_by_dlg_id[db.id]
            if db.id in canonical_matched_ids:
                how = "canonical_override"
            elif db.id in exact_matched_ids:
                how = "exact"
            else:
                how = "fuzzy"
            logger.info(
                "{} [dry-run] match ({}) DlgBrand id={} external_id={!r} name={!r} "
                "-> Brand id={} name={!r}".format(
                    _LOG_PREFIX,
                    how,
                    db.id,
                    db.external_id,
                    db.name,
                    brand.id,
                    brand.name,
                )
            )
        would_create = [db for db in unmapped if db.id not in resolved_by_dlg_id]
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
    for db in sorted(unmapped, key=lambda x: x.id):
        if db.id in resolved_by_dlg_id:
            continue
        new_brand_specs.add(_dlg_brand_name_upper_for_sync(db))

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
        for db in unmapped:
            if db.id not in resolved_by_dlg_id:
                nu = _dlg_brand_name_upper_for_sync(db)
                resolved_by_dlg_id[db.id] = by_name[nu]

    mapping_models = [
        src_models.BrandDlgBrandMapping(
            brand_id=resolved_by_dlg_id[db.id].id,
            dlg_brand_id=db.id,
        )
        for db in unmapped
    ]
    try:
        pgbulk.upsert(
            src_models.BrandDlgBrandMapping,
            mapping_models,
            unique_fields=["brand", "dlg_brand"],
            update_fields=[],
            returning=False,
        )
    except Exception as e:
        logger.error("{} Error upserting BrandDlgBrandMapping: {}.".format(_LOG_PREFIX, str(e)))
        raise

    created_bp = 0
    created_cb = 0
    for db in unmapped:
        brand = resolved_by_dlg_id[db.id]
        _, bpc = src_models.BrandProviders.objects.get_or_create(
            brand=brand,
            provider=dlg_provider,
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
        "{} DLG brand sync done. Canonical overrides: {}, brands created: {}, fuzzy name matches: {}, "
        "BrandDlgBrandMapping upserted: {}, BrandProviders: {}, CompanyBrands: {}.".format(
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


def _part_from_row(
    row: typing.Dict,
    brand_by_key: typing.Dict[str, src_models.DlgBrand],
) -> typing.Optional[src_models.DlgParts]:
    bkey = _dlg_brand_key(row.get("Brand"))
    pname = _clean_csv_value(row.get("Name"))
    if not bkey or not pname:
        return None
    dlg_brand = brand_by_key.get(bkey)
    if not dlg_brand:
        return None
    return src_models.DlgParts(
        brand=dlg_brand,
        part_number=pname.strip(),
        display_name=_clean_csv_value(row.get("Display Name")),
        available_on_hand=_safe_int(row.get("Available On Hand")),
        units=_clean_csv_value(row.get("Units")),
        base_price=_safe_decimal(row.get("Base Price")),
        raw_data=_json_safe_row(row) if row else None,
    )


def _dlg_unique_pairs_from_records(
    records: typing.List[typing.Dict],
    brand_by_key: typing.Dict[str, src_models.DlgBrand],
) -> typing.List[typing.Tuple[int, str]]:
    seen_order: typing.List[typing.Tuple[int, str]] = []
    uniq: typing.Set[typing.Tuple[int, str]] = set()
    for row in records:
        bkey = _dlg_brand_key(row.get("Brand"))
        pname = _clean_csv_value(row.get("Name"))
        if not bkey or not pname:
            continue
        db = brand_by_key.get(bkey)
        if not db:
            continue
        t = (int(db.id), pname.strip())
        if t not in uniq:
            uniq.add(t)
            seen_order.append(t)
    return seen_order


def _dlg_chunked_part_id_map(
    pairs: typing.List[typing.Tuple[int, str]],
    chunk_size: int = DLG_PART_ID_LOOKUP_CHUNK,
) -> typing.Dict[typing.Tuple[int, str], int]:
    out: typing.Dict[typing.Tuple[int, str], int] = {}
    for i in range(0, len(pairs), chunk_size):
        chunk = pairs[i : i + chunk_size]
        if not chunk:
            continue
        with connection.cursor() as cur:
            cur.execute(
                "SELECT id, brand_id, part_number FROM dlg_parts WHERE (brand_id, part_number) IN %s",
                (tuple(chunk),),
            )
            for pid, bid, pn in cur.fetchall():
                pn_s = (pn or "").strip() if isinstance(pn, str) else str(pn or "").strip()
                out[(int(bid), pn_s)] = int(pid)
    return out


def _upsert_dlg_company_pricing_for_company(
    company: src_models.Company,
    records: typing.List[typing.Dict],
) -> int:
    """Map inventory rows to ``DlgParts`` and upsert ``DlgCompanyPricing`` (Base Price column)."""
    brand_keys: typing.Set[str] = set()
    for row in records:
        k = _dlg_brand_key(row.get("Brand"))
        if k:
            brand_keys.add(k)
    if not brand_keys:
        return 0
    brand_by_key = _brand_map_for_names(brand_keys)
    pairs = _dlg_unique_pairs_from_records(records, brand_by_key)
    part_id_by_pair = _dlg_chunked_part_id_map(pairs)
    if not part_id_by_pair:
        return 0

    by_conflict: typing.Dict[typing.Tuple[int, int], src_models.DlgCompanyPricing] = {}
    for row in records:
        bkey = _dlg_brand_key(row.get("Brand"))
        pname = _clean_csv_value(row.get("Name"))
        if not bkey or not pname:
            continue
        db = brand_by_key.get(bkey)
        if not db:
            continue
        pid = part_id_by_pair.get((int(db.id), pname.strip()))
        if not pid:
            continue
        by_conflict[(pid, int(company.id))] = src_models.DlgCompanyPricing(
            part_id=pid,
            company=company,
            base_price=_safe_decimal(row.get("Base Price")),
        )

    pricing_rows = list(by_conflict.values())
    total = 0
    for j in range(0, len(pricing_rows), DLG_PRICING_UPSERT_BATCH):
        batch = pricing_rows[j : j + DLG_PRICING_UPSERT_BATCH]
        pgbulk.upsert(
            src_models.DlgCompanyPricing,
            batch,
            unique_fields=["part", "company"],
            update_fields=["base_price", "updated_at"],
            returning=False,
        )
        total += len(batch)
        connection.close()
    return total


def sync_dlg_company_pricing_for_company_provider(
    company_provider_id: int,
    force_download: bool = True,
) -> None:
    """
    Download this company’s copy of the DLG inventory CSV from the relay and upsert ``DlgCompanyPricing``.
    SFTP user/password are from Django settings, not company credentials. Requires matching ``DlgParts`` (run global catalog first).
    """
    cp = (
        src_models.CompanyProviders.objects.filter(
            id=company_provider_id,
            provider__kind=src_enums.BrandProviderKind.DLG.value,
            provider__status=src_enums.BrandProviderStatus.ACTIVE.value,
        )
        .select_related("company", "provider")
        .first()
    )
    if not cp:
        logger.warning(
            "{} No active DLG CompanyProviders id={}. Skipping pricing.".format(
                _LOG_PREFIX, company_provider_id,
            )
        )
        return

    creds = dict(cp.credentials or {})
    lf = str(creds.get("local_feed_path") or "").strip()
    if not lf:
        creds["local_feed_path"] = "/tmp/dlg_inventory_company_{}.csv".format(cp.company_id)

    try:
        sftp = dlg_client.DlgSFTPClient(credentials=creds)
    except ValueError as e:
        logger.error(
            "{} company_id={}: {} (SFTP user/password are from settings, not this connection).".format(
                _LOG_PREFIX, cp.company_id, str(e),
            )
        )
        raise

    try:
        local_path = sftp.download_inventory_file(force_download=force_download)
    except dlg_exceptions.DlgException as e:
        logger.error(
            "{} DLG pricing download error company_id={}: {}.".format(
                _LOG_PREFIX, cp.company_id, str(e),
            )
        )
        raise

    records = _records_from_csv(local_path)
    if not records:
        logger.warning("{} DLG pricing file empty: {}.".format(_LOG_PREFIX, local_path))
        return
    n = _upsert_dlg_company_pricing_for_company(cp.company, records)
    logger.info(
        "{} DlgCompanyPricing upserted {} rows for company_id={} (company_provider id={}).".format(
            _LOG_PREFIX, n, cp.company_id, company_provider_id,
        )
    )


def fetch_and_save_dlg_catalog(force_download: bool = False) -> None:
    """
    Download the DLG inventory CSV from the relay (host/path/filename: ``src.constants``; SFTP user/password:
    ``DLG_RELAY_SFTP_USER`` / ``DLG_RELAY_SFTP_PASSWORD`` in settings). ``email_from`` on the primary
    CompanyProvider is metadata (which mailbox DLG uses). Upserts DlgBrand / DlgParts.
    """
    logger.info("{} Starting DLG inventory ingest.".format(_LOG_PREFIX))

    catalog_cp = _primary_dlg_company_provider()
    if not catalog_cp:
        logger.info("{} No active DLG CompanyProviders. Skipping.".format(_LOG_PREFIX))
        return

    # Optional per-flow local path only; never SFTP user/password.
    try:
        sftp = dlg_client.DlgSFTPClient(credentials=catalog_cp.credentials)
    except ValueError as e:
        logger.error("{} {}".format(_LOG_PREFIX, str(e)))
        raise

    try:
        local_path = sftp.download_inventory_file(force_download=force_download)
    except dlg_exceptions.DlgException as e:
        logger.error("{} {}".format(_LOG_PREFIX, str(e)))
        raise

    records = _records_from_csv(local_path)
    if not records:
        logger.warning("{} Inventory file empty or unreadable: {}.".format(_LOG_PREFIX, local_path))
        return

    brand_keys: typing.Set[str] = set()
    for row in records:
        k = _dlg_brand_key(row.get("Brand"))
        if k:
            brand_keys.add(k)

    brand_name_by_key: typing.Dict[str, str] = {}
    for row in records:
        k = _dlg_brand_key(row.get("Brand"))
        raw = row.get("Brand")
        if k and raw is not None and str(raw).strip():
            brand_name_by_key.setdefault(k, str(raw).strip())
    brand_objs = [
        src_models.DlgBrand(
            external_id=n,
            name=brand_name_by_key.get(n, n),
            aaia_code=None,
        )
        for n in sorted(brand_keys)
    ]

    if brand_objs:
        pgbulk.upsert(
            src_models.DlgBrand,
            brand_objs,
            unique_fields=["external_id"],
            update_fields=["name"],
        )
        connection.close()

    brand_by_key = _brand_map_for_names(brand_keys)
    if not brand_by_key:
        logger.warning("{} No DLG brands after upsert.".format(_LOG_PREFIX))
        return

    parts_raw: typing.List[src_models.DlgParts] = []
    for row in records:
        p = _part_from_row(row, brand_by_key)
        if p:
            parts_raw.append(p)

    parts = _dedupe_dlg_parts_for_upsert(parts_raw)
    if len(parts) < len(parts_raw):
        logger.info(
            "{} Deduped {} -> {} DlgParts on (brand, part_number).".format(
                _LOG_PREFIX, len(parts_raw), len(parts),
            )
        )

    if not parts:
        logger.warning("{} No DlgParts to upsert.".format(_LOG_PREFIX))
        return

    total_batches = (len(parts) + DLG_PARTS_UPSERT_BATCH - 1) // DLG_PARTS_UPSERT_BATCH
    logger.info(
        "{} Upserting {} DlgParts in {} batches (batch_size={}).".format(
            _LOG_PREFIX, len(parts), total_batches, DLG_PARTS_UPSERT_BATCH,
        )
    )
    batch_num = 0
    for i in range(0, len(parts), DLG_PARTS_UPSERT_BATCH):
        batch_num += 1
        batch = parts[i : i + DLG_PARTS_UPSERT_BATCH]
        _now = timezone.now()
        for _p in batch:
            _p.updated_at = _now
        pgbulk.upsert(
            src_models.DlgParts,
            batch,
            unique_fields=["part_number", "brand"],
            update_fields=DLG_PARTS_UPDATE_FIELDS,
        )
        connection.close()
        logger.info(
            "{} DlgParts upsert progress: batch {}/{} (through ~{}/{})".format(
                _LOG_PREFIX, batch_num, total_batches, min(i + len(batch), len(parts)), len(parts),
            )
        )
        if i + DLG_PARTS_UPSERT_BATCH < len(parts):
            time.sleep(DLG_PARTS_UPSERT_DELAY)

    logger.info("{} Finished DLG inventory ingest ({} parts).".format(_LOG_PREFIX, len(parts)))

    for cp in _active_dlg_company_providers_queryset():
        try:
            sync_dlg_company_pricing_for_company_provider(cp.id, force_download=force_download)
        except Exception as e:
            logger.error(
                "{} DLG per-company pricing failed after catalog ingest (company_provider_id={}): {}.".format(
                    _LOG_PREFIX,
                    cp.id,
                    str(e),
                )
            )
