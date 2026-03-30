import logging
import re
import time
import typing
from decimal import Decimal

import pandas as pd
import pgbulk
from django.db import connection, transaction
from django.db.models.functions import Lower, Upper

from src import enums as src_enums
from src import models as src_models
from src.integrations.clients.meyer import client as meyer_client
from src.integrations.clients.meyer import exceptions as meyer_exceptions
from src.integrations.utils.brand_matching import (
    best_fuzzy_brand_match,
    brands_by_first_token_upper,
    normalize_upper_words,
)

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[MEYER-SERVICES]"

_EXCEL_FORMULA_PATTERN = re.compile(r'^="?([^"]*)"?$|^=(\d+(?:\.\d+)?)$')


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


def _meyer_brand_key(value: typing.Any) -> typing.Optional[str]:
    """Canonical key for MeyerBrand.external_id and name (uppercase)."""
    raw = _clean_csv_value(value)
    if not raw:
        return None
    return raw.upper()


def _meyer_brand_name_upper_for_sync(meyer_brand: src_models.MeyerBrand) -> str:
    name_upper = (meyer_brand.name or "").strip().upper()
    if not name_upper:
        name_upper = "BRAND_{}".format(meyer_brand.external_id)
    return name_upper


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


def _truthy(value: typing.Any) -> bool:
    """TRUE/FALSE, Yes/No, 1/0."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return False
    s = str(value).strip().upper()
    return s in ("TRUE", "1", "YES", "Y", "T")


def _records_from_csv(path: str) -> typing.List[typing.Dict]:
    df = pd.read_csv(path, dtype=object, encoding="utf-8-sig", keep_default_na=False)
    records = df.to_dict(orient="records")
    out = []
    for row in records:
        out.append({
            k: (None if v is None or v == "" or (isinstance(v, float) and pd.isna(v)) else v)
            for k, v in row.items()
        })
    return out


def _primary_meyer_company_provider() -> typing.Optional[src_models.CompanyProviders]:
    """Prefer primary=True; otherwise any active Meyer CompanyProviders (same pattern as WheelPros)."""
    base = src_models.CompanyProviders.objects.filter(
        provider__kind=src_enums.BrandProviderKind.MEYER.value,
        provider__status=src_enums.BrandProviderStatus.ACTIVE.value,
    ).select_related("provider")
    cp = base.filter(primary=True).first()
    if cp:
        return cp
    return base.first()


def fetch_and_save_meyer_brands() -> None:
    """Download Meyer Pricing CSV and upsert MeyerBrand rows from unique MFG values."""
    logger.info("{} Fetching Meyer brands from pricing file.".format(_LOG_PREFIX))

    primary_provider = _primary_meyer_company_provider()
    if not primary_provider:
        logger.info("{} No active primary Meyer CompanyProviders row. Skipping.".format(_LOG_PREFIX))
        return

    credentials = primary_provider.credentials
    try:
        sftp = meyer_client.MeyerSFTPClient(credentials=credentials)
    except ValueError as e:
        logger.error("{} {}".format(_LOG_PREFIX, str(e)))
        raise

    try:
        local_path = sftp.download_pricing_file()
    except meyer_exceptions.MeyerException as e:
        logger.error("{} {}".format(_LOG_PREFIX, str(e)))
        raise

    records = _records_from_csv(local_path)
    if not records:
        logger.warning("{} No pricing records.".format(_LOG_PREFIX))
        return

    names = set()
    for row in records:
        mfg = _meyer_brand_key(row.get("MFG"))
        if mfg:
            names.add(mfg)

    brand_instances = [
        src_models.MeyerBrand(external_id=n, name=n, aaia_code=None)
        for n in sorted(names)
    ]
    if not brand_instances:
        return

    pgbulk.upsert(
        src_models.MeyerBrand,
        brand_instances,
        unique_fields=["external_id"],
        update_fields=["name"],
    )
    logger.info("{} Upserted {} Meyer brands.".format(_LOG_PREFIX, len(brand_instances)))


def normalize_brand_match_key(value: typing.Optional[str]) -> str:
    """Normalize for comparison: strip, collapse whitespace, uppercase."""
    if not value:
        return ""
    s = str(value).strip().upper()
    s = re.sub(r"\s+", " ", s)
    return s


# MeyerBrand name or external_id (after normalize_brand_match_key) -> exact catalog ``Brands.name``.
# Fuzzy matching prefers the *longest* catalog name, so FOX-adjacent feed labels would otherwise
# map to FOX POWERSPORTS instead of the parent FOX row unless we pin them here.
_MEYER_UNMAPPED_SYNC_CANONICAL_BRAND: typing.Dict[str, str] = {
    normalize_brand_match_key("FOX POWERSPORTS/FOX FACTORY POWERSPORTS"): "FOX",
    normalize_brand_match_key("FOX SHOCKS"): "FOX",
}


def _meyer_unmapped_sync_canonical_brand_name(
    mb: src_models.MeyerBrand,
) -> typing.Optional[str]:
    for raw in (mb.external_id, mb.name):
        k = normalize_brand_match_key(raw)
        if not k:
            continue
        target = _MEYER_UNMAPPED_SYNC_CANONICAL_BRAND.get(k)
        if target:
            return target
    return None


def _mapping_csv_scalar(value: typing.Any) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    s = str(value).strip()
    if not s or s.lower() == "nan":
        return ""
    return s


def _meyer_brand_lookup_by_normalized_name() -> typing.Dict[str, src_models.MeyerBrand]:
    """Map normalize_brand_match_key(name or external_id) -> MeyerBrand (first wins)."""
    out: typing.Dict[str, src_models.MeyerBrand] = {}
    for mb in src_models.MeyerBrand.objects.only("id", "name", "external_id").iterator():
        k = normalize_brand_match_key(mb.name)
        if k:
            out.setdefault(k, mb)
        ek = normalize_brand_match_key(mb.external_id)
        if ek:
            out.setdefault(ek, mb)
    return out


def apply_meyer_brand_mappings_from_csv(
    file_path: str,
    dry_run: bool = False,
    create_brands_for_no_match: bool = True,
) -> typing.Dict[str, typing.Any]:
    """
    Apply Brand links from a CSV with columns: meyer_brand, matched_brand; optional score, match_type
    (headers case-insensitive; extra columns ignored).

    - Non-empty matched_brand: link to Brands where name matches case-insensitively (match_type ignored).
    - Empty matched_brand: by default create a Brands row (normalized Meyer name) and link; set
      create_brands_for_no_match=False to skip those rows.

    Uses batched queries and bulk_create (not per-row get_or_create) for performance on large CSVs.
    """
    logger.info(
        "{} {}Meyer brand mapping CSV: {}".format(
            _LOG_PREFIX, "[DRY RUN] " if dry_run else "",
            file_path,
        )
    )

    stats = {
        "rows": 0,
        "linked": 0,
        "skipped_no_meyer": 0,
        "skipped_no_catalog_brand": 0,
        "skipped_no_match_row": 0,
        "created_brands": 0,
        "created_mappings": 0,
        "created_brand_providers": 0,
        "created_company_brands": 0,
        "would_mappings": 0,
        "would_bp": 0,
        "would_cb": 0,
        "would_create_brands": 0,
        "new_mappings_brand_provider_reused": 0,
    }

    meyer_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.MEYER.value,
    ).first()
    if not meyer_provider:
        logger.error("{} Meyer provider not found.".format(_LOG_PREFIX))
        return stats

    tick_company = src_models.Company.objects.filter(name="TICK_PERFORMANCE").first()
    if not tick_company:
        logger.error("{} Company TICK_PERFORMANCE not found.".format(_LOG_PREFIX))
        return stats

    df = pd.read_csv(file_path, dtype=object, encoding="utf-8-sig", keep_default_na=True)
    df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]

    if "meyer_brand" not in df.columns or "matched_brand" not in df.columns:
        raise ValueError(
            "CSV must include columns meyer_brand and matched_brand (got {}).".format(
                list(df.columns)
            )
        )
    if "match_type" not in df.columns:
        df["match_type"] = ""

    meyer_by_key = _meyer_brand_lookup_by_normalized_name()

    pending_matched_lower: typing.Set[str] = set()
    pending_new_names: typing.Set[str] = set()

    # Each entry: matched -> ("m", mb, lower_name) | new -> ("n", mb, name_upper)
    work: typing.List[typing.Tuple[str, src_models.MeyerBrand, str]] = []
    no_meyer_samples: typing.List[typing.Tuple[str, str]] = []
    no_catalog_samples: typing.List[typing.Tuple[str, int, str]] = []

    for _, row in df.iterrows():
        stats["rows"] += 1
        raw_meyer = _mapping_csv_scalar(row.get("meyer_brand"))
        raw_matched = _mapping_csv_scalar(row.get("matched_brand"))

        if not raw_meyer:
            continue

        mb_key = normalize_brand_match_key(raw_meyer)
        mb = meyer_by_key.get(mb_key)
        if not mb:
            stats["skipped_no_meyer"] += 1
            if len(no_meyer_samples) < 5:
                no_meyer_samples.append((raw_meyer, mb_key))
            continue

        if raw_matched:
            ln = raw_matched.strip().lower()
            pending_matched_lower.add(ln)
            work.append(("m", mb, ln))
        else:
            if not create_brands_for_no_match:
                stats["skipped_no_match_row"] += 1
                continue
            name_upper = mb_key or "BRAND_{}".format(mb.external_id)
            pending_new_names.add(name_upper)
            work.append(("n", mb, name_upper))

    if stats["skipped_no_meyer"]:
        logger.warning(
            "{} CSV rows with no MeyerBrand match: {} (sample meyer_brand / normalized: {}).".format(
                _LOG_PREFIX,
                stats["skipped_no_meyer"],
                no_meyer_samples,
            )
        )

    brands_by_lower: typing.Dict[str, src_models.Brands] = {}
    if pending_matched_lower:
        for b in (
            src_models.Brands.objects.annotate(lname=Lower("name"))
            .filter(lname__in=pending_matched_lower)
            .order_by("id")
        ):
            if b.lname not in brands_by_lower:
                brands_by_lower[b.lname] = b

    brands_by_name: typing.Dict[str, src_models.Brands] = {}
    if pending_new_names and not dry_run:
        existing_list = list(
            src_models.Brands.objects.filter(name__in=list(pending_new_names))
        )
        brands_by_name = {b.name: b for b in existing_list}
        to_create = [
            src_models.Brands(
                name=n,
                status=src_enums.BrandProviderStatus.ACTIVE.value,
                status_name=src_enums.BrandProviderStatus.ACTIVE.name,
                aaia_code=None,
            )
            for n in pending_new_names
            if n not in brands_by_name
        ]
        if to_create:
            src_models.Brands.objects.bulk_create(to_create)
            stats["created_brands"] = len(to_create)
        brands_by_name = {
            b.name: b
            for b in src_models.Brands.objects.filter(name__in=list(pending_new_names))
        }

    resolved: typing.List[typing.Dict[str, typing.Any]] = []
    for mode, mb, arg in work:
        if mode == "m":
            ln = arg
            b = brands_by_lower.get(ln)
            if not b:
                stats["skipped_no_catalog_brand"] += 1
                if len(no_catalog_samples) < 5:
                    no_catalog_samples.append((ln, mb.id, mb.name))
                continue
            resolved.append(
                {
                    "brand_id": b.id,
                    "meyer_brand_id": mb.id,
                    "dry_new": False,
                }
            )
        else:
            name_upper = arg
            if dry_run:
                # Match legacy behavior: do not resolve Brands for no-match rows in dry run;
                # always count would_create_brands / would_mappings / would_bp / would_cb per row.
                resolved.append(
                    {
                        "brand_id": None,
                        "meyer_brand_id": mb.id,
                        "dry_new": True,
                    }
                )
            else:
                b = brands_by_name[name_upper]
                resolved.append(
                    {
                        "brand_id": b.id,
                        "meyer_brand_id": mb.id,
                        "dry_new": False,
                    }
                )

    if stats["skipped_no_catalog_brand"] and no_catalog_samples:
        logger.warning(
            "{} CSV rows with matched_brand not in catalog: {} (sample lower / MeyerBrand id / name: {}).".format(
                _LOG_PREFIX,
                stats["skipped_no_catalog_brand"],
                no_catalog_samples,
            )
        )

    meyer_ids_for_exists = {entry["meyer_brand_id"] for entry in resolved}
    brand_ids_for_exists = {
        entry["brand_id"] for entry in resolved if entry["brand_id"] is not None
    }

    existing_pairs: typing.Set[typing.Tuple[int, int]] = set()
    if meyer_ids_for_exists:
        existing_pairs = set(
            src_models.BrandMeyerBrandMapping.objects.filter(
                meyer_brand_id__in=meyer_ids_for_exists,
            ).values_list("brand_id", "meyer_brand_id")
        )

    have_bp_before: typing.Set[int] = set()
    have_cb_before: typing.Set[int] = set()
    if brand_ids_for_exists:
        have_bp_before = set(
            src_models.BrandProviders.objects.filter(
                provider_id=meyer_provider.id,
                brand_id__in=brand_ids_for_exists,
            ).values_list("brand_id", flat=True)
        )
        have_cb_before = set(
            src_models.CompanyBrands.objects.filter(
                company_id=tick_company.id,
                brand_id__in=brand_ids_for_exists,
            ).values_list("brand_id", flat=True)
        )

    if dry_run:
        sim_pairs = set(existing_pairs)
        sim_bp = set(have_bp_before)
        sim_cb = set(have_cb_before)
        for entry in resolved:
            if entry["dry_new"]:
                stats["would_create_brands"] += 1
                stats["would_mappings"] += 1
                stats["would_bp"] += 1
                stats["would_cb"] += 1
                continue
            bid = entry["brand_id"]
            mid = entry["meyer_brand_id"]
            assert bid is not None
            if (bid, mid) not in sim_pairs:
                stats["would_mappings"] += 1
                sim_pairs.add((bid, mid))
            if bid not in sim_bp:
                stats["would_bp"] += 1
                sim_bp.add(bid)
            if bid not in sim_cb:
                stats["would_cb"] += 1
                sim_cb.add(bid)
        logger.info("{} CSV mapping finished. Stats: {}.".format(_LOG_PREFIX, stats))
        return stats

    link_pairs: typing.List[typing.Tuple[int, int]] = []
    for entry in resolved:
        bid = entry["brand_id"]
        mid = entry["meyer_brand_id"]
        assert bid is not None
        link_pairs.append((bid, mid))

    stats["linked"] = len(link_pairs)
    if not link_pairs:
        logger.info("{} CSV mapping finished. Stats: {}.".format(_LOG_PREFIX, stats))
        return stats

    to_map = [(b, m) for b, m in link_pairs if (b, m) not in existing_pairs]
    stats["created_mappings"] = len(to_map)
    stats["new_mappings_brand_provider_reused"] = sum(
        1 for b, m in to_map if b in have_bp_before
    )

    all_brand_ids = {b for b, _ in link_pairs}
    bp_needed = [bid for bid in all_brand_ids if bid not in have_bp_before]
    cb_needed = [bid for bid in all_brand_ids if bid not in have_cb_before]

    mapping_objs = [
        src_models.BrandMeyerBrandMapping(brand_id=bid, meyer_brand_id=mid)
        for bid, mid in to_map
    ]
    bp_objs = [
        src_models.BrandProviders(brand_id=bid, provider=meyer_provider)
        for bid in bp_needed
    ]
    cb_objs = [
        src_models.CompanyBrands(
            company_id=tick_company.id,
            brand_id=bid,
            status=src_enums.CompanyBrandStatus.ACTIVE.value,
            status_name=src_enums.CompanyBrandStatus.ACTIVE.name,
        )
        for bid in cb_needed
    ]

    with transaction.atomic():
        if mapping_objs:
            src_models.BrandMeyerBrandMapping.objects.bulk_create(
                mapping_objs, ignore_conflicts=True
            )
        if bp_objs:
            src_models.BrandProviders.objects.bulk_create(bp_objs, ignore_conflicts=True)
            stats["created_brand_providers"] = len(bp_objs)
        if cb_objs:
            src_models.CompanyBrands.objects.bulk_create(cb_objs, ignore_conflicts=True)
            stats["created_company_brands"] = len(cb_objs)

    if stats["created_mappings"] and stats["new_mappings_brand_provider_reused"]:
        logger.info(
            "{} Note: {} new BrandMeyerBrandMapping row(s) did not add BrandProviders because "
            "that catalog Brand already had BrandProviders for Meyer (multiple MeyerBrand rows → one Brand).".format(
                _LOG_PREFIX,
                stats["new_mappings_brand_provider_reused"],
            )
        )
    logger.info("{} CSV mapping finished. Stats: {}.".format(_LOG_PREFIX, stats))
    return stats


def sync_unmapped_meyer_brands_to_brands(dry_run: bool = False) -> typing.List[src_models.MeyerBrand]:
    """
    For each MeyerBrand without BrandMeyerBrandMapping: resolve Brand by exact name (uppercase),
    then fuzzy word-prefix match (shared util with WheelPros / Keystone / Rough Country); otherwise create.
    Upserts mapping, BrandProviders, CompanyBrands for TICK_PERFORMANCE.

    If dry_run is True, no database writes; logs only Meyer brands that matched an existing Brand (exact or fuzzy).
    """
    logger.info(
        "{} Syncing unmapped Meyer brands to Brands{}.".format(
            _LOG_PREFIX,
            " (dry run)" if dry_run else "",
        )
    )

    meyer_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.MEYER.value,
    ).first()
    if not meyer_provider:
        logger.warning("{} Meyer provider not found.".format(_LOG_PREFIX))
        return []

    tick_company = src_models.Company.objects.filter(name="TICK_PERFORMANCE").first()
    if not tick_company:
        logger.warning("{} Company TICK_PERFORMANCE not found. Skipping.".format(_LOG_PREFIX))
        return []

    if not dry_run:
        _, cp_created = src_models.CompanyProviders.objects.get_or_create(
            company=tick_company,
            provider=meyer_provider,
            defaults={"credentials": {}, "primary": False},
        )
        if cp_created:
            logger.info("{} Created CompanyProviders for TICK_PERFORMANCE + Meyer.".format(_LOG_PREFIX))

    mapped_ids = set(
        src_models.BrandMeyerBrandMapping.objects.values_list("meyer_brand_id", flat=True).distinct()
    )
    unmapped = list(
        src_models.MeyerBrand.objects.exclude(id__in=mapped_ids).order_by("id")
    )
    if not unmapped:
        logger.info("{} No unmapped Meyer brands.".format(_LOG_PREFIX))
        return []

    name_upper_keys: typing.Set[str] = set()
    for mb in unmapped:
        if (mb.name or "").strip():
            name_upper_keys.add((mb.name or "").strip().upper())

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
    for mb in unmapped:
        t = _meyer_unmapped_sync_canonical_brand_name(mb)
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

    resolved_by_meyer_id: typing.Dict[int, src_models.Brands] = {}
    canonical_matched_ids: typing.Set[int] = set()
    exact_matched_ids: typing.Set[int] = set()
    for mb in sorted(unmapped, key=lambda x: x.id):
        canon_name = _meyer_unmapped_sync_canonical_brand_name(mb)
        if canon_name:
            b = brands_by_upper_for_canonical.get(canon_name.strip().upper())
            if b:
                resolved_by_meyer_id[mb.id] = b
                canonical_matched_ids.add(mb.id)
            else:
                logger.warning(
                    "{} Meyer canonical brand override {!r} -> {!r} but no Brands row with that name.".format(
                        _LOG_PREFIX,
                        mb.name or mb.external_id,
                        canon_name,
                    )
                )

    for mb in sorted(unmapped, key=lambda x: x.id):
        if mb.id in resolved_by_meyer_id:
            continue
        nm = (mb.name or "").strip().upper()
        if nm:
            brand = brands_by_upper_name.get(nm)
            if brand:
                resolved_by_meyer_id[mb.id] = brand
                exact_matched_ids.add(mb.id)

    unresolved_after_exact = [mb for mb in unmapped if mb.id not in resolved_by_meyer_id]
    brands_first_index = brands_by_first_token_upper() if unresolved_after_exact else {}
    all_brands_fallback: typing.Optional[typing.List[src_models.Brands]] = None
    fuzzy_matches = 0
    fuzzy_matched_ids: typing.Set[int] = set()
    for mb in unresolved_after_exact:
        parts = normalize_upper_words(mb.name or "").split()
        candidates: typing.List[src_models.Brands] = []
        if parts:
            candidates = list(brands_first_index.get(parts[0], ()))
        if not candidates:
            if all_brands_fallback is None:
                all_brands_fallback = list(
                    src_models.Brands.objects.only("id", "name", "aaia_code").order_by("id")
                )
            candidates = all_brands_fallback
        brand = best_fuzzy_brand_match(mb.name or "", candidates)
        if brand:
            resolved_by_meyer_id[mb.id] = brand
            fuzzy_matched_ids.add(mb.id)
            fuzzy_matches += 1
            if not dry_run:
                logger.debug(
                    "{} Fuzzy-matched Meyer brand {!r} to Brand id={} name={!r}.".format(
                        _LOG_PREFIX,
                        mb.name,
                        brand.id,
                        brand.name,
                    )
                )

    if dry_run:
        for mb in sorted(unmapped, key=lambda x: x.id):
            if mb.id not in resolved_by_meyer_id:
                continue
            brand = resolved_by_meyer_id[mb.id]
            if mb.id in canonical_matched_ids:
                how = "canonical_override"
            elif mb.id in exact_matched_ids:
                how = "exact"
            else:
                how = "fuzzy"
            logger.info(
                "{} [dry-run] match ({}) MeyerBrand id={} external_id={!r} name={!r} "
                "-> Brand id={} name={!r}".format(
                    _LOG_PREFIX,
                    how,
                    mb.id,
                    mb.external_id,
                    mb.name,
                    brand.id,
                    brand.name,
                )
            )
        would_create = [mb for mb in unmapped if mb.id not in resolved_by_meyer_id]
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
    for mb in sorted(unmapped, key=lambda x: x.id):
        if mb.id in resolved_by_meyer_id:
            continue
        new_brand_specs.add(_meyer_brand_name_upper_for_sync(mb))

    created_brands = 0
    if new_brand_specs:
        existing_names = set(
            src_models.Brands.objects.filter(name__in=list(new_brand_specs)).values_list("name", flat=True)
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
        for mb in unmapped:
            if mb.id not in resolved_by_meyer_id:
                nu = _meyer_brand_name_upper_for_sync(mb)
                resolved_by_meyer_id[mb.id] = by_name[nu]

    mapping_models = [
        src_models.BrandMeyerBrandMapping(
            brand_id=resolved_by_meyer_id[mb.id].id,
            meyer_brand_id=mb.id,
        )
        for mb in unmapped
    ]
    try:
        pgbulk.upsert(
            src_models.BrandMeyerBrandMapping,
            mapping_models,
            unique_fields=["brand", "meyer_brand"],
            update_fields=[],
            returning=False,
        )
    except Exception as e:
        logger.error("{} Error upserting BrandMeyerBrandMapping: {}.".format(_LOG_PREFIX, str(e)))
        raise

    created_bp = 0
    created_cb = 0
    for mb in unmapped:
        brand = resolved_by_meyer_id[mb.id]
        _, bpc = src_models.BrandProviders.objects.get_or_create(
            brand=brand,
            provider=meyer_provider,
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
        "{} Meyer brand sync done. Canonical overrides: {}, brands created: {}, fuzzy name matches: {}, "
        "BrandMeyerBrandMapping upserted: {}, BrandProviders: {}, CompanyBrands: {}.".format(
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


PRICING_UPDATE_FIELDS = [
    "mfg_item_number",
    "description",
    "jobber_price",
    "cost",
    "core_charge",
    "upc",
    "map_price",
    "length",
    "width",
    "height",
    "weight",
    "category",
    "sub_category",
    "is_ltl",
    "is_discontinued",
    "is_oversize",
    "addtl_handling_charge",
    "raw_data",
]

INVENTORY_UPDATE_FIELDS = [
    "available_qty",
    "mfg_qty_available",
    "inventory_ltl",
    "is_stocking",
    "is_special_order",
    "is_oversize",
    "addtl_handling_charge",
    "mfg_item_number",
]


def _brand_map_for_names(names: typing.Collection[str]) -> typing.Dict[str, src_models.MeyerBrand]:
    brands = src_models.MeyerBrand.objects.filter(external_id__in=list(names))
    return {b.external_id: b for b in brands}


def _part_from_pricing_row(
    row: typing.Dict,
    brand_by_mfg: typing.Dict[str, src_models.MeyerBrand],
) -> typing.Optional[src_models.MeyerParts]:
    mfg = _meyer_brand_key(row.get("MFG"))
    meyer_part = _clean_csv_value(row.get("Meyer Part"))
    if not mfg or not meyer_part:
        return None
    brand = brand_by_mfg.get(mfg)
    if not brand:
        return None
    return src_models.MeyerParts(
        brand=brand,
        meyer_part=meyer_part,
        mfg_item_number=_clean_csv_value(row.get("MFG Item Number")),
        description=_clean_csv_value(row.get("Description")),
        jobber_price=_safe_decimal(row.get("Jobber Price")),
        cost=_safe_decimal(row.get("Your Price")),
        core_charge=_safe_decimal(row.get("Core-Charge")),
        upc=_clean_csv_value(row.get("UPC")),
        map_price=_safe_decimal(row.get("MAP")),
        length=_safe_decimal(row.get("LENGTH")),
        width=_safe_decimal(row.get("WIDTH")),
        height=_safe_decimal(row.get("HEIGHT")),
        weight=_safe_decimal(row.get("WEIGHT")),
        category=_clean_csv_value(row.get("Category")),
        sub_category=_clean_csv_value(row.get("Sub-Category")),
        is_ltl=_truthy(row.get("LTL")),
        is_discontinued=_truthy(row.get("Discontinued")),
        is_oversize=_truthy(row.get("Oversize")),
        addtl_handling_charge=_truthy(row.get("Addtl Handling Charge")),
        raw_data={k: row.get(k) for k in row.keys()},
    )


def _part_from_inventory_row(
    row: typing.Dict,
    brand_by_mfg: typing.Dict[str, src_models.MeyerBrand],
) -> typing.Optional[src_models.MeyerParts]:
    mfg = _meyer_brand_key(row.get("MFGName"))
    meyer_part = _clean_csv_value(row.get("Item Number"))
    if not mfg or not meyer_part:
        return None
    brand = brand_by_mfg.get(mfg)
    if not brand:
        return None
    return src_models.MeyerParts(
        brand=brand,
        meyer_part=meyer_part,
        mfg_item_number=_clean_csv_value(row.get("MFG Item Number")),
        available_qty=_safe_decimal(row.get("Available")),
        mfg_qty_available=_safe_int(row.get("MFG Qty Available")),
        inventory_ltl=_safe_int(row.get("LTL")),
        is_stocking=_truthy(row.get("Stocking")),
        is_special_order=_truthy(row.get("Special Order")),
        is_oversize=_truthy(row.get("Oversize")),
        addtl_handling_charge=_truthy(row.get("Addtl Handling Charge")),
    )


def _dedupe_meyer_parts_for_upsert(
    parts: typing.List[src_models.MeyerParts],
) -> typing.List[src_models.MeyerParts]:
    """
    One row per (brand_id, meyer_part) so a single INSERT ... ON CONFLICT batch
    never targets the same unique constraint twice (PostgreSQL CardinalityViolation).
    Later CSV rows win.
    """
    by_key: typing.Dict[typing.Tuple[int, str], src_models.MeyerParts] = {}
    for p in parts:
        bid = p.brand_id
        if bid is None and p.brand is not None:
            bid = p.brand.pk
        sku = (p.meyer_part or "").strip()
        if not bid or not sku:
            continue
        by_key[(int(bid), sku)] = p
    return list(by_key.values())


def fetch_and_save_meyer_catalog_and_inventory(force_download: bool = False) -> None:
    """
    Download Meyer Pricing + Meyer Inventory from SFTP, upsert brands, parts (pricing),
    then overlay availability from inventory (same Meyer Part / Item Number + MFG).
    """
    logger.info("{} Starting Meyer pricing + inventory ingest.".format(_LOG_PREFIX))

    primary_provider = _primary_meyer_company_provider()
    if not primary_provider:
        logger.info("{} No active primary Meyer CompanyProviders. Skipping.".format(_LOG_PREFIX))
        return

    credentials = primary_provider.credentials
    try:
        sftp = meyer_client.MeyerSFTPClient(credentials=credentials)
    except ValueError as e:
        logger.error("{} {}".format(_LOG_PREFIX, str(e)))
        raise

    try:
        pricing_path = sftp.download_pricing_file(force_download=force_download)
        inventory_path = sftp.download_inventory_file(force_download=force_download)
    except meyer_exceptions.MeyerException as e:
        logger.error("{} {}".format(_LOG_PREFIX, str(e)))
        raise

    pricing_records = _records_from_csv(pricing_path)
    inventory_records = _records_from_csv(inventory_path)

    if not pricing_records and not inventory_records:
        logger.warning("{} Both feeds empty.".format(_LOG_PREFIX))
        return

    mfg_names = set()
    for row in pricing_records:
        m = _meyer_brand_key(row.get("MFG"))
        if m:
            mfg_names.add(m)
    for row in inventory_records:
        m = _meyer_brand_key(row.get("MFGName"))
        if m:
            mfg_names.add(m)

    brand_objs = [
        src_models.MeyerBrand(external_id=n, name=n, aaia_code=None)
        for n in sorted(mfg_names)
    ]
    if brand_objs:
        pgbulk.upsert(
            src_models.MeyerBrand,
            brand_objs,
            unique_fields=["external_id"],
            update_fields=["name"],
        )

    brand_by_mfg = _brand_map_for_names(mfg_names)
    if not brand_by_mfg:
        logger.warning("{} No Meyer brands after upsert.".format(_LOG_PREFIX))
        return

    BATCH = 10000
    DELAY = 0.3

    if pricing_records:
        parts_raw: typing.List[src_models.MeyerParts] = []
        for row in pricing_records:
            p = _part_from_pricing_row(row, brand_by_mfg)
            if p:
                parts_raw.append(p)
        parts = _dedupe_meyer_parts_for_upsert(parts_raw)
        if len(parts) < len(parts_raw):
            logger.info(
                "{} Pricing: {} duplicate (brand, meyer_part) rows in feed; kept {}.".format(
                    _LOG_PREFIX, len(parts_raw) - len(parts), len(parts),
                )
            )
        for i in range(0, len(parts), BATCH):
            batch = parts[i : i + BATCH]
            pgbulk.upsert(
                src_models.MeyerParts,
                batch,
                unique_fields=["meyer_part", "brand"],
                update_fields=PRICING_UPDATE_FIELDS,
            )
            connection.close()
            if i + BATCH < len(parts):
                time.sleep(DELAY)
        logger.info("{} Upserted {} rows from pricing.".format(_LOG_PREFIX, len(parts)))

    if inventory_records:
        overlay_raw: typing.List[src_models.MeyerParts] = []
        missing_brands = set()
        for row in inventory_records:
            mfg = _meyer_brand_key(row.get("MFGName"))
            if mfg and mfg not in brand_by_mfg:
                missing_brands.add(mfg)
        if missing_brands:
            extra = [
                src_models.MeyerBrand(external_id=n, name=n, aaia_code=None)
                for n in sorted(missing_brands)
            ]
            pgbulk.upsert(
                src_models.MeyerBrand,
                extra,
                unique_fields=["external_id"],
                update_fields=["name"],
            )
            brand_by_mfg.update(_brand_map_for_names(missing_brands))

        for row in inventory_records:
            p = _part_from_inventory_row(row, brand_by_mfg)
            if p:
                overlay_raw.append(p)

        overlay = _dedupe_meyer_parts_for_upsert(overlay_raw)
        if len(overlay) < len(overlay_raw):
            logger.info(
                "{} Inventory: {} duplicate (brand, meyer_part) rows in feed; kept {}.".format(
                    _LOG_PREFIX, len(overlay_raw) - len(overlay), len(overlay),
                )
            )

        for i in range(0, len(overlay), BATCH):
            batch = overlay[i : i + BATCH]
            pgbulk.upsert(
                src_models.MeyerParts,
                batch,
                unique_fields=["meyer_part", "brand"],
                update_fields=INVENTORY_UPDATE_FIELDS,
            )
            connection.close()
            if i + BATCH < len(overlay):
                time.sleep(DELAY)
        logger.info("{} Upserted {} inventory overlays.".format(_LOG_PREFIX, len(overlay)))

    logger.info("{} Meyer ingest complete.".format(_LOG_PREFIX))
