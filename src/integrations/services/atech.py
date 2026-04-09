import logging
import re
import time
import typing
from decimal import Decimal

import pandas as pd
import pgbulk
from django.db import connection

from src import enums as src_enums
from src import models as src_models
from src.integrations.clients.atech import client as atech_client
from src.integrations.clients.atech import exceptions as atech_exceptions

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[ATECH-SERVICES]"

_EXCEL_FORMULA_PATTERN = re.compile(r'^="?([^"]*)"?$|^=(\d+(?:\.\d+)?)$')

# Larger batches = fewer DB round-trips on multi-million-row feeds (tune down if OOM on small workers).
ATECH_PARTS_UPSERT_BATCH = 50000
ATECH_PARTS_UPSERT_DELAY = 0.05

ATECH_PRICING_UPSERT_BATCH = 15000

# ``feed_part_number__in`` lookups when building AtechCompanyPricing (max params per query).
ATECH_FEED_KEY_LOOKUP_CHUNK = 15000

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


def _read_atech_feed_dataframe(path: str) -> pd.DataFrame:
    last_error: typing.Optional[Exception] = None
    for encoding in _ATECH_FEED_CSV_ENCODINGS:
        try:
            df = pd.read_csv(path, dtype=object, encoding=encoding, keep_default_na=False)
            if encoding != "utf-8-sig":
                logger.info(
                    "{} Read feed {} with encoding {!r} (utf-8 failed or not used).".format(
                        _LOG_PREFIX, path, encoding
                    )
                )
            return df
        except UnicodeDecodeError as e:
            last_error = e
            continue
    if last_error:
        raise last_error
    raise ValueError("Could not read A-Tech feed at {!r}".format(path))


def _records_from_csv(path: str) -> typing.List[typing.Dict]:
    df = _read_atech_feed_dataframe(path)
    records = df.to_dict(orient="records")
    out: typing.List[typing.Dict] = []
    for row in records:
        norm = _normalize_row_keys(row)
        out.append({
            k: (None if v is None or v == "" or (isinstance(v, float) and pd.isna(v)) else v)
            for k, v in norm.items()
        })
    return out


def _atech_prefix_from_part_number(part_number: typing.Any) -> str:
    """SKU prefix: segment before first '-' (uppercase), else full token uppercase."""
    raw = _clean_csv_value(part_number)
    if not raw:
        return ""
    s = raw.strip().upper()
    if "-" in s:
        return s.split("-", 1)[0].strip()
    return s


def _atech_pricing_map_from_records(
    records: typing.List[typing.Dict],
) -> typing.Dict[str, typing.Dict[str, typing.Any]]:
    """Map feed ``part_number`` (full line) -> price payload; last row per key wins."""
    out: typing.Dict[str, typing.Dict[str, typing.Any]] = {}
    for row in records:
        fn = _clean_csv_value(row.get("part_number"))
        if not fn:
            continue
        key = fn.strip()
        out[key] = {
            "cost": _safe_decimal(row.get("price_atech_current")),
            "retail_price": _safe_decimal(row.get("price_current_month")),
            "jobber_price": _safe_decimal(row.get("cost_current_sheet")),
            "core_charge": _safe_decimal(row.get("cost_core")),
            "fee_hazmat": _safe_decimal(row.get("fee_hazmat")),
            "fee_truck_us": _safe_decimal(row.get("fee_truck_us")),
            "fee_handling_ground": _safe_decimal(row.get("fee_handling_ground")),
            "fee_handling_air": _safe_decimal(row.get("fee_handling_air")),
        }
    return out


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
    Map uppercase prefix -> AtechBrand id from manual AtechPrefixBrand rows.
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


def _dedupe_atech_parts_for_upsert(
    parts: typing.List[src_models.AtechParts],
) -> typing.List[src_models.AtechParts]:
    """One row per ``feed_part_number``; later CSV rows win."""
    by_key: typing.Dict[str, src_models.AtechParts] = {}
    for p in parts:
        sku = (p.feed_part_number or "").strip()
        if not sku:
            continue
        by_key[sku] = p
    return list(by_key.values())


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

    try:
        local_path = sftp.download_feed_file(force_download=force_download)
    except atech_exceptions.AtechException as e:
        logger.error("{} {}".format(_LOG_PREFIX, str(e)))
        raise

    records = _records_from_csv(local_path)
    if not records:
        logger.warning("{} Feed file empty or unreadable: {}.".format(_LOG_PREFIX, local_path))
        return

    parts_raw: typing.List[src_models.AtechParts] = []
    unmapped_prefix_rows = 0
    for row in records:
        prefix = _atech_prefix_from_part_number(row.get("part_number"))
        brand_id = prefix_to_brand.get(prefix) if prefix else None
        if prefix and brand_id is None:
            unmapped_prefix_rows += 1
        p = _part_from_feed_row(row, brand_id, prefix)
        if p:
            parts_raw.append(p)

    if unmapped_prefix_rows:
        logger.info(
            "{} Ingested {} feed rows with no AtechPrefixBrand mapping (brand left null).".format(
                _LOG_PREFIX,
                unmapped_prefix_rows,
            )
        )

    parts = _dedupe_atech_parts_for_upsert(parts_raw)
    if len(parts) < len(parts_raw):
        logger.info(
            "{} Deduped {} -> {} rows on feed_part_number.".format(
                _LOG_PREFIX,
                len(parts_raw),
                len(parts),
            )
        )

    if not parts:
        logger.warning("{} No AtechParts to upsert from primary feed.".format(_LOG_PREFIX))
    else:
        total_batches = (len(parts) + ATECH_PARTS_UPSERT_BATCH - 1) // ATECH_PARTS_UPSERT_BATCH
        logger.info(
            "{} Upserting {} AtechParts in {} batches (batch_size={}); this can take many minutes.".format(
                _LOG_PREFIX,
                len(parts),
                total_batches,
                ATECH_PARTS_UPSERT_BATCH,
            )
        )
        batch_num = 0
        for i in range(0, len(parts), ATECH_PARTS_UPSERT_BATCH):
            batch_num += 1
            batch = parts[i : i + ATECH_PARTS_UPSERT_BATCH]
            pgbulk.upsert(
                src_models.AtechParts,
                batch,
                unique_fields=["feed_part_number"],
                update_fields=ATECH_PARTS_UPDATE_FIELDS,
            )
            connection.close()
            logger.info(
                "{} AtechParts upsert progress: batch {}/{} (through row ~{}/{})".format(
                    _LOG_PREFIX,
                    batch_num,
                    total_batches,
                    min(i + len(batch), len(parts)),
                    len(parts),
                )
            )
            if i + ATECH_PARTS_UPSERT_BATCH < len(parts):
                time.sleep(ATECH_PARTS_UPSERT_DELAY)

        logger.info("{} Finished upserting {} AtechParts from primary feed.".format(_LOG_PREFIX, len(parts)))

    atech_kind = src_enums.BrandProviderKind.ATECH.value
    atech_provider_row = src_models.Providers.objects.filter(kind=atech_kind).first()
    pricing_cps: typing.List[src_models.CompanyProviders] = []
    if atech_provider_row:
        pricing_cps = list(
            _active_atech_company_providers_queryset().filter(provider=atech_provider_row)
        )
    total_company_pricing = 0
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
        total_company_pricing += 1
    if pricing_cps:
        logger.info(
            "{} A-Tech per-company pricing pulls completed for {} company provider(s).".format(
                _LOG_PREFIX,
                total_company_pricing,
            )
        )


def sync_atech_company_pricing_for_company_provider(
    company_provider_id: int,
    force_download: bool = True,
) -> None:
    """
    Download A-Tech feed file for one ``CompanyProviders`` row and upsert ``AtechCompanyPricing``.
    Expects ``AtechParts`` rows to exist for ``part_number`` values in the file (``feed_part_number``).
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
    try:
        company_feed_path = client.download_feed_file(force_download=force_download)
    except atech_exceptions.AtechException as e:
        logger.error(
            "{} A-Tech feed download error company_id={}: {}.".format(_LOG_PREFIX, cp.company_id, str(e)),
        )
        raise

    company_records = _records_from_csv(company_feed_path)
    pmap = _atech_pricing_map_from_records(company_records)
    if not pmap:
        logger.warning(
            "{} No pricing rows parsed for A-Tech company_provider id={}.".format(_LOG_PREFIX, company_provider_id)
        )
        return

    feed_keys = list(pmap.keys())
    id_by_feed: typing.Dict[str, int] = {}
    n_key_chunks = (
        (len(feed_keys) + ATECH_FEED_KEY_LOOKUP_CHUNK - 1) // ATECH_FEED_KEY_LOOKUP_CHUNK
        if feed_keys
        else 0
    )
    chunk_idx = 0
    for i in range(0, len(feed_keys), ATECH_FEED_KEY_LOOKUP_CHUNK):
        chunk_idx += 1
        chunk = feed_keys[i : i + ATECH_FEED_KEY_LOOKUP_CHUNK]
        if not chunk:
            continue
        for row in (
            src_models.AtechParts.objects.filter(feed_part_number__in=chunk)
            .values_list("feed_part_number", "id")
        ):
            fn = (row[0] or "").strip()
            if fn:
                id_by_feed[fn] = int(row[1])
        if n_key_chunks and (chunk_idx % 25 == 0 or chunk_idx == n_key_chunks):
            logger.info(
                "{} AtechCompanyPricing part lookup: chunk {}/{} (company_provider id={}).".format(
                    _LOG_PREFIX, chunk_idx, n_key_chunks, company_provider_id
                )
            )

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
    price_batch_num = 0
    for j in range(0, len(pricing_to_upsert), ATECH_PRICING_UPSERT_BATCH):
        price_batch_num += 1
        batch = pricing_to_upsert[j : j + ATECH_PRICING_UPSERT_BATCH]
        pgbulk.upsert(
            src_models.AtechCompanyPricing,
            batch,
            unique_fields=["part", "company"],
            update_fields=ATECH_COMPANY_PRICING_UPDATE_FIELDS,
            returning=False,
        )
        batch_total += len(batch)
        connection.close()
        if n_price_batches and (price_batch_num % 10 == 0 or price_batch_num == n_price_batches):
            logger.info(
                "{} AtechCompanyPricing upsert progress: {}/{} batches (company_provider id={}).".format(
                    _LOG_PREFIX, price_batch_num, n_price_batches, company_provider_id
                )
            )
    logger.info(
        "{} AtechCompanyPricing upserted {} rows for company_provider id={}.".format(
            _LOG_PREFIX, batch_total, company_provider_id,
        )
    )
