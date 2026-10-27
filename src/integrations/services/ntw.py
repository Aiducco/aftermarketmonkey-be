"""
NTW (TBC Corporation) feed ingest: MOVEit CSV -> NtwBrand / NtwParts / NtwCompanyPricing.

The file is **headerless**, 14 positional columns. See NtwParts' docstring in src/models.py for
the column table; the constants below are the single place those positions are named.

Two properties of this feed drive everything here, both measured against the first live file
(DELTI_0037746990_20261026230053.csv, 3,533 rows, 2026-10-27):

**It lists in-stock items only.** Not one row carried a zero quantity. A tire that sells out does
not arrive as qty 0 -- it stops appearing. An upsert alone would therefore leave a sold-out tire
showing its last known quantity forever, which is the exact failure we warned NTW about in the
August integration questionnaire. ``zero_out_absent_ntw_parts`` is the reconciliation pass that
closes it, and it is safe precisely because each file is complete for that account: anything
missing is missing because it is gone, not because the file is partial.

**It is one dealer's view of one warehouse.** Column 2 was a single location id across every row.
So quantity and cost are per-account facts, which is why NtwCompanyPricing carries quantity as
well as cost -- unusual among the per-company pricing tables, and explained there.

Sizes are never taken from columns 7-9. They are positional and change meaning with notation (for
"11X4.00-5" column 7 is an overall diameter, not a width), so ``src.domain.tire_size`` parses
column 4 instead. Measured coverage on the live file: 96.4% of rows parse; the remainder are
commercial truck sizes like "11R22.5", farm sizes like "9.5L-15", and the newer "HL" load prefix,
which that parser does not yet handle. Those rows still ingest -- they get catalog, cost and
quantity like any other, and only their TireSpec stays empty.
"""
import csv
import io
import logging
import sys
import typing
from decimal import Decimal, InvalidOperation

import pgbulk
from django.db import connection
from django.db.models.functions import Upper
from django.utils import timezone

from src import enums as src_enums
from src import models as src_models
from src.integrations import credentials as credentials_helper
from src.integrations.clients.ntw import exceptions as ntw_exceptions
from src.integrations.clients.ntw import sftp_client as ntw_client
from src.integrations.services import tire_reparse
from src.integrations.utils.brand_matching import (
    best_fuzzy_brand_match,
    brands_by_compact_key,
    brands_by_first_token_upper,
    normalize_compact_key,
    normalize_upper_words,
)

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[NTW-SERVICES]"

# Positional columns of the headerless feed (0-indexed).
COL_ACCOUNT_NUMBER = 0
COL_LOCATION_ID = 1
COL_DESCRIPTION = 2
COL_SIZE_DESCRIPTION = 3
COL_QUANTITY = 4
COL_CATEGORY = 5
COL_SIZE_COMPONENT_1 = 6
COL_SIZE_COMPONENT_2 = 7
COL_SIZE_COMPONENT_3 = 8
COL_COST = 9
COL_MODEL_NAME = 10
COL_BRAND_NAME = 11
COL_PART_NUMBER = 12
COL_PRODUCT_ID = 13
_EXPECTED_COLUMNS = 14

_NTW_PARTS_UPSERT_BATCH = 5000
_NTW_ZERO_OUT_CHUNK = 5000
_NTW_CSV_ENCODINGS = ("utf-8-sig", "utf-8", "cp1252", "latin-1")

# Feed brand label -> the Brands.name we want it to land on, for cases the matching cascade below
# cannot be trusted to get right on its own. Deliberately empty: every one of the 29 brands in the
# first live file is a plain manufacturer name, and inventing mappings without checking them
# against the live Brands table is how a feed ends up silently attached to the wrong brand. Add
# entries here only with the evidence that made them necessary, as TireRack's equivalent does.
_NTW_UNMAPPED_SYNC_CANONICAL_BRAND: typing.Dict[str, str] = {}

# Feed brand labels where the fuzzy phase finds a confident-looking match that is WRONG, so it is
# skipped and a new Brands row is created instead. Each entry is here because the candidate's
# actual parts were checked, not because the name looked odd:
#
#   Advance    -> ADVANCE MANUFACTURING LIMITED (aaia FDNP). Its 39 parts are "CustomFlow
#                 Louvered Tailgate" and similar -- a truck accessory maker, not the Advance
#                 tire brand.
#   Crosswind  -> CRO (aaia A6ND). A line-code brand whose 83 parts are described only as
#                 "CRO <number>". Nothing identifies them as Crosswind, and unverifiable is not
#                 a match.
#   Multi Mile -> MUL (aaia A6RH). Same line-code shape, and its part numbers (TR-1, MHC-48,
#                 ST-32, BBHC-16) are hitch/towing-shaped rather than tire sizes.
#
# Attaching a tire feed to the wrong brand is worse than creating a duplicate: it puts NTW's cost
# and stock onto another manufacturer's parts, and search would then serve them as that brand.
# Revisit an entry only by looking at the candidate's parts again, not by re-reading the name.
_NTW_FUZZY_MATCH_BLOCKLIST = frozenset({"ADVANCE", "CROSSWIND", "MULTI MILE"})


_maxint = sys.maxsize
while True:
    try:
        csv.field_size_limit(_maxint)
        break
    except OverflowError:
        _maxint = int(_maxint / 10)


# -- parsing -----------------------------------------------------------------


def _clean(value: typing.Any) -> typing.Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _safe_int(value: typing.Any) -> typing.Optional[int]:
    """
    Quantities arrive decimal-formatted ("4.000") but were integral in every one of the 3,533
    live rows, so they are truncated to int rather than carried as Decimal. A fractional value
    would mean something changed upstream; it floors rather than raising, because a feed that
    starts shipping halves should degrade to a usable number instead of failing the whole ingest.
    """
    text = _clean(value)
    if text is None:
        return None
    try:
        return int(Decimal(text))
    except (InvalidOperation, ValueError, TypeError):
        return None


def _safe_decimal(value: typing.Any) -> typing.Optional[Decimal]:
    text = _clean(value)
    if text is None:
        return None
    try:
        return Decimal(text)
    except (InvalidOperation, ValueError, TypeError):
        return None


def _decode(content: bytes) -> str:
    for encoding in _NTW_CSV_ENCODINGS:
        try:
            return content.decode(encoding)
        except UnicodeDecodeError:
            continue
    return content.decode("utf-8", "replace")


def parse_feed_content(content: bytes) -> typing.List[typing.Dict[str, typing.Any]]:
    """
    Decode one feed file into dicts. Rows whose column count is not 14 are skipped and counted --
    every live row had exactly 14, so a different width means the layout changed and we would
    rather drop those rows loudly than map them by position into the wrong fields.
    """
    text = _decode(content)
    rows: typing.List[typing.Dict[str, typing.Any]] = []
    malformed = 0
    for raw in csv.reader(io.StringIO(text, newline="")):
        if not raw or not any(cell.strip() for cell in raw):
            continue
        if len(raw) != _EXPECTED_COLUMNS:
            malformed += 1
            continue
        brand_name = _clean(raw[COL_BRAND_NAME])
        part_number = _clean(raw[COL_PART_NUMBER])
        if not brand_name or not part_number:
            malformed += 1
            continue
        rows.append(
            {
                "account_number": _clean(raw[COL_ACCOUNT_NUMBER]),
                "location_id": _clean(raw[COL_LOCATION_ID]),
                "description": _clean(raw[COL_DESCRIPTION]),
                "size_description": _clean(raw[COL_SIZE_DESCRIPTION]),
                "quantity": _safe_int(raw[COL_QUANTITY]),
                "category": _clean(raw[COL_CATEGORY]),
                "size_component_1": _clean(raw[COL_SIZE_COMPONENT_1]),
                "size_component_2": _clean(raw[COL_SIZE_COMPONENT_2]),
                "size_component_3": _clean(raw[COL_SIZE_COMPONENT_3]),
                "cost": _safe_decimal(raw[COL_COST]),
                "model_name": _clean(raw[COL_MODEL_NAME]),
                "brand_name": brand_name,
                "part_number": part_number,
                "product_id": _clean(raw[COL_PRODUCT_ID]),
            }
        )
    if malformed:
        logger.warning(
            "{} Skipped {} malformed feed rows (expected {} columns).".format(
                _LOG_PREFIX, malformed, _EXPECTED_COLUMNS
            )
        )
    return rows


def _json_safe_row(row: typing.Dict) -> typing.Dict[str, typing.Any]:
    """
    Stringify a parsed row for ``NtwParts.raw_data``. The parse step types two columns -- an int
    quantity and a Decimal cost -- and Django's plain JSONField cannot serialise a Decimal, so
    writing the dict straight in raises ``TypeError: Object of type Decimal is not JSON
    serializable`` at upsert time. Same helper, same reason, as TireRack's.

    Strings rather than an encoder on the field: raw_data exists to let us re-parse a feed
    without re-fetching it, and the feed itself is all text, so text is the honest shape to keep
    it in.
    """
    return {
        (k if isinstance(k, str) else str(k)): (None if v is None else str(v))
        for k, v in row.items()
    }


def _brand_key(value: typing.Any) -> typing.Optional[str]:
    text = _clean(value)
    return text.upper() if text else None


# -- connections -------------------------------------------------------------


def _active_ntw_company_providers_queryset():
    """
    Deliberately filters on the PROVIDER's status, not the connection's: a connection is
    INGESTING until its first pricing sync completes, so keying off
    CompanyProviderConnectionStatus.CONNECTED here would mean the very first catalog ingest for a
    newly onboarded dealer could never run. Same filter TireRack and DLG use.
    """
    return src_models.CompanyProviders.objects.filter(
        provider__kind=src_enums.BrandProviderKind.NTW.value,
        provider__status=src_enums.BrandProviderStatus.ACTIVE.value,
    ).select_related("company", "provider")


def _primary_ntw_company_provider() -> typing.Optional[src_models.CompanyProviders]:
    """
    The connection whose feed fills the shared catalog: the one flagged ``primary``, else the
    first. Matches _primary_tirerack_company_provider so the shared catalog does not shuffle
    between runs.
    """
    base = _active_ntw_company_providers_queryset()
    cp = base.filter(primary=True).first()
    if cp:
        return cp
    return base.first()


def _client_for(company_provider: src_models.CompanyProviders) -> ntw_client.NtwSFTPClient:
    return ntw_client.NtwSFTPClient(
        credentials=credentials_helper.get_feed_credentials(company_provider)
    )


# -- brand mapping -----------------------------------------------------------


def sync_unmapped_ntw_brands_to_brands(dry_run: bool = False) -> typing.List[src_models.NtwBrand]:
    """
    For each NtwBrand without BrandNtwBrandMapping: canonical override, then exact match on
    upper(Brands.name), then compact-key, then fuzzy word-prefix -- the same cascade order as
    TireRack's and DLG's, reusing src.integrations.utils.brand_matching. Anything still unmatched
    gets a new Brands row rather than being guessed into an existing one.

    Like TireRack this does not touch CompanyBrands: NtwBrand -> Brands plus BrandProviders is
    enough, since the catalog is platform-wide once a company is connected.
    """
    logger.info(
        "{} Syncing unmapped NTW brands to Brands{}.".format(
            _LOG_PREFIX, " (dry run)" if dry_run else ""
        )
    )

    ntw_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.NTW.value,
    ).first()
    if not ntw_provider:
        logger.warning("{} NTW provider not found.".format(_LOG_PREFIX))
        return []

    mapped_ids = set(
        src_models.BrandNtwBrandMapping.objects.values_list("ntw_brand_id", flat=True).distinct()
    )
    unmapped = list(src_models.NtwBrand.objects.exclude(id__in=mapped_ids).order_by("id"))
    if not unmapped:
        logger.info("{} No unmapped NTW brands.".format(_LOG_PREFIX))
        return []

    name_upper_keys = {(nb.name or "").strip().upper() for nb in unmapped if (nb.name or "").strip()}
    brands_by_upper_name: typing.Dict[str, src_models.Brands] = {}
    if name_upper_keys:
        for b in (
            src_models.Brands.objects.annotate(_name_u=Upper("name"))
            .filter(_name_u__in=name_upper_keys)
            .order_by("id")
        ):
            brands_by_upper_name.setdefault((b.name or "").strip().upper(), b)

    resolved_by_id: typing.Dict[int, src_models.Brands] = {}
    canonical_matched: typing.Set[int] = set()
    exact_matched: typing.Set[int] = set()
    compact_matched: typing.Set[int] = set()
    fuzzy_matched: typing.Set[int] = set()

    for nb in unmapped:
        canon = _NTW_UNMAPPED_SYNC_CANONICAL_BRAND.get((nb.name or "").strip().upper())
        if canon:
            b = brands_by_upper_name.get(canon.strip().upper())
            if b:
                resolved_by_id[nb.id] = b
                canonical_matched.add(nb.id)

    for nb in unmapped:
        if nb.id in resolved_by_id:
            continue
        b = brands_by_upper_name.get((nb.name or "").strip().upper())
        if b:
            resolved_by_id[nb.id] = b
            exact_matched.add(nb.id)

    still_unresolved = [nb for nb in unmapped if nb.id not in resolved_by_id]
    if still_unresolved:
        compact_index = brands_by_compact_key()
        for nb in still_unresolved:
            key = normalize_compact_key(nb.name or "")
            if not key:
                continue
            b = compact_index.get(key)
            if b:
                resolved_by_id[nb.id] = b
                compact_matched.add(nb.id)

    unresolved_after_compact = [nb for nb in unmapped if nb.id not in resolved_by_id]
    brands_first_index = brands_by_first_token_upper() if unresolved_after_compact else {}
    all_brands_fallback: typing.Optional[typing.List[src_models.Brands]] = None
    for nb in unresolved_after_compact:
        source = nb.name or ""
        if source.strip().upper() in _NTW_FUZZY_MATCH_BLOCKLIST:
            continue
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
            resolved_by_id[nb.id] = brand
            fuzzy_matched.add(nb.id)

    if dry_run:
        would_create = [nb for nb in unmapped if nb.id not in resolved_by_id]
        logger.info(
            "{} [dry-run] Summary: {} canonical, {} exact, {} compact, {} fuzzy, {} unmatched "
            "(would create Brand). No writes performed.".format(
                _LOG_PREFIX, len(canonical_matched), len(exact_matched), len(compact_matched),
                len(fuzzy_matched), len(would_create),
            )
        )
        for nb in would_create:
            logger.info("{} [dry-run]   unmatched: {}".format(_LOG_PREFIX, nb.name))
        return unmapped

    new_brand_names: typing.Set[str] = set()
    for nb in unmapped:
        if nb.id not in resolved_by_id:
            new_brand_names.add((nb.name or "").strip().upper() or "BRAND_{}".format(nb.id))

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
        for nb in unmapped:
            if nb.id not in resolved_by_id:
                key = (nb.name or "").strip().upper() or "BRAND_{}".format(nb.id)
                resolved_by_id[nb.id] = by_name[key]

    mapping_models = [
        src_models.BrandNtwBrandMapping(brand_id=resolved_by_id[nb.id].id, ntw_brand_id=nb.id)
        for nb in unmapped
        if nb.id in resolved_by_id
    ]
    if mapping_models:
        pgbulk.upsert(
            src_models.BrandNtwBrandMapping,
            mapping_models,
            unique_fields=["brand", "ntw_brand"],
            update_fields=[],
            returning=False,
        )

    created_bp = 0
    for nb in unmapped:
        brand = resolved_by_id.get(nb.id)
        if not brand:
            continue
        _, bpc = src_models.BrandProviders.objects.get_or_create(brand=brand, provider=ntw_provider)
        if bpc:
            created_bp += 1

    logger.info(
        "{} NTW brand sync done. canonical={}, exact={}, compact={}, fuzzy={}, created={}, "
        "mappings upserted={}, BrandProviders created={}.".format(
            _LOG_PREFIX, len(canonical_matched), len(exact_matched), len(compact_matched),
            len(fuzzy_matched), created_brands, len(mapping_models), created_bp,
        )
    )
    return unmapped


# -- catalog ingest ----------------------------------------------------------


_NTW_PARTS_UPDATE_FIELDS = [
    "product_id", "description", "model_name", "size_description", "category",
    "size_component_1", "size_component_2", "size_component_3", "quantity", "cost",
    "location_id", "raw_data", "updated_at",
]


def _upsert_brands(rows: typing.List[typing.Dict]) -> typing.Dict[str, src_models.NtwBrand]:
    brand_name_by_key: typing.Dict[str, str] = {}
    for row in rows:
        key = _brand_key(row.get("brand_name"))
        if key:
            brand_name_by_key.setdefault(key, row["brand_name"])

    if not brand_name_by_key:
        return {}

    pgbulk.upsert(
        src_models.NtwBrand,
        [src_models.NtwBrand(name=name) for name in sorted(brand_name_by_key.values())],
        unique_fields=["name"],
        update_fields=[],
    )
    connection.close()

    return {
        (b.name or "").strip().upper(): b
        for b in src_models.NtwBrand.objects.filter(name__in=list(brand_name_by_key.values()))
    }


def _parts_from_rows(
    rows: typing.List[typing.Dict],
    brand_by_key: typing.Dict[str, src_models.NtwBrand],
) -> typing.List[src_models.NtwParts]:
    """Dedupe on (brand, part_number) as we build -- last row wins, matching the upsert's key."""
    by_key: typing.Dict[typing.Tuple[int, str], src_models.NtwParts] = {}
    for row in rows:
        brand = brand_by_key.get(_brand_key(row.get("brand_name")) or "")
        part_number = row.get("part_number")
        if not brand or not part_number:
            continue
        by_key[(brand.id, part_number)] = src_models.NtwParts(
            brand=brand,
            part_number=part_number,
            product_id=row.get("product_id"),
            description=row.get("description"),
            model_name=row.get("model_name"),
            size_description=row.get("size_description"),
            category=row.get("category"),
            size_component_1=row.get("size_component_1"),
            size_component_2=row.get("size_component_2"),
            size_component_3=row.get("size_component_3"),
            quantity=row.get("quantity"),
            cost=row.get("cost"),
            location_id=row.get("location_id"),
            raw_data=_json_safe_row(row),
        )
    return list(by_key.values())


def zero_out_absent_ntw_parts(present_ids: typing.Set[int]) -> int:
    """
    Set ``quantity = 0`` on every NtwParts row that was NOT in the file we just applied.

    This is the reconciliation an upsert cannot do, and the feed requires it: the file carries
    in-stock items only (zero rows with qty 0 across 3,533 live rows), so a sold-out tire leaves
    the file rather than arriving as a zero. Without this pass its last known quantity would stand
    forever and we would sell something NTW does not have.

    Rows are zeroed, never deleted -- the catalog row is what MasterPart/ProviderPart identity and
    every company's pricing history hang off, and the tire usually comes back in stock within days.
    """
    stale = list(
        src_models.NtwParts.objects.exclude(id__in=present_ids)
        .exclude(quantity=0)
        .values_list("id", flat=True)
    )
    if not stale:
        logger.info("{} Zero-out pass: nothing stale.".format(_LOG_PREFIX))
        return 0

    now = timezone.now()
    for i in range(0, len(stale), _NTW_ZERO_OUT_CHUNK):
        chunk = stale[i : i + _NTW_ZERO_OUT_CHUNK]
        src_models.NtwParts.objects.filter(id__in=chunk).update(quantity=0, updated_at=now)
        connection.close()

    logger.info(
        "{} Zero-out pass: set quantity=0 on {} NtwParts absent from the latest file.".format(
            _LOG_PREFIX, len(stale)
        )
    )
    return len(stale)


def fetch_and_save_ntw_catalog() -> None:
    """
    Download the newest feed file from the PRIMARY NTW connection and upsert the shared
    NtwBrand / NtwParts catalog, then zero out anything the file no longer lists.

    Catalog, quantity and cost here all come from that one connection, exactly as TireRack and DLG
    do it; per-company cost and quantity are pulled separately into NtwCompanyPricing by
    sync_ntw_company_pricing_for_company_provider.
    """
    logger.info("{} Starting NTW catalog ingest.".format(_LOG_PREFIX))

    catalog_cp = _primary_ntw_company_provider()
    if not catalog_cp:
        logger.info("{} No active NTW CompanyProviders. Skipping.".format(_LOG_PREFIX))
        return

    try:
        client = _client_for(catalog_cp)
    except ValueError as e:
        logger.error("{} {}".format(_LOG_PREFIX, str(e)))
        raise

    try:
        filename, content = client.download_latest_feed()
    except ntw_exceptions.NtwException as e:
        logger.error("{} {}".format(_LOG_PREFIX, getattr(e, "message", str(e))))
        raise

    rows = parse_feed_content(content)
    if not rows:
        logger.warning("{} Feed file {} produced no usable rows.".format(_LOG_PREFIX, filename))
        return

    brand_by_key = _upsert_brands(rows)
    if not brand_by_key:
        logger.warning("{} No NTW brands after upsert.".format(_LOG_PREFIX))
        return

    parts = _parts_from_rows(rows, brand_by_key)
    if not parts:
        logger.warning("{} No NtwParts to upsert from {}.".format(_LOG_PREFIX, filename))
        return

    for i in range(0, len(parts), _NTW_PARTS_UPSERT_BATCH):
        batch = parts[i : i + _NTW_PARTS_UPSERT_BATCH]
        now = timezone.now()
        for p in batch:
            p.updated_at = now
        pgbulk.upsert(
            src_models.NtwParts,
            batch,
            unique_fields=["part_number", "brand"],
            update_fields=_NTW_PARTS_UPDATE_FIELDS,
        )
        connection.close()

    present_pairs = [(p.brand_id or p.brand.pk, p.part_number) for p in parts]
    present_ids = set(
        src_models.NtwParts.objects.filter(
            brand_id__in={b for b, _ in present_pairs},
            part_number__in={pn for _, pn in present_pairs},
        ).values_list("id", flat=True)
    )
    zero_out_absent_ntw_parts(present_ids)

    logger.info(
        "{} Finished NTW catalog ingest from {} ({} parts, {} brands).".format(
            _LOG_PREFIX, filename, len(parts), len(brand_by_key)
        )
    )


# -- per-company pricing -----------------------------------------------------


def _upsert_ntw_company_pricing_for_company(
    company: src_models.Company,
    rows: typing.List[typing.Dict],
) -> int:
    """Map this company's own feed rows onto the shared NtwParts catalog and upsert its pricing."""
    brand_by_key = {
        (b.name or "").strip().upper(): b for b in src_models.NtwBrand.objects.all()
    }
    if not brand_by_key:
        return 0

    by_pair: typing.Dict[typing.Tuple[int, str], typing.Dict] = {}
    for row in rows:
        brand = brand_by_key.get(_brand_key(row.get("brand_name")) or "")
        part_number = row.get("part_number")
        if not brand or not part_number:
            continue
        by_pair[(brand.id, part_number)] = row

    if not by_pair:
        return 0

    part_id_by_pair = {
        (brand_id, part_number): part_id
        for part_id, brand_id, part_number in src_models.NtwParts.objects.filter(
            brand_id__in={b for b, _ in by_pair},
            part_number__in={pn for _, pn in by_pair},
        ).values_list("id", "brand_id", "part_number")
    }

    pricing_rows = []
    unmatched = 0
    for pair, row in by_pair.items():
        part_id = part_id_by_pair.get(pair)
        if not part_id:
            # This company carries something the primary connection's file did not list. It is a
            # real gap rather than an error -- different branches stock different tires -- and the
            # part appears as soon as any connection's file includes it.
            unmatched += 1
            continue
        pricing_rows.append(
            src_models.NtwCompanyPricing(
                part_id=part_id,
                company=company,
                cost=row.get("cost"),
                quantity=row.get("quantity"),
                location_id=row.get("location_id"),
            )
        )

    if unmatched:
        logger.info(
            "{} {} rows in company {}'s feed are not in the shared catalog yet.".format(
                _LOG_PREFIX, unmatched, company.id
            )
        )

    if not pricing_rows:
        return 0

    now = timezone.now()
    for i in range(0, len(pricing_rows), _NTW_PARTS_UPSERT_BATCH):
        batch = pricing_rows[i : i + _NTW_PARTS_UPSERT_BATCH]
        for r in batch:
            r.updated_at = now
        pgbulk.upsert(
            src_models.NtwCompanyPricing,
            batch,
            unique_fields=["part", "company"],
            update_fields=["cost", "quantity", "location_id", "updated_at"],
        )
        connection.close()

    return len(pricing_rows)


def sync_ntw_company_pricing_for_company_provider(company_provider_id: int) -> None:
    """Pull one company's own NTW feed and upsert NtwCompanyPricing (cost + warehouse quantity)."""
    cp = (
        src_models.CompanyProviders.objects.select_related("company", "provider")
        .filter(id=company_provider_id, provider__kind=src_enums.BrandProviderKind.NTW.value)
        .first()
    )
    if not cp:
        logger.warning("{} CompanyProviders {} is not an NTW connection.".format(_LOG_PREFIX, company_provider_id))
        return

    try:
        client = _client_for(cp)
    except ValueError as e:
        logger.error("{} Company {}: {}".format(_LOG_PREFIX, cp.company_id, str(e)))
        raise

    try:
        filename, content = client.download_latest_feed()
    except ntw_exceptions.NtwException as e:
        logger.error(
            "{} Company {}: {}".format(_LOG_PREFIX, cp.company_id, getattr(e, "message", str(e)))
        )
        raise

    rows = parse_feed_content(content)
    if not rows:
        logger.warning("{} Company {}: {} produced no usable rows.".format(_LOG_PREFIX, cp.company_id, filename))
        return

    written = _upsert_ntw_company_pricing_for_company(cp.company, rows)
    logger.info(
        "{} Company {}: upserted {} NtwCompanyPricing rows from {}.".format(
            _LOG_PREFIX, cp.company_id, written, filename
        )
    )


# -- tire specs from the inventory API ---------------------------------------

# TBC's inventory response carries seven structured product attributes the MOVEit feed does not.
# Which TireSpec tier each belongs to is NOT uniform, and that decides whether we may write it:
#
#   speedRating/loadRange/loadIndexRange -> the parser already derives these from the sidewall
#       string, so NTW is a FALLBACK: written only where the column is still NULL.
#   treadPattern -> tread_design, sideWall -> sidewall_style. Both sit in the block that
#       src/models.py calls "from a manufacturer-grade catalog (SimpleTire)", whose comment says
#       nothing below a catalog can supply them. NTW can, so that comment is now too narrow --
#       but SimpleTire remains the better source, so these are filled only where NULL too.
#   runFlat -> is_run_flat and lightTruck -> vehicle_class are LLM-owned identification fields.
#       A distributor stating them outright beats a model inferring them, and filling them here
#       is what removes them from the enrichment prompt's job.
#
# Nothing here ever overwrites a value that is already set. The precedence table in TireSpec's
# docstring puts distributor fields first, but "first" is about who should own a field, not a
# licence to clobber a catalog match that is already more specific than we are.
_NTW_TREAD_DESIGNS = {"SYMMETRICAL": "Symmetrical", "ASYMMETRICAL": "Asymmetrical", "DIRECTIONAL": "Directional"}

# Fields written on an existing row. Deliberately excludes the size block: a row that already
# exists has a size from the parser or a catalog, and NTW's tireSize object is a coarser
# three-field form that would be a downgrade.
_NTW_SPEC_FILL_FIELDS = (
    "speed_rating", "load_range", "load_index",
    "tread_design", "sidewall_style", "is_run_flat", "vehicle_class",
)


def _ntw_yes_no(value: typing.Any) -> typing.Optional[bool]:
    """``"Y"``/``"N"`` to a bool. Anything else is unknown -- never False, per TireSpec's rule
    that a nullable boolean means unknown."""
    text = (str(value or "")).strip().upper()
    if text in ("Y", "YES", "TRUE"):
        return True
    if text in ("N", "NO", "FALSE"):
        return False
    return None


def _ntw_tread_design(value: typing.Any) -> typing.Optional[str]:
    """
    TBC's enumeration is SYMMETRICAL / ASYMMETRICAL / DIRECTIONAL / ALL / NO. Only the first
    three name a real tread design; "ALL" and "NO" are filter sentinels that mean "unspecified"
    and must not be stored as if they were one.
    """
    return _NTW_TREAD_DESIGNS.get((str(value or "")).strip().upper())


def _ntw_vehicle_class(light_truck: typing.Any) -> typing.Optional[str]:
    """
    ``lightTruck == "Y"`` is a positive statement and maps straight through. ``"N"`` is NOT
    evidence of a passenger tire -- a 295/80R22.5 commercial truck tire and a farm implement tire
    are both "N" -- so it yields unknown rather than a wrong class.
    """
    return src_models.TireSpec.VEHICLE_CLASS_LIGHT_TRUCK if _ntw_yes_no(light_truck) else None


def _ntw_spec_values(row: typing.Dict) -> typing.Dict[str, typing.Any]:
    """The TireSpec-shaped subset of one API product row; keys whose value is unknown are absent."""
    load_index = _safe_int(row.get("loadIndexRange"))
    values = {
        "speed_rating": _clean(row.get("speedRating")),
        "load_range": _clean(row.get("loadRange")),
        "load_index": load_index if load_index and 0 < load_index < 1000 else None,
        "tread_design": _ntw_tread_design(row.get("treadPattern")),
        "sidewall_style": _clean(row.get("sideWall")),
        "is_run_flat": _ntw_yes_no(row.get("runFlat")),
        "vehicle_class": _ntw_vehicle_class(row.get("lightTruck")),
    }
    return {k: v for k, v in values.items() if v is not None}


def fetch_ntw_product_attributes(
    customer_id: str,
    brand_part_pairs: typing.Sequence[typing.Tuple[str, str]],
    batch_size: int = 200,
) -> typing.Dict[typing.Tuple[str, str], typing.Dict]:
    """
    Look up ``(brandName, partNo)`` pairs through ByProductId and return the raw product rows,
    keyed by ``(BRAND_UPPER, part_number)``.

    ``requestedQty=0`` on purpose. That parameter filters rather than requests: at 1 the gateway
    returns only what is in stock, so a tire NTW carries but is out of right now would silently
    have no attributes to read. Measured on the same 25 parts, qty=1 returned 7 and qty=0
    returned 11, the extra four being zero-stock rows. Omitting it entirely is not an option --
    that returns errorCd 1006 for the whole call.

    A pair whose part number is blank is dropped rather than sent: an empty ``partNo`` is a
    silent brand wildcard on this API (``{"brandName": "Cooper", "partNo": ""}`` returned
    Cooper's entire 131-product catalog, HTTP 200, no warning), which would both corrupt the
    mapping and quietly inflate the response.
    """
    from src.integrations.clients.ntw import api_client as ntw_api

    client = ntw_api.NtwApiClient()
    pairs = [(b, pn) for b, pn in brand_part_pairs if (pn or "").strip() and (b or "").strip()]
    dropped = len(brand_part_pairs) - len(pairs)
    if dropped:
        logger.warning(
            "{} Skipped {} parts with a blank brand or part number (a blank partNo is a brand "
            "wildcard on this API).".format(_LOG_PREFIX, dropped)
        )

    found: typing.Dict[typing.Tuple[str, str], typing.Dict] = {}
    for i in range(0, len(pairs), batch_size):
        chunk = pairs[i : i + batch_size]
        criteria = [
            {"brandName": brand, "partNo": part_number, "requestedQty": 0}
            for brand, part_number in chunk
        ]
        rows = client.get_tire_inventory(customer_id, criteria)
        for row in rows:
            part_number = (str(row.get("partNo") or "")).strip()
            brand = (str(row.get("brandName") or "")).strip().upper()
            if part_number and brand:
                found.setdefault((brand, part_number), row)
        logger.info(
            "{} Product attributes: batch {}/{} -- asked {}, matched {} so far.".format(
                _LOG_PREFIX, (i // batch_size) + 1,
                (len(pairs) + batch_size - 1) // batch_size, len(chunk), len(found),
            )
        )
    return found


def sync_tire_specs_from_ntw_api(
    *,
    customer_id: typing.Optional[str] = None,
    company_provider_id: typing.Optional[int] = None,
    brands: typing.Optional[typing.Sequence[str]] = None,
    limit: typing.Optional[int] = None,
    batch_size: int = 200,
    apply_changes: bool = False,
) -> typing.Dict[str, int]:
    """
    Fill TireSpec from TBC's inventory API for NTW's parts, creating a row where none exists.

    This is the one NTW path that CREATES TireSpec rows, which the equivalent ATD sync
    deliberately does not do. The reason the rule differs: ATD supplies only measured extras
    (tread depth, UTQG, rim widths) and has nothing to build a row around, whereas every NTW part
    arrives with a sidewall string in the feed, so ``src.domain.tire_size`` can own the size block
    exactly as it does for an enrichment-built row. ``rim_diameter_in`` and ``size_display`` are
    NOT NULL, so a part whose size will not parse is skipped rather than given a stub.

    Creating a row moves a part from the enrichment pipeline's MODE_MISSING to MODE_INCOMPLETE.
    That is not a dead end -- ``enrich_tire_specs --mode incomplete`` still reaches it for the
    identification fields only the model can supply (model_name, season, tier, noise) -- but a
    default enrichment run looks for missing rows, so anything created here needs that flag.

    The lookup tier (max_load_lb, max_speed_mph, ply_rating) is left alone: those are derived
    from the load index and load range by ``resolve_tire_derived_fields``, which is free and
    idempotent, and duplicating it here would be a second place to get it wrong.

    Nothing is written unless ``apply_changes`` is True.
    """
    logger.info(
        "{} TireSpec sync from the NTW API{}.".format(_LOG_PREFIX, "" if apply_changes else " (dry run)")
    )

    from src.domain import tire_size as tire_size_domain

    if not customer_id:
        cp = None
        if company_provider_id:
            cp = src_models.CompanyProviders.objects.filter(id=company_provider_id).first()
        else:
            cp = _primary_ntw_company_provider()
        creds = credentials_helper.get_feed_credentials(cp) if cp else {}
        customer_id = (creds.get("customer_id") or "").strip()
    if not customer_id:
        raise ValueError(
            "No NTW customer_id. Pass one, or set it on the connection's feed credentials -- "
            "TBC issues it separately from the MOVEit login."
        )

    ntw_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.NTW.value
    ).first()
    if not ntw_provider:
        logger.info("{} No NTW provider found.".format(_LOG_PREFIX))
        return {}

    rows = (
        src_models.NtwParts.objects.select_related("brand")
        .filter(brand__brand_mappings__isnull=False)
        .values("part_number", "size_description", "brand__name", "brand__brand_mappings__brand_id")
        .order_by("part_number")
    )
    if brands:
        rows = rows.filter(brand__name__in=list(brands))
    if limit:
        rows = rows[:limit]
    rows = list(rows)
    if not rows:
        logger.info("{} No mapped NTW parts to work on.".format(_LOG_PREFIX))
        return {}

    master_part_id_by_key = {
        (r["brand_id"], r["part_number"]): r["id"]
        for r in src_models.MasterPart.objects.filter(
            brand_id__in={r["brand__brand_mappings__brand_id"] for r in rows},
            part_number__in={r["part_number"] for r in rows},
        ).values("id", "brand_id", "part_number")
    }
    existing_spec_ids = set(
        src_models.TireSpec.objects.filter(
            master_part_id__in=set(master_part_id_by_key.values())
        ).values_list("master_part_id", flat=True)
    )

    attributes = fetch_ntw_product_attributes(
        customer_id,
        [(r["brand__name"], r["part_number"]) for r in rows],
        batch_size=batch_size,
    )

    stats = {
        "parts": len(rows), "matched_in_api": 0, "updated": 0, "created": 0,
        "unchanged": 0, "unparseable_size": 0, "no_master_part": 0, "fields_written": 0,
    }
    to_update: typing.List[src_models.TireSpec] = []
    to_create: typing.List[src_models.TireSpec] = []
    specs_by_mp = {
        s.master_part_id: s
        for s in src_models.TireSpec.objects.filter(master_part_id__in=existing_spec_ids)
    }

    for r in rows:
        master_part_id = master_part_id_by_key.get(
            (r["brand__brand_mappings__brand_id"], r["part_number"])
        )
        if not master_part_id:
            stats["no_master_part"] += 1
            continue
        row = attributes.get(((r["brand__name"] or "").strip().upper(), r["part_number"]))
        if not row:
            continue
        stats["matched_in_api"] += 1
        values = _ntw_spec_values(row)
        if not values:
            continue

        spec = specs_by_mp.get(master_part_id)
        if spec is not None:
            changed = [f for f in _NTW_SPEC_FILL_FIELDS
                       if f in values and getattr(spec, f) in (None, "")]
            if not changed:
                stats["unchanged"] += 1
                continue
            for f in changed:
                setattr(spec, f, values[f])
            stats["updated"] += 1
            stats["fields_written"] += len(changed)
            to_update.append(spec)
            continue

        parsed = tire_size_domain.parse(r["size_description"])
        if not parsed:
            # rim_diameter_in and size_display are NOT NULL -- no size, no row.
            stats["unparseable_size"] += 1
            continue
        spec = src_models.TireSpec(master_part_id=master_part_id)
        for field in tire_reparse.PARSER_FIELDS:
            setattr(spec, field, getattr(parsed, field, None))
        for field, value in values.items():
            if getattr(spec, field, None) in (None, ""):
                setattr(spec, field, value)
        spec.spec_source = src_models.TireSpec.SPEC_SOURCE_PARSER
        stats["created"] += 1
        stats["fields_written"] += len(values)
        to_create.append(spec)

    if apply_changes:
        if to_update:
            now = timezone.now()
            for spec in to_update:
                spec.updated_at = now
            src_models.TireSpec.objects.bulk_update(
                to_update, list(_NTW_SPEC_FILL_FIELDS) + ["updated_at"], batch_size=500
            )
        if to_create:
            src_models.TireSpec.objects.bulk_create(to_create, batch_size=500, ignore_conflicts=True)
    else:
        logger.info("{} Dry run -- nothing written.".format(_LOG_PREFIX))

    logger.info("{} TireSpec from NTW API: {}".format(_LOG_PREFIX, stats))
    return stats
