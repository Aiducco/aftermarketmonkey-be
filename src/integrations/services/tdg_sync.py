"""
Merge the TDG catalog into ``tire_specs``, behind SimpleTire.

TDG is the second scraped catalog. It is *thinner* than SimpleTire but not worse, and it is worth
merging for three separate reasons:

**It reaches tires SimpleTire never had.** 8,656 of them, because it carries the Michelin group --
Michelin, BFGoodrich, Uniroyal -- which the SimpleTire crawl does not, plus Kumho and Dunlop.

**It carries fields SimpleTire has no column for.** Original-equipment homologation
(``oe_marking``: 'N0 - Porsche', 'MO - Mercedes-Benz'), a real ``is_run_flat``, a 3PMSF flag, and a
product image.

**Its run-flat flag is real.** SimpleTire's is ``False`` on all 58,124 rows -- an empty column --
so we excluded it there. TDG's is non-null on every row with 1,312 positives, and it fills 20,427
of our nulls.

Precedence is measured, not assumed. Across 10,740 tires described by both catalogs, SimpleTire has
better coverage on every shared field (tread depth present on 1,703 rows where TDG is silent
against 3 the other way; rim widths 3,508 against 6) and finer precision where both answer -- TDG
rounds tread depth to whole 32nds while 26% of SimpleTire's are fractional. So on a row SimpleTire
already owns, TDG may only fill nulls; on any other row it becomes the catalog source outright.
``_writable_fields`` is where that rule lives.

Two TDG columns are deliberately never read:

``warranty_mileage_miles`` **is kilometres.** Its modal values are 80,000 / 105,000 / 120,000 where
SimpleTire's are 50,000 / 60,000 / 45,000, and 80,000 km is 49,710 miles -- US tire warranties do
not run to 120,000 miles. Converting would mean inventing round numbers out of a rounding, and
SimpleTire already carries the field correctly.

``max_load_lb`` is NULL on all 34,996 TDG tires.

Read-only unless the caller passes ``apply_changes``.
"""
import dataclasses
import logging
import re
import typing

from django.db import transaction
from django.utils import timezone

from src import models as src_models
from src.integrations.services import simpletire_sync, tire_catalog

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[TDG-SYNC]"

WRITE_BATCH = 500

# Inferred from part-number evidence and verified on size agreement, exactly as SimpleTire's map
# was: Continental 1616/1621 sizes agree, Falken 1015/1015, General 673/673, Yokohama 1058/1061,
# Cooper 141/143. Carlisle -> Carlstar scores 143/165, below the threshold used to auto-accept an
# alias, but the brands are the same company and the per-row size gate rejects the 22 that differ.
# TDG needs far fewer aliases than SimpleTire because it does not carry the brands whose names we
# spell differently.
BRAND_ALIASES = {
    "YOKOHAMATIRE": "YOKOHAMA",
    "CONTINENTALTIRE": "CONTINENTAL",
    "FALKENTIRE": "FALKEN",
    "COOPERTIRES": "COOPER",
    "GENERALTIRE": "GENERAL",
    "CARLISLETIREANDWHEELCOMPANY": "CARLSTAR",
    # Wheel Pros is a distributor, not a maker: its rows resolve to Falken (340), Nitto (27) and
    # Toyo (6) on model+size evidence. Several targets are safe here only because every match tier
    # still requires the size to agree.
    "WHEELPROS": ("FALKEN", "NITTO", "TOYO"),
    "ROUGHCOUNTRY": "NITTO",
}

TIRE_PRODUCT_TYPE = "Tire"

CATALOG_COLUMNS = (
    "id",
    "brand_name",
    "part_number",
    "item_number",
    "tire_size_display",
    "product_line_name",
    "tread_depth_32nds",
    "utqg_treadwear",
    "utqg_traction",
    "utqg_temperature",
    "rim_width_min_in",
    "rim_width_max_in",
    "load_range",
    "load_index",
    "speed_rating",
    "sidewall",
    "season",
    "tire_type",
    "service_type",
    "is_run_flat",
    "is_3pmsf",
    "winter_studding",
    "oe_marking",
    "product_image_url",
    "gtin",
)

# Plain copies. Every one of these is also a SimpleTire field, so on a SimpleTire-owned row they
# are downgraded to fill-only by _writable_fields.
TAKE_THEIRS = {
    "tread_depth_32nds": "tread_depth_32nds",
    "utqg_treadwear": "utqg_treadwear",
    "utqg_traction": "utqg_traction",
    "utqg_temperature": "utqg_temperature",
    "rim_width_min_in": "rim_width_min_in",
    "rim_width_max_in": "rim_width_max_in",
    "load_index": "load_index",
    "speed_rating": "speed_rating",
}

# TDG is the only source for these, so they are written whatever else owns the row.
TDG_ONLY_FIELDS = ("oe_marking", "is_run_flat", "is_3pmsf", "is_studdable")

DERIVED_FIELDS = ("load_range", "ply_rating", "sidewall_style", "season_category_id", "tread_category_id")

PROVENANCE_FIELDS = ("tdg_product_id", "tdg_match_tier", "tdg_synced_at", "spec_source")

WRITE_FIELDS = tuple(TAKE_THEIRS) + TDG_ONLY_FIELDS + DERIVED_FIELDS + PROVENANCE_FIELDS

_NEVER_WRITE = tire_catalog.NEVER_WRITE | frozenset(
    [
        # Kilometres despite the column name; see the module docstring.
        "mileage_warranty_miles",
        # NULL on all 34,996 TDG tires.
        "max_load_lb",
        # TDG has no equivalent, and ours is the manufacturer's own line name from SimpleTire.
        "model_name",
        "search_aliases",
    ]
)
assert not (set(WRITE_FIELDS) & _NEVER_WRITE), "tdg sync would write a field it must not"

# TDG writes load ranges in its own notation, not SimpleTire's. Where SimpleTire says
# 'Extra (XL)' and 'E (10 Ply)', TDG says 'XL' and either 'E' or 'LRE' -- the LR prefix is just
# "Load Range" spelled out. Reusing SimpleTire's reader dropped 24,000 rows on the floor before
# this existed, silently, because an unmapped value is a skip rather than an error.
_TDG_LOAD_RANGE_RE = re.compile(r"^(?:LR)?(?P<letter>[A-N])$", re.IGNORECASE)
_TDG_PLY_ONLY_RE = re.compile(r"^(?P<ply>\d{1,2})\s*PLY$", re.IGNORECASE)


def parse_load_range(raw: typing.Optional[str]) -> simpletire_sync.LoadRangeReading:
    """
    Read TDG's load range.

    Deliberately narrow. '3*' and '2*' are European heavy-truck markings rather than load ranges;
    'HL' is a real designation above XL and 'NCS' a non-chain-service marking, but ``load_range_ply``
    has codes for neither; and the bare numerics ('14', '16', '0') are ambiguous between a ply
    rating and a typo. All of those are reported as unmapped rather than coerced into a neighbour.
    """
    # A handful of rows carry mojibake ('XL\xc2'); strip anything outside the printable set.
    text = re.sub(r"[^\x20-\x7E]", "", raw or "").strip().upper()
    if not text:
        return simpletire_sync.LoadRangeReading(None, None, None)
    if text in ("SL", "XL", "LL"):
        return simpletire_sync.LoadRangeReading(text, None, None)
    match = _TDG_LOAD_RANGE_RE.match(text)
    if match:
        code = match.group("letter").upper()
        if code in simpletire_sync.KNOWN_LOAD_RANGES:
            return simpletire_sync.LoadRangeReading(code, None, None)
        return simpletire_sync.LoadRangeReading(None, None, text)
    ply = _TDG_PLY_ONLY_RE.match(text)
    if ply:
        # A ply count with no letter. Real, and usable on its own.
        return simpletire_sync.LoadRangeReading(None, int(ply.group("ply")), None)
    return simpletire_sync.LoadRangeReading(None, None, text)


# 'Black Sidewall' where SimpleTire says 'Blackwall'. Same fact, two house styles; normalised to
# SimpleTire's wording so one column does not hold both vocabularies.
SIDEWALL_FROM_TDG = {
    "Black Sidewall": "Blackwall",
    "White Sidewall": "Whitewall",
    "Outlined White Letters": "Outlined White Lettering",
    "Raised White Letters": "Raised White Lettering",
    "Outlined Raised White Letters": "Outlined Raised White Lettering",
    "Smooth Red Letters": "Red Letter",
    "Raised Black Letters": "Raised Black Lettering",
}

# TDG's season vocabulary is a clean four, unlike its tire_type which mixes seasons with off-road
# weight classes ('10 - Off Road Pneumatic 1<15kg').
SEASON_FROM_TDG = {
    "All Season": "ALL_SEASON",
    "All Weather": "ALL_WEATHER",
    "Summer": "SUMMER",
    "Winter": "WINTER",
}

# Only the unambiguous half of tire_type. The off-road weight classes and 'OTS Exempt' say nothing
# about tread pattern, and guessing a category from them would be worse than leaving ours.
CATEGORY_FROM_TDG = {
    "4 - Commercial Truck Tires": "COMMERCIAL",
    "4B - Commercial Tires Up to 19.5": "COMMERCIAL",
    "19 - High Speed Trailer Tires": "TRAILER",
}


@dataclasses.dataclass
class SyncStats:
    scanned: int = 0
    matched: int = 0
    by_tier: typing.Dict[int, int] = dataclasses.field(default_factory=dict)
    unmatched: int = 0
    new_to_any_catalog: int = 0
    behind_simpletire: int = 0
    changed: int = 0
    written: int = 0
    field_changes: typing.Dict[str, int] = dataclasses.field(default_factory=dict)
    unmapped_load_ranges: typing.Dict[str, int] = dataclasses.field(default_factory=dict)
    run_flat_conflicts: int = 0
    samples: typing.List[str] = dataclasses.field(default_factory=list)

    def bump(self, bucket: str, key) -> None:
        target = getattr(self, bucket)
        target[key] = target.get(key, 0) + 1


def load_catalog() -> tire_catalog.CatalogIndex:
    rows = (
        src_models.TdgProduct.objects.filter(product_type=TIRE_PRODUCT_TYPE)
        .values(*CATALOG_COLUMNS)
        .iterator(chunk_size=5000)
    )
    index = tire_catalog.CatalogIndex(
        rows,
        brand_field="brand_name",
        part_field="part_number",
        size_field="tire_size_display",
        model_field="product_line_name",
        # A distributor may have filed either identifier as the part number.
        extra_part_fields=("item_number",),
    )
    missing = {t for t in BRAND_ALIASES.values() if t not in index.brands}
    if missing:
        logger.warning("%s alias targets absent from the catalog: %s", _LOG_PREFIX, sorted(missing))
    return index


def _writable_fields(spec) -> typing.Callable[[str, typing.Any], bool]:
    """
    Decide, per field, whether TDG is allowed to speak on this row.

    SimpleTire-owned rows accept TDG only where they are empty; everything else takes TDG outright.
    ``TDG_ONLY_FIELDS`` bypass the rule because there is no competing value to defer to.
    """
    simpletire_owns = spec.spec_source == src_models.TireSpec.SPEC_SOURCE_SIMPLETIRE

    def allowed(field: str, _value) -> bool:
        if field in TDG_ONLY_FIELDS:
            return True
        if simpletire_owns:
            return getattr(spec, field, None) is None
        return True

    return allowed


def build_updates(spec, row: dict, *, stats: typing.Optional[SyncStats] = None) -> typing.Dict[str, typing.Any]:
    """Work out what one matched row would become. Pure with respect to ``spec``."""
    allowed = _writable_fields(spec)
    proposed: typing.Dict[str, typing.Any] = {}

    def offer(field, value):
        if value is not None and allowed(field, value):
            proposed[field] = value

    for ours_field, theirs_field in TAKE_THEIRS.items():
        offer(ours_field, row.get(theirs_field))

    reading = parse_load_range(row.get("load_range"))
    if reading.code:
        offer("load_range", reading.code)
    elif reading.unmapped and stats is not None:
        stats.bump("unmapped_load_ranges", reading.unmapped)
    if reading.ply_rating is not None:
        offer("ply_rating", reading.ply_rating)

    offer("sidewall_style", SIDEWALL_FROM_TDG.get((row.get("sidewall") or "").strip()))
    offer("season_category_id", SEASON_FROM_TDG.get((row.get("season") or "").strip()))
    if spec.tread_category_id is None:
        offer("tread_category_id", CATEGORY_FROM_TDG.get((row.get("tire_type") or "").strip()))

    offer("oe_marking", row.get("oe_marking"))

    # Positive-only: TDG marks 3PMSF certification when present and says nothing otherwise. It
    # agreed with our LLM on 3,987 of 3,987 rows where both had an answer, which is why we let it
    # fill the 1,024 we had left null -- but a missing line is still not a denial.
    if row.get("is_3pmsf") is True:
        proposed["is_3pmsf"] = True
    if (row.get("winter_studding") or "").strip() in ("Studdable", "Studded"):
        proposed["is_studdable"] = True

    # Fill-only, unlike the rest of TDG_ONLY_FIELDS. TDG's run-flat flag is a real boolean and
    # fills 20,427 of our nulls, but where we already have an answer the two disagree on 15% of
    # rows -- and ours came from an explicit "RF" in the distributor's own title. Filling is the
    # large, safe win; overruling a title is not.
    if row.get("is_run_flat") is not None:
        if spec.is_run_flat is None:
            proposed["is_run_flat"] = row["is_run_flat"]
        elif spec.is_run_flat != row["is_run_flat"] and stats is not None:
            stats.run_flat_conflicts += 1

    return {f: v for f, v in proposed.items() if tire_catalog.differs(getattr(spec, f), v)}


def _specs(brand_ids):
    qs = src_models.TireSpec.objects.select_related("master_part", "master_part__brand").order_by("master_part_id")
    if brand_ids:
        qs = qs.filter(master_part__brand_id__in=list(brand_ids))
    return qs


def run(
    *,
    brand_ids: typing.Optional[typing.Sequence[int]] = None,
    apply_changes: bool = False,
    limit: typing.Optional[int] = None,
    on_result: typing.Optional[typing.Callable] = None,
) -> SyncStats:
    stats = SyncStats()
    catalog = load_catalog()
    logger.info("%s catalog loaded: %s tires, %s brands", _LOG_PREFIX, len(catalog.by_id), len(catalog.brands))

    now = timezone.now()
    pending: typing.List[typing.Any] = []

    for spec in _specs(brand_ids).iterator(chunk_size=1000):
        if limit is not None and stats.scanned >= limit:
            break
        stats.scanned += 1
        brand = spec.master_part.brand.name if spec.master_part.brand_id else None
        found = catalog.match(
            brand=brand,
            part_number=spec.master_part.part_number,
            size_display=spec.size_display,
            model_name=spec.model_name,
            aliases=BRAND_ALIASES,
        )
        if found is None:
            stats.unmatched += 1
            if on_result:
                on_result(spec, None, {})
            continue

        stats.matched += 1
        stats.bump("by_tier", found.tier)
        simpletire_owns = spec.spec_source == src_models.TireSpec.SPEC_SOURCE_SIMPLETIRE
        if simpletire_owns:
            stats.behind_simpletire += 1
        else:
            stats.new_to_any_catalog += 1

        updates = build_updates(spec, found.row, stats=stats)
        if on_result:
            on_result(spec, found, updates)
        for field in updates:
            stats.bump("field_changes", field)
        if updates:
            stats.changed += 1
            if len(stats.samples) < 12:
                stats.samples.append(
                    "{} {} [{}] {}".format(
                        brand,
                        spec.master_part.part_number,
                        found.tier,
                        ", ".join("{}={}".format(f, updates[f]) for f in list(updates)[:4]),
                    )
                )

        for field, value in updates.items():
            setattr(spec, field, value)
        spec.tdg_product_id = found.row["id"]
        spec.tdg_match_tier = found.tier
        spec.tdg_synced_at = now
        # Only claim the row if no better catalog already has it.
        if not simpletire_owns:
            spec.spec_source = src_models.TireSpec.SPEC_SOURCE_TDG
        pending.append(spec)

        if apply_changes and len(pending) >= WRITE_BATCH:
            stats.written += _write(pending)
            pending = []

    if apply_changes and pending:
        stats.written += _write(pending)
    return stats


@transaction.atomic
def _write(specs: typing.Sequence[typing.Any]) -> int:
    """Persist, naming every column explicitly -- the same guard as the SimpleTire merge."""
    src_models.TireSpec.objects.bulk_update(list(specs), list(WRITE_FIELDS), batch_size=WRITE_BATCH)
    return len(specs)
