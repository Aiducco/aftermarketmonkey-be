"""
Merge manufacturer-grade specs from ``simpletire_skus`` into ``tire_specs``.

``simpletire_skus`` is a scrape of a competitor's catalog, which publishes the spec sheet the
manufacturer gives it: tread depth, max PSI, rim-width range, UTQG, tire weight. We have none of
those -- no distributor feed we ingest carries them and they are not encoded in the sidewall
string, so neither the parser nor the LLM can produce them. Where a SKU matches one of ours, its
values are simply better than anything we can derive, and this module copies them across.

What this is *not* is a wholesale replacement. Three principles decide every field, and each of
them came out of measuring the two catalogs against each other over 22,155 matched rows rather
than out of a preference for one source:

**Their column has to actually contain something.** ``is_run_flat`` is ``False`` on all 58,124
scraped rows including tires whose own model name says "Run Flat" -- the field is never populated,
and merging it would have overwritten 558 correct values with a default. It is excluded. The same
shape of mistake, NULL read as a value, once made an audit report 10% agreement on ``is_3pmsf``.

**Same name is not same quantity.** Our ``overall_diameter_in`` for a 35X12.50R20 is 35.0, the
nominal diameter the size prints; theirs is 33.07, measured. Both correct, and a customer
searching "35 inch" must keep finding it, so ours stays. ``max_speed_mph`` likewise: both sides
derive it from the speed rating, but we floor 160 km/h to 99 and they ceil to 100, and the
``speed_sort`` range filter would straddle the boundary if one column held both conventions.

**A coarser answer is not a correction.** Their taxonomy has no Rugged or Extreme Terrain, so
taking their category wholesale would flatten 269 RT rows to AT and 43 XT rows to MT. Instead the
two axes are separated: see ``_merge_category``.

Everything actually taken is listed in ``TAKE_THEIRS`` / ``FILL_ONLY``, and ``_NEVER_WRITE`` is
asserted against both at import so a later edit cannot quietly widen the merge.

Rows written here are stamped ``spec_source='simpletire'``, which makes ``tire_reparse`` skip the
six fields it would otherwise recompute from the sidewall on the next parser fix. That stamp is
the only thing standing between a catalog merge and a silent revert, and the merge stays fully
reversible because every field it overwrites is derived and can be recomputed: clear the stamp,
re-run the reparse.

Read-only unless the caller passes ``apply_changes``.
"""
import dataclasses
import decimal
import logging
import re
import typing

from django.db import transaction
from django.utils import timezone

from src import models as src_models
from src.domain import tire_size

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[SIMPLETIRE-SYNC]"

WRITE_BATCH = 500


# ---------------------------------------------------------------------------------------------
# Brand names
# ---------------------------------------------------------------------------------------------
# We and SimpleTire spell the same manufacturer differently. Matching on the raw name finds only
# 22.7% of our tires; with this map it is 38.1% on part number alone.
#
# These are not guesses and not fuzzy string matches. Each was inferred from part-number evidence
# -- our parts for that brand landing under exactly one of theirs -- and then verified by checking
# that the *size* agrees on the rows the alias buys. Every entry below scored ~100%: Continental
# 1292/1292, Falken 887/887, General 683/683, Cooper 531/531; the two weakest are Yokohama at
# 1206/1210 and Atturo at 328/329. Six rows in total disagreed, and the size gate in ``_match``
# rejects those individually anyway.
#
# The last four are distributor buckets rather than manufacturers -- our "brand" there is whoever
# shipped the part. They are included because the evidence says the part numbers really are that
# manufacturer's (ATD->Ironman 468/468, U.S. AutoForce->Advanta 409/409), and the size gate keeps
# a coincidental collision out. Do not extend this map by eyeballing names; re-run the inference.
BRAND_ALIASES = {
    "YOKOHAMATIRE": "YOKOHAMA",
    "CONTINENTALTIRE": "CONTINENTAL",
    "FALKENTIRE": "FALKEN",
    "COOPERTIRES": "COOPER",
    "GENERALTIRE": "GENERAL",
    "NEXENTIRE": "NEXEN",
    "ATTUROTIRE": "ATTURO",
    "IRONMANTIRES": "IRONMAN",
    "ARROYOTIRES": "ARROYO",
    "CARLISLETIREANDWHEELCOMPANY": "CARLSTAR",
    "AMPTIRES": "AMP",
    "FURYOFFROAD": "FURY",
    "TESCHETIRE": "TESCHE",
    # distributor buckets, not manufacturers
    "AMERICANTIREDISTRIBUTORS": "IRONMAN",
    "USAUTOFORCEDIRECT": "ADVANTA",
    "THEWHEELGROUPTWG": "AMP",
    "DIRTYLIFE": "AMP",
}


def brand_key(name: typing.Optional[str]) -> str:
    return re.sub(r"[^A-Z0-9]", "", (name or "").upper())


def aliased_brand_key(name: typing.Optional[str]) -> str:
    key = brand_key(name)
    return BRAND_ALIASES.get(key, key)


def part_key(part_number: typing.Optional[str]) -> str:
    """The two catalogs punctuate MPNs differently; nothing else about them differs."""
    return re.sub(r"[^A-Z0-9]", "", (part_number or "").upper())


_MODEL_NOISE_RE = re.compile(r"\b(TIRE|TIRES|TYRE)\b")


def model_key(name: typing.Optional[str]) -> str:
    return re.sub(r"[^A-Z0-9]", "", _MODEL_NOISE_RE.sub(" ", (name or "").upper()))


# ---------------------------------------------------------------------------------------------
# Sizes
# ---------------------------------------------------------------------------------------------
_size_cache: typing.Dict[str, typing.Optional[tuple]] = {}


def canonical_size(display: typing.Optional[str]) -> typing.Optional[tuple]:
    """
    Reduce either catalog's size string to comparable dimensions.

    They print '10.00-15' and '265/70R18'; we print '35X12.50R20LT'. Rather than compare
    typography, both sides go through our own parser -- the same one that is the source of truth
    for the size block -- and the result is compared on numbers.
    """
    if display in _size_cache:
        return _size_cache[display]
    parsed = tire_size.parse(display or "")
    value = None
    if parsed is not None and parsed.rim_diameter_in is not None:
        value = (parsed.section_width_mm, parsed.aspect_ratio, str(parsed.rim_diameter_in))
    _size_cache[display] = value
    return value


# ---------------------------------------------------------------------------------------------
# Load range
# ---------------------------------------------------------------------------------------------
# Their load range is a display string that packs two facts: 'E (10 Ply)' is load range E *and* a
# ply rating of 10. Reading the suffix yields a ply rating for 5,495 matched rows, where their
# dedicated spec_ply_rating column covers only a quarter of them.
_LOAD_RANGE_RE = re.compile(r"^(?P<letter>[A-N])(?:\s*\((?P<ply>\d+)\s*Ply\))?$", re.IGNORECASE)
_LOAD_RANGE_WORDS = {"STANDARD (SL)": "SL", "EXTRA (XL)": "XL", "LIGHT (LL)": "LL"}

# Codes ``load_range_ply`` knows. 'HL' (High Load, a designation above XL) and 'K' are real but
# absent from the lookup table, so rows carrying them are skipped and counted rather than coerced
# into a neighbouring code. '3*'/'2*'/'1*' are European heavy-truck load markings, not load ranges
# at all.
KNOWN_LOAD_RANGES = frozenset(["SL", "XL", "LL"] + list("ABCDEFGHJLMN"))


class LoadRangeReading(typing.NamedTuple):
    code: typing.Optional[str]
    ply_rating: typing.Optional[int]
    unmapped: typing.Optional[str]


def parse_load_range(raw: typing.Optional[str]) -> LoadRangeReading:
    text = (raw or "").strip()
    if not text:
        return LoadRangeReading(None, None, None)
    word = _LOAD_RANGE_WORDS.get(text.upper())
    if word:
        return LoadRangeReading(word, None, None)
    match = _LOAD_RANGE_RE.match(text)
    if match:
        code = match.group("letter").upper()
        ply = int(match.group("ply")) if match.group("ply") else None
        if code not in KNOWN_LOAD_RANGES:
            return LoadRangeReading(None, ply, text)
        return LoadRangeReading(code, ply, None)
    return LoadRangeReading(None, None, text)


# ---------------------------------------------------------------------------------------------
# Sidewall
# ---------------------------------------------------------------------------------------------
# One source field, two unrelated facts: 'Blackwall' describes appearance, 'Tube-Type' describes
# construction, and a tire has both. Splitting them is why 0187 added two columns.
_TUBE_VALUES = {"TUBELESS": True, "TUBE-TYPE": False, "TUBE TYPE": False}


def split_sidewall(raw: typing.Optional[str]) -> typing.Tuple[typing.Optional[str], typing.Optional[bool]]:
    text = (raw or "").strip()
    if not text:
        return None, None
    tube = _TUBE_VALUES.get(text.upper())
    if tube is not None:
        return None, tube
    return text, None


# ---------------------------------------------------------------------------------------------
# Taxonomy
# ---------------------------------------------------------------------------------------------
# Their single category maps onto one or two of ours. 'UHP All Season' is the clearest case of why
# it has to be a list: it states a performance tier and a season at once, and our taxonomy has a
# separate axis for each.
CATEGORY_FROM_SIMPLETIRE: typing.Dict[str, typing.Tuple[str, ...]] = {
    "All Season": ("ALL_SEASON",),
    "All Weather": ("ALL_WEATHER",),
    "Summer": ("SUMMER",),
    "Winter": ("WINTER",),
    "All Terrain": ("AT",),
    "Highway Terrain": ("HT",),
    "Mud Terrain": ("MT",),
    "Mud": ("MT",),
    "Sand": ("SAND",),
    "UHP": ("UHP",),
    "UHP All Season": ("UHP", "ALL_SEASON"),
    "Performance": ("PERFORMANCE",),
    "Touring": ("TOURING",),
    "Racing": ("TRACK",),
    "Track/Competition": ("TRACK",),
    "Autocross": ("TRACK",),
    "Drag Racing": ("TRACK",),
    "Commercial Van": ("COMMERCIAL",),
    "Antique": ("VINTAGE",),
}
# Deliberately unmapped: 'Sport' and 'Trail' (47 and 14 matched rows). Both are ambiguous across
# our axes -- 'Sport' is MC_STREET on a motorcycle and ATV_SPORT on a quad -- and guessing would
# put a car category on a powersports tire.

SEASON_CODES = frozenset(["ALL_SEASON", "ALL_WEATHER", "SUMMER", "WINTER"])

# Their vehicle vocabulary is wider than ours. SUV/Crossover has no separate value on our side and
# is a passenger tire by every other measure. Farm & Agricultural, Industrial, OTR, Lawn & Garden,
# Golf and Temp Spare are left unmapped on purpose: together they cover 157 matched rows, and
# adding six enum values that reach that little is not worth the churn in every consumer of the
# field. Revisit if agricultural stock is ever carried in volume.
VEHICLE_CLASS_FROM_SIMPLETIRE = {
    "Passenger": "passenger",
    "SUV/Crossover": "passenger",
    "Light Truck": "light_truck",
    "Trailer": "trailer",
    "Commercial": "commercial",
    "Motorcycle": "motorcycle",
    "ATV/UTV": "atv_utv",
}


# ---------------------------------------------------------------------------------------------
# What gets written
# ---------------------------------------------------------------------------------------------
# Straight copies where the catalog is authoritative and we either have nothing or are derived.
TAKE_THEIRS: typing.Dict[str, str] = {
    "tread_depth_32nds": "spec_tread_depth_32nds",
    "max_psi": "spec_max_psi",
    "rim_width_min_in": "spec_rim_width_min_in",
    "rim_width_max_in": "spec_rim_width_max_in",
    "utqg_treadwear": "spec_utqg_treadwear",
    "utqg_traction": "spec_utqg_traction",
    "utqg_temperature": "spec_utqg_temperature",
    "tread_design": "spec_tread_design",
    "mileage_warranty_miles": "spec_mileage_warranty_miles",
    "commercial_position": "spec_commercial_position",
    "tire_weight_lb": "spec_tire_weight_lb",
    "load_index": "spec_load_index",
    "load_index_dual": "spec_load_index_dual",
    "speed_rating": "spec_speed_rating",
    "max_load_lb": "spec_max_load_lb",
}

# Filled only when we have nothing, because ours is a considered answer and theirs is coarser.
FILL_ONLY: typing.Dict[str, str] = {
    "vehicle_class": "spec_vehicle",
}

# Handled by their own function because they are not a copy: see _merge_category, _merge_model_name
# and the load-range/sidewall readers above.
DERIVED_FIELDS = (
    "load_range",
    "ply_rating",
    "sidewall_style",
    "is_tubeless",
    "is_studdable",
    "model_name",
    "search_aliases",
    "tread_category_id",
    "season_category_id",
)

PROVENANCE_FIELDS = ("simpletire_sku_id", "simpletire_match_tier", "simpletire_synced_at", "spec_source")

WRITE_FIELDS = tuple(TAKE_THEIRS) + tuple(FILL_ONLY) + DERIVED_FIELDS + PROVENANCE_FIELDS

# The merge must never touch these, and the reasons differ per field -- see the module docstring.
# Asserted rather than commented so that adding a field to TAKE_THEIRS cannot quietly break it.
_NEVER_WRITE = frozenset(
    [
        "is_run_flat",  # their column is False on all 58,124 rows: an empty field, not a fact
        "overall_diameter_in",  # ours is nominal and drives '35 inch' search; theirs is measured
        "max_speed_mph",  # same quantity, different rounding convention; ours feeds speed_sort
        "sub_model",  # ours holds OE / Front / Rear, which they have no equivalent for
        "size_display",
        "notation",
        "service_type",
        "section_width_mm",
        "section_width_in",
        "aspect_ratio",
        "construction",
        "rim_diameter_in",
        "size_disputed",
        "is_3pmsf",
        "is_ms",
        "has_reinforced_sidewall",
        "llm_confidence",
        "llm_reason",
        "llm_model_used",
    ]
)
assert not (set(WRITE_FIELDS) & _NEVER_WRITE), "simpletire sync would write a field it must not"


# ---------------------------------------------------------------------------------------------
# Matching
# ---------------------------------------------------------------------------------------------
CATALOG_COLUMNS = (
    "id",
    "brand_name",
    "part_number",
    "size_display",
    "product_line_name",
    "spec_category",
    "spec_vehicle",
    "spec_sidewall",
    "spec_load_range",
    "spec_ply_rating",
    "spec_is_studdable",
) + tuple(TAKE_THEIRS.values())


class Catalog:
    """The scraped catalog, indexed three ways -- one per match tier."""

    def __init__(self, rows: typing.Iterable[dict]):
        self.by_id: typing.Dict[int, dict] = {}
        self.by_brand_part: typing.Dict[tuple, typing.List[int]] = {}
        self.by_part: typing.Dict[str, typing.List[int]] = {}
        self.by_brand_model_size: typing.Dict[tuple, typing.Set[int]] = {}
        self.brands: typing.Set[str] = set()

        for row in rows:
            sku_id = row["id"]
            self.by_id[sku_id] = row
            bkey, pkey = brand_key(row["brand_name"]), part_key(row["part_number"])
            self.brands.add(bkey)
            self.by_brand_part.setdefault((bkey, pkey), []).append(sku_id)
            self.by_part.setdefault(pkey, []).append(sku_id)
            size = canonical_size(row["size_display"])
            if size and row["product_line_name"]:
                self.by_brand_model_size.setdefault((bkey, model_key(row["product_line_name"]), size), set()).add(
                    sku_id
                )

    @classmethod
    def load(cls) -> "Catalog":
        rows = src_models.SimpleTireSku.objects.values(*CATALOG_COLUMNS).iterator(chunk_size=5000)
        catalog = cls(rows)
        missing = {t for t in BRAND_ALIASES.values() if t not in catalog.brands}
        if missing:
            # An alias pointing at a brand the scrape no longer has would silently stop matching.
            logger.warning("%s alias targets absent from the catalog: %s", _LOG_PREFIX, sorted(missing))
        return catalog


class Match(typing.NamedTuple):
    sku: dict
    tier: int


def match(
    *,
    brand: typing.Optional[str],
    part_number: typing.Optional[str],
    size_display: typing.Optional[str],
    model_name: typing.Optional[str],
    catalog: Catalog,
) -> typing.Optional[Match]:
    """
    Find the catalog row for one of our tires, most trustworthy key first.

    Every tier is gated on the size agreeing. That gate is what makes tier 2 usable at all: 7,814
    of our part numbers exist under *some* other-named brand on their side, because short numeric
    MPNs collide across manufacturers -- Michelin parts colliding with Atturo and Petlas. Requiring
    the dimensions to match as well cuts that to 297 rows that are actually the same tire.
    """
    bkey = aliased_brand_key(brand)
    pkey = part_key(part_number)
    ours = canonical_size(size_display)

    for sku_id in catalog.by_brand_part.get((bkey, pkey), ()):
        sku = catalog.by_id[sku_id]
        theirs = canonical_size(sku["size_display"])
        if ours and theirs and ours != theirs:
            return None  # same brand, same MPN, different tire: trust neither
        return Match(sku, 1)

    if ours:
        agreeing = [
            i for i in catalog.by_part.get(pkey, ()) if canonical_size(catalog.by_id[i]["size_display"]) == ours
        ]
        if len(agreeing) == 1:
            return Match(catalog.by_id[agreeing[0]], 2)

    if ours and model_name and bkey in catalog.brands:
        candidates = catalog.by_brand_model_size.get((bkey, model_key(model_name), ours))
        if candidates and len(candidates) == 1:
            return Match(catalog.by_id[next(iter(candidates))], 3)

    return None


# ---------------------------------------------------------------------------------------------
# Per-field merge rules that are more than a copy
# ---------------------------------------------------------------------------------------------
def _merge_category(
    ours: typing.Optional[str], their_category: typing.Optional[str]
) -> typing.Tuple[typing.Optional[str], typing.Optional[str], typing.Optional[str]]:
    """
    Resolve one of their categories against ours across two axes.

    Returns ``(tread_category, season_category, note)``. Their single field can name a season, a
    non-season, or both; ours holds exactly one code of either kind. The rules:

    * whatever season is named -- by either side -- lands in ``season_category``
    * a non-season from them fills ``tread_category`` when ours is empty or is itself a season,
      which is a strict gain: the season is preserved in its own column and we acquire the axis we
      were missing (732 rows where we said ALL_SEASON and they say UHP All Season)
    * a non-season from them that *contradicts* a non-season of ours is left alone and reported.
      Those 982 rows are the only real disagreement, and we are often the finer-grained side --
      their taxonomy has no RT or XT -- so overwriting would lose information.
    """
    codes = CATEGORY_FROM_SIMPLETIRE.get((their_category or "").strip(), ())
    their_season = next((c for c in codes if c in SEASON_CODES), None)
    their_other = next((c for c in codes if c not in SEASON_CODES), None)

    ours_is_season = ours in SEASON_CODES if ours else False
    season = their_season or (ours if ours_is_season else None)

    if their_other is None:
        return ours, season, None
    if ours is None or ours_is_season:
        return their_other, season, None
    if ours == their_other:
        return ours, season, None
    return ours, season, "category-conflict:{}->{}".format(ours, their_other)


def _merge_model_name(
    ours: typing.Optional[str], theirs: typing.Optional[str], aliases: typing.Sequence[str]
) -> typing.Tuple[typing.Optional[str], typing.List[str]]:
    """
    Take their product line name, keeping ours as a search alias when the two are the same product.

    Theirs is the manufacturer's own line name: never null across 22,155 matched rows against our
    1,161 nulls, complete where ours is truncated ('Terra Grappler' -> 'Terra Grappler G2'), and
    right where ours named the wrong generation entirely ('NT555 G2' for an NT555RII) or glued the
    brand on ('Kanati Mud Hog' -> 'Mud Hog M/T').

    Ours is kept as an alias only when the two names are the same product -- one contains the other
    once punctuation is stripped. That covers the distributor spellings a customer may actually
    type, without making a genuinely wrong name searchable and pointing it at the wrong tire.
    """
    if not theirs:
        return ours, list(aliases)
    updated = list(aliases)
    if ours:
        a, b = model_key(ours), model_key(theirs)
        related = a == b or a in b or b in a
        if related and a != b and not any(model_key(x) == a for x in updated):
            updated.append(ours)
    return theirs, updated


# ---------------------------------------------------------------------------------------------
# Running the merge
# ---------------------------------------------------------------------------------------------
@dataclasses.dataclass
class SyncStats:
    scanned: int = 0
    matched: int = 0
    by_tier: typing.Dict[int, int] = dataclasses.field(default_factory=dict)
    unmatched: int = 0
    changed: int = 0
    unchanged: int = 0
    written: int = 0
    field_changes: typing.Dict[str, int] = dataclasses.field(default_factory=dict)
    unmapped_load_ranges: typing.Dict[str, int] = dataclasses.field(default_factory=dict)
    unmapped_categories: typing.Dict[str, int] = dataclasses.field(default_factory=dict)
    category_conflicts: typing.Dict[str, int] = dataclasses.field(default_factory=dict)
    samples: typing.List[str] = dataclasses.field(default_factory=list)

    def bump(self, bucket: str, key) -> None:
        target = getattr(self, bucket)
        target[key] = target.get(key, 0) + 1


def _differs(old: typing.Any, new: typing.Any) -> bool:
    """Compare as the database would, so Decimal('9') and Decimal('9.0') are not a change."""
    if old is None and new is None:
        return False
    if old is None or new is None:
        return True
    if isinstance(old, (decimal.Decimal, float, int)) and not isinstance(old, bool):
        try:
            return decimal.Decimal(str(old)) != decimal.Decimal(str(new))
        except (decimal.InvalidOperation, ValueError):
            pass
    return old != new


def build_updates(spec, sku: dict, *, stats: typing.Optional[SyncStats] = None) -> typing.Dict[str, typing.Any]:
    """
    Work out what one matched row would become. Returns only the fields that actually change.

    Pure with respect to ``spec``: nothing is mutated here, so the same function serves the dry
    run and the write.
    """
    proposed: typing.Dict[str, typing.Any] = {}

    for ours_field, theirs_field in TAKE_THEIRS.items():
        value = sku.get(theirs_field)
        if value is not None:
            proposed[ours_field] = value

    for ours_field, theirs_field in FILL_ONLY.items():
        if getattr(spec, ours_field) is None:
            mapped = VEHICLE_CLASS_FROM_SIMPLETIRE.get((sku.get(theirs_field) or "").strip())
            if mapped:
                proposed[ours_field] = mapped

    reading = parse_load_range(sku.get("spec_load_range"))
    if reading.code:
        proposed["load_range"] = reading.code
    elif reading.unmapped and stats is not None:
        stats.bump("unmapped_load_ranges", reading.unmapped)
    ply = reading.ply_rating if reading.ply_rating is not None else sku.get("spec_ply_rating")
    if ply is not None:
        proposed["ply_rating"] = ply

    style, tubeless = split_sidewall(sku.get("spec_sidewall"))
    if style is not None:
        proposed["sidewall_style"] = style
    if tubeless is not None:
        proposed["is_tubeless"] = tubeless

    # Their column is True or NULL, never False -- a positive claim with no negative. Reading NULL
    # as "not studdable" would overwrite 1,203 of our answers with an absence of evidence.
    if sku.get("spec_is_studdable") is True:
        proposed["is_studdable"] = True

    name, aliases = _merge_model_name(spec.model_name, sku.get("product_line_name"), spec.search_aliases or [])
    proposed["model_name"] = name
    proposed["search_aliases"] = aliases

    raw_category = (sku.get("spec_category") or "").strip()
    if raw_category and raw_category not in CATEGORY_FROM_SIMPLETIRE and stats is not None:
        stats.bump("unmapped_categories", raw_category)
    tread, season, note = _merge_category(spec.tread_category_id, sku.get("spec_category"))
    proposed["tread_category_id"] = tread
    proposed["season_category_id"] = season
    if note and stats is not None:
        stats.bump("category_conflicts", note)

    return {f: v for f, v in proposed.items() if _differs(getattr(spec, f), v)}


def _specs(brand_ids: typing.Optional[typing.Sequence[int]]):
    qs = src_models.TireSpec.objects.select_related("master_part", "master_part__brand").order_by("master_part_id")
    if brand_ids:
        qs = qs.filter(master_part__brand_id__in=list(brand_ids))
    return qs


def run(
    *,
    brand_ids: typing.Optional[typing.Sequence[int]] = None,
    apply_changes: bool = False,
    limit: typing.Optional[int] = None,
    on_result: typing.Optional[typing.Callable[[typing.Any, typing.Optional[Match], dict], None]] = None,
) -> SyncStats:
    """Match every tire spec against the catalog and merge what the catalog is authoritative for."""
    stats = SyncStats()
    catalog = Catalog.load()
    logger.info("%s catalog loaded: %s skus, %s brands", _LOG_PREFIX, len(catalog.by_id), len(catalog.brands))

    now = timezone.now()
    pending: typing.List[typing.Any] = []

    for spec in _specs(brand_ids).iterator(chunk_size=1000):
        if limit is not None and stats.scanned >= limit:
            break
        stats.scanned += 1
        brand = spec.master_part.brand.name if spec.master_part.brand_id else None
        found = match(
            brand=brand,
            part_number=spec.master_part.part_number,
            size_display=spec.size_display,
            model_name=spec.model_name,
            catalog=catalog,
        )
        if found is None:
            stats.unmatched += 1
            if on_result:
                on_result(spec, None, {})
            continue

        stats.matched += 1
        stats.bump("by_tier", found.tier)
        updates = build_updates(spec, found.sku, stats=stats)
        if on_result:
            on_result(spec, found, updates)

        # The provenance stamp is written even when no value moved: it is what tells reparse the
        # row is catalog-backed, and a re-crawl needs the timestamp to know what it has passed.
        for field in updates:
            stats.bump("field_changes", field)
        if updates:
            stats.changed += 1
            if len(stats.samples) < 15:
                stats.samples.append(
                    "{} {} [{}] {}".format(
                        brand,
                        spec.master_part.part_number,
                        found.tier,
                        ", ".join("{}={}".format(f, updates[f]) for f in list(updates)[:4]),
                    )
                )
        else:
            stats.unchanged += 1

        for field, value in updates.items():
            setattr(spec, field, value)
        spec.simpletire_sku_id = found.sku["id"]
        spec.simpletire_match_tier = found.tier
        spec.simpletire_synced_at = now
        spec.spec_source = src_models.TireSpec.SPEC_SOURCE_SIMPLETIRE
        pending.append(spec)

        if apply_changes and len(pending) >= WRITE_BATCH:
            stats.written += _write(pending)
            pending = []

    if apply_changes and pending:
        stats.written += _write(pending)

    return stats


@transaction.atomic
def _write(specs: typing.Sequence[typing.Any]) -> int:
    """
    Persist the merge, naming every column explicitly.

    The instances are the rows themselves, loaded and mutated, so any field the merge did not
    touch goes back exactly as it came out. ``WRITE_FIELDS`` is the second guard -- it is asserted
    against ``_NEVER_WRITE`` at import, so a column this module must not own cannot reach the
    database even if the merge logic sets it.
    """
    src_models.TireSpec.objects.bulk_update(list(specs), list(WRITE_FIELDS), batch_size=WRITE_BATCH)
    return len(specs)
