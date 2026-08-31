"""
Say which tires no external catalog has validated, and *why* each one was missed.

The merges reach 64.6% of the catalog. The value of this report is entirely in splitting the
remaining third by cause, because the causes need completely different responses: one needs a new
data source, one needs a crawl of sizes already in reach, one needs better name normalisation, and
one is a bug in our own parser. A single "16,847 unmatched" number hides all of that.

The classification asks one question per tire, in order, and stops at the first answer. Order
matters: a brand nobody carries cannot also be a naming problem.

  1. brand absent          Neither catalog lists the brand at all. Only a new source fixes this.
  2. size will not parse   ``canonical_size`` returns nothing for our own ``size_display``, so the
                           row cannot be compared to anything. Our bug, and the cheapest to fix.
  3. size not carried      The catalog has the brand but not this size. Genuine coverage gap.
  4. model not carried     The size is carried, but under no model name we recognise. Either the
                           catalog does not sell that line, or our name for it does not normalise
                           to theirs -- worth reading before assuming which.
  5. ambiguous             Several SKUs share brand+model+size. A match exists but we cannot tell
                           which, so we refuse rather than guess.
  6. matchable             Exactly one SKU shares brand+model+size and we still missed it. Should
                           be near zero; anything here is a matcher bug.

Deliberately checks co-occurrence on a single SKU rather than each field against the brand's whole
inventory. Checking separately reported 4,825 tires as matchable when the true figure was 20 --
the brand had the size, and had the model, but never on the same product.
"""
import collections
import dataclasses
import typing

from django.db import connection

from src.integrations.services import simpletire_sync, tdg_sync
from src.integrations.services.tire_catalog import alias_keys, brand_key, canonical_size, model_key

REASON_NO_BRAND = "1. brand absent from both catalogs"
REASON_BAD_SIZE = "2. our own size string will not parse"
REASON_NO_SIZE = "3. brand carried, but not this size"
REASON_NO_MODEL = "4. size carried, but not this model name"
REASON_AMBIGUOUS = "5. several SKUs share brand+model+size"
REASON_MATCHABLE = "6. one SKU shares brand+model+size -- matcher bug"

REASON_ORDER = (
    REASON_NO_BRAND,
    REASON_BAD_SIZE,
    REASON_NO_SIZE,
    REASON_NO_MODEL,
    REASON_AMBIGUOUS,
    REASON_MATCHABLE,
)

# What to do about each, printed with the counts so the report is a work list rather than a tally.
REASON_ACTION = {
    REASON_NO_BRAND: "find a source that carries the brand",
    REASON_BAD_SIZE: "fix src.domain.tire_size",
    REASON_NO_SIZE: "crawl deeper, or accept: they may not sell it",
    REASON_NO_MODEL: "check our model name against theirs before crawling",
    REASON_AMBIGUOUS: "add a disambiguator to the tier-3 key",
    REASON_MATCHABLE: "investigate -- this should be empty",
}


@dataclasses.dataclass
class Gap:
    reason: str
    brand: str
    tires: int


@dataclasses.dataclass
class GapReport:
    total_specs: int = 0
    validated: int = 0
    by_reason: typing.Dict[str, int] = dataclasses.field(default_factory=dict)
    by_reason_brand: typing.Dict[str, typing.Counter] = dataclasses.field(default_factory=dict)
    brand_totals: typing.Counter = dataclasses.field(default_factory=collections.Counter)

    @property
    def unvalidated(self) -> int:
        return self.total_specs - self.validated


def _catalog_index():
    """Brand -> sizes, brand -> models, and (brand, model, size) -> how many SKUs, across both."""
    sizes = collections.defaultdict(set)
    models = collections.defaultdict(set)
    triples: typing.Counter = collections.Counter()

    with connection.cursor() as cursor:
        cursor.execute("SELECT brand_name, size_display, product_line_name FROM simpletire_skus")
        rows = list(cursor.fetchall())
        cursor.execute(
            "SELECT brand_name, tire_size_display, product_line_name FROM tdg_products WHERE product_type = 'Tire'"
        )
        rows += cursor.fetchall()

    for brand, size, model in rows:
        key = brand_key(brand)
        canon = canonical_size(size)
        if canon:
            sizes[key].add(canon)
        if model:
            models[key].add(model_key(model))
        if canon and model:
            triples[(key, model_key(model), canon)] += 1
    return sizes, models, triples


def _brand_keys(name: typing.Optional[str]) -> typing.Set[str]:
    """
    Every catalog brand key this tire could be filed under, across both sources.

    Must go through ``alias_keys`` rather than reading the maps directly: an alias value may be a
    tuple (Wheel Pros is Falken *and* Nitto *and* Toyo), and indexing a set with the tuple itself
    silently matches nothing -- which reports a well-covered brand as one no catalog carries.
    """
    return set(alias_keys(name, simpletire_sync.BRAND_ALIASES)) | set(alias_keys(name, tdg_sync.BRAND_ALIASES))


def classify(*, brand, size_display, model_name, sizes, models, triples) -> str:
    keys = _brand_keys(brand)
    known_sizes = set().union(*(sizes.get(k, set()) for k in keys))
    known_models = set().union(*(models.get(k, set()) for k in keys))
    if not known_sizes and not known_models:
        return REASON_NO_BRAND

    canon = canonical_size(size_display)
    if canon is None:
        return REASON_BAD_SIZE
    if canon not in known_sizes:
        return REASON_NO_SIZE
    if not model_name or model_key(model_name) not in known_models:
        return REASON_NO_MODEL

    count = sum(triples.get((k, model_key(model_name), canon), 0) for k in keys)
    if count == 1:
        return REASON_MATCHABLE
    if count > 1:
        return REASON_AMBIGUOUS
    # The size and the model are each carried, but never together on one product.
    return REASON_NO_MODEL


def build() -> GapReport:
    sizes, models, triples = _catalog_index()
    report = GapReport()
    report.by_reason = {reason: 0 for reason in REASON_ORDER}
    report.by_reason_brand = {reason: collections.Counter() for reason in REASON_ORDER}

    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT b.name, ts.size_display, ts.model_name,
                   (ts.simpletire_sku_id IS NOT NULL OR ts.tdg_product_id IS NOT NULL) AS validated
            FROM tire_specs ts
            JOIN master_parts mp ON mp.id = ts.master_part_id
            LEFT JOIN brands b ON b.id = mp.brand_id
            """
        )
        for brand, size_display, model_name, validated in cursor.fetchall():
            report.total_specs += 1
            label = brand or "(no brand)"
            report.brand_totals[label] += 1
            if validated:
                report.validated += 1
                continue
            reason = classify(
                brand=brand,
                size_display=size_display,
                model_name=model_name,
                sizes=sizes,
                models=models,
                triples=triples,
            )
            report.by_reason[reason] += 1
            report.by_reason_brand[reason][label] += 1
    return report
