"""
Which tire brands we carry, which of them a source exists for, and what those sources delivered.

The registry is only useful if it can be read as a work list, and a work list needs the two halves
put together: what our catalog contains, and what this department has collected. Both queries live
here so the seeding command and the reporting command answer with the same numbers.

The brand a tire belongs to is ``master_parts.brand``, not anything on ``tire_specs`` -- and it is
matched to a source through ``tire_catalog.brand_key``, the same punctuation-insensitive key the
catalog merges match on, because we spell brands one way and manufacturers spell themselves
another ('BF Goodrich' / 'BFGOODRICH').

``unvalidated`` counts tires no reseller catalog has confirmed -- neither SimpleTire nor TDG
matched them -- which is the number that should decide which brand to chase next. It is the
existing gap report's headline (``src.integrations.services.tire_gap_report``) grouped by brand,
and this module deliberately does not re-derive its reasons: which cause a gap has is that
report's job, and duplicating the classification would let the two drift.
"""
import dataclasses
import typing

from django.db import connection
from django.db.models import Count, Q

from src import models as src_models
from src.integrations.services.tire_catalog import brand_key


@dataclasses.dataclass
class BrandCoverage:
    """One brand in our tire catalog, and what this department has for it."""

    brand_name: str
    tires: int = 0
    unvalidated: int = 0
    sources: typing.List[src_models.TireBrandSource] = dataclasses.field(default_factory=list)
    raw_rows: int = 0

    @property
    def has_source(self) -> bool:
        return bool(self.sources)

    @property
    def has_runnable_source(self) -> bool:
        return any(source.is_runnable for source in self.sources)


@dataclasses.dataclass
class SourceCoverage:
    """One registry row, with what is actually behind it."""

    source: src_models.TireBrandSource
    raw_rows: int = 0
    rows_unparsed_size: int = 0
    rows_with_warnings: int = 0
    last_run: typing.Optional[src_models.TireBrandSourceRun] = None


def catalog_brands(*, min_tires: int = 1) -> typing.List[BrandCoverage]:
    """
    Every brand with tires in ``tire_specs``, biggest gap first.

    Raw SQL for the same reason ``tire_gap_report`` uses it: this is one aggregate over the whole
    tire catalog, and the ORM's version of it is three joins of indirection over a query that is
    four lines of SQL.
    """
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT COALESCE(b.name, '(no brand)') AS brand,
                   COUNT(*) AS tires,
                   COUNT(*) FILTER (
                       WHERE ts.simpletire_sku_id IS NULL AND ts.tdg_product_id IS NULL
                   ) AS unvalidated
            FROM tire_specs ts
            JOIN master_parts mp ON mp.id = ts.master_part_id
            LEFT JOIN brands b ON b.id = mp.brand_id
            GROUP BY 1
            HAVING COUNT(*) >= %s
            ORDER BY unvalidated DESC, tires DESC
            """,
            [min_tires],
        )
        rows = cursor.fetchall()

    sources = sources_by_brand_key()
    raw_counts = raw_rows_by_source_id()
    coverage = []
    for brand, tires, unvalidated in rows:
        matched = sources.get(brand_key(brand), [])
        coverage.append(
            BrandCoverage(
                brand_name=brand,
                tires=tires,
                unvalidated=unvalidated,
                sources=matched,
                raw_rows=sum(raw_counts.get(source.id, 0) for source in matched),
            )
        )
    return coverage


def sources_by_brand_key() -> typing.Dict[str, typing.List[src_models.TireBrandSource]]:
    """Registry rows grouped by the key a catalog brand name reduces to. A brand may have several."""
    grouped: typing.Dict[str, typing.List[src_models.TireBrandSource]] = {}
    for source in src_models.TireBrandSource.objects.exclude(status=src_models.TireBrandSource.STATUS_RETIRED):
        grouped.setdefault(brand_key(source.brand_name), []).append(source)
    return grouped


def raw_rows_by_source_id() -> typing.Dict[int, int]:
    return dict(
        src_models.RawTireSpec.objects.values_list("source_id")
        .annotate(total=Count("id"))
        .values_list("source_id", "total")
    )


def source_coverage() -> typing.List[SourceCoverage]:
    """Every registry row with its row counts and its last run, in priority order."""
    counts = {
        row["source_id"]: row
        for row in src_models.RawTireSpec.objects.values("source_id").annotate(
            total=Count("id"),
            unparsed=Count("id", filter=Q(parsed_size__isnull=True)),
            warned=Count("id", filter=~Q(warnings=[])),
        )
    }
    last_runs = {}
    for run in src_models.TireBrandSourceRun.objects.order_by("source_id", "-started_at"):
        last_runs.setdefault(run.source_id, run)

    out = []
    for source in src_models.TireBrandSource.objects.order_by("priority", "brand_name"):
        stats = counts.get(source.id, {})
        out.append(
            SourceCoverage(
                source=source,
                raw_rows=stats.get("total", 0),
                rows_unparsed_size=stats.get("unparsed", 0),
                rows_with_warnings=stats.get("warned", 0),
                last_run=last_runs.get(source.id),
            )
        )
    return out
