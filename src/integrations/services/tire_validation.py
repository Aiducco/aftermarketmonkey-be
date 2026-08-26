"""
Data-quality checks over ``tire_specs``. Read-only: it reports, it never repairs.

The point is to answer "is this data good enough to ship" with numbers rather than a spot check.
Three kinds of check, in descending order of how much they should worry you:

  **Correctness against outside evidence.** ``overall_diameter_cross_check`` compares the diameter
  we computed from the sidewall string against the diameter the distributor appended to its own
  title. Wheel Pros writes "TER GRAP G3 235/70R16 109/T XL 28.98" -- that trailing 28.98 is their
  figure, arrived at independently, and it is the only free ground truth in the whole pipeline. A
  systematic disagreement means the parser is wrong, not the feed.

  **Internal consistency.** Denormalised lookups matching their source tables, tires taller than
  their rims, categories inside the vocabulary, powersports sizes not classified as car tires.
  These are cheap and should always be zero; a non-zero is a bug, not a data gap.

  **Coverage.** What fraction of tires got a model name, a category, a load rating. These are
  never 100% and the number itself is the deliverable -- it tells you what a facet can be trusted
  to filter on.
"""
import collections
import dataclasses
import decimal
import re
import typing

from django.db import connection

from src.domain import tire_size

# The trailing overall diameter Wheel Pros and Premier append to a title, e.g. "... XL 28.98".
# Anchored to the end so a load index or a part number cannot be read as a diameter, and rejected
# after a slash because commercial titles end in a load-range/ply pair -- "245/70R19.5 H/16" ends
# in 16 plies, not a 16-inch tire.
_TRAILING_DIAMETER_RE = re.compile(r"(?<![\d./])(\d{2}(?:\.\d{1,2})?)\s*$")

# Tolerance on that cross-check. Manufacturers publish a measured diameter that differs from the
# arithmetic one by a few tenths (tread depth, actual section width), so an exact match is not
# expected; 3% is wide enough to absorb that and narrow enough to catch a transposed size.
_DIAMETER_TOLERANCE = decimal.Decimal("0.03")

# Codes that only make sense on a car or light truck. A tire whose size is a motorcycle size
# should never carry one -- see migration 0183 for why the powersports axis exists.
_LIGHT_VEHICLE_ONLY = ("HT", "AT", "RT", "MT", "XT", "TOURING", "UHP", "TRAILER", "COMMERCIAL", "SPARE")

# Categories that are seasonally exclusive, where severe-snow certification is a real
# contradiction. UHP is deliberately NOT here: the taxonomy defines it as "summer OR all-season",
# and terrain/performance is orthogonal to season by design -- a UHP all-season like the Pilot
# Sport All Season 4 carrying 3PMSF is correct, not a conflict.
_NEVER_SEVERE_SNOW = ("SUMMER", "TRACK", "MC_TRACK", "SAND")


@dataclasses.dataclass
class Check:
    name: str
    failures: int
    total: int
    detail: str = ""
    samples: typing.List[str] = dataclasses.field(default_factory=list)

    @property
    def rate(self) -> float:
        return (self.failures / self.total) if self.total else 0.0

    @property
    def ok(self) -> bool:
        return self.failures == 0


def _scalar(sql: str, params: typing.Optional[typing.Sequence] = None) -> int:
    with connection.cursor() as cursor:
        cursor.execute(sql, params or [])
        return cursor.fetchone()[0]


def _rows(sql: str, params: typing.Optional[typing.Sequence] = None) -> typing.List[tuple]:
    with connection.cursor() as cursor:
        cursor.execute(sql, params or [])
        return cursor.fetchall()


def _brand_clause(brand_ids: typing.Optional[typing.Sequence[int]]) -> typing.Tuple[str, list]:
    if not brand_ids:
        return "", []
    return "AND mp.brand_id = ANY(%s)", [list(brand_ids)]


# ==========================================================================================
# Correctness against outside evidence
# ==========================================================================================


def overall_diameter_cross_check(
    brand_ids=None, tolerance=_DIAMETER_TOLERANCE
) -> typing.Tuple[Check, typing.List[str]]:
    """
    Our computed diameter vs the one the distributor wrote at the end of its own title.

    Only rows where a distributor actually stated one are considered; the rest are not evidence
    either way and are excluded from the denominator rather than counted as passes.
    """
    clause, params = _brand_clause(brand_ids)
    rows = _rows(
        """
        SELECT ts.master_part_id, ts.size_display, ts.notation, ts.overall_diameter_in, mp.description
        FROM tire_specs ts JOIN master_parts mp ON mp.id = ts.master_part_id
        WHERE mp.description IS NOT NULL {}
        """.format(
            clause
        ),
        params,
    )
    compared = 0
    failures: typing.List[str] = []
    nominal: typing.List[str] = []
    for _mpid, size_display, notation, ours, description in rows:
        text = description.strip()
        match = _TRAILING_DIAMETER_RE.search(text)
        if match is None:
            continue
        # The trailing number must come AFTER the size, not be part of it. "Toyo Proxes TQ Tire -
        # P255/50R16" ends in 16, which is the rim diameter, not an appended overall diameter --
        # comparing against it reported every such row as a 10-inch discrepancy.
        parsed = tire_size.parse(text)
        if parsed is None or match.start() < parsed.span[1]:
            continue
        # Only the appended-diameter format is evidence, and it always carries the service
        # description first: "RECON GRAP 275/60R20 116S XL 32.9". Without that, a trailing number
        # is part of the model name -- "BS TUR ER33" is a Turanza ER33, "MI SCORCHER 31" is a
        # Scorcher 31, "PI CINTURATO CN36" is a Cinturato CN36. Comparing against those reported
        # 5.86% disagreement where the real figure is under 1%.
        if parsed.load_index is None:
            continue
        stated = decimal.Decimal(match.group(1))
        # A trailing number that is nowhere near a tire diameter is a part number or a price, not
        # a stated diameter. It also must not simply restate the rim.
        if not decimal.Decimal(15) <= stated <= decimal.Decimal(60):
            continue
        if stated == parsed.rim_diameter_in:
            continue
        # Numeric notation states no aspect ratio, so its diameter is derived from a convention
        # and is documented as nominal. Counting it as a parser failure would bury a real
        # regression under a known limitation -- and the two families in the data disagree with
        # each other anyway (7.50R16 measures ~97% aspect, 8.75R16.5 ~81%), so no single constant
        # fits. Reported separately instead.
        bucket = nominal if notation == tire_size.NOTATION_NUMERIC else None
        if bucket is None:
            compared += 1
        if abs(stated - ours) > ours * tolerance:
            line = "{} computed {} vs stated {} -- {}".format(size_display, ours, stated, description[:60])
            (nominal if bucket is not None else failures).append(line)
    return (
        Check(
            name="overall diameter vs distributor-stated",
            failures=len(failures),
            total=compared,
            detail="rows where a distributor stated its own diameter; tolerance {:.0%}".format(float(tolerance)),
            samples=failures[:8],
        ),
        nominal,
    )


# ==========================================================================================
# Internal consistency -- every one of these should be zero
# ==========================================================================================


def consistency_checks(brand_ids=None) -> typing.List[Check]:
    clause, params = _brand_clause(brand_ids)
    total = _scalar(
        "SELECT count(*) FROM tire_specs ts JOIN master_parts mp ON mp.id=ts.master_part_id WHERE TRUE {}".format(
            clause
        ),
        params,
    )

    def count(where: str, extra: typing.Optional[list] = None) -> int:
        return _scalar(
            "SELECT count(*) FROM tire_specs ts JOIN master_parts mp ON mp.id=ts.master_part_id "
            "WHERE ({}) {}".format(where, clause),
            (extra or []) + params,
        )

    checks = [
        Check("tire taller than its rim", count("ts.overall_diameter_in <= ts.rim_diameter_in"), total),
        Check(
            "max_load_lb matches tire_load_index",
            _scalar(
                "SELECT count(*) FROM tire_specs ts JOIN master_parts mp ON mp.id=ts.master_part_id "
                "LEFT JOIN tire_load_index li ON li.load_index = ts.load_index "
                "WHERE ts.load_index IS NOT NULL "
                "  AND ts.max_load_lb IS DISTINCT FROM round(li.max_load_kg * 2.20462262) {}".format(clause),
                params,
            ),
            total,
        ),
        Check(
            "max_speed_mph matches speed_rating",
            _scalar(
                "SELECT count(*) FROM tire_specs ts JOIN master_parts mp ON mp.id=ts.master_part_id "
                "JOIN speed_rating sr ON sr.code = ts.speed_rating "
                "WHERE ts.max_speed_mph IS DISTINCT FROM round(sr.max_speed_kmh * 0.621371) {}".format(clause),
                params,
            ),
            total,
        ),
        Check(
            "ply_rating matches load_range_ply",
            _scalar(
                "SELECT count(*) FROM tire_specs ts JOIN master_parts mp ON mp.id=ts.master_part_id "
                "JOIN load_range_ply lr ON lr.load_range = ts.load_range "
                "WHERE ts.ply_rating IS DISTINCT FROM lr.ply_rating {}".format(clause),
                params,
            ),
            total,
        ),
        Check(
            "tread_category inside the vocabulary",
            _scalar(
                "SELECT count(*) FROM tire_specs ts JOIN master_parts mp ON mp.id=ts.master_part_id "
                "LEFT JOIN tread_category tc ON tc.code = ts.tread_category "
                "WHERE ts.tread_category IS NOT NULL AND tc.code IS NULL {}".format(clause),
                params,
            ),
            total,
        ),
        Check(
            "is_3pmsf=true above the confidence gate",
            count("ts.is_3pmsf = TRUE AND (ts.llm_confidence IS NULL OR ts.llm_confidence < 0.80)"),
            total,
        ),
        Check(
            "one spec per master part",
            _scalar(
                "SELECT coalesce(sum(n-1),0) FROM (SELECT count(*) n FROM tire_specs ts "
                "JOIN master_parts mp ON mp.id=ts.master_part_id WHERE TRUE {} "
                "GROUP BY ts.master_part_id HAVING count(*) > 1) d".format(clause),
                params,
            ),
            total,
        ),
    ]

    checks.append(
        Check(
            "is_3pmsf only on categories that can carry it",
            count("ts.is_3pmsf = TRUE AND ts.tread_category = ANY(%s)", [list(_NEVER_SEVERE_SNOW)]),
            total,
            detail="a summer or track tire cannot be severe-snow certified",
        )
    )
    checks.append(
        Check(
            "vehicle_class agrees with a powersports category",
            count(
                "(ts.tread_category LIKE 'MC!_%%' ESCAPE '!' AND ts.vehicle_class IS DISTINCT FROM 'motorcycle') "
                "OR (ts.tread_category LIKE 'ATV!_%%' ESCAPE '!' AND ts.vehicle_class IS DISTINCT FROM 'atv_utv')"
            ),
            total,
        )
    )

    # Powersports leakage: a motorcycle-shaped size wearing a car category. Motorcycle sizes are
    # the ones with a 2-digit section width or an aspect ratio above 95 -- no car tire has either.
    leak = _rows(
        "SELECT ts.size_display, ts.model_name, ts.tread_category FROM tire_specs ts "
        "JOIN master_parts mp ON mp.id=ts.master_part_id "
        "WHERE (ts.section_width_mm < 100 OR ts.aspect_ratio > 95) "
        "  AND ts.tread_category = ANY(%s) {} LIMIT 200".format(clause),
        [list(_LIGHT_VEHICLE_ONLY)] + params,
    )
    checks.append(
        Check(
            "powersports size not classified as a car tire",
            len(leak),
            total,
            detail="motorcycle-shaped size carrying a light-vehicle tread code",
            samples=["{} {} -> {}".format(*row) for row in leak[:8]],
        )
    )

    return checks


def possible_abbreviations(brand_ids=None) -> typing.List[str]:
    """
    model_name values that *look* unexpanded: all caps, no digits, short.

    Advisory only, never a failure. Plenty of real tire models are exactly this shape -- LTX M/S,
    G-MAX RS, LO-PRO, HD PRO T/A -- so a non-zero count means "worth a glance", not "a bug".
    """
    clause, params = _brand_clause(brand_ids)
    return [
        row[0]
        for row in _rows(
            "SELECT DISTINCT ts.model_name FROM tire_specs ts JOIN master_parts mp ON mp.id=ts.master_part_id "
            "WHERE ts.model_name IS NOT NULL AND ts.model_name = upper(ts.model_name) "
            "  AND ts.model_name !~ '[0-9]' AND length(ts.model_name) <= 12 {} "
            "ORDER BY 1 LIMIT 200".format(clause),
            params,
        )
    ]


# ==========================================================================================
# Coverage
# ==========================================================================================


def coverage(brand_ids=None) -> typing.List[typing.Tuple[str, int, int]]:
    """``(field, populated, total)`` for the fields a facet or a spec line depends on."""
    clause, params = _brand_clause(brand_ids)
    fields = [
        ("model_name", "ts.model_name IS NOT NULL"),
        ("tread_category", "ts.tread_category IS NOT NULL"),
        ("vehicle_class", "ts.vehicle_class IS NOT NULL"),
        ("load_index", "ts.load_index IS NOT NULL"),
        ("max_load_lb", "ts.max_load_lb IS NOT NULL"),
        ("speed_rating", "ts.speed_rating IS NOT NULL"),
        ("load_range", "ts.load_range IS NOT NULL"),
        ("is_3pmsf known", "ts.is_3pmsf IS NOT NULL"),
        ("search_aliases", "cardinality(ts.search_aliases) > 0"),
    ]
    total = _scalar(
        "SELECT count(*) FROM tire_specs ts JOIN master_parts mp ON mp.id=ts.master_part_id WHERE TRUE {}".format(
            clause
        ),
        params,
    )
    out = []
    for label, predicate in fields:
        out.append(
            (
                label,
                _scalar(
                    "SELECT count(*) FROM tire_specs ts JOIN master_parts mp ON mp.id=ts.master_part_id "
                    "WHERE ({}) {}".format(predicate, clause),
                    params,
                ),
                total,
            )
        )
    return out


def per_brand(limit: int = 40) -> typing.List[tuple]:
    """
    Every brand that has tire specs, with the coverage that matters per brand.

    Ordered by size, because a 2% category gap on Michelin is a different problem from a 2% gap
    on a brand with 12 SKUs.
    """
    return _rows(
        """
        SELECT b.name,
               count(*)                                                   AS specs,
               count(*) FILTER (WHERE ts.model_name IS NOT NULL)          AS with_model,
               count(*) FILTER (WHERE ts.tread_category IS NOT NULL)      AS with_category,
               count(*) FILTER (WHERE ts.size_disputed)                   AS disputed,
               count(DISTINCT lower(ts.model_name))                       AS models
        FROM tire_specs ts
        JOIN master_parts mp ON mp.id = ts.master_part_id
        JOIN brands b ON b.id = mp.brand_id
        GROUP BY b.name
        ORDER BY specs DESC
        LIMIT %s
        """,
        [limit],
    )


def split_votes() -> typing.List[tuple]:
    """Models whose sizes still disagree on a category -- the review queue after reconciliation."""
    return _rows(
        """
        SELECT b.name, ts.model_name, count(DISTINCT ts.tread_category) AS categories, count(*) AS sizes
        FROM tire_specs ts
        JOIN master_parts mp ON mp.id = ts.master_part_id
        JOIN brands b ON b.id = mp.brand_id
        WHERE ts.model_name IS NOT NULL AND ts.tread_category IS NOT NULL
        GROUP BY b.name, lower(ts.model_name), ts.model_name
        HAVING count(DISTINCT ts.tread_category) > 1
        ORDER BY sizes DESC
        LIMIT 30
        """
    )
