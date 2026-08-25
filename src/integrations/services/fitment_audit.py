"""
Audit ``MasterPartFitment`` coverage: how much of the catalog has vehicle fitment, which brands
have none, and -- the part that makes the number mean anything -- which of those brands were
never supposed to have any.

The rules live in ``src.integrations.utils.fitment_expectation``; this module is the machinery
that feeds them the catalog and rolls the answers up per brand.

**Read-only.** Nothing here writes to a permanent table. The staging work happens in session
TEMP tables that vanish when the connection closes, because the output of an audit is a report,
not a column -- if a fitment expectation ever needs to be stored per part, that is a migration
and a decision, not a side effect of running a report.

Why SQL rather than a Python scan: the rules are small closed vocabularies (a few hundred
strings) and the catalog is 3.2M master parts across 4.5M provider rows. Pushing the
vocabularies down as VALUES lists and letting Postgres do the join turns a multi-hour row-by-row
pass into a couple of minutes, and keeps the rules themselves in one place -- the Python dicts
are still the only definition, this module just serialises them.

The distinction the whole report turns on, for a brand with no fitment:

  ``not_applicable``  tires, wheels, apparel, helmets, tools, fluids -- correctly empty
  ``expected``, no capable provider   vehicle-specific, but no feed we pull carries fitment
                                      for it. A sourcing gap: ASAP Network is the existing
                                      answer (see ``asap.brands_with_enrichment_gap``).
  ``expected``, has capable provider  we pull a feed that ships fitment and still got none.
                                      A pipeline or matching bug, and the most actionable row
                                      in the report.
  ``unknown``                         no signal decided. Reported, never assumed either way.
"""
import logging
import typing

from django.db import connection, transaction

from src import enums as src_enums
from src.integrations.utils import fitment_expectation as fe

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[FITMENT-AUDIT]"

# Providers whose feeds actually carry vehicle fitment -- confirmed against
# master_part_fitments.source_provider_id, which holds exactly these three and nothing else
# (Turn 14 14.3M rows, ASAP Network 905k, Rough Country 273k). A vehicle-specific part sold
# only through anything else cannot have fitment no matter how correct the rest of the pipeline
# is, and that difference is the whole point of splitting the gap in two.
FITMENT_CAPABLE_PROVIDERS = ("Turn 14", "ASAP Network", "Rough Country")

# Feeds whose entire catalog is one non-fitted product class, keyed by BrandProviderKind.
_CATALOG_LEVEL_SOURCES = (
    (src_enums.BrandProviderKind.ELITE_WHEEL.value, "elite_wheel:catalog"),
    (src_enums.BrandProviderKind.THE_WHEEL_GROUP.value, "the_wheel_group:catalog"),
    (src_enums.BrandProviderKind.VOSSEN.value, "vossen:catalog"),
    (src_enums.BrandProviderKind.TIRERACK.value, "tirerack:catalog"),
    (src_enums.BrandProviderKind.HELMHOUSE.value, "helmet_house:catalog"),
)

UNKNOWN = "unknown"


def _values_list(pairs: typing.Sequence[typing.Tuple[str, str, str]]) -> str:
    """Render (key, expectation, source) triples as a SQL VALUES body. Keys are vocabulary
    strings from our own rule tables, never user input, but they are still passed through
    quoting rather than concatenated raw."""
    return ", ".join("(%s, %s, %s)" for _ in pairs)


def _flatten(pairs: typing.Sequence[typing.Tuple[str, str, str]]) -> typing.List[str]:
    return [item for row in pairs for item in row]


def _turn14_rules() -> typing.List[typing.Tuple[str, str, str]]:
    return [
        (category, entry.expectation, "turn14:{}".format(category))
        for category, entry in fe.TURN14_CATEGORY_EXPECTATION.items()
        if entry.expectation is not None
    ]


def _premier_rules() -> typing.List[typing.Tuple[str, str, str]]:
    return [
        (category, expectation, "premier:{}".format(category))
        for category, expectation in fe.PREMIER_CATEGORY_EXPECTATION.items()
        if expectation is not None
    ]


def _meyer_rules() -> typing.List[typing.Tuple[str, str, str]]:
    return [
        (category, expectation, "meyer:{}".format(category))
        for category, expectation in fe.MEYER_CATEGORY_EXPECTATION.items()
        if expectation is not None
    ]


def _wps_rules() -> typing.List[typing.Tuple[str, str, str]]:
    return [
        (product_type, expectation, "wps:{}".format(product_type))
        for product_type, expectation in fe.WPS_PRODUCT_TYPE_EXPECTATION.items()
        if expectation is not None
    ]


def _build_base_tables(cursor) -> None:
    """``fitted_mp``, ``capable_mp`` and ``mp_flags``: one row per master part with the two
    booleans every later query needs."""
    cursor.execute("create temp table fitted_mp on commit drop as "
                   "select distinct master_part_id from master_part_fitments")
    cursor.execute("create unique index on fitted_mp (master_part_id)")

    # Built as its own set rather than a correlated EXISTS per master part: the EXISTS form is
    # 3.2M index probes and runs for tens of minutes, this scans ~800k provider_parts rows once.
    cursor.execute(
        "create temp table capable_mp on commit drop as "
        "select distinct pp.master_part_id from provider_parts pp "
        "join providers p on p.id = pp.provider_id where p.name in %s",
        [FITMENT_CAPABLE_PROVIDERS],
    )
    cursor.execute("create unique index on capable_mp (master_part_id)")

    cursor.execute("""
        create temp table mp_flags on commit drop as
        select mp.id,
               mp.brand_id,
               (f.master_part_id is not null) as has_fit,
               (c.master_part_id is not null) as fit_capable
        from master_parts mp
        left join fitted_mp f on f.master_part_id = mp.id
        left join capable_mp c on c.master_part_id = mp.id
    """)
    cursor.execute("create index on mp_flags (brand_id)")
    cursor.execute("analyze fitted_mp")
    cursor.execute("analyze capable_mp")
    cursor.execute("analyze mp_flags")


# ``provider_external_id`` is "<raw_brand_id>_<key>" for every distributor except Turn 14 and
# Meyer, which store the bare key. Same shape the product_type collector splits apart -- see
# _COMPOSITE_BATCH_CTE there. The regex guard makes a malformed id yield NULL and simply fail to
# join, rather than aborting the statement on a bad ::bigint cast.
_COMPOSITE_BRAND = ("case when pp.provider_external_id ~ '^[0-9]+_' "
                    "then split_part(pp.provider_external_id, '_', 1)::bigint end")
_COMPOSITE_KEY = ("case when pp.provider_external_id ~ '^[0-9]+_' "
                  "then substr(pp.provider_external_id, strpos(pp.provider_external_id, '_') + 1) end")


def _build_candidates(cursor) -> None:
    """
    One row per (master part, deciding signal). A part sold by four distributors contributes up
    to four candidates, and ``_resolve`` keeps the best tier only where it is unanimous -- same
    contract as the product_type resolver, and left as separate rows rather than collapsed in
    the query so a surprising verdict can be traced back to the feed that produced it.

    Providers are matched on ``providers.kind`` (the BrandProviderKind enum) rather than on
    ``name``, since the display name is editable data and the kind is what the rest of the
    integration layer keys on.
    """
    cursor.execute("create temp table fe_candidates (master_part_id bigint, "
                   "expectation text, source text, tier int) on commit drop")

    # Turn 14: the category is denormalised onto provider_parts already (794k of 796k rows),
    # so this needs no join out to turn14_items at all.
    turn14 = _turn14_rules()
    cursor.execute("""
        insert into fe_candidates
        select pp.master_part_id, r.expectation, r.source, 1
        from provider_parts pp
        join providers p on p.id = pp.provider_id and p.kind = %s
        join (values {}) as r(category, expectation, source) on r.category = pp.category
    """.format(_values_list(turn14)),
        [src_enums.BrandProviderKind.TURN_14.value] + _flatten(turn14))

    premier = _premier_rules()
    cursor.execute("""
        insert into fe_candidates
        select pp.master_part_id, r.expectation, r.source, 1
        from provider_parts pp
        join providers p on p.id = pp.provider_id and p.kind = %s
        join premier_parts raw
          on raw.brand_id = {brand} and raw.premier_part_number = {key}
        join (values {rules}) as r(category, expectation, source)
          on r.category = raw.part_category
    """.format(brand=_COMPOSITE_BRAND, key=_COMPOSITE_KEY, rules=_values_list(premier)),
        [src_enums.BrandProviderKind.PREMIER_PERFORMANCE.value] + _flatten(premier))

    # Meyer stores the bare ``meyer_part`` as provider_external_id, and that key is NOT unique
    # in meyer_parts: 54,881 values exist under two MeyerBrands. Without the brand scope the
    # join fans out and stages contradictory candidates for one part -- the same trap
    # product_type_classification._Source.brand_scope_sql exists to avoid.
    meyer = _meyer_rules()
    cursor.execute("""
        insert into fe_candidates
        select pp.master_part_id, r.expectation, r.source, 1
        from provider_parts pp
        join providers p on p.id = pp.provider_id and p.kind = %s
        join master_parts mp on mp.id = pp.master_part_id
        join meyer_parts raw
          on raw.meyer_part = pp.provider_external_id
         and raw.brand_id in (
             select m.meyer_brand_id from brand_meyer_brand_mapping m where m.brand_id = mp.brand_id
         )
        join (values {rules}) as r(category, expectation, source) on r.category = raw.category
    """.format(rules=_values_list(meyer)),
        [src_enums.BrandProviderKind.MEYER.value] + _flatten(meyer))

    wps = _wps_rules()
    cursor.execute("""
        insert into fe_candidates
        select pp.master_part_id, r.expectation, r.source, 2
        from provider_parts pp
        join providers p on p.id = pp.provider_id and p.kind = %s
        join wps_items raw on raw.brand_id = {brand} and raw.sku = {key}
        join (values {rules}) as r(product_type, expectation, source)
          on r.product_type = raw.product_type
    """.format(brand=_COMPOSITE_BRAND, key=_COMPOSITE_KEY, rules=_values_list(wps)),
        [src_enums.BrandProviderKind.WESTERN_POWER_SPORTS.value] + _flatten(wps))

    # Wheel Pros: feed_type separates the three SFTP feeds structurally. All three are
    # sized-or-universal rather than YMM-fitted.
    cursor.execute("""
        insert into fe_candidates
        select pp.master_part_id, %s, 'wheelpros:' || raw.feed_type, 2
        from provider_parts pp
        join providers p on p.id = pp.provider_id and p.kind = %s
        join wheelpros_parts raw on raw.brand_id = {brand} and raw.part_number = {key}
        where raw.feed_type in ('wheel', 'tire', 'accessories')
    """.format(brand=_COMPOSITE_BRAND, key=_COMPOSITE_KEY),
        [fe.NOT_APPLICABLE, src_enums.BrandProviderKind.WHEELPROS.value])

    # Single-purpose catalogs: the provider itself is the signal, no raw-table join needed.
    for kind, source in _CATALOG_LEVEL_SOURCES:
        cursor.execute("""
            insert into fe_candidates
            select pp.master_part_id, %s, %s, 2
            from provider_parts pp
            join providers p on p.id = pp.provider_id and p.kind = %s
        """, [fe.NOT_APPLICABLE, source, kind])

    cursor.execute("create index on fe_candidates (master_part_id)")
    cursor.execute("analyze fe_candidates")


def _resolve(cursor) -> None:
    """
    Keep the best (lowest) tier per master part and accept it only where unanimous. Everything
    else -- no candidate at all, or a disagreement at the best tier -- lands as ``unknown``.
    """
    cursor.execute("""
        create temp table fe_resolved on commit drop as
        with best as (
            select master_part_id, min(tier) as tier
            from fe_candidates group by master_part_id
        ),
        agreed as (
            select c.master_part_id,
                   min(c.expectation) as expectation,
                   min(c.source) as source,
                   count(distinct c.expectation) as n_distinct
            from fe_candidates c
            join best b on b.master_part_id = c.master_part_id and b.tier = c.tier
            group by c.master_part_id
        )
        select m.id as master_part_id,
               m.brand_id,
               m.has_fit,
               m.fit_capable,
               case when a.n_distinct = 1 then a.expectation else %s end as expectation,
               case when a.n_distinct = 1 then a.source else null end as source
        from mp_flags m
        left join agreed a on a.master_part_id = m.id
    """, [UNKNOWN])
    cursor.execute("create index on fe_resolved (brand_id)")
    cursor.execute("analyze fe_resolved")


def run() -> typing.Dict[str, typing.Any]:
    """
    Build the audit and return it.

    ``transaction.atomic`` is load-bearing, not decoration: Django runs in autocommit, and
    under autocommit an ``ON COMMIT DROP`` temp table is dropped by the commit of its own
    CREATE statement, so every later query would fail on a table that no longer exists. The
    block is also what guarantees the staging is gone when the audit ends -- the point of a
    read-only report is that it leaves nothing behind.
    """
    with transaction.atomic(), connection.cursor() as cursor:
        logger.info("{} building base tables".format(_LOG_PREFIX))
        _build_base_tables(cursor)
        logger.info("{} collecting expectation candidates".format(_LOG_PREFIX))
        _build_candidates(cursor)
        logger.info("{} resolving".format(_LOG_PREFIX))
        _resolve(cursor)
        logger.info("{} rolling up".format(_LOG_PREFIX))

        cursor.execute("""
            select expectation, has_fit, fit_capable, count(*)
            from fe_resolved group by 1, 2, 3 order by 4 desc
        """)
        part_totals = cursor.fetchall()

        cursor.execute("""
            select b.id, b.name,
                   count(*)                                                          as parts,
                   count(*) filter (where r.has_fit)                                 as fitted,
                   count(*) filter (where r.expectation = 'expected')                as expect_parts,
                   count(*) filter (where r.expectation = 'not_applicable')          as na_parts,
                   count(*) filter (where r.expectation = %s)                        as unknown_parts,
                   count(*) filter (where r.expectation = 'expected'
                                      and not r.has_fit and r.fit_capable)           as bug_parts,
                   count(*) filter (where r.expectation = 'expected'
                                      and not r.has_fit and not r.fit_capable)       as sourcing_parts,
                   count(*) filter (where r.fit_capable)                             as capable_parts
            from brands b
            join fe_resolved r on r.brand_id = b.id
            group by b.id, b.name
            order by parts desc
        """, [UNKNOWN])
        brand_rows = cursor.fetchall()

        cursor.execute("""
            select coalesce(source, '(undecided)'), expectation,
                   count(*), count(*) filter (where has_fit)
            from fe_resolved group by 1, 2 order by 3 desc
        """)
        by_source = cursor.fetchall()

    return {
        "part_totals": part_totals,
        "brands": brand_rows,
        "by_source": by_source,
    }


def classify_brand(row: typing.Sequence) -> str:
    """
    One label per brand for the report's headline table. Order matters: a brand is only called
    ``ok`` once there is nothing left to explain.

    ``row`` is a tuple from ``run()["brands"]``.
    """
    (_id, _name, parts, fitted, expect_parts, na_parts,
     unknown_parts, bug_parts, sourcing_parts, _capable) = row

    if fitted > 0 and bug_parts == 0 and sourcing_parts == 0:
        return "ok"
    if expect_parts == 0 and na_parts > 0 and unknown_parts == 0:
        return "not_applicable"
    if bug_parts > 0:
        return "pipeline_gap"
    if sourcing_parts > 0:
        return "sourcing_gap"
    if unknown_parts == parts:
        return "unknown"
    return "mixed"
