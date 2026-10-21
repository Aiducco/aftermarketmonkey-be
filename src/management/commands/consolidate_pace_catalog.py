"""
Build the consolidated PACE catalog -- pace_parts / pace_brands / pace_categories /
pace_subcategories / pace_series -- by deduplicating the nine per-member pace_<member>_*
landing tables.

Read-only over the member tables; it only writes the five consolidated ones. Idempotent: each
table is rebuilt from scratch inside one transaction, so re-running after another ingest pass
simply refreshes it.

WHY object_id IS THE DEDUP KEY
------------------------------
Verified, not assumed: of the 197,799 parts that Tri-State and DPN both carry, ALL 197,799 have
byte-identical ``object_id`` values. It hashes the shared catalog identity, so fields travelling
with it agree across members essentially always -- part 100%, name 100%, image 100%, ext 99.99%,
series 99.99%, fitment 99.92%.

WHAT IS RESOLVED, AND WHAT IS DROPPED
-------------------------------------
``category`` and ``subcategory`` do NOT travel with the hash -- they diverge on 15% of shared
parts, because each distributor files the same part its own way. They are resolved by
most-frequent-wins (Postgres ``mode()``), with the per-member tables left authoritative for any
single distributor's own filing.

``price``, ``inventory``, ``viewed`` and ``sales`` are deliberately absent. price turned out to
be a shared list price rather than any distributor's actual cost (it agrees 100% across
members), and the rest are per-member stock/telemetry. Real pricing comes from each
distributor's own API, not from this search index.

WHY THE TAXONOMY IS DERIVED FROM THE PARTS
------------------------------------------
Each PACE_<member>_series index holds 9,396-16,845 documents but a plain walk is capped at
1,000, so the per-member series tables are badly truncated. Every part carries its series name
though, so reading the parts recovers the complete unique set -- which is the whole reason these
tables are built this way rather than from the taxonomy indexes. Series rows carry no parts
array; the link is pace_parts.series.

Subcategory names are not unique on their own (the same name recurs under several categories),
so pace_subcategories is keyed on (category, name). Same for series, keyed on
(aaia_brand_code, name).

HOW IT RUNS, and why it is two phases
-------------------------------------
Phase 1 does all the expensive aggregation into UNLOGGED staging tables, touching none of the
live ones. Phase 2 is a single short transaction that truncates each live table and copies its
staging rows in.

The reason is that ``TRUNCATE`` takes an ACCESS EXCLUSIVE lock held until the transaction
commits. Doing the aggregation inside that transaction -- as the first version did -- locked
pace_parts for the entire ~15-minute run: count(*), a LIMIT 1, even pg_total_relation_size all
blocked, so the job was completely unobservable while it ran and anything querying the
consolidated catalog would have hung. Splitting it keeps the exclusive lock down to the time it
takes to copy prebuilt rows.

    python manage.py consolidate_pace_catalog
    python manage.py consolidate_pace_catalog --member tse --member dpn
    python manage.py consolidate_pace_catalog --dry-run
"""
import logging
import time

from django.core.management.base import BaseCommand, CommandError
from django.db import connection, transaction

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[CONSOLIDATE-PACE]"

MEMBERS = ["tse", "dpn", "lor", "tft", "a1d", "apd", "aas", "rpt", "mww"]


def _parts_union(members: list[str]) -> str:
    """Each member's parts table unioned with its own member code attached."""
    return " union all ".join(
        """select object_id, catalog_object_id, aaia_brand_code, part, brand, name, ext, slug,
                  image, has_image, category, subcategory, series, aces, part_search, makes,
                  models, years, fitment, '{code}' as mem
             from pace_{m}_parts""".format(m=m, code=m.upper())
        for m in members
    )


def _brands_union(members: list[str]) -> str:
    return " union all ".join(
        "select aaia_brand_code, slug, image from pace_{m}_brands".format(m=m) for m in members
    )

# mode() gives the most common value, which is how category/subcategory disagreement is
# resolved. Empty strings are filtered out first so a member that simply has no category for a
# part cannot outvote one that does.
_PARTS_COLS = (
    "object_id, catalog_object_id, aaia_brand_code, part, brand, name, ext, slug, image, "
    "has_image, category, subcategory, series, aces, part_search, makes, models, years, "
    "fitment, has_real_aaia_code, member_codes, member_count, created_at, updated_at")

_PARTS_SQL = """
select
    object_id                                   as object_id,
    min(catalog_object_id)                      as catalog_object_id,
    min(aaia_brand_code)                        as aaia_brand_code,
    min(part)                                   as part,
    coalesce(max(brand), '')                    as brand,
    coalesce(max(name), '')                     as name,
    coalesce(max(ext), '')                      as ext,
    coalesce(max(slug), '')                     as slug,
    coalesce(max(image), '')                    as image,
    bool_or(has_image)                          as has_image,
    coalesce(mode() within group (order by nullif(category, '')), '')    as category,
    coalesce(mode() within group (order by nullif(subcategory, '')), '') as subcategory,
    -- series agrees across members on 99.99% of shared parts, so max() is enough; only
    -- category/subcategory genuinely diverge (15%) and need mode().
    coalesce(max(nullif(series, '')), '')       as series,
    max(aces)                                  as aces,
    -- Plain max() rather than "longest array wins". JSONB has a total order in Postgres so
    -- this is deterministic, and it avoids five per-group ordered-set sorts plus a
    -- jsonb_array_length() parse on every one of 5.25M rows -- which took >39 minutes and
    -- bought nothing: ~90% of parts are carried by a single distributor (one candidate row,
    -- nothing to order) and across members these four fields agree 99.9%+ anyway.
    -- max() over the text form: Postgres has comparison operators for jsonb but no
    -- max(jsonb) aggregate, so the cast is required. Still a plain scan, no per-group sort.
    coalesce(max(part_search::text)::jsonb, '[]'::jsonb)   as part_search,
    coalesce(max(makes::text)::jsonb,       '[]'::jsonb)   as makes,
    coalesce(max(models::text)::jsonb,      '[]'::jsonb)   as models,
    coalesce(max(years::text)::jsonb,       '[]'::jsonb)   as years,
    coalesce(max(fitment::text)::jsonb,     '{{}}'::jsonb) as fitment,
    -- A6/A7/A8 prefixes are AAM member-local jobber line codes, not AAIA codes. Decided by
    -- the code alone because the evidence is categorical: not one of the 1,028 such codes
    -- appears in more than one member, against 320 of 628 real codes shared across members.
    not (min(aaia_brand_code) ~ '^A[678]')     as has_real_aaia_code,
    to_jsonb(array_agg(distinct mem order by mem)) as member_codes,
    count(distinct mem)                        as member_count,
    now()                                      as created_at,
    now()                                      as updated_at
from ({union}) u
group by object_id
"""

# Brands come from the parts, not the members' brands tables: a brand only matters here if a
# part actually references it, and the brands tables carry codes with no parts.
_BRANDS_COLS = ("aaia_brand_code, name, slug, image, has_real_aaia_code, "
                "member_codes, part_count, created_at, updated_at")

_BRANDS_SQL = """
select p.aaia_brand_code                            as aaia_brand_code,
       coalesce(max(p.brand), '')                   as name,
       coalesce(max(b.slug), '')                    as slug,
       coalesce(max(b.image), '')                   as image,
       not (p.aaia_brand_code ~ '^A[678]')          as has_real_aaia_code,
       to_jsonb(array_agg(distinct m order by m))   as member_codes,
       count(*)                                     as part_count,
       now()                                        as created_at,
       now()                                        as updated_at
from (select aaia_brand_code, brand, jsonb_array_elements_text(member_codes) as m
        from {parts_src}) p
left join ({brand_union}) b on b.aaia_brand_code = p.aaia_brand_code
where p.aaia_brand_code <> ''
group by p.aaia_brand_code
"""

_CATEGORIES_COLS = "name, member_codes, part_count, created_at, updated_at"

_CATEGORIES_SQL = """
select category                                   as name,
       to_jsonb(array_agg(distinct m order by m))  as member_codes,
       count(*)                                    as part_count,
       now() as created_at, now() as updated_at
from (select category, jsonb_array_elements_text(member_codes) as m
        from {parts_src} where category <> '') x
group by category
"""

_SUBCATEGORIES_COLS = "category, name, member_codes, part_count, created_at, updated_at"

_SUBCATEGORIES_SQL = """
select category                                   as category,
       subcategory                                 as name,
       to_jsonb(array_agg(distinct m order by m))  as member_codes,
       count(*)                                    as part_count,
       now() as created_at, now() as updated_at
from (select category, subcategory, jsonb_array_elements_text(member_codes) as m
        from {parts_src} where subcategory <> '') x
group by category, subcategory
"""

_SERIES_COLS = ("aaia_brand_code, brand, name, member_codes, part_count, "
               "created_at, updated_at")

_SERIES_SQL = """
select aaia_brand_code                            as aaia_brand_code,
       coalesce(max(brand), '')                    as brand,
       series                                      as name,
       to_jsonb(array_agg(distinct m order by m))  as member_codes,
       count(*)                                    as part_count,
       now() as created_at, now() as updated_at
from (select aaia_brand_code, brand, series, jsonb_array_elements_text(member_codes) as m
        from {parts_src} where series <> '') x
group by aaia_brand_code, series
"""

_STAGING_PREFIX = "stg_"


def _steps(members: list[str]) -> list[tuple[str, str, str]]:
    """(live table, column list, SELECT) in dependency order.

    Order matters: the four taxonomy tables read back out of the parts result, so parts first.
    In phase 1 they read the staging parts table, which is why the SELECTs take {parts_src}.
    """
    parts_src = _STAGING_PREFIX + "pace_parts"
    return [
        ("pace_parts", _PARTS_COLS, _PARTS_SQL.format(union=_parts_union(members))),
        ("pace_brands", _BRANDS_COLS,
         _BRANDS_SQL.format(brand_union=_brands_union(members), parts_src=parts_src)),
        ("pace_categories", _CATEGORIES_COLS, _CATEGORIES_SQL.format(parts_src=parts_src)),
        ("pace_subcategories", _SUBCATEGORIES_COLS, _SUBCATEGORIES_SQL.format(parts_src=parts_src)),
        ("pace_series", _SERIES_COLS, _SERIES_SQL.format(parts_src=parts_src)),
    ]


class Command(BaseCommand):
    help = (
        "Rebuild the consolidated PACE catalog (pace_parts/brands/categories/subcategories/"
        "series) by deduplicating the nine per-member pace_<member>_* tables on object_id. "
        "Idempotent -- re-run after any ingest pass."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--member", action="append", metavar="CODE",
            help="Consolidate only these members (repeatable). Omit for all nine. NOTE: this "
                 "is still a full rebuild -- the consolidated tables will afterwards contain "
                 "ONLY the members given, so re-run without --member once every ingest is "
                 "complete.")
        parser.add_argument("--dry-run", action="store_true",
                            help="Report what each member table holds, write nothing.")

    def handle(self, *args, **options):
        requested = [c.strip().lower() for c in (options["member"] or []) if c and c.strip()]
        unknown = [c for c in requested if c not in MEMBERS]
        if unknown:
            raise CommandError("unknown member(s) %s -- known: %s" % (
                ", ".join(unknown), ", ".join(MEMBERS)))
        members = requested or list(MEMBERS)
        if requested:
            self.stdout.write(self.style.WARNING(
                "PARTIAL rebuild: only %s. The consolidated tables will contain just these "
                "members until re-run without --member." % ", ".join(c.upper() for c in members)))

        with connection.cursor() as cursor:
            cursor.execute("select %s" % ", ".join(
                "(select count(*) from pace_%s_parts)" % m for m in members))
            counts = cursor.fetchone()
        total = sum(counts)
        self.stdout.write("member parts tables:")
        for m, n in zip(members, counts):
            self.stdout.write("  pace_%s_parts %12s" % (m, format(n, ",")))
        self.stdout.write("  %-18s %12s rows to deduplicate" % ("TOTAL", format(total, ",")))
        self.stdout.flush()

        if options["dry_run"]:
            self.stdout.write(self.style.WARNING("dry run -- nothing written"))
            return

        started = time.monotonic()
        plan = _steps(members)

        # PHASE 1 -- all the expensive aggregation into UNLOGGED staging tables. Nothing here
        # touches a live table, so the consolidated catalog stays readable throughout and
        # progress is observable. Unlogged skips WAL, which is safe because these are rebuilt
        # from scratch every run and are dropped at the end.
        self.stdout.write("phase 1/2 -- building staging tables")
        self.stdout.flush()
        staged = {}
        with connection.cursor() as cursor:
            for table, cols, sql in plan:
                stg = _STAGING_PREFIX + table
                step = time.monotonic()
                cursor.execute("drop table if exists %s" % stg)
                cursor.execute("create unlogged table %s as %s" % (stg, sql))
                cursor.execute("select count(*) from %s" % stg)
                n = cursor.fetchone()[0]
                staged[table] = (stg, cols, n)
                self.stdout.write("  %-24s %12s rows  (%.0fs)" % (
                    stg, format(n, ","), time.monotonic() - step))
                self.stdout.flush()
                logger.info("%s %s: %s rows", _LOG_PREFIX, stg, n)

        # PHASE 2 -- one short transaction that swaps the prebuilt rows in. TRUNCATE holds an
        # ACCESS EXCLUSIVE lock until commit, so this is kept to the time it takes to copy
        # already-computed rows rather than wrapping the whole aggregation.
        self.stdout.write("phase 2/2 -- swapping into the live tables")
        self.stdout.flush()
        swap = time.monotonic()
        with transaction.atomic():
            with connection.cursor() as cursor:
                cursor.execute("truncate pace_parts, pace_brands, pace_categories, "
                               "pace_subcategories, pace_series")
                for table, cols, _sql in plan:
                    stg, _c, _n = staged[table]
                    cursor.execute("insert into %s (%s) select %s from %s" % (
                        table, cols, cols, stg))
        self.stdout.write("  swapped in %.0fs (exclusive lock held only for this)" % (
            time.monotonic() - swap))
        self.stdout.flush()

        with connection.cursor() as cursor:
            for table, _cols, _sql in plan:
                cursor.execute("select count(*) from %s" % table)
                self.stdout.write(self.style.SUCCESS(
                    "  %-20s %12s rows" % (table, format(cursor.fetchone()[0], ","))))
            for table, _cols, _sql in plan:
                cursor.execute("drop table if exists %s" % (_STAGING_PREFIX + table))
        self.stdout.flush()

        elapsed = time.monotonic() - started
        with connection.cursor() as cursor:
            cursor.execute("select count(*), count(*) filter (where member_count > 1), "
                           "count(*) filter (where aces = 1), "
                           "count(*) filter (where has_real_aaia_code) from pace_parts")
            parts, shared, fitted, aaia = cursor.fetchone()
        self.stdout.write("")
        self.stdout.write(self.style.SUCCESS("Done in %.0fs" % elapsed))
        self.stdout.write("  %s unique parts from %s member rows (%s collapsed, %.1f%%)" % (
            format(parts, ","), format(total, ","), format(total - parts, ","),
            100 * (total - parts) / total if total else 0))
        self.stdout.write("  %s part(s) carried by more than one distributor" % format(shared, ","))
        self.stdout.write("  %s part(s) with ACES fitment (aces = 1)" % format(fitted, ","))
        self.stdout.write("  %s AAIA-coded part(s), %s on AAM member-local line codes "
                          "(kept and flagged, not dropped)" % (
                              format(aaia, ","), format(parts - aaia, ",")))
