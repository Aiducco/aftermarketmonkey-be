"""
Push PACE's ACES fitment onto MasterPart rows, then refresh the Meilisearch indexes that the
fitment filter reads.

Source is ``pace_parts`` (the consolidated nine-member PACE catalog, see consolidate_pace_catalog)
rather than pace_catalog, and brands resolve through ``brand_pace_brand_mapping`` rather than a
raw aaia_code comparison -- which is what makes this worth running: on an aaia_code join only
235,602 fitment-bearing PACE parts reach a MasterPart, against 307,043 through the mapping,
because PACE and Brands assign different codes to the same company.

WHAT IT RESOLVES, AND WHAT IT CANNOT

Only ``(year, make, model)``. A part's ``fitment`` blob holds BaseVehicleID and SubModelID as two
independent FLAT lists covering everywhere the part fits -- they are not paired per application,
so there is no way to tell which SubModelID belongs with which BaseVehicleID. submodel, engine
and drive_type are therefore left blank, matching MasterPartFitment's own "unknown" default.

That is still exact, not approximate: a VCdb BaseVehicle IS a (year, make, model) triple by
definition, so decoding BaseVehicleID through vcdb_vehicles is unambiguous. What must never be
done instead is deriving fitment from the part's ``makes``/``models``/``years`` arrays -- those
are three independent flattened sets, and cross-producting them fabricates combinations that do
not exist (for CRK2046: 264 combinations of which 205 were never built, e.g. "Cadillac Silverado
1500"). See PacePartBase.makes for the full note.

HOW IT RUNS

The heavy lifting is SQL, not a Python row loop, because the input expands enormously: ~307k
matched parts whose BaseVehicleID lists run from a handful to thousands of entries each. Four
steps, each into an unlogged staging table so nothing live is locked while they build:

  1. master_parts keyed by (brand_id, normalized part number)
  2. pace_parts fitment expanded to one row per (master_part_id, year, make, model), joining
     through brand_pace_brand_mapping and decoding BaseVehicleID via vcdb_vehicles
  3. years collapsed into contiguous ranges per (master_part_id, make, model) -- gaps and
     islands, so a part fitting 2010-2012 and 2015 yields two rows rather than one 2010-2015
     row that would claim three model years it does not fit
  4. insert into master_part_fitments, skipping rows that already exist

Collapsing globally rather than per batch matters: several PACE parts can map to one MasterPart
(duplicate part-number spellings in master_parts), and batch-local collapsing would emit
overlapping ranges for the same vehicle.

source_provider is left NULL -- the fitment comes from PACE's shared catalog, not any one
distributor's feed.

AFTERWARDS, BOTH MEILISEARCH INDEXES ARE REFRESHED

``fitment_keys`` on the parts index is derived from MasterPartFitment rows at index time, so new
fitment is invisible to the filter until the parts index is rebuilt. The ``vehicles`` reference
index -- the year/make/model list the filter's pickers read -- is built from the same table and
would otherwise not offer the newly covered vehicles at all. Skipping either leaves the filter
half-working, so both run unless --skip-reindex is passed.

    python manage.py sync_master_part_fitments_from_pace --dry-run
    python manage.py sync_master_part_fitments_from_pace
    python manage.py sync_master_part_fitments_from_pace --skip-reindex
"""
import logging
import time

from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import connection, transaction

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[PACE-FITMENT]"

# Mirrors src.integrations.utils.part_numbers.normalize_part_number (upper, strip non-alnum).
# Kept as SQL because the join runs over millions of rows; the Python function stays the
# reference definition and this must track it.
_NORM = "upper(regexp_replace(%s, '[^A-Za-z0-9]', '', 'g'))"

_SQL_MP_KEYS = """
create unlogged table stg_pf_mp_keys as
select id as master_part_id, brand_id, {np} as np
from master_parts
where part_number is not null and part_number <> ''
""".format(np=_NORM % "part_number")

# One row per (master_part, vehicle) the part fits. jsonb_array_elements_text handles the
# BaseVehicleID array; parts whose BaseVehicleID is a bare number rather than an array are
# picked up by the second branch of the union.
_SQL_EXPAND = """
create unlogged table stg_pf_expanded as
with pace_fit as (
    select p.object_id,
           m.brand_id,
           {np} as np,
           (jsonb_array_elements_text(p.fitment->'BaseVehicleID'))::bigint as bvid
    from pace_parts p
    join pace_brands pb on pb.aaia_brand_code = p.aaia_brand_code
    join brand_pace_brand_mapping m on m.pace_brand_id = pb.id
    where p.aces = 1
      and jsonb_typeof(p.fitment->'BaseVehicleID') = 'array'
    union all
    select p.object_id,
           m.brand_id,
           {np} as np,
           (p.fitment->>'BaseVehicleID')::bigint as bvid
    from pace_parts p
    join pace_brands pb on pb.aaia_brand_code = p.aaia_brand_code
    join brand_pace_brand_mapping m on m.pace_brand_id = pb.id
    where p.aces = 1
      and jsonb_typeof(p.fitment->'BaseVehicleID') = 'number'
)
select distinct k.master_part_id, v.year, v.make, v.model
from pace_fit f
join stg_pf_mp_keys k on k.brand_id = f.brand_id and k.np = f.np
join (
    select distinct base_vehicle_id, year, make, model from vcdb_vehicles
) v on v.base_vehicle_id = f.bvid
where v.make <> '' and v.model <> ''
""".format(np=_NORM % "p.part")

# Gaps and islands: subtracting a dense row_number from the year turns each contiguous run into
# a constant group key, so min/max per group gives the ranges.
_SQL_RANGES = """
create unlogged table stg_pf_ranges as
select master_part_id, make, model,
       min(year) as year_start,
       max(year) as year_end
from (
    select master_part_id, make, model, year,
           year - row_number() over (
               partition by master_part_id, make, model order by year
           ) as island
    from stg_pf_expanded
) g
group by master_part_id, make, model, island
"""

_SQL_INSERT = """
insert into master_part_fitments
    (master_part_id, year_start, year_end, make, model, submodel, engine, drive_type,
     source_provider_id, created_at, updated_at)
select r.master_part_id, r.year_start, r.year_end, r.make, r.model, '', '', '',
       null, now(), now()
from stg_pf_ranges r
on conflict (master_part_id, year_start, year_end, make, model, submodel, engine, drive_type)
do nothing
"""

_STAGING = ("stg_pf_ranges", "stg_pf_expanded", "stg_pf_mp_keys")


class Command(BaseCommand):
    help = (
        "Decode pace_parts ACES fitment via vcdb_vehicles and upsert into master_part_fitments, "
        "matching MasterPart through brand_pace_brand_mapping, then refresh the Meilisearch "
        "parts and vehicles indexes so the fitment filter sees it."
    )

    def add_arguments(self, parser):
        parser.add_argument("--dry-run", action="store_true",
                            help="Build the staging tables and report, but write no fitment and "
                                 "touch no index.")
        parser.add_argument("--skip-reindex", action="store_true",
                            help="Write fitment but leave Meilisearch alone (the filter will not "
                                 "see the new fitment until an index rebuild).")
        parser.add_argument("--keep-staging", action="store_true",
                            help="Leave the stg_pf_* tables behind for inspection.")

    def handle(self, *args, **options):
        dry_run = options["dry_run"]
        started = time.monotonic()

        with connection.cursor() as c:
            c.execute("set statement_timeout='0'")  # this is a long backfill, not a web request

            self._drop_staging(c)

            before = self._count(c, "master_part_fitments")
            self.stdout.write("master_part_fitments before: %s rows%s" % (
                format(before, ","), " [DRY RUN]" if dry_run else ""))
            self.stdout.flush()

            for label, sql, table in (
                ("master_parts keys", _SQL_MP_KEYS, "stg_pf_mp_keys"),
                ("fitment expanded", _SQL_EXPAND, "stg_pf_expanded"),
                ("years collapsed", _SQL_RANGES, "stg_pf_ranges"),
            ):
                t = time.monotonic()
                c.execute(sql)
                if table == "stg_pf_mp_keys":
                    c.execute("create index on stg_pf_mp_keys (brand_id, np)")
                n = self._count(c, table)
                self.stdout.write("  %-20s %12s rows  (%.0fs)" % (label, format(n, ","), time.monotonic() - t))
                self.stdout.flush()
                logger.info("%s %s: %s rows", _LOG_PREFIX, table, n)

            c.execute("select count(distinct master_part_id) from stg_pf_expanded")
            parts_touched = c.fetchone()[0]
            c.execute("select count(*) from stg_pf_expanded")
            vehicle_rows = c.fetchone()[0]
            self.stdout.write("  -> %s MasterPart(s) gain fitment, from %s (part, vehicle) pairs" % (
                format(parts_touched, ","), format(vehicle_rows, ",")))

            if dry_run:
                if not options["keep_staging"]:
                    self._drop_staging(c)
                self.stdout.write(self.style.WARNING("dry run -- nothing written"))
                return

            t = time.monotonic()
            with transaction.atomic():
                c.execute(_SQL_INSERT)
                inserted = c.rowcount
            after = self._count(c, "master_part_fitments")
            self.stdout.write(self.style.SUCCESS(
                "  inserted %s fitment row(s) in %.0fs  (table %s -> %s)" % (
                    format(inserted, ","), time.monotonic() - t,
                    format(before, ","), format(after, ","))))
            self.stdout.flush()
            logger.info("%s inserted=%s before=%s after=%s", _LOG_PREFIX, inserted, before, after)

            if not options["keep_staging"]:
                self._drop_staging(c)

        if options["skip_reindex"]:
            self.stdout.write(self.style.WARNING(
                "--skip-reindex: the fitment filter will NOT see this until the parts and "
                "vehicles indexes are rebuilt."))
        else:
            self._reindex()

        self.stdout.write(self.style.SUCCESS("Done in %.0fs" % (time.monotonic() - started)))

    def _reindex(self):
        """Both indexes: fitment_keys lives on the parts index, the filter's year/make/model
        pickers read the separate vehicles index. One without the other half-works."""
        from src.search import meilisearch_client

        # is_configured() only checks that MEILISEARCH_HOST is SET, and it defaults to
        # http://localhost:7700 -- so on a developer machine pointed at the production database
        # it returns True while nothing is listening, and the reindex then grinds through every
        # batch collecting connection-refused errors before reporting failure. Probe the server
        # before starting so that case fails in one line instead.
        if not meilisearch_client.is_configured():
            self._reindex_unavailable("MEILISEARCH_HOST is not configured")
            return
        host = getattr(settings, "MEILISEARCH_HOST", "")
        try:
            # is_healthy() on the SDK client swallows transport errors and returns False; the
            # try/except is for _get_client() itself failing (bad host, missing key).
            healthy = bool(meilisearch_client._get_client().is_healthy())
        except Exception as e:  # noqa: BLE001 -- any transport failure means "cannot reach it"
            healthy = False
            logger.warning("%s Meilisearch health check failed for %s: %s", _LOG_PREFIX, host, e)
        if not healthy:
            self._reindex_unavailable("cannot reach Meilisearch at {}".format(host))
            return

        t = time.monotonic()
        self.stdout.write("reindexing parts with fitment (fitment_keys)...")
        self.stdout.flush()
        indexed, failed = meilisearch_client.reindex_master_parts_with_fitment()
        self.stdout.write(self.style.SUCCESS(
            "  parts index: %s indexed, %s failed  (%.0fs)" % (
                format(indexed, ","), format(failed, ","), time.monotonic() - t)))

        t = time.monotonic()
        self.stdout.write("rebuilding the vehicles reference index...")
        self.stdout.flush()
        v_indexed, v_failed = meilisearch_client.reindex_vehicles_index()
        self.stdout.write(self.style.SUCCESS(
            "  vehicles index: %s indexed, %s failed  (%.0fs)" % (
                format(v_indexed, ","), format(v_failed, ","), time.monotonic() - t)))
        logger.info("%s reindex parts=%s/%s vehicles=%s/%s",
                    _LOG_PREFIX, indexed, failed, v_indexed, v_failed)

    def _reindex_unavailable(self, reason: str) -> None:
        """Fitment is already committed at this point, so this is a warning, not an error --
        but it has to name the two commands, because the filter stays stale until both run."""
        self.stdout.write(self.style.WARNING(
            "Meilisearch not refreshed: {}.\n"
            "  Fitment IS written to the database. The fitment filter will not see it until\n"
            "  these run where Meilisearch lives:\n"
            "    python manage.py index_parts_meilisearch --fitment-only\n"
            "    python manage.py index_vehicles_meilisearch\n"
            "  --fitment-only upserts into the live parts index by id (no delete, no empty\n"
            "  window). index_vehicles_meilisearch deletes and re-uploads, so the year/make/\n"
            "  model pickers are briefly empty -- prefer a quiet window for that one.".format(reason)
        ))

    @staticmethod
    def _count(cursor, table: str) -> int:
        cursor.execute("select count(*) from %s" % table)
        return cursor.fetchone()[0]

    @staticmethod
    def _drop_staging(cursor) -> None:
        for t in _STAGING:
            cursor.execute("drop table if exists %s" % t)
