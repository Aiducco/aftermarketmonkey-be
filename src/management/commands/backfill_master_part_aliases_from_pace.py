"""
Fill ``MasterPart.part_aliases`` from the PACE catalog's search-key expansion.

WHAT THESE ALIASES ARE
``pace_parts.part_search`` is the array AAM/PACE maintains so their own storefronts find a part by
any spelling a counter person types. Mostly it is the part's own number respelled -- punctuation
stripped, then each form prefixed with the AAM member's line code, with and without a hyphen:

    71822-00-88  ->  [718220088, COV718220088, COV-718220088, COV71822-00-88, COV-71822-00-88]
    2635-003     ->  [2635003, ORL2635003, ORL-2635003, ORL2635-003, ORL-2635-003]

Measured on the 1,947,905 PACE rows that match a master part, 97.3% carry one, averaging 3.9
entries. Two things make this worth indexing:

1. **The punctuation-stripped form.** Meilisearch tokenizes on the hyphens in '71822-00-88', so a
   customer typing '718220088' does not match it today. Applies to every part number that carries
   punctuation.
2. **The line-code forms.** A customer holding an AAM member's number lands on our part.

THE ONLY THINGS DELETED ARE THE ONES THAT WOULD HURT SEARCH
Three filters, each aimed at a specific measured failure. Nothing is dropped for merely looking
unusual -- an alias that collides with nothing we sell costs nothing and adds recall, including
genuine cross-references like GM 22268768 carrying Lund's 21-63725.

1. **An alias that IS another real master part's number, same brand** (20,562 of 7,283,706 in an
   unfiltered run). The worst case: the number is typed *correctly* and lands on the wrong
   product.

       part 30-2404  carried alias 30-2004   (30-2004 is a real part)
       part 4227     carried alias 23227     (and 23227 carried 4227 back, mutually)
       part 8551     carried alias ND10313B  (ND10313B is a real part)

   The same harm also arrives wearing a line code -- 30-2404 carried 'AEM-30-2004', and
   RS999215 carried 'RAN-RS994215' (RS994215 is real). 56,573 aliases, 0.783%, were this. An
   alias is dropped when removing a 2, 3 or 4 character line code leaves another real part's
   number, unless the alias also contains this part's own number (which keeps the legitimate
   'AEM30-2404'). Each length is probed separately: one greedy regexp strips too much --
   'RANRS994215' loses 'RANR' and the real part 'RS994215' is never found.

2. **An alias value spread across more distinct parts than any real number would be**
   (``--max-parts-per-alias``, default 10). ``'2'`` appears as an alias on 1,085 distinct parts
   and ``'MAL2'`` on 1,067 -- feed artifacts, not numbers. Indexed, a search for either returns a
   thousand unrelated parts. Disabling typo tolerance does NOT help here: these are *exact*
   matches. Legitimate collisions sit at 7-8 parts (one alternate number covering a few SKUs), so
   the default keeps them.

3. **Aliases too short to be a part number** (``--min-length``, default 4). Only 0.109% of
   entries, but they match almost anything.

Also dropped: an alias identical to the part's own ``part_number`` (already the first searchable
attribute, so storing it again on 1.9M rows only inflates the index). Aliases are capped per part
(``--max-aliases``, default 25, shortest first) so one pathological row cannot bloat a document.

MATCHING
(brand_id, normalised part number) through ``brand_pace_brand_mapping`` -- the same path
``backfill_master_part_data_from_pace`` and ``sync_master_part_fitments_from_pace`` use. The
normalisation expression is character-for-character identical to
``master_part_matching._NORMALIZED_PART_NUMBER_SQL`` and to
``master_parts_normalized_part_number_idx``; if they drift, the join degrades to a sequential scan
over 4.5M rows. Where several PACE rows map onto one master part (~31k cases, the '26158 PT-1' /
'26158PT-1' split), their alias sets are unioned.

Writes are idempotent and full-replace per part: re-running produces the same list, and a part
whose aliases are all filtered out is cleared rather than left holding a previous run's values.

    python manage.py backfill_master_part_aliases_from_pace --dry-run
    python manage.py backfill_master_part_aliases_from_pace
"""
import logging
import time

from django.core.management.base import BaseCommand
from django.db import connection

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[PACE-ALIASES]"
# Must stay identical to master_part_matching._NORMALIZED_PART_NUMBER_SQL -- see MATCHING above.
_NORM = "upper(regexp_replace(%s, '[^A-Za-z0-9]', '', 'g'))"

_SQL_STAGE = """
create unlogged table stg_pace_alias_src as
select mp.id as master_part_id, mp.brand_id, mp.part_number, p.part_search
from pace_parts p
join pace_brands pb on pb.aaia_brand_code = p.aaia_brand_code
join brand_pace_brand_mapping m on m.pace_brand_id = pb.id
join master_parts mp on mp.brand_id = m.brand_id and {np_mp} = {np_p}
where p.part_search is not null
  and jsonb_typeof(p.part_search::jsonb) = 'array'
  and jsonb_array_length(p.part_search::jsonb) > 0
""".format(np_mp=_NORM % "mp.part_number", np_p=_NORM % "p.part")

# jsonb_array_elements_text is what makes the numeric entries safe -- PACE ships the stripped form
# as a JSON number (718220088, not "718220088") and this renders both as text. Zeros are never
# stripped: '010140' has a sibling spelling '10140' and the leading zero is meaningful.
_SQL_AGG = """
create unlogged table stg_pace_alias as
with flat as (
    select distinct s.master_part_id, s.brand_id, btrim(a.alias) as alias,
           {np_alias} as na, {np_self} as np
    from stg_pace_alias_src s
    cross join lateral jsonb_array_elements_text(s.part_search::jsonb) as a(alias)
    where btrim(a.alias) <> ''
      and btrim(a.alias) <> s.part_number
      and length({np_alias}) >= %(min_length)s
), too_broad as (
    -- Filter 2: an alias value covering more distinct parts than any real number would be is a
    -- feed artifact ('2' on 1,085 parts, 'MAL2' on 1,067). Excluded wholesale rather than
    -- per-part, so it cannot be searched for at all.
    select na from flat group by na
    having count(distinct master_part_id) > %(max_parts_per_alias)s
), ranked as (
    select f.master_part_id, f.alias,
           row_number() over (partition by f.master_part_id
                              order by length(f.alias), f.alias) as rn
    from flat f
    where not exists (select 1 from too_broad b where b.na = f.na)
      -- Filter 1: the alias IS another real master part's number in this brand, so indexing it
      -- would send a correctly-typed search to the wrong product. Index-driven via
      -- master_parts_normalized_part_number_idx (brand_id, normalised part_number).
      and not exists (
          select 1 from master_parts other
          where other.brand_id = f.brand_id
            and {np_other} = f.na
            and other.id <> f.master_part_id
      )
      -- Filter 1b: the same harm wearing a line code. Part 30-2404 carried 'AEM-30-2004' and
      -- 30-2004 is a real part, so a customer searching AEM's spelling of 30-2004 landed on
      -- 30-2404. Stripping the 2-4 letter line code makes this an equality test the normalised
      -- index can serve; a suffix LIKE cannot and times out over 7.2M aliases. Skipped when the
      -- alias also contains this part's own number, which keeps the legitimate 'AEM30-2404'.
      and not (
          f.na not like '%%' || f.np
          and exists (
              select 1 from master_parts other
              where other.brand_id = f.brand_id
                and other.id <> f.master_part_id
                and {np_other} in (substr(f.na, 3), substr(f.na, 4), substr(f.na, 5))
                and length({np_other}) >= %(min_length)s
          )
      )
)
select master_part_id, jsonb_agg(alias order by alias) as aliases
from ranked
where rn <= %(max_aliases)s
group by master_part_id
""".format(
    np_alias=_NORM % "btrim(a.alias)",
    np_self=_NORM % "s.part_number",
    np_other=_NORM % "other.part_number",
)

# A part that matched a PACE row but whose aliases were ALL filtered out must be cleared, not left
# holding whatever a previous run wrote. Without this the command is not idempotent across a
# change to the thresholds: tightening a filter would leave the old, looser values in place on
# exactly the parts the tightening was meant to clean.
_SQL_RESET = """
update master_parts mp set part_aliases = '[]'::jsonb, updated_at = now()
where jsonb_array_length(mp.part_aliases) > 0
  and exists (select 1 from stg_pace_alias_src s where s.master_part_id = mp.id)
  and not exists (select 1 from stg_pace_alias a where a.master_part_id = mp.id)
"""

_SQL_APPLY = """
with batch as (
    select master_part_id, aliases from stg_pace_alias order by master_part_id limit %(limit)s
), upd as (
    update master_parts mp set part_aliases = b.aliases, updated_at = now()
    from batch b where mp.id = b.master_part_id returning mp.id
)
delete from stg_pace_alias s using batch b where s.master_part_id = b.master_part_id
"""

# Reported every run so the cost of each filter is visible rather than assumed.
_SQL_FILTER_STATS = """
with flat as (
    select distinct s.master_part_id, s.brand_id, btrim(a.alias) as alias,
           {np_alias} as na, {np_self} as np
    from stg_pace_alias_src s
    cross join lateral jsonb_array_elements_text(s.part_search::jsonb) as a(alias)
    where btrim(a.alias) <> '' and btrim(a.alias) <> s.part_number
)
select
    count(*) as total,
    count(*) filter (where length(flat.na) < %(min_length)s) as too_short,
    count(*) filter (where exists (
        select 1 from master_parts other
        where other.brand_id = flat.brand_id and {np_other} = flat.na
          and other.id <> flat.master_part_id)) as collides_with_real_part,
    count(*) filter (where flat.na not like '%%' || flat.np
        and exists (
            select 1 from master_parts other
            where other.brand_id = flat.brand_id and other.id <> flat.master_part_id
              and {np_other} in (substr(flat.na, 3), substr(flat.na, 4), substr(flat.na, 5))
              and length({np_other}) >= %(min_length)s)) as line_code_form_of_other_part
from flat
""".format(
    np_alias=_NORM % "btrim(a.alias)",
    np_self=_NORM % "s.part_number",
    np_other=_NORM % "other.part_number",
)


class Command(BaseCommand):
    help = (
        "Fill MasterPart.part_aliases from pace_parts.part_search, dropping only the aliases "
        "that would send a search to the wrong part."
    )

    def add_arguments(self, parser):
        parser.add_argument("--dry-run", action="store_true",
                            help="Report what would be written, write nothing.")
        parser.add_argument("--max-aliases", type=int, default=25,
                            help="Cap on aliases stored per part, shortest first (default 25).")
        parser.add_argument("--min-length", type=int, default=4,
                            help="Drop aliases with fewer than this many alphanumeric chars (default 4).")
        parser.add_argument("--max-parts-per-alias", type=int, default=10,
                            help="Drop an alias value entirely when it covers more than this many "
                                 "distinct master parts -- catches feed artifacts like '2' and "
                                 "'MAL2', which sit on >1,000 parts each (default 10).")
        parser.add_argument("--batch-size", type=int, default=50000,
                            help="Rows per UPDATE statement (default 50000).")

    def handle(self, *args, **options):
        dry_run = options["dry_run"]
        max_aliases = options["max_aliases"]
        min_length = options["min_length"]
        max_parts_per_alias = options["max_parts_per_alias"]
        batch_size = options["batch_size"]
        started = time.monotonic()
        done = 0

        with connection.cursor() as c:
            c.execute("set statement_timeout='0'")
            c.execute("set work_mem='96MB'")
            c.execute("drop table if exists stg_pace_alias_src")
            c.execute("drop table if exists stg_pace_alias")

            t = time.monotonic()
            c.execute(_SQL_STAGE)
            c.execute("create index on stg_pace_alias_src (master_part_id)")
            c.execute("select count(*) from stg_pace_alias_src")
            src_rows = c.fetchone()[0]
            self.stdout.write("  %s PACE row(s) matched a master part and carry part_search  (%.0fs)" % (
                format(src_rows, ","), time.monotonic() - t))
            self.stdout.flush()

            t = time.monotonic()
            c.execute(_SQL_FILTER_STATS, {"min_length": min_length})
            total, too_short, collides, line_code_collides = c.fetchone()
            self.stdout.write("  what the filters remove  (%.0fs)" % (time.monotonic() - t))
            self.stdout.write("    candidate aliases                          %12s" % format(total, ","))
            self.stdout.write(self.style.WARNING(
                "    IS another real part's number (same brand) %12s   <- would misdirect a search"
                % format(collides, ",")))
            self.stdout.write(self.style.WARNING(
                "    line-code form of another real part      %12s   <- same harm, prefixed"
                % format(line_code_collides, ",")))
            self.stdout.write("    shorter than %s chars                       %12s" % (
                min_length, format(too_short, ",")))
            self.stdout.flush()

            t = time.monotonic()
            c.execute(_SQL_AGG, {
                "max_aliases": max_aliases,
                "min_length": min_length,
                "max_parts_per_alias": max_parts_per_alias,
            })
            c.execute("""select count(*), sum(jsonb_array_length(aliases)),
                                max(jsonb_array_length(aliases))
                         from stg_pace_alias""")
            parts, total_aliases, max_seen = c.fetchone()
            self.stdout.write("  %s master part(s) will get aliases  (%.0fs)" % (
                format(parts, ","), time.monotonic() - t))
            self.stdout.write("    %s alias entries kept, avg %.1f, max %s per part" % (
                format(total_aliases or 0, ","),
                (total_aliases or 0) / parts if parts else 0.0, max_seen or 0))
            self.stdout.flush()

            c.execute("""select mp.part_number, s.aliases from stg_pace_alias s
                         join master_parts mp on mp.id = s.master_part_id limit 5""")
            self.stdout.write("    examples:")
            for part_number, aliases in c.fetchall():
                self.stdout.write("      %-18s %s" % (part_number, str(aliases)[:96]))
            self.stdout.flush()

            if dry_run:
                c.execute("drop table if exists stg_pace_alias_src")
                c.execute("drop table if exists stg_pace_alias")
                self.stdout.write(self.style.WARNING("dry run -- nothing written"))
                return

            t = time.monotonic()
            c.execute(_SQL_RESET)
            self.stdout.write("  cleared stale aliases on %s part(s) whose entries are all "
                              "filtered out now  (%.0fs)" % (
                                  format(c.rowcount, ","), time.monotonic() - t))
            self.stdout.flush()

            # Batched: one transaction per batch so a 1.9M-row rewrite never holds that many row
            # locks at once, and so it can be interrupted and resumed without losing what landed.
            t = time.monotonic()
            while True:
                c.execute(_SQL_APPLY, {"limit": batch_size})
                n = c.rowcount
                if not n:
                    break
                done += n
                self.stdout.write("    applied %s / %s  (%.0fs)" % (
                    format(done, ","), format(parts, ","), time.monotonic() - t))
                self.stdout.flush()

            c.execute("drop table if exists stg_pace_alias_src")
            c.execute("drop table if exists stg_pace_alias")
            c.execute("select count(*) from master_parts where jsonb_array_length(part_aliases) > 0")
            live = c.fetchone()[0]

        self.stdout.write("")
        self.stdout.write(self.style.SUCCESS(
            "Done in %.0fs -- %s master part(s) now carry part_aliases" % (
                time.monotonic() - started, format(live, ","))))
        self.stdout.write(
            "Next: reindex Meilisearch so the new searchable attribute takes effect "
            "(index_parts_meilisearch).")
        logger.info("%s parts_updated=%s", _LOG_PREFIX, done)
