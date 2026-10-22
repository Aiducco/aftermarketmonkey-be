"""
Step 4 of the Tri-State backfill: fill MasterPartData for Tri-State master parts from
pace_tse_parts.

The official ingest (master_parts._ingest_tse_parts_for_mapped_brands) sets MasterPart.description
and MasterPart.image_url, but never writes MasterPartData -- so images, series and the spec blob
stay empty for everything it creates. This fills them.

WHAT PACE CAN ACTUALLY FILL, measured on pace_tse_parts' 1,447,304 rows

    images       <- image      245,675 (17%)
    series       <- series     317,961 (22%)
    description  <- ext        282,589 (20%)
    field_specs  <- category/subcategory as spec pairs  326,266 (23%)

color, material, warranty, youtube_video, installation_instructions and vehicle_type have no
PACE source and are left NULL rather than invented. The low percentages are the source data,
not an underperforming backfill: 758,465 of those rows are AAM line-code parts which carry no
description, image, category or series at all (see PaceBrand.has_real_aaia_code).

TWO RULES THAT MATTER

1. Never overwrite. master_part_data already holds 2,248,674 rows with 1,966,803 descriptions
   and 925,247 image sets from other providers, and Tri-State's thinner data must not clobber
   richer data someone else supplied. Every field is written only where it is currently NULL or
   empty, via COALESCE/NULLIF, so re-running is safe and provider order does not matter.

2. Shapes follow the existing convention exactly, checked against live rows:
       images       ["https://...jpg", ...]                      plain URL list
       field_specs  [{"spec_name": "...", "spec_value": "..."}]
   Inventing a different shape here would silently break whatever renders these.

source_provider is set to Tri-State and source_external_id to the PACE catalog_object_id, but
only on rows this command creates -- on an existing row they are left as whoever wrote it first,
since that is the provenance of the data already there.

It also fills MasterPart.image_url and MasterPart.description where those are blank, for the
same reason -- the ingest only sets them on creation.

    python manage.py backfill_master_part_data_from_tse --dry-run
    python manage.py backfill_master_part_data_from_tse
"""
import logging
import time

from django.core.management.base import BaseCommand, CommandError
from django.db import connection

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[TSE-MPD-BACKFILL]"
_NORM = "upper(regexp_replace(%s, '[^A-Za-z0-9]', '', 'g'))"

# Tri-State master parts joined to their PACE row. Join is (brand_id, part_number) through the
# official brand mapping -- the same path the ingest used, so this covers exactly what it created.
_SQL_PAIR = """
create unlogged table stg_mpd_pair as
select distinct on (mp.id)
       mp.id as master_part_id,
       p.image, p.series, p.ext, p.category, p.subcategory, p.catalog_object_id
from pace_tse_parts p
join tse_brands tb on upper(tb.aaia_code) = upper(p.aaia_brand_code)
join brand_tse_brand_mapping m on m.tse_brand_id = tb.id
join master_parts mp on mp.brand_id = m.brand_id and {np_mp} = {np_p}
where p.image <> '' or p.series <> '' or p.ext <> '' or p.category <> ''
order by mp.id, (p.image <> '') desc, (p.ext <> '') desc
""".format(np_mp=_NORM % "mp.part_number", np_p=_NORM % "p.part")

# INSERT for master parts with no MasterPartData row at all.
_SQL_INSERT = """
insert into master_part_data
    (master_part_id, images, description, series, field_specs,
     source_provider_id, source_external_id, created_at, updated_at)
select s.master_part_id,
       case when s.image <> '' then to_jsonb(array[s.image]) else null end,
       nullif(s.ext, ''),
       nullif(s.series, ''),
       case when s.category <> '' or s.subcategory <> '' then
            (select jsonb_agg(jsonb_build_object('spec_name', k, 'spec_value', v))
             from (values ('Category', nullif(s.category,'')),
                          ('Subcategory', nullif(s.subcategory,''))) as t(k, v)
             where v is not null)
       else null end,
       %(provider_id)s,
       nullif(s.catalog_object_id, ''),
       now(), now()
from stg_mpd_pair s
where not exists (select 1 from master_part_data d where d.master_part_id = s.master_part_id)
"""

# UPDATE for existing rows -- fills blanks only, never overwrites.
_SQL_UPDATE = """
update master_part_data d
set images = coalesce(d.images,
        case when s.image <> '' then to_jsonb(array[s.image]) else null end),
    description = coalesce(nullif(d.description, ''), nullif(s.ext, '')),
    series = coalesce(nullif(d.series, ''), nullif(s.series, '')),
    field_specs = coalesce(d.field_specs,
        case when s.category <> '' or s.subcategory <> '' then
             (select jsonb_agg(jsonb_build_object('spec_name', k, 'spec_value', v))
              from (values ('Category', nullif(s.category,'')),
                           ('Subcategory', nullif(s.subcategory,''))) as t(k, v)
              where v is not null)
        else null end),
    updated_at = now()
from stg_mpd_pair s
where d.master_part_id = s.master_part_id
  and (d.images is null or coalesce(d.description,'') = ''
       or coalesce(d.series,'') = '' or d.field_specs is null)
"""


# The official ingest passes description/image_url only when it CREATES a MasterPart -- for a
# part that already existed it updates aaia_code and nothing else (see
# master_parts._ingest_tse_parts_for_mapped_brands). So an existing part with a blank image stays
# blank even when Tri-State has one. This fills those two columns, blanks only.
#
# Note `ext` is used for description, never `name`: for the 758k line-code parts `name` is just
# the "<code> <part number>" template, which is not a description and would be worse than NULL.
_SQL_UPDATE_MASTER_PART = """
update master_parts mp
set image_url = coalesce(nullif(mp.image_url, ''), nullif(s.image, '')),
    description = coalesce(nullif(mp.description, ''), nullif(s.ext, '')),
    updated_at = now()
from stg_mpd_pair s
where mp.id = s.master_part_id
  and (coalesce(mp.image_url, '') = '' or coalesce(mp.description, '') = '')
  and (s.image <> '' or s.ext <> '')
"""


class Command(BaseCommand):
    help = (
        "Fill MasterPartData (images, series, description, field_specs) for Tri-State master "
        "parts from pace_tse_parts. Fills blanks only -- never overwrites another provider's data."
    )

    def add_arguments(self, parser):
        parser.add_argument("--dry-run", action="store_true",
                            help="Build the join and report what would be filled, write nothing.")
        parser.add_argument("--keep-staging", action="store_true", help="Leave stg_mpd_pair behind.")

    def handle(self, *args, **options):
        dry_run = options["dry_run"]
        started = time.monotonic()
        with connection.cursor() as c:
            c.execute("set statement_timeout='0'")
            c.execute("""select id from providers
                         where name ilike '%%tri%%state%%' or name ilike '%%tristate%%' limit 1""")
            row = c.fetchone()
            if not row:
                raise CommandError("no Tri-State Providers row found")
            provider_id = row[0]

            c.execute("drop table if exists stg_mpd_pair")
            t = time.monotonic()
            c.execute(_SQL_PAIR)
            c.execute("create index on stg_mpd_pair (master_part_id)")
            c.execute("select count(*) from stg_mpd_pair")
            pairs = c.fetchone()[0]
            self.stdout.write("  matched %s master part(s) to a PACE row with data  (%.0fs)" % (
                format(pairs, ","), time.monotonic() - t))
            self.stdout.flush()

            c.execute("""select count(*) from stg_mpd_pair s
                         where not exists (select 1 from master_part_data d
                                           where d.master_part_id = s.master_part_id)""")
            missing = c.fetchone()[0]
            self.stdout.write("    %s have NO master_part_data row (insert)" % format(missing, ","))
            self.stdout.write("    %s already have one (fill blanks only)" % format(pairs - missing, ","))
            self.stdout.flush()

            c.execute("""select count(*) from master_parts mp join stg_mpd_pair s on mp.id = s.master_part_id
                         where (coalesce(mp.image_url,'') = '' or coalesce(mp.description,'') = '')
                           and (s.image <> '' or s.ext <> '')""")
            mp_blanks = c.fetchone()[0]
            self.stdout.write("    %s master_part(s) have a blank image_url/description that "
                              "PACE can fill" % format(mp_blanks, ","))
            self.stdout.flush()

            if dry_run:
                if not options["keep_staging"]:
                    c.execute("drop table if exists stg_mpd_pair")
                self.stdout.write(self.style.WARNING("dry run -- nothing written"))
                return

            t = time.monotonic()
            c.execute(_SQL_INSERT, {"provider_id": provider_id})
            inserted = c.rowcount
            self.stdout.write(self.style.SUCCESS("  inserted %s row(s)  (%.0fs)" % (
                format(inserted, ","), time.monotonic() - t)))
            self.stdout.flush()

            t = time.monotonic()
            c.execute(_SQL_UPDATE)
            updated = c.rowcount
            self.stdout.write(self.style.SUCCESS("  updated  %s row(s)  (%.0fs)" % (
                format(updated, ","), time.monotonic() - t)))

            t = time.monotonic()
            c.execute(_SQL_UPDATE_MASTER_PART)
            mp_updated = c.rowcount
            self.stdout.write(self.style.SUCCESS(
                "  master_parts image_url/description filled on %s row(s)  (%.0fs)" % (
                    format(mp_updated, ","), time.monotonic() - t)))
            self.stdout.flush()

            if not options["keep_staging"]:
                c.execute("drop table if exists stg_mpd_pair")

            c.execute("select count(*) from master_part_data")
            total = c.fetchone()[0]

        self.stdout.write("")
        self.stdout.write(self.style.SUCCESS("Done in %.0fs -- master_part_data now %s rows" % (
            time.monotonic() - started, format(total, ","))))
        logger.info("%s mpd_inserted=%s mpd_updated=%s mp_updated=%s",
                    _LOG_PREFIX, inserted, updated, mp_updated)
