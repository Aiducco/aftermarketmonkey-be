"""
Fill blank images / descriptions / series / specs on master parts from the consolidated PACE
catalog -- the LAST step of the image backfill, after every distributor feed has had its turn.

PACE goes last on purpose. It is the widest source we have (3,435,124 deduplicated parts across
nine AAM members, 1,033,829 of them carrying an image) but it is a *catalog* scrape, not a feed we
hold a supplier relationship with, so anything a real distributor supplies wins. Everything here
is fill-if-blank, which makes the ordering automatic: run Turn14, then
``master_part_enrichment.RECOMMENDED_ORDER``, then this, and each part keeps the richest source
that reached it first.

This generalises ``backfill_master_part_data_from_tse`` (Tri-State only, via
``brand_tse_brand_mapping``) to all nine members, via ``brand_pace_brand_mapping``.

JUNK IS FILTERED HERE, NOT JUST DOWNSTREAM
``pace_parts.image`` holds 3,255 values that are not images -- the same placeholder family that
put the literal text "NA" on 412,682 master parts (Premier's feed, 556,786 rows). The predicate
below mirrors ``src.integrations.utils.images.clean_image_url`` so this command can never
reintroduce it. 'blank' and 'missing' are deliberately NOT treated as placeholder words: they
occur in real product names (blank-drilled wheels, ``Uni-Hitch-Step-Blank.jpg``).

MATCHING
(brand_id, normalised part number) through ``brand_pace_brand_mapping`` -- the same path
``sync_master_part_fitments_from_pace`` uses. Normalising is what makes this worth running at all:
PACE spells part numbers differently from every distributor feed (``'26158 PT-1'`` vs
``'26158PT1'``), so an exact match would miss most of the catalog.

PROVENANCE -- AND WHAT THIS DELIBERATELY DOES NOT DO
No ``ProviderPart`` row is ever created, and ``source_provider`` is left NULL. PACE is not a
supplier: it is a catalog shared by nine AAM members, and attributing its data to a ``Providers``
row would assert a distributor relationship for a part that distributor may not carry (Tri-State
is the only one of the nine that exists as a provider at all). ``source_external_id`` carries the
PACE ``catalog_object_id`` instead, which is enough to trace any value back to its source row.
Provenance is written only on rows this command CREATES; on an existing row whoever wrote it
first is preserved.

    python manage.py backfill_master_part_data_from_pace --dry-run
    python manage.py backfill_master_part_data_from_pace
    python manage.py backfill_master_part_data_from_pace --images-only
"""
import logging
import time

from django.core.management.base import BaseCommand
from django.db import connection

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[PACE-MPD-BACKFILL]"
_NORM = "upper(regexp_replace(%s, '[^A-Za-z0-9]', '', 'g'))"

# Mirror of images.clean_image_url, in SQL. Must stay in step with that module.
_IMAGE_OK = """(
    (p.image ilike 'http://%' or p.image ilike 'https://%')
    and lower(btrim(p.image)) not in
        ('na','n/a','n.a.','none','null','unknown','-','--','tbd','nan','0','false',
         'no image','noimage','not available','#n/a','undefined')
    and p.image not ilike '%noimage%'      and p.image not ilike '%no-image%'
    and p.image not ilike '%no_image%'     and p.image not ilike '%placeholder%'
    and p.image not ilike '%comingsoon%'   and p.image not ilike '%coming_soon%'
    and p.image not ilike '%coming-soon%'  and p.image not ilike '%coming soon%'
    and p.image not ilike '%notavailable%' and p.image not ilike '%image_not_available%'
)"""

# One PACE row per master part. Prefer a row that has an image, then one that has prose, so a
# part carried by several members gets the richest of them rather than the lowest id.
_SQL_PAIR = """
create unlogged table stg_pace_mpd as
select distinct on (mp.id)
       mp.id as master_part_id,
       case when {image_ok} then p.image else '' end as image,
       p.series, p.ext, p.category, p.subcategory, p.catalog_object_id
from pace_parts p
join pace_brands pb on pb.aaia_brand_code = p.aaia_brand_code
join brand_pace_brand_mapping m on m.pace_brand_id = pb.id
join master_parts mp on mp.brand_id = m.brand_id and {np_mp} = {np_p}
where ({image_ok} or p.ext <> '' or p.series <> '' or p.category <> '')
order by mp.id, ({image_ok}) desc, (p.ext <> '') desc, (p.series <> '') desc
""".format(
    image_ok=_IMAGE_OK,
    np_mp=_NORM % "mp.part_number",
    np_p=_NORM % "p.part",
)

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
       null,
       nullif(s.catalog_object_id, ''),
       now(), now()
from stg_pace_mpd s
where not exists (select 1 from master_part_data d where d.master_part_id = s.master_part_id)
"""

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
from stg_pace_mpd s
where d.master_part_id = s.master_part_id
  and (d.images is null or coalesce(d.description,'') = ''
       or coalesce(d.series,'') = '' or d.field_specs is null)
"""

# master_parts.image_url is the column the catalog and Meilisearch actually render. Note the
# blank test includes NULL *and* '': the junk cleanup set 412,906 junk values to NULL, which is
# precisely what makes those parts eligible here.
#
# `ext` is used for description, never `name`: for the AAM line-code parts `name` is just the
# "<code> <part number>" template, which is not a description and would be worse than NULL.
_SQL_UPDATE_MASTER_PART = """
update master_parts mp
set image_url = coalesce(nullif(mp.image_url, ''), nullif(s.image, '')),
    description = coalesce(nullif(mp.description, ''), nullif(s.ext, '')),
    updated_at = now()
from stg_pace_mpd s
where mp.id = s.master_part_id
  and (coalesce(mp.image_url, '') = '' or coalesce(mp.description, '') = '')
  and (s.image <> '' or s.ext <> '')
"""

_SQL_UPDATE_MASTER_PART_IMAGES_ONLY = """
update master_parts mp
set image_url = s.image,
    updated_at = now()
from stg_pace_mpd s
where mp.id = s.master_part_id
  and coalesce(mp.image_url, '') = ''
  and s.image <> ''
"""


class Command(BaseCommand):
    help = (
        "Fill blank master_parts.image_url/description and MasterPartData from the consolidated "
        "PACE catalog. Fills blanks only -- never overwrites a distributor's data. Run last."
    )

    def add_arguments(self, parser):
        parser.add_argument("--dry-run", action="store_true",
                            help="Build the join and report what would be filled, write nothing.")
        parser.add_argument("--images-only", action="store_true",
                            help="Only fill master_parts.image_url; skip MasterPartData and descriptions.")
        parser.add_argument("--keep-staging", action="store_true",
                            help="Leave stg_pace_mpd behind for inspection.")

    def handle(self, *args, **options):
        dry_run = options["dry_run"]
        images_only = options["images_only"]
        started = time.monotonic()
        inserted = updated = mp_updated = 0

        with connection.cursor() as c:
            c.execute("set statement_timeout='0'")
            c.execute("set work_mem='96MB'")
            c.execute("drop table if exists stg_pace_mpd")
            t = time.monotonic()
            c.execute(_SQL_PAIR)
            c.execute("create index on stg_pace_mpd (master_part_id)")
            c.execute("select count(*), count(*) filter (where image <> '') from stg_pace_mpd")
            pairs, with_image = c.fetchone()
            self.stdout.write("  matched %s master part(s) to a PACE row with data  (%.0fs)" % (
                format(pairs, ","), time.monotonic() - t))
            self.stdout.write("    %s of them carry a usable image" % format(with_image, ","))
            self.stdout.flush()

            c.execute("""select count(*) from master_parts mp join stg_pace_mpd s on mp.id = s.master_part_id
                         where coalesce(mp.image_url,'') = '' and s.image <> ''""")
            img_fillable = c.fetchone()[0]
            self.stdout.write(self.style.SUCCESS(
                "    %s master part(s) have NO image and PACE can supply one" % format(img_fillable, ",")))
            self.stdout.flush()

            if not images_only:
                c.execute("""select count(*) from stg_pace_mpd s
                             where not exists (select 1 from master_part_data d
                                               where d.master_part_id = s.master_part_id)""")
                missing = c.fetchone()[0]
                self.stdout.write("    %s have NO master_part_data row (insert)" % format(missing, ","))
                self.stdout.write("    %s already have one (fill blanks only)" % format(pairs - missing, ","))
                self.stdout.flush()

            if dry_run:
                if not options["keep_staging"]:
                    c.execute("drop table if exists stg_pace_mpd")
                self.stdout.write(self.style.WARNING("dry run -- nothing written"))
                return

            if images_only:
                t = time.monotonic()
                c.execute(_SQL_UPDATE_MASTER_PART_IMAGES_ONLY)
                mp_updated = c.rowcount
                self.stdout.write(self.style.SUCCESS(
                    "  master_parts.image_url filled on %s row(s)  (%.0fs)" % (
                        format(mp_updated, ","), time.monotonic() - t)))
            else:
                t = time.monotonic()
                c.execute(_SQL_INSERT)
                inserted = c.rowcount
                self.stdout.write(self.style.SUCCESS("  master_part_data inserted %s row(s)  (%.0fs)" % (
                    format(inserted, ","), time.monotonic() - t)))
                self.stdout.flush()

                t = time.monotonic()
                c.execute(_SQL_UPDATE)
                updated = c.rowcount
                self.stdout.write(self.style.SUCCESS("  master_part_data updated  %s row(s)  (%.0fs)" % (
                    format(updated, ","), time.monotonic() - t)))
                self.stdout.flush()

                t = time.monotonic()
                c.execute(_SQL_UPDATE_MASTER_PART)
                mp_updated = c.rowcount
                self.stdout.write(self.style.SUCCESS(
                    "  master_parts image_url/description filled on %s row(s)  (%.0fs)" % (
                        format(mp_updated, ","), time.monotonic() - t)))
                self.stdout.flush()

            if not options["keep_staging"]:
                c.execute("drop table if exists stg_pace_mpd")

            c.execute("select count(*) from master_parts where coalesce(image_url,'') <> ''")
            with_images_now = c.fetchone()[0]

        self.stdout.write("")
        self.stdout.write(self.style.SUCCESS(
            "Done in %.0fs -- master_parts with an image_url now %s" % (
                time.monotonic() - started, format(with_images_now, ","))))
        logger.info("%s mpd_inserted=%s mpd_updated=%s mp_updated=%s",
                    _LOG_PREFIX, inserted, updated, mp_updated)
