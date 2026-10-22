"""
Repair line-code TSEBrand rows that the official fuzzy matcher merged into unrelated real Brands.

WHAT WENT WRONG

sync_unmapped_tse_brands_to_brands fuzzy-matches on name, and 506 of Tri-State's 670 brands
carry AAM member-local jobber line codes whose "name" is three characters -- DOR, STA, SEA, SPC.
Word-prefix alignment accepts any >=3-character token as a prefix, so those names matched real
companies that merely start with the same letters:

    DOR (Dorman)          -> DORMONT MFG. COMPANY (gas hoses)     68,853 parts
    STA (Standard Motor)  -> STARS AND STRIPES DEF                60,470 parts
    SEA                   -> SEA DOG (marine hardware)            17,055 parts
    DEN (Denso)           -> DEN MATTRESS                          4,643 parts

Left alone, the master-parts ingest would file 419,114 real Tri-State parts under the wrong
manufacturer. Thin data is tolerable; confidently wrong attribution is not.

THE RULE APPLIED

  - KEEP a mapping whose Brands row shares the TSEBrand's aaia_code. That is identity, not a
    guess -- the one case here is AGS -> AMERICAN GREASE STICK, which is genuinely the same
    company.
  - REPLACE every name-based mapping with a dedicated Brands row named after the line code, so
    DOR becomes a brand called "DOR" rather than being merged into Dormont.

Uniform by rule rather than case-by-case judgement. A few of the 205 are probably correct
(MOO -> MOOG, TYC -> TYC, SKF -> SKF BEARINGS), and those lose a legitimate merge -- but
splitting a brand that should be merged is recoverable later with a curated line-code alias
table, whereas merging parts into the wrong manufacturer is not. Phase 4 of the official sync
already creates dedicated rows exactly this way for the 307 line codes that matched nothing, so
this makes the whole regime consistent.

Only touches mappings where the TSEBrand code matches ^A[678] AND the Brands row predates this
backfill, so it cannot disturb the curated 145 brands that were mapped before.

    python manage.py repair_tse_line_code_brand_mappings --dry-run
    python manage.py repair_tse_line_code_brand_mappings
"""
import logging

from django.core.management.base import BaseCommand
from django.db import connection, transaction

from src import enums as src_enums
from src import models as src_models

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[TSE-LINECODE-REPAIR]"

_FIND_SQL = """
select tb.id, tb.aaia_code, tb.name, b.id, b.name, m.id,
       (select count(*) from tse_parts p where p.brand_id = tb.id) as parts
from brand_tse_brand_mapping m
join tse_brands tb on tb.id = m.tse_brand_id
join brands b on b.id = m.brand_id
where tb.aaia_code ~ '^A[678]'
  and b.created_at < current_date
  and upper(coalesce(b.aaia_code, '')) <> upper(tb.aaia_code)
order by parts desc
"""


class Command(BaseCommand):
    help = (
        "Replace line-code TSEBrand -> real-Brands fuzzy mappings with dedicated Brands rows, "
        "so Tri-State's jobber line codes are not merged into unrelated manufacturers."
    )

    def add_arguments(self, parser):
        parser.add_argument("--dry-run", action="store_true", help="Report only, change nothing.")
        parser.add_argument("--show", type=int, default=12, help="Rows to print (default 12).")

    def handle(self, *args, **options):
        dry_run = options["dry_run"]
        with connection.cursor() as c:
            c.execute(_FIND_SQL)
            rows = c.fetchall()

        if not rows:
            self.stdout.write("No line-code mappings need repair.")
            return

        total_parts = sum(r[6] for r in rows)
        self.stdout.write("%s mapping(s) to replace, covering %s tse_parts%s" % (
            format(len(rows), ","), format(total_parts, ","), " [DRY RUN]" if dry_run else ""))
        self.stdout.write("")
        self.stdout.write("  %-7s %-9s was mapped to                      %10s" % ("code", "name", "parts"))
        for tb_id, code, tname, b_id, bname, m_id, parts in rows[: options["show"]]:
            self.stdout.write("  %-7s %-9s -> #%-6s %-24s %10s" % (
                code, (tname or "")[:9], b_id, (bname or "")[:24], format(parts, ",")))
        if len(rows) > options["show"]:
            self.stdout.write("  ... %s more" % format(len(rows) - options["show"], ","))
        self.stdout.flush()

        if dry_run:
            self.stdout.write(self.style.WARNING("dry run -- nothing changed"))
            return

        # Three bulk statements, not 205 x (create + delete + insert): the row-at-a-time
        # version held one transaction open for minutes, which is both slow and a long lock on
        # brands/brand_tse_brand_mapping for no benefit.
        codes = {r[1]: (r[2] or r[1]).strip().upper()[:255] for r in rows}
        mapping_ids = [r[5] for r in rows]
        tse_ids_by_code = {r[1]: r[0] for r in rows}

        existing = {
            b.aaia_code: b for b in src_models.Brands.objects.filter(aaia_code__in=list(codes))
        }
        by_name = {
            b.name: b for b in src_models.Brands.objects.filter(name__in=list(codes.values()))
        }
        to_create = [
            src_models.Brands(
                name=name,
                # status/status_name are NOT NULL; mirror phase 4 of
                # sync_unmapped_tse_brands_to_brands exactly.
                status=src_enums.BrandProviderStatus.ACTIVE.value,
                status_name=src_enums.BrandProviderStatus.ACTIVE.name,
                aaia_code=code,
            )
            for code, name in codes.items()
            if code not in existing and name not in by_name
        ]

        created = remapped = 0
        with transaction.atomic():
            if to_create:
                src_models.Brands.objects.bulk_create(to_create, ignore_conflicts=True)
                created = len(to_create)
            # re-read so every code resolves, including the ones just created
            resolved = {}
            for b in src_models.Brands.objects.filter(aaia_code__in=list(codes)):
                resolved[b.aaia_code] = b
            for b in src_models.Brands.objects.filter(name__in=list(codes.values())):
                for code, name in codes.items():
                    if name == b.name:
                        resolved.setdefault(code, b)

            src_models.BrandTSEBrandMapping.objects.filter(id__in=mapping_ids).delete()
            new_maps = [
                src_models.BrandTSEBrandMapping(brand=resolved[code], tse_brand_id=tse_id)
                for code, tse_id in tse_ids_by_code.items() if code in resolved
            ]
            src_models.BrandTSEBrandMapping.objects.bulk_create(new_maps, ignore_conflicts=True)
            remapped = len(new_maps)

        self.stdout.write("")
        self.stdout.write(self.style.SUCCESS(
            "Repaired %s mapping(s): %s new dedicated Brands row(s) created" % (
                format(remapped, ","), format(created, ","))))
        logger.info("%s remapped=%s created=%s parts=%s", _LOG_PREFIX, remapped, created, total_parts)

        # prove the invariant now holds
        with connection.cursor() as c:
            c.execute(_FIND_SQL)
            left = len(c.fetchall())
            c.execute("""
              select count(*) from (
                select m.brand_id from brand_tse_brand_mapping m
                join tse_brands tb on tb.id = m.tse_brand_id
                where tb.aaia_code ~ '^A[678]'
                group by m.brand_id having count(*) > 1) x
            """)
            shared = c.fetchone()[0]
        self.stdout.write("  verification: %s mis-merged mapping(s) remain, %s Brands row(s) "
                          "still shared by multiple line codes" % (left, shared))
