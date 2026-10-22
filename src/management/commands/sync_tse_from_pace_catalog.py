"""
Seed the OFFICIAL Tri-State tables (tse_brands, tse_parts) from the full PACE catalog
(pace_tse_brands, pace_tse_parts) instead of the narrower sources they had before.

This is step 1 of the Tri-State backfill. The later steps are the OFFICIAL service functions,
invoked exactly as ingest_all_providers._run_tristate and seed_tse_brands invoke them -- there
are no standalone commands for them:

    python manage.py sync_tse_from_pace_catalog                         # this command
    src.integrations.services.tristate.sync_unmapped_tse_brands_to_brands()
    src.integrations.services.master_parts.sync_master_parts_from_tristate()
    python manage.py backfill_master_part_data_from_tse                 # MasterPartData

WHY: tse_parts was seeded from tristate_parts (TriStatePart), an Algolia-derived slice holding
264,348 parts for 146 curated brands. pace_tse_parts holds 1,447,304 parts across 670 brands --
Tri-State's actual catalog as their own storefront search exposes it. Everything Tri-State sells
should be in our catalog, so the wider source wins.

TWO REGIMES ARE BOTH INCLUDED, DELIBERATELY

506 of the 670 brands carry AAM member-local jobber line codes rather than real AAIA codes
(A6/A7/A8 prefixes -- DOR, STA, RAY, SPC), covering 758,465 parts. Those parts are empty shells:
no description, no image, no category, no fitment, and a name that is just "<code> <part
number>". They are included anyway because Tri-State genuinely sells them -- their own site
lists them -- so leaving them out would under-represent the catalog. Expect them to arrive with
almost no enrichment; that is the source data, not a bug here.

Consequence to be aware of downstream: the official brand matcher fuzzy-matches on name, and a
3-character name prefix-matches almost anything, so line-code brands will produce some wrong
Brands matches and (via its phase 4) new Brands rows named "DOR", "STA" and so on. That is
accepted for coverage.

WHAT IS NOT CARRIED

Inventory and pricing. pace_tse_parts comes from a search index, which has an `inventory` number
but no per-warehouse breakdown and no account pricing. TSEParts.quantity_available,
orderable, warehouse_availability and inventory_synced_at are therefore left alone -- they stay
NULL until Tri-State's real Customer API fills them via
tristate.fetch_tristate_inventory_and_pricing_for_company. Not overwriting them also means
re-running this never clobbers live inventory that the API has already fetched.

warehouse_code on tse_brands is likewise left alone: it comes from the API's get_brands and PACE
does not have it.

    python manage.py sync_tse_from_pace_catalog --dry-run
    python manage.py sync_tse_from_pace_catalog
"""
import logging
import time
import typing

import pgbulk
from django.core.management.base import BaseCommand

from src import models as src_models

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[TSE-FROM-PACE]"
_BATCH = 5000


class Command(BaseCommand):
    help = (
        "Seed the official tse_brands/tse_parts from the full PACE catalog "
        "(pace_tse_brands/pace_tse_parts). Does not touch inventory, pricing or warehouse_code."
    )

    def add_arguments(self, parser):
        parser.add_argument("--dry-run", action="store_true",
                            help="Report what would be written, write nothing.")
        parser.add_argument("--limit", type=int, default=None,
                            help="Process only the first N pace_tse_parts rows (test run).")
        parser.add_argument("--brands-only", action="store_true",
                            help="Do the tse_brands step and stop.")

    def handle(self, *args, **options):
        dry_run = options["dry_run"]
        started = time.monotonic()

        src_brands = src_models.PaceTseBrand.objects.count()
        src_parts = src_models.PaceTsePart.objects.count()
        self.stdout.write("source: pace_tse_brands=%s  pace_tse_parts=%s%s" % (
            format(src_brands, ","), format(src_parts, ","), " [DRY RUN]" if dry_run else ""))
        self.stdout.write("target before: tse_brands=%s  tse_parts=%s" % (
            format(src_models.TSEBrand.objects.count(), ","),
            format(src_models.TSEParts.objects.count(), ",")))
        self.stdout.flush()

        brands_written = self._sync_brands(dry_run)
        if options["brands_only"]:
            self.stdout.write(self.style.SUCCESS("brands-only: done (%s)" % format(brands_written, ",")))
            return

        parts_written = self._sync_parts(dry_run, options["limit"])

        self.stdout.write("")
        self.stdout.write(self.style.SUCCESS(
            "Done in %.0fs: %s brand(s), %s part(s)%s" % (
                time.monotonic() - started, format(brands_written, ","),
                format(parts_written, ","), " (dry run)" if dry_run else "")))
        if not dry_run:
            self.stdout.write("target after : tse_brands=%s  tse_parts=%s" % (
                format(src_models.TSEBrand.objects.count(), ","),
                format(src_models.TSEParts.objects.count(), ",")))
            self.stdout.write("")
            self.stdout.write("next: sync_tse_brands_to_brands, then sync_master_parts_from_tse")
        logger.info("%s brands=%s parts=%s", _LOG_PREFIX, brands_written, parts_written)

    def _sync_brands(self, dry_run: bool) -> int:
        """Upsert TSEBrand from pace_tse_brands on aaia_code.

        update_fields is name only: warehouse_code comes from Tri-State's get_brands endpoint
        and PACE has no equivalent, so re-running must never blank it.
        """
        rows = list(src_models.PaceTseBrand.objects.values("aaia_brand_code", "name"))
        instances, seen = [], set()
        for r in rows:
            code = (r["aaia_brand_code"] or "").strip().upper()
            if not code or code in seen:
                continue
            seen.add(code)
            instances.append(src_models.TSEBrand(
                aaia_code=code[:16], name=(r["name"] or code)[:255]))

        existing = set(src_models.TSEBrand.objects.values_list("aaia_code", flat=True))
        new = [i for i in instances if i.aaia_code not in existing]
        self.stdout.write("  tse_brands: %s from PACE, %s already present, %s new" % (
            format(len(instances), ","), format(len(instances) - len(new), ","), format(len(new), ",")))
        self.stdout.flush()
        if dry_run:
            return len(instances)

        pgbulk.upsert(
            src_models.TSEBrand, instances,
            unique_fields=["aaia_code"],
            update_fields=["name", "updated_at"],
            returning=False,
        )
        return len(instances)

    def _sync_parts(self, dry_run: bool, limit: typing.Optional[int]) -> int:
        brand_by_code = {
            b.aaia_code.upper(): b
            for b in src_models.TSEBrand.objects.all() if b.aaia_code
        }
        if not brand_by_code:
            self.stdout.write(self.style.ERROR("  no TSEBrand rows -- brands step must run first"))
            return 0

        qs = src_models.PaceTsePart.objects.order_by("id").values(
            "id", "aaia_brand_code", "part", "name", "ext", "image", "category", "subcategory")

        total = 0
        skipped_no_brand = 0
        last_id = 0
        batch_num = 0
        while True:
            if limit is not None and total >= limit:
                break
            take = _BATCH if limit is None else min(_BATCH, limit - total)
            batch = list(qs.filter(id__gt=last_id)[:take])
            if not batch:
                break
            batch_num += 1
            last_id = batch[-1]["id"]

            instances, seen = [], set()
            for row in batch:
                brand = brand_by_code.get((row["aaia_brand_code"] or "").upper())
                if not brand:
                    skipped_no_brand += 1
                    continue
                part_number = (row["part"] or "").strip()
                if not part_number:
                    continue
                key = (brand.id, part_number)
                if key in seen:
                    continue
                seen.add(key)
                # ext is the real long description; name is a "<brand> <part>" template, so it
                # is only a fallback.
                instances.append(src_models.TSEParts(
                    brand=brand,
                    part_number=part_number[:255],
                    description=(row["ext"] or row["name"] or None),
                    image_url=(row["image"] or None),
                    category=(row["category"] or None),
                    subcategory=(row["subcategory"] or None),
                ))

            if instances and not dry_run:
                pgbulk.upsert(
                    src_models.TSEParts, instances,
                    unique_fields=["brand", "part_number"],
                    # inventory/orderable/warehouse_availability/inventory_synced_at are NOT
                    # listed: they belong to the live API sync and must survive a re-run.
                    update_fields=["description", "image_url", "category", "subcategory",
                                   "updated_at"],
                    returning=False,
                )
            total += len(instances)
            if batch_num % 20 == 0:
                self.stdout.write("    ... %s parts staged" % format(total, ","))
                self.stdout.flush()

        self.stdout.write("  tse_parts: %s upserted%s" % (
            format(total, ","), " (dry run)" if dry_run else ""))
        if skipped_no_brand:
            self.stdout.write(self.style.WARNING(
                "    %s part(s) skipped -- no TSEBrand for their aaia_brand_code" % format(
                    skipped_no_brand, ",")))
        return total
