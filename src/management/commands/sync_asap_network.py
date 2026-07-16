from django.core.management.base import BaseCommand

from src import models as src_models
from src.integrations.services import asap


class Command(BaseCommand):
    help = (
        "Sync ASAP Network (paid catalog) brand, fitment, and enrichment data into MasterPart / "
        "MasterPartData / MasterPartFitment. Not a recurring cron job - a brand already fully "
        "synced is skipped unless --force is passed."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--brands",
            type=str,
            default=None,
            help="Comma-separated ASAP brand external_id(s) to restrict the run to.",
        )
        parser.add_argument(
            "--force",
            action="store_true",
            help="Re-sync brands even if already marked synced (last_synced_at is set).",
        )
        parser.add_argument(
            "--brands-only",
            action="store_true",
            help="Only refresh the ASAP brand catalog and canonical-Brand matching; skip product sync.",
        )

    def handle(self, *args, **options):
        self.stdout.write("Starting ASAP Network sync...")

        brand_stats = asap.sync_asap_brands()
        self.stdout.write(
            self.style.SUCCESS(
                "Brand catalog: synced {brands_synced}, matched {brands_matched} this run, "
                "{brands_total} total.".format(**brand_stats)
            )
        )

        if options.get("brands_only"):
            self.stdout.write(self.style.SUCCESS("--brands-only set; skipping product sync."))
            return

        queryset = src_models.AsapBrand.objects.filter(brand__isnull=False).order_by("id")
        requested = options.get("brands")
        if requested:
            external_ids = [x.strip() for x in requested.split(",") if x.strip()]
            queryset = queryset.filter(external_id__in=external_ids)

        force = bool(options.get("force"))
        totals = {"brands_run": 0, "brands_skipped": 0, "processed": 0, "matched": 0, "unmatched": 0, "fetch_failed": 0, "fitments": 0}

        try:
            for asap_brand in queryset:
                stats = asap.sync_asap_products_for_brand(asap_brand, force=force)
                if stats.get("skipped"):
                    totals["brands_skipped"] += 1
                    continue
                totals["brands_run"] += 1
                for key in ("processed", "matched", "unmatched", "fetch_failed", "fitments"):
                    totals[key] += stats.get(key, 0)

            self.stdout.write(
                self.style.SUCCESS(
                    "Done. Brands run: {brands_run}, skipped: {brands_skipped}. "
                    "Products processed: {processed}, matched: {matched}, unmatched: {unmatched}, "
                    "fetch failed: {fetch_failed}. Fitment rows written: {fitments}.".format(**totals)
                )
            )
        except Exception as e:
            self.stdout.write(self.style.ERROR("Error: {}".format(str(e))))
            raise
