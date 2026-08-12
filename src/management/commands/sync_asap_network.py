from django.core.management.base import BaseCommand

from src import models as src_models
from src.integrations.services import asap


class Command(BaseCommand):
    help = (
        "Sync ASAP Network (paid catalog) brand, fitment, and enrichment data into MasterPart / "
        "MasterPartData / MasterPartFitment. Not a recurring cron job - a brand already fully "
        "synced is skipped unless --force is passed. By default, only brands we actually carry "
        "AND are missing fitment and/or MasterPartData are attempted (see "
        "asap.brands_needing_sync) -- pass --all to fall back to every ASAP-matched brand "
        "regardless of gap, or --brands to target specific ones explicitly."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--brands",
            type=str,
            default=None,
            help="Comma-separated ASAP brand external_id(s) to restrict the run to. Overrides "
            "the default gap-based selection and --all.",
        )
        parser.add_argument(
            "--all",
            action="store_true",
            help="Consider every ASAP-matched brand, not just ones with a fitment/data gap. "
            "Ignored if --brands is given.",
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
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Print which brands would be synced and why, then exit -- no API calls at all "
            "(not even the brand-list refresh), so it's free to run.",
        )

    def handle(self, *args, **options):
        dry_run = bool(options.get("dry_run"))
        requested = options.get("brands")
        use_all = bool(options.get("all"))

        if dry_run:
            self._dry_run(requested, use_all)
            return

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

        queryset = self._select_brands(requested, use_all)

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

    def _select_brands(self, requested, use_all):
        if requested:
            external_ids = [x.strip() for x in requested.split(",") if x.strip()]
            return src_models.AsapBrand.objects.filter(
                brand__isnull=False, external_id__in=external_ids
            ).order_by("id")
        if use_all:
            return src_models.AsapBrand.objects.filter(brand__isnull=False).order_by("id")
        return asap.brands_needing_sync()

    def _dry_run(self, requested, use_all):
        queryset = self._select_brands(requested, use_all).select_related("brand")
        rows = list(queryset)
        already_synced = sum(1 for ab in rows if ab.last_synced_at is not None)

        self.stdout.write(
            "DRY RUN -- no API calls made. {} brand(s) selected ({} already ASAP-synced before; "
            "those will still be skipped unless --force):".format(len(rows), already_synced)
        )
        for ab in rows:
            synced_note = (
                "already synced {}, still has a gap".format(ab.last_synced_at)
                if ab.last_synced_at
                else "never synced"
            )
            self.stdout.write(
                "  external_id={} name={!r} brand={!r} ({})".format(
                    ab.external_id, ab.name, ab.brand.name if ab.brand else None, synced_note
                )
            )
