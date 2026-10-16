"""
Backfill AllProParts.description/image_url/category/subcategory/series from the shared Algolia
catalog (not in AllPro's own feed at all). See
src.integrations.services.allpro.enrich_allpro_parts_from_algolia's docstring -- image_url
written here is display-only and is never read by master_parts.py's AllPro ingest.

Not wired into the regular ingest_all_providers cadence -- catalog metadata like this rarely
changes, so it's meant to be run by hand, same as the one-off Tri-State Algolia imports.

    python manage.py enrich_allpro_parts_from_algolia
    python manage.py enrich_allpro_parts_from_algolia --aaia-codes BHTJ,BKDK
"""
from django.core.management.base import BaseCommand

from src.integrations.services import allpro as allpro_services


class Command(BaseCommand):
    help = "Backfill AllProParts description/image/category from the shared Algolia catalog."

    def add_arguments(self, parser):
        parser.add_argument(
            "--aaia-codes", type=str, default=None,
            help="Comma-separated AAIA codes to limit enrichment to (default: all mapped AllPro brands).",
        )

    def handle(self, *args, **options):
        codes = None
        if options.get("aaia_codes"):
            codes = [c.strip() for c in options["aaia_codes"].split(",") if c.strip()]

        total = allpro_services.enrich_allpro_parts_from_algolia(aaia_codes=codes)
        self.stdout.write(self.style.SUCCESS("Done. {} AllProParts rows enriched.".format(total)))
