"""
Full AllPro Distributing sync: brands (APDLineCodes.txt) + unmapped-brand mapping +
catalog/inventory (APDInventory.txt) via FTP for one connection. Thin wrapper around
src.integrations.services.allpro -- see that module's docstring for the full pipeline.

    python manage.py sync_allpro_catalog --company-provider-id 178
    python manage.py sync_allpro_catalog   # uses the primary AllPro connection
"""
from django.core.management.base import BaseCommand

from src.integrations.services import allpro as allpro_services


class Command(BaseCommand):
    help = "Full AllPro sync: brands + unmapped-brand mapping + catalog/inventory via FTP."

    def add_arguments(self, parser):
        parser.add_argument(
            "--company-provider-id", type=int, default=None,
            help="CompanyProviders id to fetch with. Defaults to the primary active AllPro connection.",
        )

    def handle(self, *args, **options):
        cp_id = options.get("company_provider_id")
        if not cp_id:
            cp = allpro_services.primary_allpro_company_provider()
            if not cp:
                self.stdout.write(self.style.ERROR("No active AllPro connection found."))
                return
            cp_id = cp.id

        self.stdout.write("Fetching AllPro brands (company_provider_id={})...".format(cp_id))
        brand_count = allpro_services.fetch_and_save_allpro_brands(cp_id)
        self.stdout.write("  {} AllProBrand rows upserted.".format(brand_count))

        mapped = allpro_services.sync_unmapped_allpro_brands_to_brands()
        self.stdout.write("  {} AllProBrand rows newly mapped to Brands.".format(mapped))

        self.stdout.write("Fetching AllPro catalog/inventory (this is a large feed, ~491k rows)...")
        written = allpro_services.fetch_and_save_allpro_catalog(cp_id)
        self.stdout.write(self.style.SUCCESS("Done. {} AllProParts rows upserted.".format(written)))
