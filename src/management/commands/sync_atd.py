"""
Full ATD Connect sync for one company's connection: catalog + this company's own cost/stock
(``productcatalog`` sweep) + unmapped-brand mapping + propagation into MasterPart/ProviderPart/
ProviderPartInventory/ProviderPartCompanyPricing/TireSpec. Thin wrapper around
src.integrations.services.atd and src.integrations.services.master_parts -- see those modules'
docstrings for the full pipeline.

Unlike AllPro/Tri-State there is no "primary" connection to default to: ATD ties catalog, cost
AND stock all to the dealer's own locationnumber, so --company-provider-id is required.

    python manage.py sync_atd --company-provider-id 180
    python manage.py sync_atd --company-provider-id 180 --max-pages 2  # quick test
    python manage.py sync_atd --company-provider-id 180 --skip-derived  # raw fetch only
"""
from django.core.management.base import BaseCommand, CommandError

from src.integrations.services import atd as atd_services
from src.integrations.services import master_parts


class Command(BaseCommand):
    help = "Full ATD sync for one connection: catalog/pricing/stock fetch + propagation."

    def add_arguments(self, parser):
        parser.add_argument(
            "--company-provider-id", type=int, required=True,
            help="CompanyProviders id to fetch with (ATD has no shared/primary connection).",
        )
        parser.add_argument(
            "--max-pages", type=int, default=None,
            help="Stop after this many productcatalog pages (testing; omit for a full sweep).",
        )
        parser.add_argument(
            "--skip-derived", action="store_true",
            help="Only run the raw fetch (ATDPart/ATDCompanyPricing); skip MasterPart propagation.",
        )
        parser.add_argument(
            "--reindex-meilisearch", action="store_true",
            help="Run a full Meilisearch reindex after propagation.",
        )

    def handle(self, *args, **options):
        cp_id = options["company_provider_id"]
        max_pages = options.get("max_pages")

        self.stdout.write(
            "Fetching ATD catalog/pricing/stock (company_provider_id={}{})...".format(
                cp_id, ", max_pages={}".format(max_pages) if max_pages else ""
            )
        )
        brands, parts, pricing = atd_services.fetch_and_save_atd_catalog_for_company_provider(
            cp_id, max_pages=max_pages
        )
        if not parts and not pricing:
            raise CommandError(
                "No ATD data fetched for company_provider_id={} -- check credentials/connection.".format(cp_id)
            )
        self.stdout.write(
            "  {} brands, {} ATDPart rows, {} ATDCompanyPricing rows upserted.".format(brands, parts, pricing)
        )

        mapped = atd_services.sync_unmapped_atd_brands_to_brands()
        self.stdout.write("  {} ATDBrand rows newly mapped to Brands.".format(mapped))

        if options.get("skip_derived"):
            self.stdout.write(self.style.SUCCESS("Raw fetch complete. Skipped propagation (--skip-derived)."))
            return

        self.stdout.write("Propagating into MasterPart/ProviderPart/pricing/inventory/TireSpec...")
        master_parts.sync_derived_from_atd(reindex_meilisearch=options.get("reindex_meilisearch", False))
        self.stdout.write(self.style.SUCCESS("Done."))
