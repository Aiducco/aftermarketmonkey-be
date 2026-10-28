from django.core.management.base import BaseCommand

from src.audit import scheduled_tasks as audit_scheduled_tasks
from src.integrations.services import master_parts, xdp


class Command(BaseCommand):
    help = (
        "Fetch XDP's public catalog (SearchSpring, swept brand by brand because the index "
        "enforces a 10,000-result window), enrich weights from Miva json.mvc, read the primary "
        "connection's house-brand dealer CSV, sync unmapped brands, then propagate into "
        "MasterPart / ProviderPart / ProviderPartInventory. No credentials are needed for the "
        "catalog itself. Per-company pricing is handled separately by the "
        "IntegrationPricingSyncJob queue. Fitment is a separate command: fetch_xdp_fitment."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--skip-catalog",
            action="store_true",
            help="Skip the SearchSpring catalog sweep; only run the later stages.",
        )
        parser.add_argument(
            "--skip-weights",
            action="store_true",
            help="Skip the Miva json.mvc weight enrichment pass (~13 requests).",
        )
        parser.add_argument(
            "--skip-dealer-feed",
            action="store_true",
            help="Skip reading the primary connection's house-brand dealer CSV.",
        )
        parser.add_argument(
            "--skip-brands",
            action="store_true",
            help="Skip unmapped-brand sync.",
        )
        parser.add_argument(
            "--skip-derived",
            action="store_true",
            help="Skip propagating into MasterPart / ProviderPart / ProviderPartInventory.",
        )

    def handle(self, *args, **options):
        audit_scheduled_tasks.cleanup_stale_started_executions("fetch_xdp_feed")
        execution = audit_scheduled_tasks.start_scheduled_task_execution("fetch_xdp_feed")
        try:
            if not options["skip_catalog"]:
                self.stdout.write("Sweeping XDP SearchSpring catalog (per brand)...")
                upserted = xdp.fetch_and_save_xdp_catalog()
                self.stdout.write("Catalog rows upserted: {}".format(upserted))

            if not options["skip_weights"]:
                self.stdout.write("Enriching weights from Miva json.mvc...")
                enriched = xdp.enrich_xdp_catalog_from_json_mvc()
                self.stdout.write("Weights updated: {}".format(enriched))

            if not options["skip_dealer_feed"]:
                self.stdout.write("Reading primary connection's house-brand dealer feed...")
                matched = xdp.fetch_and_save_xdp_dealer_feed()
                self.stdout.write("House-brand parts updated from feed: {}".format(matched))

            if not options["skip_brands"]:
                self.stdout.write("Syncing unmapped XDP brands...")
                xdp.sync_unmapped_xdp_brands_to_brands()

            if not options["skip_derived"]:
                self.stdout.write("Propagating into MasterPart / ProviderPart / ProviderPartInventory...")
                master_parts.sync_derived_from_xdp(skip_pricing=True)

            audit_scheduled_tasks.mark_scheduled_task_completed(
                execution, message="Successfully completed XDP feed ingest."
            )
            self.stdout.write(self.style.SUCCESS("Done."))
        except Exception as e:
            audit_scheduled_tasks.mark_scheduled_task_failed(execution, error_message=str(e))
            self.stdout.write(self.style.ERROR("Error: {}".format(str(e))))
            raise
