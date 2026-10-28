"""
Full RANDYS Worldwide sync: public catalogue + Shopify enrichment + brand mapping + derived
master parts/inventory. Same orchestration pattern as sync_meyer / sync_xdp, plus a
ScheduledTaskExecution audit row.

Fitment and the inventory push are deliberately NOT here:
  * fitment is ~17k single-product requests (~15 min) -- fetch_randys_fitment +
    sync_master_part_fitments_from_randys, on their own cadence;
  * the inventory push lands whenever RANDYS send it -- apply_randys_inventory, run more often
    than this command.
"""
from django.core.management.base import BaseCommand

from src.audit import scheduled_tasks as audit_scheduled_tasks
from src.integrations.services import master_parts, randys


class Command(BaseCommand):
    help = "Sync the RANDYS Worldwide catalogue, brand mappings and derived master parts."

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Only report how unmapped RANDYS brands would resolve, then exit. No writes.",
        )
        parser.add_argument(
            "--dry-run-brands",
            action="store_true",
            help="Run the catalogue sweep for real, but only report the brand mapping.",
        )
        parser.add_argument(
            "--skip-shopify",
            action="store_true",
            help="Skip the Shopify enrichment pass (descriptions, image lists, tags, weights).",
        )

    def handle(self, *args, **options):
        dry_run = options.get("dry_run", False)
        dry_run_brands = options.get("dry_run_brands", False)
        skip_shopify = options.get("skip_shopify", False)

        audit_scheduled_tasks.cleanup_stale_started_executions("sync_randys")

        if dry_run:
            self.stdout.write("Dry run: reporting RANDYS brand mapping only.")
            randys.sync_unmapped_randys_brands_to_brands(dry_run=True)
            self.stdout.write(self.style.SUCCESS("Dry run complete."))
            return

        execution = audit_scheduled_tasks.start_scheduled_task_execution("sync_randys")
        try:
            self.stdout.write("Step 1/4: sweeping the public Convermax catalogue.")
            parts = randys.fetch_and_save_randys_catalog()

            enriched = 0
            if skip_shopify:
                self.stdout.write("Step 2/4: skipped (--skip-shopify).")
            else:
                self.stdout.write("Step 2/4: enriching from Shopify.")
                enriched = randys.enrich_randys_catalog_from_shopify()

            self.stdout.write("Step 3/4: mapping RANDYS brands to Brands.")
            randys.sync_unmapped_randys_brands_to_brands(dry_run=dry_run_brands)

            self.stdout.write("Step 4/4: derived master parts and inventory.")
            master_parts.sync_derived_from_randys()

            message = "RANDYS sync complete: {} parts upserted, {} enriched.".format(parts, enriched)
            audit_scheduled_tasks.mark_scheduled_task_completed(execution, message=message)
            self.stdout.write(self.style.SUCCESS(message))
        except Exception as e:
            audit_scheduled_tasks.mark_scheduled_task_failed(execution, error_message=str(e))
            self.stdout.write(self.style.ERROR("RANDYS sync failed: {}".format(str(e))))
            raise
