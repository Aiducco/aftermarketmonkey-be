"""
Daily full sweep of Turn 14's shared catalog -- the "Daily Full Sweep" tier of their proposed
integration model.

Runs the flat, unscoped collections rather than paging 464 brands individually: ~776 requests
against ~4 200 for identical data (Turn 14 serves the unscoped endpoints 1 000 rows to a page,
the brand-scoped ones 200).

Order is not arbitrary. Only /v1/items carries brand_id, so items must land before the
collections whose brand has to be resolved through Turn14Items.
"""
import typing

from django.core.management.base import BaseCommand
from django.utils import timezone

from src.audit import scheduled_tasks as audit_scheduled_tasks
from src.integrations import rate_limit as rate_limit_base
from src.integrations.services import integration_pricing_sync_jobs
from src.integrations.services import master_parts
from src.integrations.services import turn_14 as turn_14_services
from src.integrations.services import turn_14_global, turn_14_sweeps

_TASK_NAME = "sync_turn14_global_sweep"


class Command(BaseCommand):
    help = (
        "Daily Turn 14 global sweep: brands, locations, items, items/data, inventory, "
        "dropship controllers and shipping estimates, using the flat catalog-wide endpoints."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--skip-shipping-estimates",
            action="store_true",
            help="Skip the shipping estimate sweep (~795 requests, flat -- see turn_14_sweeps.sweep_shipping_estimates).",
        )
        parser.add_argument(
            "--skip-dropship",
            action="store_true",
            help="Skip resolving dropship controllers.",
        )
        parser.add_argument(
            "--deactivate-missing",
            action="store_true",
            help=(
                "After a fully successful items sweep, mark items Turn 14 no longer returns as "
                "inactive. Off by default: it is only safe when the sweep truly completed."
            ),
        )

    def handle(self, *args, **options):
        audit_scheduled_tasks.cleanup_stale_started_executions(_TASK_NAME)
        execution = audit_scheduled_tasks.start_scheduled_task_execution(_TASK_NAME)
        meter = rate_limit_base.UsageMeter("t14:get")
        started_at = timezone.now()
        results: typing.Dict[str, typing.Any] = {}

        meter.__enter__()
        try:
            client = turn_14_global.get_global_client()

            turn_14_services.fetch_and_save_turn_14_brands()
            turn_14_services.sync_unmapped_turn_14_brands_to_brands()
            turn_14_services.fetch_and_save_turn_14_locations()

            results["items"] = turn_14_sweeps.sweep_items(client)
            results["items_data"] = turn_14_sweeps.sweep_items_data(client)
            results["inventory"] = turn_14_sweeps.sweep_inventory(client)

            if not options["skip_dropship"]:
                results["dropship"] = turn_14_sweeps.sweep_dropship_controllers(client)
            if not options["skip_shipping_estimates"]:
                results["shipping_estimates"] = turn_14_sweeps.sweep_shipping_estimates(client)

            # Only after every raw sweep above completed -- propagating from a half-swept catalog
            # would push incomplete data into MasterPart/ProviderPart. Pricing sync itself stays
            # separate (the per-company IntegrationPricingSyncJob queue), but Turn 14's *recurring*
            # enqueue happens right here rather than on ingest_all_providers' every-4-hours cycle
            # -- pricing should be checked against the catalog just swept, not against up to ~20h
            # stale data (see enqueue_all_active_turn14_pricing_jobs). On-connect enqueue for a
            # brand new connection is unaffected -- that still fires immediately, unrelated to
            # this daily cycle.
            self.stdout.write("Propagating swept Turn14 data into MasterPart/ProviderPart...")
            master_parts.sync_derived_from_turn14(skip_pricing=True)
            self.stdout.write("Propagation complete.")

            results["pricing_jobs_enqueued"] = integration_pricing_sync_jobs.enqueue_all_active_turn14_pricing_jobs()

            # Only after everything above completed -- a sweep cut short by a spent budget has
            # seen an arbitrary prefix of the catalog, and "deactivate everything unseen" would
            # then take out most of it.
            if options["deactivate_missing"]:
                results["deactivated"] = turn_14_sweeps.deactivate_items_missing_from_sweep(started_at)

        except rate_limit_base.RateBudgetExhausted as e:
            meter.__exit__(None, None, None)
            audit_scheduled_tasks.mark_scheduled_task_failed(
                execution,
                error_message="Deferred, rate budget spent: {} || partial={} || {}".format(
                    e, results, meter.summary("api_usage")
                ),
            )
            self.stderr.write("Rate budget exhausted: {}".format(e))
            return
        except Exception as e:
            meter.__exit__(None, None, None)
            audit_scheduled_tasks.mark_scheduled_task_failed(execution, error_message=str(e))
            raise

        meter.__exit__(None, None, None)
        message = "{} || {}".format(results, meter.summary("api_usage"))
        audit_scheduled_tasks.mark_scheduled_task_completed(execution, message=message)
        self.stdout.write(message)
