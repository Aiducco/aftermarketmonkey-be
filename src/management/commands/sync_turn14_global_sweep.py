"""
Daily full sweep of Turn 14's shared catalog -- the "Daily Full Sweep" tier of their proposed
integration model.

Runs the flat, unscoped collections rather than paging 464 brands individually: ~776 requests
against ~4 200 for identical data (Turn 14 serves the unscoped endpoints 1 000 rows to a page,
the brand-scoped ones 200).

Order is not arbitrary. Only /v1/items carries brand_id, so items must land before the
collections whose brand has to be resolved through Turn14Items. That dependency also bounds
``--phase``: items_data rides along in phase 1 (it only needs items, just above it), leaving
phase 2 as inventory/dropship/shipping estimates plus propagation. The whole sweep's ~4 200
requests fit under the 5 000/hour budget in principle, but competing crons (the 10-minute
inventory-delta job, per-company pricing syncs) share the same budget, and a single run that
spills past its hour forces long internal rate-limit retries that starve those crons for the
rest of the day (observed live 2026-08-26).

Phase 3 -- enqueuing every active Turn 14 company's own pricing sync -- is split out separately
rather than tacked onto phase 2, because it is not more API requests against *this* budget, it's
kicking off up to ~17 minutes of per-company work (a ~9 minute raw fetch plus a ~7 minute
master-parts sync, measured live 2026-08-26) for every active connection, 4 at a time. Firing
that immediately after phase 2's own heavy sweep -- rather than after phase 2 has had time to
fully finish and the host to settle -- is exactly what caused a real production incident
(2026-08-26): the resulting host memory/CPU pressure evicted Postgres's buffer cache badly
enough to take /api/search/ down for several minutes. Scheduling all three phases as separate
cron entries, spaced out, keeps each one's own cost from compounding into the next:

    0 1 * * *  manage.py sync_turn14_global_sweep --phase 1 --deactivate-missing
    0 3 * * *  manage.py sync_turn14_global_sweep --phase 2
    0 5 * * *  manage.py sync_turn14_global_sweep --phase 3

Omitting ``--phase`` runs everything in one invocation, as before (useful for ad hoc/manual
runs where splitting buys nothing).
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
        "Daily Turn 14 global sweep: brands, locations, shipping options, items, items/data, "
        "inventory, dropship controllers, shipping estimates and per-company pricing, using the "
        "flat catalog-wide endpoints. --phase 1/2/3 splits it into scheduled invocations; omit "
        "for one full run."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--phase",
            type=int,
            choices=[1, 2, 3],
            default=None,
            help=(
                "Run only phase 1 (brands/locations/shipping options/items/items data), phase 2 "
                "(inventory/dropship/shipping estimates/propagation), or phase 3 (enqueue every "
                "active Turn 14 company's pricing sync). Omit to run the full sweep in one "
                "invocation."
            ),
        )
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
                "inactive. Off by default: it is only safe when the items sweep truly "
                "completed -- this only depends on that sweep, so it belongs with --phase 1."
            ),
        )

    def handle(self, *args, **options):
        phase = options["phase"]
        task_name = _TASK_NAME if phase is None else "{}_phase{}".format(_TASK_NAME, phase)
        run_phase1 = phase in (None, 1)
        run_phase2 = phase in (None, 2)
        run_phase3 = phase in (None, 3)

        audit_scheduled_tasks.cleanup_stale_started_executions(task_name)
        execution = audit_scheduled_tasks.start_scheduled_task_execution(task_name)
        meter = rate_limit_base.UsageMeter("t14:get")
        started_at = timezone.now()
        results: typing.Dict[str, typing.Any] = {}

        meter.__enter__()
        try:
            client = turn_14_global.get_global_client()

            if run_phase1:
                rate_limit_base.retry_on_rate_budget(
                    "brands", turn_14_services.fetch_and_save_turn_14_brands, self.stdout.write
                )
                rate_limit_base.retry_on_rate_budget(
                    "brand_mapping", turn_14_services.sync_unmapped_turn_14_brands_to_brands, self.stdout.write
                )
                rate_limit_base.retry_on_rate_budget(
                    "locations", turn_14_services.fetch_and_save_turn_14_locations, self.stdout.write
                )
                results["shipping_options"] = rate_limit_base.retry_on_rate_budget(
                    "shipping_options", lambda: turn_14_sweeps.sweep_shipping_options(client), self.stdout.write
                )
                results["items"] = rate_limit_base.retry_on_rate_budget(
                    "items", rate_limit_base.resumable_sweep(turn_14_sweeps.sweep_items, client=client), self.stdout.write
                )

                # Only depends on the items sweep just above -- a completed flat items sweep
                # touches every item Turn 14 still carries, so anything untouched is gone. Placed
                # here (not phase 2) because that is its only real dependency.
                if options["deactivate_missing"]:
                    results["deactivated"] = turn_14_sweeps.deactivate_items_missing_from_sweep(started_at)

                # items_data is the heaviest single endpoint (~1724 requests) but only depends on
                # items (just above) for brand resolution, so it rides along in phase 1 rather
                # than phase 2 -- phase 2 is then just inventory/dropship/shipping estimates plus
                # propagation, comfortably lighter.
                results["items_data"] = rate_limit_base.retry_on_rate_budget(
                    "items_data", rate_limit_base.resumable_sweep(turn_14_sweeps.sweep_items_data, client=client),
                    self.stdout.write,
                )

            if run_phase2:
                results["inventory"] = rate_limit_base.retry_on_rate_budget(
                    "inventory", rate_limit_base.resumable_sweep(turn_14_sweeps.sweep_inventory, client=client),
                    self.stdout.write,
                )

                if not options["skip_dropship"]:
                    results["dropship"] = rate_limit_base.retry_on_rate_budget(
                        "dropship", lambda: turn_14_sweeps.sweep_dropship_controllers(client), self.stdout.write
                    )
                if not options["skip_shipping_estimates"]:
                    results["shipping_estimates"] = rate_limit_base.retry_on_rate_budget(
                        "shipping_estimates",
                        rate_limit_base.resumable_sweep(turn_14_sweeps.sweep_shipping_estimates, client=client),
                        self.stdout.write,
                    )

                # Only after every raw sweep above completed -- propagating from a half-swept
                # catalog would push incomplete data into MasterPart/ProviderPart.
                self.stdout.write("Propagating swept Turn14 data into MasterPart/ProviderPart...")
                master_parts.sync_derived_from_turn14(skip_pricing=True)
                self.stdout.write("Propagation complete.")

            if run_phase3:
                # Pricing sync itself stays separate (the per-company IntegrationPricingSyncJob
                # queue), but Turn 14's *recurring* enqueue happens from this daily cycle rather
                # than on ingest_all_providers' every-4-hours one -- pricing should be checked
                # against the catalog just swept, not against up to ~20h stale data (see
                # enqueue_all_active_turn14_pricing_jobs). On-connect enqueue for a brand new
                # connection is unaffected -- that still fires immediately, unrelated to this
                # daily cycle. Split into its own phase, scheduled after phase 2 has had time to
                # finish and the host to settle, rather than tacked onto phase 2 directly: each
                # enqueued job is ~17 minutes of real per-company work (see this file's own
                # docstring), and firing that immediately after phase 2's own heavy sweep is what
                # caused a real production incident (2026-08-26) -- the combined host pressure
                # took /api/search/ down. When run standalone (--phase 3), this assumes phases 1
                # and 2 already ran -- true on the scheduled cron split, not guaranteed for an ad
                # hoc run.
                results["pricing_jobs_enqueued"] = integration_pricing_sync_jobs.enqueue_all_active_turn14_pricing_jobs()

        except rate_limit_base.RateBudgetExhausted as e:
            # Reaches here only after retry_on_rate_budget's own retries were exhausted -- a
            # genuinely stuck budget (the daily cap, or Turn 14 down), not an ordinary hourly
            # cooldown, which the retry loop already waited out.
            meter.__exit__(None, None, None)
            audit_scheduled_tasks.mark_scheduled_task_failed(
                execution,
                error_message="Gave up after {} rate-limit retries: {} || partial={} || {}".format(
                    rate_limit_base.DEFAULT_MAX_RATE_LIMIT_RETRIES, e, results, meter.summary("api_usage")
                ),
            )
            self.stderr.write("Rate budget exhausted after retries: {}".format(e))
            return
        except Exception as e:
            meter.__exit__(None, None, None)
            audit_scheduled_tasks.mark_scheduled_task_failed(
                execution,
                error_message="{} || partial={} || {}".format(e, results, meter.summary("api_usage")),
            )
            raise

        meter.__exit__(None, None, None)
        message = "{} || {}".format(results, meter.summary("api_usage"))
        audit_scheduled_tasks.mark_scheduled_task_completed(execution, message=message)
        self.stdout.write(message)
