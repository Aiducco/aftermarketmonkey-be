"""
Weekly Turn 14 fitment sweep -- the "Weekly/Every Few Days" tier of their model.

Turn 14 provides ACES vehicle ids per item; joined against a licensed VCdb copy (VcdbVehicle)
this is what makes year/make/model search possible over their catalog. Decodes straight into
MasterPartFitment -- no intermediate Turn14ItemFitment table, no per-vehicle row explosion -- so
output size is bounded by real distinct fitment combinations, the same way Rough Country's and
ASAP's MasterPartFitment rows already are.

Deliberately its own command rather than part of the daily sweep: it is the single largest
read+decode in the integration (each item carries dozens of vehicle ids), so it wants its own
window.
"""
from django.core.management.base import BaseCommand

from src.audit import scheduled_tasks as audit_scheduled_tasks
from src.integrations import rate_limit as rate_limit_base
from src.integrations.services import turn_14_global, turn_14_sweeps

_TASK_NAME = "sync_turn14_fitment_sweep"


class Command(BaseCommand):
    help = "Weekly Turn 14 fitment sweep via the flat /v1/items/fitment endpoint."

    def add_arguments(self, parser):
        parser.add_argument(
            "--max-pages",
            type=int,
            default=None,
            help="Stop after this many pages (200 rows/page) -- for a bounded smoke test before a full run.",
        )

    def handle(self, *args, **options):
        audit_scheduled_tasks.cleanup_stale_started_executions(_TASK_NAME, max_age_minutes=600)
        execution = audit_scheduled_tasks.start_scheduled_task_execution(_TASK_NAME)
        meter = rate_limit_base.UsageMeter("t14:get")

        meter.__enter__()
        try:
            seen, written = rate_limit_base.retry_on_rate_budget(
                "fitment",
                lambda: turn_14_sweeps.sweep_fitment(
                    turn_14_global.get_global_client(), max_pages=options["max_pages"]
                ),
                self.stdout.write,
            )
        except rate_limit_base.RateBudgetExhausted as e:
            # Reaches here only after retry_on_rate_budget's own retries were exhausted -- a
            # genuinely stuck budget (the daily cap, or Turn 14 down), not an ordinary hourly
            # cooldown, which the retry loop already waited out. Each retry restarts fitment
            # from page 1 (sweep_fitment has no resume cursor) -- acceptable here since the
            # whole sweep (~3 879 pages) already fits inside one hour's budget when uncontended,
            # so a retry is a full redo, not a redo of everything ever attempted.
            meter.__exit__(None, None, None)
            audit_scheduled_tasks.mark_scheduled_task_failed(
                execution, error_message="Gave up after {} rate-limit retries: {} || {}".format(
                    rate_limit_base.DEFAULT_MAX_RATE_LIMIT_RETRIES, e, meter.summary("api_usage")
                ),
            )
            return
        except Exception as e:
            meter.__exit__(None, None, None)
            audit_scheduled_tasks.mark_scheduled_task_failed(execution, error_message=str(e))
            raise

        meter.__exit__(None, None, None)
        message = "fitment rows seen={} written={} || {}".format(seen, written, meter.summary("api_usage"))
        audit_scheduled_tasks.mark_scheduled_task_completed(execution, message=message)
        self.stdout.write(message)
