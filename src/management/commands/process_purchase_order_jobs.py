from django.core.management.base import BaseCommand, CommandError

from src import enums as src_enums
from src.integrations.services import purchase_order_jobs

_OPERATION_CHOICES = {op.name.lower(): op.value for op in src_enums.PurchaseOrderOperation}


class Command(BaseCommand):
    help = (
        "Process OPEN PurchaseOrderJob rows (status-check/cancel against a distributor's order "
        "API — quote and submit are both called synchronously from the request that triggers "
        "them and never reach this queue; see src.integrations.services.purchase_order_jobs). "
        "--operations is REQUIRED and has no default on purpose: CANCEL can affect a real "
        "order, so cron should only ever be configured with --operations status_check — never "
        "cancel. Run with --operations cancel by hand, while watching, for that."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--operations",
            type=str,
            required=True,
            help=(
                "Required, comma-separated. One or more of: {}. Cron entries must only ever "
                "use status_check. (quote/submit are accepted for backwards compatibility but "
                "are always a no-op now — no job of either kind is ever enqueued.)".format(
                    ",".join(_OPERATION_CHOICES.keys())
                )
            ),
        )
        parser.add_argument(
            "--limit",
            type=int,
            default=10,
            help="Maximum number of jobs to process in one invocation (default: 10).",
        )
        parser.add_argument(
            "--workers",
            type=int,
            default=1,
            help=(
                "Number of parallel worker threads (default: 1). "
                "Each worker claims its own job atomically — safe to run multiple "
                "concurrent invocations of this command as well."
            ),
        )

    def handle(self, *args, **options):
        limit = max(1, int(options.get("limit") or 10))
        workers = max(1, int(options.get("workers") or 1))

        raw_operations = [o.strip().lower() for o in options["operations"].split(",") if o.strip()]
        if not raw_operations:
            raise CommandError("--operations must name at least one operation.")
        unknown = [o for o in raw_operations if o not in _OPERATION_CHOICES]
        if unknown:
            raise CommandError(
                "Unknown operation(s): {}. Valid choices: {}.".format(
                    ", ".join(unknown), ", ".join(_OPERATION_CHOICES.keys())
                )
            )
        allowed_operations = [_OPERATION_CHOICES[o] for o in raw_operations]

        if "cancel" in raw_operations:
            self.stdout.write(
                self.style.WARNING(
                    "--operations includes cancel — this can affect a REAL order. "
                    "Only run this manually while watching, never from cron."
                )
            )
        if "submit" in raw_operations or "quote" in raw_operations:
            self.stdout.write(
                self.style.WARNING(
                    "--operations includes {} — this is a permanent no-op: quote and submit "
                    "are both called synchronously and no job of either kind is ever enqueued "
                    "for this command to claim.".format(
                        ", ".join(o for o in raw_operations if o in ("submit", "quote"))
                    )
                )
            )

        self.stdout.write(
            "Processing up to {} purchase order job(s) ({}) with {} worker(s)…".format(
                limit, ", ".join(raw_operations), workers
            )
        )
        processed = purchase_order_jobs.process_purchase_order_jobs(
            limit=limit, workers=workers, allowed_operations=allowed_operations
        )
        self.stdout.write(self.style.SUCCESS("Processed {} job(s).".format(processed)))
