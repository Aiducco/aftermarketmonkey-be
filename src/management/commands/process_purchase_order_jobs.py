from django.core.management.base import BaseCommand

from src.integrations.services import purchase_order_jobs


class Command(BaseCommand):
    help = (
        "Process OPEN PurchaseOrderJob rows (quote/submit/status-check/cancel against a "
        "distributor's order API). Intended to run from cron every 30-60 seconds, since a "
        "staff user is typically watching the UI live for a quote/submit result. "
        "Use --workers to process multiple jobs in parallel within one invocation."
    )

    def add_arguments(self, parser):
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
        self.stdout.write(
            "Processing up to {} purchase order job(s) with {} worker(s)…".format(limit, workers)
        )
        processed = purchase_order_jobs.process_purchase_order_jobs(limit=limit, workers=workers)
        self.stdout.write(self.style.SUCCESS("Processed {} job(s).".format(processed)))
