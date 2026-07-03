from django.core.management.base import BaseCommand

from src.integrations.services import integration_pricing_sync_jobs


class Command(BaseCommand):
    help = (
        "Process OPEN IntegrationPricingSyncJob rows (per-company-provider pricing sync). "
        "Intended to run from cron every minute or few minutes. "
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
            "Processing up to {} pricing sync job(s) with {} worker(s)…".format(limit, workers)
        )
        processed = integration_pricing_sync_jobs.process_pricing_sync_jobs(
            limit=limit,
            workers=workers,
        )
        self.stdout.write(self.style.SUCCESS("Processed {} job(s).".format(processed)))
