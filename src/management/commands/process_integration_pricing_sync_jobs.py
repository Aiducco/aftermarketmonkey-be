from django.core.management.base import BaseCommand

from src.integrations.services import integration_pricing_sync_jobs


class Command(BaseCommand):
    help = (
        "Process OPEN IntegrationPricingSyncJob rows (per-company-provider pricing sync). "
        "Intended to run from cron every minute or few minutes."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--limit",
            type=int,
            default=10,
            help="Maximum number of jobs to process in one invocation (default: 10).",
        )

    def handle(self, *args, **options):
        limit = max(1, int(options["limit"] or 10))
        processed = 0
        while processed < limit:
            job = integration_pricing_sync_jobs.claim_next_open_job()
            if not job:
                break
            self.stdout.write("Running integration pricing sync job id={}…".format(job.id))
            integration_pricing_sync_jobs.run_integration_pricing_sync_job(job)
            processed += 1
        self.stdout.write(self.style.SUCCESS("Processed {} job(s).".format(processed)))
