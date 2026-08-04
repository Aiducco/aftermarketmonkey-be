from django.core.management.base import BaseCommand
from django.db import connection

from src.integrations.services import integration_pricing_sync_jobs

# Arbitrary fixed key identifying this command's session-scoped Postgres advisory lock -- any
# 64-bit int works, this one has no other meaning. Session-scoped (not transaction-scoped) so it
# stays held for the whole invocation, and Postgres releases it automatically when the
# connection closes for ANY reason (normal exit, crash, or being OOM-killed -- confirmed live
# this cron job's worker process gets killed by the OOM killer under memory pressure), so a
# killed run can never leave a stale lock behind the way a PID/lockfile guard could.
_ADVISORY_LOCK_KEY = 0x1F5F5F5F


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
                "concurrent invocations of this command as well, though see the "
                "overlap-guard note below for why this cron job no longer relies on that."
            ),
        )

    def handle(self, *args, **options):
        limit = max(1, int(options.get("limit") or 10))
        workers = max(1, int(options.get("workers") or 1))

        # Overlap guard: this cron job fires every 5 minutes, but a single invocation can take
        # well over that under load (confirmed live: 15-40+ minutes), so without this, every
        # missed cron cycle stacks another full --workers-worth of concurrent invocations on
        # top of the last, uncapped -- confirmed live this compounded to 3 simultaneous
        # invocations using ~4.5GB combined, which was starving the whole container of memory
        # and getting gunicorn itself OOM-killed, not just this job. Each worker still claims
        # jobs correctly/safely if two invocations somehow did overlap (unchanged), but a new
        # cron-triggered invocation now skips its whole run rather than piling on if a previous
        # one is still going, instead of assuming 5 minutes is always enough.
        with connection.cursor() as cursor:
            cursor.execute("SELECT pg_try_advisory_lock(%s)", [_ADVISORY_LOCK_KEY])
            got_lock = cursor.fetchone()[0]
        if not got_lock:
            self.stdout.write(
                self.style.WARNING(
                    "Another invocation is still running — skipping this cycle."
                )
            )
            return

        try:
            self.stdout.write(
                "Processing up to {} pricing sync job(s) with {} worker(s)…".format(limit, workers)
            )
            processed = integration_pricing_sync_jobs.process_pricing_sync_jobs(
                limit=limit,
                workers=workers,
            )
            self.stdout.write(self.style.SUCCESS("Processed {} job(s).".format(processed)))
        finally:
            with connection.cursor() as cursor:
                cursor.execute("SELECT pg_advisory_unlock(%s)", [_ADVISORY_LOCK_KEY])
