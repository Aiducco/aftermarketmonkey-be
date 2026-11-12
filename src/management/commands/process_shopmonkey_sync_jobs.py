"""
Claim and run QUEUED ShopMonkeySyncExecution rows -- the async drain for the "run now" button
(Phase 3). The scheduled (every 4 hours) sync keeps calling sync_brand_parts() directly and
synchronously via sync_shopmonkey_inventory; this command is only for user-triggered runs, which
now return a QUEUED execution immediately instead of blocking the HTTP request on a ShopMonkey
API round trip. See src.integrations.services.shopmonkey_sync.enqueue_sync_brand_run /
claim_next_queued_execution.

Intended for cron every 5 minutes.

    manage.py process_shopmonkey_sync_jobs
"""
from django.core.management.base import BaseCommand
from django.db import connection

from src.audit import scheduled_tasks as audit_scheduled_tasks
from src.integrations.services import shopmonkey_sync as shopmonkey_sync_services

_TASK_NAME = "process_shopmonkey_sync_jobs"
# Arbitrary 64-bit int identifying this command's session-scoped Postgres advisory lock --
# distinct from process_integration_pricing_sync_jobs's 0x1F5F5F5F. Any 64-bit int works, this
# one has no other meaning beyond being memorable ("SHMKSYNC" in hex). Session-scoped (not
# transaction-scoped) so it stays held for the whole invocation, and Postgres releases it
# automatically when the connection closes for any reason (normal exit, crash, OOM kill) -- same
# overlap-guard reasoning as process_integration_pricing_sync_jobs.py, which records a real
# incident of stacked invocations OOM-killing gunicorn.
_ADVISORY_LOCK_KEY = 0x53484D4B53594E43


class Command(BaseCommand):
    help = "Claim and run QUEUED ShopMonkeySyncExecution rows. Intended for cron every 5 minutes."

    def add_arguments(self, parser):
        parser.add_argument(
            "--limit",
            type=int,
            default=10,
            help="Max executions to process in one invocation (default: 10).",
        )

    def handle(self, *args, **options):
        limit = max(1, int(options.get("limit") or 10))

        with connection.cursor() as cursor:
            cursor.execute("SELECT pg_try_advisory_lock(%s)", [_ADVISORY_LOCK_KEY])
            got_lock = cursor.fetchone()[0]
        if not got_lock:
            self.stdout.write(self.style.WARNING("Another invocation is still running — skipping this cycle."))
            return

        # Both bookkeeping calls sit inside the try so the advisory lock is released on every
        # path -- matching process_integration_pricing_sync_jobs, which keeps its own
        # cleanup call inside the guarded block for the same reason.
        task_execution = None
        try:
            audit_scheduled_tasks.cleanup_stale_started_executions(_TASK_NAME, max_age_minutes=60)
            task_execution = audit_scheduled_tasks.start_scheduled_task_execution(_TASK_NAME)
            processed = 0
            # A delete that runs out of its time slice goes back to QUEUED with the lowest id; without this it
            # would be claimed again straight away and starve every sync queued behind it.
            handled_ids = set()
            for _ in range(limit):
                execution = shopmonkey_sync_services.claim_next_queued_execution(exclude_ids=handled_ids)
                if not execution:
                    break
                handled_ids.add(execution.id)
                if (execution.metadata or {}).get("kind") == shopmonkey_sync_services.EXECUTION_KIND_DELETE_BRAND:
                    shopmonkey_sync_services.run_brand_delete(execution)
                else:
                    shopmonkey_sync_services.sync_brand_parts(execution.sync_brand_id, execution=execution)
                processed += 1
            message = "Processed {} execution(s).".format(processed)
            self.stdout.write(self.style.SUCCESS(message))
            audit_scheduled_tasks.mark_scheduled_task_completed(task_execution, message=message)
        except Exception as e:
            # task_execution is None only if start_scheduled_task_execution itself failed, in
            # which case there is no row to mark -- and calling the helper with None would raise
            # an AttributeError that masks the real exception.
            if task_execution is not None:
                audit_scheduled_tasks.mark_scheduled_task_failed(task_execution, error_message=str(e))
            raise
        finally:
            with connection.cursor() as cursor:
                cursor.execute("SELECT pg_advisory_unlock(%s)", [_ADVISORY_LOCK_KEY])
