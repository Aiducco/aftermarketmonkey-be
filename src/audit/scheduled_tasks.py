"""
Helpers for recording scheduled task / cron execution in ScheduledTaskExecution audit table.
Use from management commands or any caller that runs tasks on a schedule.
"""
import logging
from datetime import timedelta

from django.utils import timezone

from src import enums as src_enums
from src import models as src_models

logger = logging.getLogger(__name__)


def start_scheduled_task_execution(task_name: str) -> src_models.ScheduledTaskExecution:
    """Create and return a new execution record with status STARTED."""
    return src_models.ScheduledTaskExecution.objects.create(
        name=task_name,
        status=src_enums.ScheduledTaskExecutionStatus.STARTED.value,
        status_name=src_enums.ScheduledTaskExecutionStatus.STARTED.name,
    )


def mark_scheduled_task_completed(
    execution: src_models.ScheduledTaskExecution,
    message: str | None = None,
) -> None:
    """Update execution to COMPLETED and set completed_at."""
    execution.status = src_enums.ScheduledTaskExecutionStatus.COMPLETED.value
    execution.status_name = src_enums.ScheduledTaskExecutionStatus.COMPLETED.name
    execution.message = message
    execution.completed_at = timezone.now()
    execution.save(update_fields=["status", "status_name", "message", "completed_at", "updated_at"])


def mark_scheduled_task_failed(
    execution: src_models.ScheduledTaskExecution,
    error_message: str,
) -> None:
    """Update execution to FAILED and set error_message and completed_at."""
    execution.status = src_enums.ScheduledTaskExecutionStatus.FAILED.value
    execution.status_name = src_enums.ScheduledTaskExecutionStatus.FAILED.name
    execution.error_message = error_message
    execution.completed_at = timezone.now()
    execution.save(update_fields=["status", "status_name", "error_message", "completed_at", "updated_at"])


def cleanup_stale_started_executions(
    task_names: "str | list[str]",
    max_age_minutes: int = 120,
) -> int:
    """
    Mark any STARTED records for the given task name(s) older than max_age_minutes as FAILED.

    Call this at the start of each task's handle() so that records left dangling by
    OOM kills or container restarts (SIGKILL cannot be caught by Python, so try/finally
    cleanup never runs in those cases) are self-healed on the next run.

    Returns the number of records updated.
    """
    if isinstance(task_names, str):
        task_names = [task_names]

    cutoff = timezone.now() - timedelta(minutes=max_age_minutes)
    updated = src_models.ScheduledTaskExecution.objects.filter(
        name__in=task_names,
        status=src_enums.ScheduledTaskExecutionStatus.STARTED.value,
        created_at__lt=cutoff,
    ).update(
        status=src_enums.ScheduledTaskExecutionStatus.FAILED.value,
        status_name=src_enums.ScheduledTaskExecutionStatus.FAILED.name,
        error_message=(
            "Stale STARTED record — process was killed (OOM killer or container restart) "
            "before it could mark itself as failed. Cleaned up automatically on next run."
        ),
        completed_at=timezone.now(),
        updated_at=timezone.now(),
    )
    if updated:
        logger.warning(
            "cleanup_stale_started_executions: marked %d stale STARTED record(s) as FAILED "
            "(task_names=%r, max_age_minutes=%d)",
            updated,
            task_names,
            max_age_minutes,
        )
    return updated


def mark_scheduled_task_skipped(
    execution: src_models.ScheduledTaskExecution,
    message: str | None = None,
) -> None:
    """Update execution to SKIPPED (e.g. no work to do) and set completed_at."""
    execution.status = src_enums.ScheduledTaskExecutionStatus.SKIPPED.value
    execution.status_name = src_enums.ScheduledTaskExecutionStatus.SKIPPED.name
    execution.message = message
    execution.completed_at = timezone.now()
    execution.save(update_fields=["status", "status_name", "message", "completed_at", "updated_at"])
