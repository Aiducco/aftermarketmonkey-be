"""
Helpers for recording scheduled task / cron execution in ScheduledTaskExecution audit table.
Use from management commands or any caller that runs tasks on a schedule.
"""
from django.utils import timezone

from src import enums as src_enums
from src import models as src_models


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
