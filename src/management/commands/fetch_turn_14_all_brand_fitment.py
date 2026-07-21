from django.core.management.base import BaseCommand

from src.audit import scheduled_tasks as audit_scheduled_tasks
from src.integrations.services import turn_14


class Command(BaseCommand):
    help = (
        'On-demand fetch of raw item/vehicle fitment pairs for every Turn14 brand '
        '(GET /v1/items/fitment/brand/{brand_id}), using the primary company\'s Turn14 '
        'credentials. Not scheduled — run manually.'
    )

    def add_arguments(self, parser):
        parser.add_argument(
            '--resume',
            action='store_true',
            default=False,
            help=(
                'Skip brands that already have at least one saved fitment row, so an '
                'interrupted run can pick back up without re-fetching completed brands.'
            ),
        )

    def handle(self, *args, **options):
        resume = options['resume']
        audit_scheduled_tasks.cleanup_stale_started_executions('fetch_turn_14_all_brand_fitment')
        self.stdout.write('Starting Turn 14 all brand fitment fetch (resume={})...'.format(resume))
        execution = audit_scheduled_tasks.start_scheduled_task_execution('fetch_turn_14_all_brand_fitment')
        try:
            turn_14.fetch_and_save_turn_14_fitment_for_all_brands(resume=resume)
            audit_scheduled_tasks.mark_scheduled_task_completed(
                execution,
                message='Successfully completed Turn 14 all brand fitment fetch.',
            )
            self.stdout.write(self.style.SUCCESS('Successfully completed Turn 14 all brand fitment fetch.'))
        except Exception as e:
            audit_scheduled_tasks.mark_scheduled_task_failed(execution, error_message=str(e))
            self.stdout.write(self.style.ERROR('Error: {}'.format(str(e))))
            raise
