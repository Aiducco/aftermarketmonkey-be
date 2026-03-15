"""
Fetch Turn 14 pricing changes (GET /v1/pricing/changes) for a date range,
then sync brand pricing only for Turn14 brands that have items in those changes.
Default: last day (yesterday 00:00 to today 00:00).
"""
from datetime import date, timedelta

from django.core.management.base import BaseCommand

from src.audit import scheduled_tasks as audit_scheduled_tasks
from src.integrations.services import turn_14


class Command(BaseCommand):
    help = (
        'Fetch Turn 14 pricing changes for a date range, get distinct Turn14 brands '
        'for affected items, then sync brand pricing for those brands only. '
        'Default: last 1 day.'
    )

    def add_arguments(self, parser):
        parser.add_argument(
            '--days',
            type=int,
            default=1,
            help='Number of days to look back (used when --start-date/--end-date not set). Default: 1.',
        )
        parser.add_argument(
            '--start-date',
            type=str,
            default=None,
            help='Start date YYYY-MM-DD. If set, --end-date is required.',
        )
        parser.add_argument(
            '--end-date',
            type=str,
            default=None,
            help='End date YYYY-MM-DD. If set, --start-date is required.',
        )

    def handle(self, *args, **options):
        days = options['days']
        start_date_arg = options['start_date']
        end_date_arg = options['end_date']

        if start_date_arg and end_date_arg:
            start_date = start_date_arg
            end_date = end_date_arg
        elif start_date_arg or end_date_arg:
            self.stdout.write(
                self.style.ERROR('Provide both --start-date and --end-date, or neither (use --days).')
            )
            return
        else:
            end_date_d = date.today()
            start_date_d = end_date_d - timedelta(days=days)
            start_date = start_date_d.strftime('%Y-%m-%d')
            end_date = end_date_d.strftime('%Y-%m-%d')

        self.stdout.write(
            'Fetching Turn 14 pricing changes from {} to {}...'.format(start_date, end_date)
        )
        execution = audit_scheduled_tasks.start_scheduled_task_execution(
            'fetch_turn_14_pricing_changes'
        )
        try:
            turn_14.fetch_and_save_turn_14_pricing_changes(start_date=start_date, end_date=end_date)
            audit_scheduled_tasks.mark_scheduled_task_completed(
                execution,
                message='Successfully completed Turn 14 pricing changes fetch and sync.',
            )
            self.stdout.write(
                self.style.SUCCESS('Successfully completed Turn 14 pricing changes fetch and sync.')
            )
        except Exception as e:
            audit_scheduled_tasks.mark_scheduled_task_failed(
                execution, error_message=str(e)
            )
            self.stdout.write(self.style.ERROR('Error: {}'.format(str(e))))
            raise
