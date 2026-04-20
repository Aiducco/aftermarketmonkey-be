from django.core.management.base import BaseCommand

from src.audit import scheduled_tasks as audit_scheduled_tasks
from src.integrations.services import master_parts, turn_14


class Command(BaseCommand):
    help = 'Fetch and save Turn 14 brand inventory for all brands'

    def handle(self, *args, **options):
        self.stdout.write('Starting Turn 14 all brand inventory fetch...')
        execution = audit_scheduled_tasks.start_scheduled_task_execution('fetch_turn_14_all_brand_inventory')
        try:
            turn_14.fetch_and_save_all_turn_14_brand_inventory()
            self.stdout.write('Propagating Turn14 inventory into ProviderPartInventory...')
            master_parts.sync_provider_inventory_from_turn14()
            audit_scheduled_tasks.mark_scheduled_task_completed(
                execution,
                message='Successfully completed Turn 14 all brand inventory fetch and derived inventory sync.',
            )
            self.stdout.write(self.style.SUCCESS('Successfully completed Turn 14 all brand inventory fetch and derived sync.'))
        except Exception as e:
            audit_scheduled_tasks.mark_scheduled_task_failed(execution, error_message=str(e))
            self.stdout.write(self.style.ERROR('Error: {}'.format(str(e))))
            raise
