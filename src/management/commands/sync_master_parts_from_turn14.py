from django.core.management.base import BaseCommand

from src.integrations.services import master_parts


class Command(BaseCommand):
    help = "Sync MasterPart and ProviderPart from Turn14"

    def handle(self, *args, **options):
        self.stdout.write("Syncing master parts from Turn14...")
        try:
            master_parts.sync_master_parts_from_turn14()
            self.stdout.write(self.style.SUCCESS("Done."))
        except Exception as e:
            self.stdout.write(self.style.ERROR("Error: {}".format(str(e))))
            raise
