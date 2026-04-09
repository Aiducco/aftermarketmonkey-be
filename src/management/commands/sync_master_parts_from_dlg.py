from django.core.management.base import BaseCommand

from src.integrations.services import master_parts


class Command(BaseCommand):
    help = (
        "Sync MasterPart and ProviderPart from DlgParts (BrandDlgBrandMapping only). "
        "Matches existing master rows by (brand, part_number) only — not by sku."
    )

    def handle(self, *args, **options):
        self.stdout.write("Syncing master parts from DLG...")
        try:
            master_parts.sync_master_parts_from_dlg()
            self.stdout.write(self.style.SUCCESS("Done."))
        except Exception as e:
            self.stdout.write(self.style.ERROR("Error: {}".format(str(e))))
            raise
