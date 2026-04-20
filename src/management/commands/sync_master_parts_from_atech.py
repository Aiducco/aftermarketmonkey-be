from django.core.management.base import BaseCommand

from src.integrations.services import master_parts


class Command(BaseCommand):
    help = (
        "Sync MasterPart and ProviderPart from AtechParts (BrandAtechBrandMapping + resolved brand only). "
        "Uses AtechParts.part_number as MasterPart part_number + sku; ProviderPart.provider_external_id is "
        "{atech_brand_id}_{part_number}."
    )

    def handle(self, *args, **options):
        self.stdout.write("Syncing master parts from A-Tech...")
        try:
            master_parts.sync_master_parts_from_atech()
            self.stdout.write(self.style.SUCCESS("Done."))
        except Exception as e:
            self.stdout.write(self.style.ERROR("Error: {}".format(str(e))))
            raise
