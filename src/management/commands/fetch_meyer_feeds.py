from django.core.management.base import BaseCommand

from src.integrations.services import master_parts, meyer


class Command(BaseCommand):
    help = (
        "Download Meyer Pricing + Meyer Inventory from SFTP and upsert MeyerBrand / MeyerParts "
        "(requires CompanyProviders.credentials for Meyer: sftp_user, sftp_password; optional filename overrides). "
        "SFTP host/path defaults from MEYER_SFTP_* settings."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--force-download",
            action="store_true",
            help="Re-download CSVs even if local cache is fresh.",
        )

    def handle(self, *args, **options):
        self.stdout.write("Fetching Meyer pricing + inventory from SFTP...")
        try:
            meyer.fetch_and_save_meyer_catalog_and_inventory(
                force_download=options.get("force_download", False),
            )
            self.stdout.write(
                "Propagating Meyer catalog into master parts, provider parts, inventory, and pricing..."
            )
            master_parts.sync_derived_from_meyer(reindex_meilisearch=True)
            self.stdout.write(self.style.SUCCESS("Done."))
        except Exception as e:
            self.stdout.write(self.style.ERROR("Error: {}".format(str(e))))
            raise
