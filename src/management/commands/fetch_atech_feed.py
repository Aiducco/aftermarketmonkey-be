from django.core.management.base import BaseCommand

from src.integrations.services import atech


class Command(BaseCommand):
    help = (
        "Download A-Tech atechfile.txt from SFTP and upsert AtechParts "
        "(requires CompanyProviders for A-Tech with sftp_user / sftp_password; "
        "AtechPrefixBrand rows must map SKU prefixes to AtechBrand)."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--force-download",
            action="store_true",
            help="Re-download feed even if local cache is fresh.",
        )

    def handle(self, *args, **options):
        self.stdout.write("Fetching A-Tech feed from SFTP...")
        try:
            atech.fetch_and_save_atech_catalog(force_download=options.get("force_download", False))
            self.stdout.write(self.style.SUCCESS("Done."))
        except Exception as e:
            self.stdout.write(self.style.ERROR("Error: {}".format(str(e))))
            raise
