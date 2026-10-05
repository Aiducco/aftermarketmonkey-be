"""
Collect Rough Country's EDI documents (855 / 856 / 810) from the Alluvia FTP mailbox and move
each one into its Archive folder, which is what Alluvia asked us to do on pickup.

This does NOT yet apply anything to PurchaseOrder rows — it downloads, verifies and archives.
See docs/ROUGH_COUNTRY_EDI_PLAN.md for the remaining phases.
"""
from django.core.management.base import BaseCommand, CommandError

from src.integrations.clients.rough_country import edi as rough_country_edi
from src.integrations.edi import exceptions as edi_exceptions


class Command(BaseCommand):
    help = (
        "Download Rough Country's 855/856/810 documents from the Alluvia FTP /Outbound folders "
        "and move each into the matching /Archive folder. Use --dry-run to list what is waiting "
        "without downloading, and --no-archive to download without moving anything."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--types",
            type=str,
            default=",".join(rough_country_edi.INBOUND_DOCUMENT_TYPES),
            help="Comma-separated document types to collect (default: {}).".format(
                ",".join(rough_country_edi.INBOUND_DOCUMENT_TYPES)
            ),
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="List what is waiting in each /Outbound folder and exit. Downloads nothing, "
            "moves nothing.",
        )
        parser.add_argument(
            "--no-archive",
            action="store_true",
            help="Download but leave the files in /Outbound. Useful for a first look; note the "
            "next run will collect them again.",
        )
        parser.add_argument(
            "--limit",
            type=int,
            default=None,
            help="Stop after this many files (default: no limit).",
        )

    def handle(self, *args, **options):
        requested = [t.strip() for t in (options.get("types") or "").split(",") if t.strip()]
        unknown = [t for t in requested if t not in rough_country_edi.INBOUND_DOCUMENT_TYPES]
        if unknown:
            raise CommandError(
                "Unknown document type(s): {}. Valid: {}.".format(
                    ", ".join(unknown), ", ".join(rough_country_edi.INBOUND_DOCUMENT_TYPES)
                )
            )

        try:
            client = rough_country_edi.RoughCountryEdiClient()
        except ValueError as e:
            raise CommandError(str(e))

        self.stdout.write(
            "Connecting to {}:{} as {} ({})…".format(
                client.host, client.port, client.user, "FTPS" if client.use_tls else "FTP"
            )
        )

        try:
            if options.get("dry_run"):
                available = client.list_available()
                total = 0
                for document_type in requested:
                    filenames = available.get(document_type, [])
                    total += len(filenames)
                    self.stdout.write(
                        "  /Outbound/{}: {} file(s){}".format(
                            document_type,
                            len(filenames),
                            "" if not filenames else " — " + ", ".join(filenames[:10]),
                        )
                    )
                self.stdout.write(self.style.SUCCESS("{} file(s) waiting.".format(total)))
                return

            results = client.collect(
                document_types=requested,
                archive=not options.get("no_archive"),
                limit=options.get("limit"),
            )
        except edi_exceptions.EdiTransportError as e:
            # By far the most likely cause is Alluvia's source-IP allowlist: every port on their
            # host is filtered from an unlisted address, so this surfaces as a connect timeout.
            raise CommandError(
                "{}\n\nIf this is a connection timeout, check that this host's public IP is "
                "allowlisted by Alluvia.".format(e)
            )

        failed = [r for r in results if r["error"]]
        archived = [r for r in results if r["archived"]]
        for entry in results:
            if entry["error"]:
                self.stdout.write(
                    self.style.ERROR(
                        "  {} {} — {}".format(entry["document_type"], entry["filename"], entry["error"])
                    )
                )
            else:
                self.stdout.write(
                    "  {} {} -> {}{}".format(
                        entry["document_type"],
                        entry["filename"],
                        entry["local_path"],
                        " (archived)" if entry["archived"] else "",
                    )
                )

        summary = "Collected {} file(s); {} archived; {} failed.".format(
            len(results), len(archived), len(failed)
        )
        self.stdout.write(self.style.WARNING(summary) if failed else self.style.SUCCESS(summary))
