"""
Collect Rough Country's EDI documents (855 / 856 / 810) from each company's Alluvia FTP mailbox
and move every one into its Archive folder, which is what Alluvia asked us to do on pickup.

Per company, not global. Alluvia issues every shop its own FTP login and its own ISA/GS
trading-partner id, so this walks each configured CompanyProviderOrderAccount in turn; a shop
that has not finished entering its credentials is skipped, not failed. With no arguments it
sweeps every configured account, which is the form the cron runs.

This does NOT apply anything to PurchaseOrder rows — it downloads, verifies and archives. Use
``process_rough_country_edi`` for the applying step; see docs/ROUGH_COUNTRY_EDI_PLAN.md.
"""
from django.core.management.base import BaseCommand, CommandError

from src import models as src_models
from src.integrations.clients.rough_country import edi as rough_country_edi
from src.integrations.clients.rough_country import edi_config
from src.integrations.edi import exceptions as edi_exceptions


class Command(BaseCommand):
    help = (
        "Download Rough Country's 855/856/810 documents from each company's Alluvia FTP "
        "/Outbound folders and move each into the matching /Archive folder. Use --dry-run to "
        "list what is waiting without downloading, and --no-archive to download without moving "
        "anything."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--company-provider-id",
            type=int,
            default=None,
            help="Only collect for this CompanyProviders connection (default: every configured "
            "Rough Country order account).",
        )
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
            help="List what is waiting in each /Outbound folder and exit. Downloads nothing, " "moves nothing.",
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
            help="Stop after this many files per company (default: no limit).",
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

        accounts = self._accounts(options.get("company_provider_id"))
        if not accounts:
            self.stdout.write(
                self.style.WARNING(
                    "No Rough Country order accounts have EDI credentials configured, so there "
                    "is nothing to collect. Credentials live on each company's order account "
                    "(ftp_user / ftp_password / edi_sender_id)."
                )
            )
            return

        total_collected = 0
        total_failed = 0
        for account in accounts:
            total_collected, total_failed = self._collect_for(
                account, requested, options, total_collected, total_failed
            )

        summary = "{} file(s) collected across {} account(s); {} failed.".format(
            total_collected, len(accounts), total_failed
        )
        self.stdout.write(self.style.WARNING(summary) if total_failed else self.style.SUCCESS(summary))

    def _accounts(self, company_provider_id):
        if company_provider_id is None:
            return edi_config.configured_accounts()
        connection = src_models.CompanyProviders.objects.filter(id=company_provider_id).first()
        if connection is None:
            raise CommandError("No CompanyProviders with id={}.".format(company_provider_id))
        return [account for account in edi_config.configured_accounts() if account.company_provider_id == connection.id]

    def _collect_for(self, account, requested, options, total_collected, total_failed):
        try:
            client = rough_country_edi.RoughCountryEdiClient.for_account(account.company_provider, account)
        except ValueError as e:
            # configured_accounts() already filtered these out, so reaching here means the
            # credentials changed underneath us. Report and carry on to the next company
            # rather than aborting the whole sweep.
            self.stdout.write(self.style.ERROR("  {}".format(e)))
            return total_collected, total_failed + 1

        self.stdout.write("company_provider_id={} — {}".format(account.company_provider_id, client.config.describe()))

        try:
            if options.get("dry_run"):
                available = client.list_available()
                for document_type in requested:
                    filenames = available.get(document_type, [])
                    total_collected += len(filenames)
                    self.stdout.write(
                        "  /Outbound/{}: {} file(s){}".format(
                            document_type,
                            len(filenames),
                            "" if not filenames else " — " + ", ".join(filenames[:10]),
                        )
                    )
                return total_collected, total_failed

            results = client.collect(
                document_types=requested,
                archive=not options.get("no_archive"),
                limit=options.get("limit"),
            )
        except edi_exceptions.EdiTransportError as e:
            # By far the most likely cause is Alluvia's source-IP allowlist: every port on their
            # host is filtered from an unlisted address, so this surfaces as a connect timeout
            # rather than a refusal.
            self.stdout.write(
                self.style.ERROR(
                    "  {}\n  If this is a connection timeout, check that this host's public IP "
                    "is allowlisted by Alluvia.".format(e)
                )
            )
            return total_collected, total_failed + 1

        for entry in results:
            if entry["error"]:
                self.stdout.write(
                    self.style.ERROR("  {} {} — {}".format(entry["document_type"], entry["filename"], entry["error"]))
                )
                total_failed += 1
            else:
                self.stdout.write(
                    "  {} {} -> {}{}".format(
                        entry["document_type"],
                        entry["filename"],
                        entry["local_path"],
                        " (archived)" if entry["archived"] else "",
                    )
                )
            total_collected += 1
        return total_collected, total_failed
