"""
Collect Rough Country's inbound EDI documents, apply them to PurchaseOrder rows and acknowledge
each with a 997. This is the cron side of the integration.

Nothing else moves a Rough Country PO past SUBMITTED. Writing the 850 only means the file was
delivered; the 855 that confirms the order, the 856 that carries tracking and the 810 that bills
it all arrive later, in a directory, and this is what reads them. If this is not scheduled, every
Rough Country order sits at SUBMITTED forever.

Per company: each shop has its own Alluvia mailbox, so this sweeps every configured order
account. A company whose credentials are incomplete is skipped, and one company's failure never
stops the others.
"""
from django.core.management.base import BaseCommand, CommandError

from src import enums as src_enums
from src import models as src_models
from src.integrations.clients.rough_country import edi as rough_country_edi
from src.integrations.clients.rough_country import edi_config
from src.integrations.services import rough_country_edi as rough_country_edi_service


class Command(BaseCommand):
    help = (
        "Collect Rough Country's 855/856/810 documents from each company's Alluvia FTP mailbox, "
        "apply them to purchase orders, archive each file and send a 997 acknowledgment. "
        "Intended to run on a cron; safe to re-run, since every document is deduplicated on its "
        "interchange control number."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--company-provider-id",
            type=int,
            default=None,
            help="Only process this CompanyProviders connection (default: every configured "
            "Rough Country order account).",
        )
        parser.add_argument(
            "--types",
            type=str,
            default=",".join(rough_country_edi.INBOUND_DOCUMENT_TYPES),
            help="Comma-separated document types to process (default: {}).".format(
                ",".join(rough_country_edi.INBOUND_DOCUMENT_TYPES)
            ),
        )
        parser.add_argument(
            "--limit",
            type=int,
            default=None,
            help="Stop after this many documents per company (default: no limit).",
        )
        parser.add_argument(
            "--no-ack",
            action="store_true",
            help="Apply documents without sending 997 acknowledgments. For debugging; Rough "
            "Country expects the 997s.",
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
                    "No Rough Country order accounts have EDI credentials configured. "
                    "Credentials live on each company's order account (ftp_user / ftp_password "
                    "/ edi_sender_id)."
                )
            )
            return

        applied = unmatched = failed = 0
        for account in accounts:
            try:
                client_config = edi_config.for_account(account.company_provider, account)
            except ValueError as e:
                self.stdout.write(self.style.ERROR("  {}".format(e)))
                failed += 1
                continue

            self.stdout.write(
                "company_provider_id={} — {}".format(account.company_provider_id, client_config.describe())
            )
            try:
                results = rough_country_edi_service.process_account(
                    account,
                    document_types=requested,
                    limit=options.get("limit"),
                    send_acknowledgments=not options.get("no_ack"),
                )
            except Exception as e:
                # A whole-mailbox failure (most often Alluvia's source-IP allowlist, which
                # surfaces as a connect timeout rather than a refusal) must not stop the sweep.
                self.stdout.write(
                    self.style.ERROR(
                        "  {}\n  If this is a connection timeout, check that this host's public "
                        "IP is allowlisted by Alluvia.".format(e)
                    )
                )
                failed += 1
                continue

            if not results:
                self.stdout.write("  nothing waiting")
            for entry in results:
                if entry.get("error"):
                    self.stdout.write(
                        self.style.ERROR(
                            "  {} {} — {}".format(entry.get("document_type"), entry.get("filename"), entry["error"])
                        )
                    )
                    failed += 1
                else:
                    self.stdout.write(
                        "  {} {} — applied={} archived={}".format(
                            entry.get("document_type"),
                            entry.get("filename"),
                            entry.get("applied"),
                            entry.get("archived"),
                        )
                    )
                    applied += 1

        # Unmatched documents are stored, not dropped, and are the thing most worth noticing:
        # an acknowledgment for a PO we do not recognise means an order nobody is tracking.
        unmatched = src_models.EdiDocument.objects.filter(status=src_enums.EdiDocumentStatus.UNMATCHED.value).count()

        summary = "{} document(s) processed; {} failed; {} unmatched document(s) stored.".format(
            applied, failed, unmatched
        )
        style = self.style.WARNING if (failed or unmatched) else self.style.SUCCESS
        self.stdout.write(style(summary))

    def _accounts(self, company_provider_id):
        accounts = edi_config.configured_accounts()
        if company_provider_id is None:
            return accounts
        if not src_models.CompanyProviders.objects.filter(id=company_provider_id).exists():
            raise CommandError("No CompanyProviders with id={}.".format(company_provider_id))
        return [a for a in accounts if a.company_provider_id == company_provider_id]
