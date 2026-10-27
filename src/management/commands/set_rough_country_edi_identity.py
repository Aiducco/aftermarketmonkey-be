"""
Set a dealer's Rough Country EDI trading-partner id (and, rarely, their environment).

This is the other half of onboarding. The dealer enters their FTP login on the Integrations
page; the ISA06/GS02 id Alluvia issues alongside it is set here, because it is not a thing a
dealer can know and the connection form deliberately does not offer it.

Until the sender id is set, the connection validates (the FTP login works) but cannot place an
order -- edi_config raises and orders.registry reports it as unconfigured. That is the intended
intermediate state, not a bug.

    manage.py set_rough_country_edi_identity --company-provider-id 39 --sender-id TRIDENT
    manage.py set_rough_country_edi_identity --company-slug trident-motorsports-9992401f --show

Environment defaults to production for every dealer and only needs setting to pin one to test,
which is what Trident runs on while Alluvia verifies it.
"""
from django.core.management.base import BaseCommand, CommandError

from src import constants as src_constants
from src import enums as src_enums
from src import models as src_models
from src.integrations.clients.rough_country import edi_config


class Command(BaseCommand):
    help = (
        "Set the Rough Country EDI trading-partner id (ISA06/GS02) and optionally the "
        "environment on a company's Rough Country order account. Use --show to print the "
        "current state without changing anything."
    )

    def add_arguments(self, parser):
        target = parser.add_mutually_exclusive_group(required=True)
        target.add_argument(
            "--company-provider-id",
            type=int,
            default=None,
            help="The CompanyProviders connection id.",
        )
        target.add_argument(
            "--company-slug",
            type=str,
            default=None,
            help="The company's slug. Resolved by slug rather than name on purpose — there are "
            "duplicate company names, and a name lookup picks the wrong row.",
        )
        parser.add_argument(
            "--sender-id",
            type=str,
            default=None,
            help="The dealer's own ISA06/GS02 id, as issued by Alluvia (max 15 characters).",
        )
        parser.add_argument(
            "--environment",
            type=str,
            choices=[edi_config.ENVIRONMENT_TEST, edi_config.ENVIRONMENT_PRODUCTION],
            default=None,
            help="Only needed to pin a dealer to test. Unset means production.",
        )
        parser.add_argument(
            "--show",
            action="store_true",
            help="Print the current configuration and exit.",
        )

    def handle(self, *args, **options):
        account = self._account(options)
        credentials = dict(account.credentials or {})

        if options.get("show"):
            self._report(account, credentials)
            return

        sender_id = (options.get("sender_id") or "").strip()
        environment = options.get("environment")
        if not sender_id and environment is None:
            raise CommandError("Nothing to do — pass --sender-id and/or --environment, or --show.")

        if sender_id:
            credentials[src_constants.ROUGH_COUNTRY_EDI_CREDENTIALS_SENDER_ID] = sender_id
        if environment is not None:
            credentials[src_constants.ROUGH_COUNTRY_EDI_CREDENTIALS_ENVIRONMENT] = environment

        # Validate before saving, not after: from_credentials is the same code the adapter runs,
        # so a value it rejects would leave a connection that looks configured and cannot order.
        try:
            config = edi_config.from_credentials(credentials)
        except ValueError as e:
            raise CommandError("Refusing to save — {}".format(e))

        account.credentials = credentials
        account.save(update_fields=["credentials", "updated_at"])

        self.stdout.write(self.style.SUCCESS("Saved. {}".format(config.describe())))
        if config.is_production:
            self.stdout.write(
                self.style.WARNING(
                    "This connection is LIVE: orders placed against it are real orders and "
                    "Rough Country will ship them."
                )
            )

    def _account(self, options) -> src_models.CompanyProviderOrderAccount:
        accounts = src_models.CompanyProviderOrderAccount.objects.filter(
            company_provider__provider__kind=src_enums.BrandProviderKind.ROUGH_COUNTRY.value,
            active=True,
        ).select_related("company_provider", "company_provider__company")

        if options.get("company_provider_id") is not None:
            accounts = accounts.filter(company_provider_id=options["company_provider_id"])
            label = "company_provider_id={}".format(options["company_provider_id"])
        else:
            accounts = accounts.filter(company_provider__company__slug=options["company_slug"])
            label = "company slug={!r}".format(options["company_slug"])

        found = list(accounts)
        if not found:
            raise CommandError(
                "No active Rough Country order account for {}. The dealer has to save their FTP "
                "login first.".format(label)
            )
        if len(found) > 1:
            raise CommandError(
                "{} has {} Rough Country order accounts ({}). Pass --company-provider-id for the "
                "one you mean.".format(label, len(found), ", ".join("id={}".format(a.id) for a in found))
            )
        return found[0]

    def _report(self, account, credentials) -> None:
        company = account.company_provider.company
        self.stdout.write(
            "{} (company_provider_id={}, order_account_id={}, label={!r})".format(
                company.name, account.company_provider_id, account.id, account.label
            )
        )
        self.stdout.write("  order method : {}".format(account.order_method_name))
        self.stdout.write("  credentials  : {}".format(", ".join(sorted(credentials.keys())) or "none"))
        try:
            self.stdout.write("  resolves to  : {}".format(edi_config.from_credentials(credentials).describe()))
        except ValueError as e:
            self.stdout.write(self.style.WARNING("  not order-ready: {}".format(e)))
