"""
Resolves one company's Rough Country EDI configuration from its order-account credentials.

Rough Country EDI is per company, like every other distributor's ordering: Alluvia issues each
shop its own FTP login *and* its own ISA/GS trading-partner id, so the mailbox, the credentials
and our side of the envelope all come from ``CompanyProviderOrderAccount.credentials`` rather
than from Django settings. Only Rough Country's own receiver id is global, because it is theirs
(see ``settings.ROUGH_COUNTRY_EDI_RECEIVER_ID_TEST`` / ``_PRODUCTION``).

That matters beyond tidiness: Trident goes live before the other shops Rough Country has in the
pipeline, so "which environment am I talking to" has to be a per-company answer. One shop runs
against ``64ROUGHCT`` while another is already placing real orders against ``64ROUGHC``.

``environment`` is deliberately ONE credential rather than letting a shop enter the receiver id
and the usage indicator separately. Those two must agree -- ``ISA15=P`` against the test id, or
``T`` against production, is exactly how a test file turns into a real shipment -- so they are
derived together here and cannot be set inconsistently. Anything that is not exactly
``"production"`` resolves to test, so a typo fails safe.
"""
import dataclasses
import typing

from django.conf import settings

from src import constants as src_constants
from src import models as src_models
from src.integrations import credentials as credentials_helper

ENVIRONMENT_TEST = "test"
ENVIRONMENT_PRODUCTION = "production"

REQUIRED_CREDENTIAL_FIELDS = (
    src_constants.ROUGH_COUNTRY_EDI_CREDENTIALS_FTP_USER,
    src_constants.ROUGH_COUNTRY_EDI_CREDENTIALS_FTP_PASSWORD,
    src_constants.ROUGH_COUNTRY_EDI_CREDENTIALS_SENDER_ID,
)
OPTIONAL_CREDENTIAL_FIELDS = (
    src_constants.ROUGH_COUNTRY_EDI_CREDENTIALS_ENVIRONMENT,
    src_constants.ROUGH_COUNTRY_EDI_CREDENTIALS_FTP_HOST,
)

# ISA06/ISA08 are fixed at 15 characters, so an over-long id cannot be encoded at all. Caught
# here, at configuration time, rather than at send time when an order is already in flight.
_MAX_PARTNER_ID = 15


@dataclasses.dataclass(frozen=True)
class RoughCountryEdiConfig:
    host: str
    port: int
    user: str
    password: str
    use_tls: bool
    # ISA06/GS02 -- ours, assigned per shop by Alluvia.
    sender_id: str
    # ISA08/GS03 -- Rough Country's, derived from `environment`.
    receiver_id: str
    # ISA15. "P" only ever paired with the production receiver id.
    usage_indicator: str
    environment: str

    @property
    def is_production(self) -> bool:
        return self.environment == ENVIRONMENT_PRODUCTION

    def describe(self) -> str:
        """One line for logs and CLI output. Never includes the password. Says the environment
        out loud because 'which mailbox am I about to place a real order in' is the single
        question worth being certain about."""
        return "{}@{}:{} {} -> {} (ISA15={}, {})".format(
            self.user,
            self.host,
            self.port,
            self.sender_id,
            self.receiver_id,
            self.usage_indicator,
            "PRODUCTION" if self.is_production else "test",
        )


def _receiver_id_for(environment: str) -> str:
    if environment == ENVIRONMENT_PRODUCTION:
        return (getattr(settings, "ROUGH_COUNTRY_EDI_RECEIVER_ID_PRODUCTION", "") or "").strip()
    return (getattr(settings, "ROUGH_COUNTRY_EDI_RECEIVER_ID_TEST", "") or "").strip()


def from_credentials(creds: typing.Optional[typing.Dict]) -> RoughCountryEdiConfig:
    """
    Build a config from one order account's credentials dict.

    Raises ``ValueError`` when a required credential is missing, which is the established
    contract for order adapters: ``orders.registry.get_adapter`` catches it and reports the
    connection as not configured, rather than letting an exception escape into job processing.
    """
    values = creds or {}
    user = (values.get(src_constants.ROUGH_COUNTRY_EDI_CREDENTIALS_FTP_USER) or "").strip()
    password = values.get(src_constants.ROUGH_COUNTRY_EDI_CREDENTIALS_FTP_PASSWORD) or ""
    sender_id = (values.get(src_constants.ROUGH_COUNTRY_EDI_CREDENTIALS_SENDER_ID) or "").strip()

    missing = [
        name
        for name, value in (
            (src_constants.ROUGH_COUNTRY_EDI_CREDENTIALS_FTP_USER, user),
            (src_constants.ROUGH_COUNTRY_EDI_CREDENTIALS_FTP_PASSWORD, password),
            (src_constants.ROUGH_COUNTRY_EDI_CREDENTIALS_SENDER_ID, sender_id),
        )
        if not value
    ]
    if missing:
        raise ValueError(
            "Rough Country EDI ordering is not configured for this account — missing: " "{}.".format(", ".join(missing))
        )
    if len(sender_id) > _MAX_PARTNER_ID:
        raise ValueError(
            "EDI sender id {!r} is {} characters; ISA06 is fixed at {}.".format(
                sender_id, len(sender_id), _MAX_PARTNER_ID
            )
        )

    raw_environment = (values.get(src_constants.ROUGH_COUNTRY_EDI_CREDENTIALS_ENVIRONMENT) or "").strip().lower()
    if not raw_environment:
        # Unset means live. The integration is proven; a new dealer only needs their mailbox.
        environment = ENVIRONMENT_PRODUCTION
    elif raw_environment in (ENVIRONMENT_TEST, ENVIRONMENT_PRODUCTION):
        environment = raw_environment
    else:
        # Never guess. "tst" resolving to production would ship real goods on a test order, and
        # resolving to test would make a real order silently never happen.
        raise ValueError(
            "Rough Country EDI environment {!r} is not recognised — use {!r} or {!r}.".format(
                raw_environment, ENVIRONMENT_TEST, ENVIRONMENT_PRODUCTION
            )
        )
    receiver_id = _receiver_id_for(environment)
    if not receiver_id:
        raise ValueError("No Rough Country receiver id is configured for the {} environment.".format(environment))

    host = (values.get(src_constants.ROUGH_COUNTRY_EDI_CREDENTIALS_FTP_HOST) or "").strip() or (
        getattr(settings, "ROUGH_COUNTRY_EDI_FTP_HOST", "") or ""
    ).strip()
    if not host:
        raise ValueError("No Rough Country EDI FTP host is configured.")

    return RoughCountryEdiConfig(
        host=host,
        port=int(getattr(settings, "ROUGH_COUNTRY_EDI_FTP_PORT", 21) or 21),
        user=user,
        password=password,
        use_tls=bool(getattr(settings, "ROUGH_COUNTRY_EDI_FTP_USE_TLS", False)),
        sender_id=sender_id,
        receiver_id=receiver_id,
        usage_indicator="P" if environment == ENVIRONMENT_PRODUCTION else "T",
        environment=environment,
    )


def for_account(
    company_provider: src_models.CompanyProviders,
    order_account: typing.Optional[src_models.CompanyProviderOrderAccount] = None,
) -> RoughCountryEdiConfig:
    """Config for this connection's order account — the default account when none is given,
    matching every other adapter (see src.integrations.credentials)."""
    return from_credentials(credentials_helper.get_order_credentials(company_provider, order_account))


def configured_accounts() -> typing.List[src_models.CompanyProviderOrderAccount]:
    """
    Every active Rough Country order account with EDI credentials filled in — what the inbound
    poller iterates. Each shop has its own mailbox, so there is no single global mailbox to
    poll any more; a company that has not finished configuring is skipped rather than raising.
    """
    from src import enums as src_enums

    accounts = src_models.CompanyProviderOrderAccount.objects.filter(
        active=True,
        company_provider__provider__kind=src_enums.BrandProviderKind.ROUGH_COUNTRY.value,
    ).select_related("company_provider", "company_provider__provider")

    usable = []
    for account in accounts:
        try:
            from_credentials(account.credentials or {})
        except ValueError:
            continue
        usable.append(account)
    return usable
