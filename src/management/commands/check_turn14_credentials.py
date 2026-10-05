"""
Probe a Turn 14 credential pair against every endpoint the integration actually calls, and
report which ones the account is entitled to.

Turn 14 issues API access per endpoint, and a credential can mint a token on a host while
being entitled to nothing on it -- the token endpoint answers 200, then every resource
answers 403 "This account does not have access to the <x> endpoint". That is a permission
grant on their side, indistinguishable from a working credential until something is actually
fetched, so this exists to answer "do these credentials work yet?" in one command rather than
by hand-rolling curl against a dozen paths.

Credentials come from --client-id/--client-secret, or from the connection the shared catalog
tables already sync with (src.integrations.services.turn_14_global) when neither is passed.
Every call here is read-only: nothing quotes, orders, or writes.
"""
import typing

from django.core.management.base import BaseCommand

from src.integrations.clients.turn_14 import client as turn_14_client
from src.integrations.clients.turn_14 import exceptions as turn_14_exceptions
from src.integrations.clients.turn_14 import order_client as turn_14_order_client
from src.integrations.services import turn_14_global


class Command(BaseCommand):
    help = (
        "Check which Turn 14 API endpoints a credential pair can actually reach. "
        "Read-only: places no orders and writes nothing."
    )

    def add_arguments(self, parser):
        parser.add_argument("--client-id", type=str, default=None)
        parser.add_argument(
            "--client-secret",
            type=str,
            default=None,
            help="Required when --client-id is given. Omit both to use the global connection.",
        )
        parser.add_argument(
            "--order-environment",
            type=str,
            default=None,
            choices=["testing", "production", "both"],
            help=(
                "Which Order API host(s) to probe. Default: the configured "
                "TURN14_ORDER_ENVIRONMENT only."
            ),
        )

    def handle(self, *args, **options):
        credentials = self._resolve_credentials(options)
        if credentials is None:
            return

        self.stdout.write(
            "Checking client_id={}...".format(self._redact(credentials["client_id"]))
        )

        ok = self._check_catalog(credentials)
        ok = self._check_orders(credentials, options["order_environment"]) and ok

        self.stdout.write("")
        if ok:
            self.stdout.write(self.style.SUCCESS("All probed endpoints are accessible."))
        else:
            self.stdout.write(
                self.style.WARNING(
                    "Some endpoints are not accessible. A 403 means the token was accepted but "
                    "the account lacks that endpoint's grant -- ask Turn 14 support to enable "
                    "it for this client_id; no code change will fix it."
                )
            )

    # -- Credentials -------------------------------------------------------------------

    def _resolve_credentials(self, options) -> typing.Optional[typing.Dict]:
        client_id = options["client_id"]
        client_secret = options["client_secret"]

        if bool(client_id) != bool(client_secret):
            self.stdout.write(
                self.style.ERROR("Provide both --client-id and --client-secret, or neither.")
            )
            return None

        if client_id:
            return {"client_id": client_id, "client_secret": client_secret}

        try:
            return turn_14_global.get_global_credentials()
        except turn_14_global.GlobalCredentialsUnavailable as e:
            self.stdout.write(self.style.ERROR("No credentials to check: {}".format(str(e))))
            return None

    @staticmethod
    def _redact(client_id: str) -> str:
        return "{}...".format(client_id[:8]) if len(client_id) > 8 else "***"

    # -- Catalog API -------------------------------------------------------------------

    def _check_catalog(self, credentials: typing.Dict) -> bool:
        self.stdout.write("")
        self.stdout.write(
            "Catalog API ({}):".format(turn_14_client.Turn14ApiClient.API_BASE_URL)
        )

        try:
            client = turn_14_client.Turn14ApiClient(credentials=credentials)
        except ValueError as e:
            self.stdout.write(self.style.ERROR("  {}".format(str(e))))
            return False

        try:
            client._get_valid_token()
        except turn_14_exceptions.Turn14APIException as e:
            # Nothing below can pass if the credential cannot even authenticate, and every
            # probe would just repeat this same error a dozen times.
            self.stdout.write(self.style.ERROR("  token: FAILED -- {}".format(str(e))))
            return False
        self.stdout.write(self.style.SUCCESS("  token: OK"))

        # One page / one unpaginated call each -- enough to prove entitlement without pulling
        # any real volume. Keyed by the endpoint path so the output matches Turn 14's own 403
        # wording ("does not have access to the <x> endpoint").
        probes = [
            ("brands", lambda: client.get_brands()),
            ("locations", lambda: client.get_locations()),
            ("items", lambda: client.get_items(page=1)),
            ("items/data", lambda: client.get_items_data(page=1)),
            ("items/fitment", lambda: client.get_items_fitment(page=1)),
            ("inventory", lambda: client.get_inventory(page=1)),
            ("pricing", lambda: client.get_pricing(page=1)),
            ("shipping", lambda: client.get_shipping_options()),
            ("shipping/item_estimation", lambda: client.get_item_shipping_estimates(page=1)),
        ]
        return all([self._probe(name, call) for name, call in probes])

    # -- Order API ---------------------------------------------------------------------

    def _check_orders(self, credentials: typing.Dict, requested: typing.Optional[str]) -> bool:
        if requested == "both":
            environments = ["testing", "production"]
        elif requested:
            environments = [requested]
        else:
            environments = [self._configured_order_environment()]

        ok = True
        for environment in environments:
            client = turn_14_order_client.Turn14OrderApiClient(
                credentials=credentials, environment=environment
            )
            self.stdout.write("")
            self.stdout.write("Order API ({}, {}):".format(client.api_base_url, environment))

            try:
                client._get_valid_token()
            except turn_14_exceptions.Turn14APIException as e:
                self.stdout.write(self.style.ERROR("  token: FAILED -- {}".format(str(e))))
                ok = False
                continue
            self.stdout.write(self.style.SUCCESS("  token: OK"))

            # Read-only only. Quote and order are deliberately absent: create_quote is
            # harmless but create_order places a real order, and a credential check must
            # never be the thing that does that.
            ok = self._probe("shipping", lambda: client.get_shipping_options()) and ok
        return ok

    @staticmethod
    def _configured_order_environment() -> str:
        from django.conf import settings

        return getattr(settings, "TURN14_ORDER_ENVIRONMENT", "testing")

    # -- Probe -------------------------------------------------------------------------

    def _probe(self, name: str, call: typing.Callable) -> bool:
        try:
            call()
        except turn_14_exceptions.Turn14APIBadResponseCodeError as e:
            style = self.style.WARNING if e.code in (401, 403) else self.style.ERROR
            label = "NO ACCESS" if e.code in (401, 403) else "ERROR"
            self.stdout.write(style("  {:<26} {} ({})".format(name, label, e.code)))
            return False
        except Exception as e:
            self.stdout.write(self.style.ERROR("  {:<26} ERROR -- {}".format(name, str(e))))
            return False

        self.stdout.write(self.style.SUCCESS("  {:<26} OK".format(name)))
        return True
