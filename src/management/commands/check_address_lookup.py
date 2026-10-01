"""
Diagnose the ship-to address autocomplete/validation stack, one layer at a time.

Read-only. It makes two Geoapify calls and writes one throwaway cache key.

Exists because /api/address/suggest/ is deliberately silent on failure -- the spec requires
that an unreachable provider never interrupts someone's typing, so EVERY fault comes back as
``200 {"suggestions": []}``, exactly like an address that genuinely matched nothing. That is
right for the user and useless for whoever has to fix it: an unreachable cache and a wrong
API key look identical from outside. This command tells them apart.

The trap it was written for: /validate needs only the provider, while /suggest needs the
provider AND the cache. So "validate works but suggest is always empty" is not a provider
problem at all -- it is the cache, every time.

    manage.py check_address_lookup
    manage.py check_address_lookup --query "13000 Research Blvd" --country US

In production:

    DEPLOY_ENV=production docker compose exec -T app python manage.py check_address_lookup
"""
import typing
import uuid

from django.conf import settings
from django.core.cache import cache
from django.core.management.base import BaseCommand

from src.api.services import address as address_services
from src.integrations.address import exceptions as address_exceptions
from src.integrations.address import registry

_DEFAULT_QUERY = "1600 Amphitheatre Parkway"
_DEFAULT_COUNTRY = "US"


class Command(BaseCommand):
    help = "Check the address provider and cache that /api/address/* depend on."

    def add_arguments(self, parser) -> None:
        parser.add_argument("--query", default=_DEFAULT_QUERY, help="Address text to look up.")
        parser.add_argument("--country", default=_DEFAULT_COUNTRY, help="ISO2 country code.")

    def handle(self, *args: typing.Any, **options: typing.Any) -> None:
        query = options["query"]
        country = (options["country"] or "").strip().upper() or None
        failures: typing.List[str] = []

        self._config()
        provider = self._provider(failures)
        if provider is not None:
            self._autocomplete(provider, query, country, failures)
            self._geocode(provider, query, country, failures)
        self._cache(failures)
        self._end_to_end(query, country, failures)

        self.stdout.write("")
        if failures:
            self.stdout.write(self.style.ERROR("FAILED ({}):".format(len(failures))))
            for failure in failures:
                self.stdout.write(self.style.ERROR("  - {}".format(failure)))
        else:
            self.stdout.write(self.style.SUCCESS("All checks passed."))

    # -- layers ----------------------------------------------------------------------------

    def _config(self) -> None:
        self.stdout.write(self.style.MIGRATE_HEADING("1. Config"))
        api_key = getattr(settings, "GEOAPIFY_API_KEY", "") or ""
        self.stdout.write("   ADDRESS_PROVIDER             = {!r}".format(settings.ADDRESS_PROVIDER))
        # Never print the key itself; length alone separates "unset" from "set to junk".
        self.stdout.write(
            "   GEOAPIFY_API_KEY             = {}".format(
                "set ({} chars)".format(len(api_key)) if api_key else "NOT SET"
            )
        )
        self.stdout.write(
            "   suggest / validate timeout   = {}s / {}s".format(
                settings.ADDRESS_PROVIDER_TIMEOUT_SECONDS, settings.ADDRESS_VALIDATE_TIMEOUT_SECONDS
            )
        )
        self.stdout.write(
            "   ADDRESS_ALLOWED_COUNTRIES    = {}".format(
                settings.ADDRESS_ALLOWED_COUNTRIES or "(none -- every country allowed)"
            )
        )
        self.stdout.write(
            "   suggest rate limit           = {}/min".format(
                settings.ADDRESS_SUGGEST_RATE_LIMIT_PER_MINUTE or "disabled"
            )
        )
        location = (settings.CACHES.get("default") or {}).get("LOCATION")
        self.stdout.write("   cache LOCATION               = {!r}".format(location))

    def _provider(self, failures: typing.List[str]):
        self.stdout.write(self.style.MIGRATE_HEADING("2. Provider construction"))
        provider = registry.get_provider()
        if provider is None:
            self.stdout.write(
                self.style.ERROR(
                    "   FAIL: no provider. Either ADDRESS_PROVIDER names something unknown or "
                    "GEOAPIFY_API_KEY is missing."
                )
            )
            failures.append("provider could not be constructed")
            return None
        self.stdout.write(self.style.SUCCESS("   OK: {}".format(provider.name)))
        return provider

    def _autocomplete(self, provider, query: str, country, failures: typing.List[str]) -> None:
        self.stdout.write(self.style.MIGRATE_HEADING("3. Provider autocomplete (what /suggest uses)"))
        try:
            suggestions = provider.suggest(q=query, country=country, session=str(uuid.uuid4()))
        except address_exceptions.AddressProviderTimeout as e:
            self.stdout.write(self.style.ERROR("   FAIL: timed out -- {}".format(e)))
            failures.append("provider autocomplete timed out")
            return
        except address_exceptions.AddressProviderError as e:
            self.stdout.write(self.style.ERROR("   FAIL: {}".format(e)))
            failures.append("provider autocomplete errored")
            return

        if not suggestions:
            self.stdout.write(
                self.style.WARNING(
                    "   No hits for {!r}. Not necessarily a fault -- try a different --query "
                    "before concluding anything.".format(query)
                )
            )
            return
        self.stdout.write(self.style.SUCCESS("   OK: {} hit(s)".format(len(suggestions))))
        for suggestion in suggestions[:3]:
            self.stdout.write("     {}".format(suggestion.label))
            if suggestion.address:
                self.stdout.write("       -> {}".format(suggestion.address.as_dict()))

    def _geocode(self, provider, query: str, country, failures: typing.List[str]) -> None:
        self.stdout.write(self.style.MIGRATE_HEADING("4. Provider geocode (what /validate uses)"))
        from src.integrations.address import base as address_base

        address = address_base.Address(
            line1=query, city="", state="", postal_code="", country=country or ""
        )
        try:
            result = provider.validate(address)
        except address_exceptions.AddressProviderError as e:
            self.stdout.write(self.style.ERROR("   FAIL: {}".format(e)))
            failures.append("provider geocode errored")
            return
        self.stdout.write(
            self.style.SUCCESS("   OK: status={} messages={}".format(result.status.name, list(result.messages)))
        )

    def _cache(self, failures: typing.List[str]) -> None:
        self.stdout.write(self.style.MIGRATE_HEADING("5. Cache round trip (/suggest needs this; /validate does not)"))
        key = "address:selfcheck:{}".format(uuid.uuid4().hex)
        try:
            cache.set(key, {"ok": True}, 30)
            value = cache.get(key)
            cache.delete(key)
        except Exception as e:
            self.stdout.write(self.style.ERROR("   FAIL: {}: {}".format(type(e).__name__, e)))
            self.stdout.write(
                self.style.ERROR(
                    "   THIS IS WHY /suggest RETURNS AN EMPTY LIST FOR EVERY QUERY while "
                    "/validate works -- /suggest caches each suggestion so /resolve can find it."
                )
            )
            self.stdout.write(
                "   Inside the app container the cache host is the compose service name, not "
                "localhost. Check CACHE_HOST / CACHE_PORT in .env.app.<env>."
            )
            failures.append("cache unreachable -- /suggest cannot work")
            return

        if value != {"ok": True}:
            self.stdout.write(self.style.ERROR("   FAIL: wrote a value but read back {!r}".format(value)))
            failures.append("cache did not return what was written")
            return
        self.stdout.write(self.style.SUCCESS("   OK: wrote, read and deleted a key"))

    def _end_to_end(self, query: str, country, failures: typing.List[str]) -> None:
        self.stdout.write(self.style.MIGRATE_HEADING("6. End to end: suggest -> resolve"))
        session = str(uuid.uuid4())
        suggestions = address_services.suggest_addresses(q=query, country=country, session=session)
        if not suggestions:
            self.stdout.write(
                self.style.ERROR(
                    "   FAIL: suggest_addresses returned []. This is exactly what the API "
                    "returns as 200 {\"suggestions\": []} -- see which layer above failed."
                )
            )
            failures.append("end-to-end suggest returned nothing")
            return

        self.stdout.write(self.style.SUCCESS("   OK: {} suggestion(s)".format(len(suggestions))))
        resolved = address_services.resolve_address(
            suggestion_id=suggestions[0]["id"], session=session
        )
        if resolved is None:
            self.stdout.write(
                self.style.ERROR("   FAIL: the id just issued did not resolve (cache write/read mismatch).")
            )
            failures.append("resolve failed for a freshly issued id")
            return
        self.stdout.write(self.style.SUCCESS("   OK: resolved -> {}".format(resolved)))
