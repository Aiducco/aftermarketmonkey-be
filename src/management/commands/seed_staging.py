"""
Seed the STAGING database with test companies and users on each plan, fake distributor connections and synthetic
pricing (task S-4; workspace docs/plans/staging-environment.md). Based on seed_dev. Idempotent.

Runs only with --settings=conf.settings_staging. Run after import_catalog_subset (it connects each company to the
providers that have the most provider_parts); without an import it still creates the companies and users.

Passwords are never stored in the repo (staging is a public host): pass --password or $STAGING_SEED_PASSWORD, or
one is generated and printed once. An existing user's password changes only when one is passed.

Usage (on the staging VM, inside the be container):
    python manage.py seed_staging --settings=conf.settings_staging
"""

import datetime
import os
import secrets
from decimal import Decimal

from django.conf import settings
from django.contrib.auth.models import User
from django.core.management import call_command
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.db.models import Count
from django.utils import timezone

from src import enums as src_enums
from src.api.services import onboarding as onboarding_services
from src.models import (
    Company,
    CompanyProviderOrderAccount,
    CompanyProviders,
    ProviderPart,
    ProviderPartCompanyPricing,
    Providers,
)

ALLOWED_SETTINGS = {"conf.settings_staging"}
FAKE_CREDENTIALS = {"username": "staging-fake", "password": "staging-fake", "note": "staging: no live calls"}
# Order-account credentials carry none of the fields any order adapter requires (account number, security key, rep
# email, ...), so order_registry.get_adapter() is None for them: seeded ordering is display-only.
FAKE_ORDER_CREDENTIALS = {"note": "staging: display-only ordering account, no live calls"}
ORDER_ACCOUNT_LABEL = "Default"

# (email, company, plan, connections). Scout's free plan allows 2 distributor connections.
ACCOUNTS = [
    ("scout@example.test", "Staging Scout Garage", "scout", 2),
    ("hunter@example.test", "Staging Hunter Trial Shop", "hunter_trial", 3),
    ("pack@example.test", "Staging Pack Shop", "pack", 3),
]


def _synthetic_prices(provider_part_id: int) -> dict:
    """Deterministic fake prices: stable across re-seeds, obviously not real."""
    cost = (Decimal(5) + Decimal(provider_part_id * 7919 % 50000) / 100).quantize(Decimal("0.01"))
    msrp = (cost * Decimal("1.6")).quantize(Decimal("0.01"))
    return {
        "cost": cost,
        "jobber_price": (cost * Decimal("1.3")).quantize(Decimal("0.01")),
        "map_price": (msrp * Decimal("0.95")).quantize(Decimal("0.01")),
        "msrp": msrp,
        "retail_price": msrp,
    }


class Command(BaseCommand):
    help = (
        "Seed staging with test companies/users per plan, fake provider connections and synthetic pricing. "
        "Seeded ordering accounts are display-only: their fake credentials never yield an order adapter, so nothing "
        "can quote or submit to a distributor."
    )

    def add_arguments(self, parser):
        parser.add_argument("--password", default=os.environ.get("STAGING_SEED_PASSWORD", ""))

    def handle(self, *args, **opts):
        if settings.SETTINGS_MODULE not in ALLOWED_SETTINGS:
            raise CommandError("seed_staging only runs with --settings=conf.settings_staging")
        if settings.DATABASES["default"]["HOST"] in settings.PRODUCTION_HOSTS:
            raise CommandError("DATABASE_HOST is production; refusing")

        if not Providers.objects.exists():
            self.stdout.write("No providers yet (no catalog import): seeding the provider catalog")
            call_command("seed_providers")
        call_command("seed_shop_management_providers")

        password = opts["password"]
        generated = not password
        if generated:
            password = secrets.token_urlsafe(12)

        top_providers = list(
            ProviderPart.objects.values("provider_id")
            .annotate(n=Count("id"))
            .order_by("-n", "provider_id")
            .values_list("provider_id", flat=True)[:3]
        )
        created_any = False
        for email, company_name, plan, connections in ACCOUNTS:
            company, created = self._ensure_account(email, company_name, password, opts["password"])
            created_any |= created
            self._apply_plan(company, plan)
            providers = top_providers[:connections]
            self._connect(company, providers)
            priced = self._price(company, providers)
            self.stdout.write(
                f"  {email:22} company_id={company.id} plan={plan:12} providers={providers} priced_parts={priced}"
            )

        if generated and created_any:
            self.stdout.write(self.style.WARNING(f"Generated password for new staging users (shown once): {password}"))
        self.stdout.write(self.style.SUCCESS("Staging seed complete"))

    def _ensure_account(self, email, company_name, password, explicit_password):
        user = User.objects.filter(email=email).first()
        if user is None:
            result = onboarding_services.register_user(
                first_name="Staging",
                last_name=company_name.split()[1],
                email=email,
                password=password,
                company_name=company_name,
                role="owner",
            )
            company = Company.objects.get(id=result["company_id"])
            created = True
        else:
            company = user.profile.company if hasattr(user, "profile") else Company.objects.get(name=company_name)
            if explicit_password:
                user.set_password(explicit_password)
                user.save(update_fields=["password"])
            created = False
        Company.objects.filter(id=company.id).update(onboarding_step=4, is_internal=True)
        company.refresh_from_db()
        return company, created

    @staticmethod
    def _apply_plan(company, plan):
        now = timezone.now()
        if plan == "scout":  # free tier: no subscription
            fields = dict(subscription_plan=None, subscription_status=None, subscription_period_end=None)
            fields["manual_trial_granted_at"] = None
        elif plan == "hunter_trial":  # what a new signup gets (reverse trial)
            fields = dict(
                subscription_plan="hunter",
                subscription_status="trialing",
                subscription_period_end=now + datetime.timedelta(days=30),
                manual_trial_granted_at=now,
            )
        else:  # pack: comp'd as active, no Stripe subscription behind it
            fields = dict(
                subscription_plan="pack",
                subscription_status="active",
                subscription_period_end=now + datetime.timedelta(days=365),
                manual_trial_granted_at=None,
            )
        Company.objects.filter(id=company.id).update(**fields)

    @staticmethod
    def _connect(company, provider_ids):
        connected = src_enums.CompanyProviderConnectionStatus.CONNECTED
        ordered = src_enums.CompanyProviderOrderConnectionStatus.CONNECTED
        for i, provider_id in enumerate(provider_ids):
            # "Connected" means a truthy credentials["feed"]; the fake values live there (older seeds stored them
            # flat, which this rewrites). Nothing here calls a distributor.
            connection, _ = CompanyProviders.objects.update_or_create(
                company=company,
                provider_id=provider_id,
                defaults=dict(
                    credentials={"feed": dict(FAKE_CREDENTIALS)},
                    primary=(i == 0),
                    active=True,
                    initial_sync_completed=True,
                    status=connected.value,
                    status_name=connected.name,
                    status_checked_at=timezone.now(),
                    order_status=ordered.value,
                    order_status_name=ordered.name,
                    order_status_checked_at=timezone.now(),
                ),
            )
            CompanyProviderOrderAccount.objects.filter(company_provider=connection, is_default=True).exclude(
                label=ORDER_ACCOUNT_LABEL
            ).update(is_default=False)
            CompanyProviderOrderAccount.objects.update_or_create(
                company_provider=connection,
                label=ORDER_ACCOUNT_LABEL,
                defaults=dict(
                    credentials=dict(FAKE_ORDER_CREDENTIALS),
                    is_default=True,
                    active=True,
                    order_status=ordered.value,
                    order_status_name=ordered.name,
                    order_status_checked_at=timezone.now(),
                ),
            )

    @staticmethod
    def _price(company, provider_ids):
        parts = list(ProviderPart.objects.filter(provider_id__in=provider_ids).values_list("id", flat=True))
        now = timezone.now()
        with transaction.atomic():
            ProviderPartCompanyPricing.objects.filter(company=company).delete()
            ProviderPartCompanyPricing.objects.bulk_create(
                [
                    ProviderPartCompanyPricing(
                        company=company, provider_part_id=pp_id, last_synced_at=now, **_synthetic_prices(pp_id)
                    )
                    for pp_id in parts
                ],
                batch_size=5000,
            )
        return len(parts)
