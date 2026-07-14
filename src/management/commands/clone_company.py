"""
Clone an existing company into a new demo account.

Copies: Company record, CompanyProviders, CompanyOnboardingPreferences,
CompanyBrands, CompanyBrandDestinations, and all per-company pricing tables
(Turn14BrandPricing, KeystoneCompanyPricing, MeyerCompanyPricing,
AtechCompanyPricing, DlgCompanyPricing, WheelProsCompanyPricing,
RoughCountryCompanyPricing, ProviderPartCompanyPricing).

Usage:
    python manage.py clone_company --source-id 16 --email gojko@aftermarketscout.com
    python manage.py clone_company --source-id 16 --email gojko@aftermarketscout.com --name "Demo Company"
"""

import uuid

from django.contrib.auth.models import User
from django.core.management.base import BaseCommand
from django.db import transaction

from src.models import (
    AtechCompanyPricing,
    Company,
    CompanyBrandDestination,
    CompanyBrands,
    CompanyOnboardingPreferences,
    CompanyProviders,
    DlgCompanyPricing,
    IntegrationPricingSyncJob,
    KeystoneCompanyPricing,
    MeyerCompanyPricing,
    ProviderPartCompanyPricing,
    RoughCountryCompanyPricing,
    Turn14BrandPricing,
    UserProfile,
    WheelProsCompanyPricing,
)

CHUNK_SIZE = 5_000


def _bulk_clone_pricing(model, company_fk_field, source_company, new_company, label, stdout):
    """Bulk-copy all rows for source_company into new_company in chunks."""
    qs = model.objects.filter(**{company_fk_field: source_company})
    total = qs.count()
    stdout.write(f"  Cloning {label}: {total:,} rows...")

    done = 0
    last_id = 0
    while True:
        batch = list(qs.filter(id__gt=last_id).order_by("id")[:CHUNK_SIZE])
        if not batch:
            break
        last_id = batch[-1].id  # capture before nulling pk
        new_rows = []
        for row in batch:
            row.pk = None
            setattr(row, company_fk_field, new_company)
            new_rows.append(row)
        model.objects.bulk_create(new_rows, ignore_conflicts=False)
        done += len(batch)
        stdout.write(f"    ... {done:,}/{total:,}")

    stdout.write(f"  Done {label}: {done:,} rows.")
    return done


class Command(BaseCommand):
    help = "Clone a company into a new demo account, copying all provider credentials and pricing."

    def add_arguments(self, parser):
        parser.add_argument("--source-id", type=int, required=True, help="ID of the source company to clone.")
        parser.add_argument("--email", required=True, help="Email for the new demo user (admin).")
        parser.add_argument("--name", default=None, help="Override company name (default: '<source> (Demo)').")
        parser.add_argument("--password", default="changeme123!", help="Password for the new user.")
        parser.add_argument(
            "--skip-pricing",
            action="store_true",
            help="Skip copying pricing tables (useful for quick smoke-test clones).",
        )

    def handle(self, *args, **options):
        source_id = options["source_id"]
        email = options["email"]
        password = options["password"]
        skip_pricing = options["skip_pricing"]

        try:
            source = Company.objects.get(id=source_id)
        except Company.DoesNotExist:
            self.stderr.write(self.style.ERROR(f"Company {source_id} not found."))
            return

        company_name = options["name"] or f"{source.name} (Demo)"
        slug = f"{source.slug}-demo-{uuid.uuid4().hex[:8]}"

        self.stdout.write(f"Cloning company '{source.name}' (id={source_id}) → '{company_name}'")
        self.stdout.write(f"  Email: {email}")
        self.stdout.write(f"  Slug:  {slug}")

        with transaction.atomic():
            # --- 1. Create Company ---
            new_company = Company.objects.create(
                name=company_name,
                slug=slug,
                status=source.status,
                status_name=source.status_name,
                business_type=source.business_type,
                country=source.country,
                state_province=source.state_province,
                city=source.city,
                postal_code=source.postal_code,
                tax_id=source.tax_id,
                onboarding_step=source.onboarding_step,
                subscription_plan=source.subscription_plan,
                subscription_id=None,
                subscription_status=source.subscription_status,
                subscription_period_end=source.subscription_period_end,
            )
            self.stdout.write(self.style.SUCCESS(f"Created company id={new_company.id}: {new_company.name}"))

            # --- 2. Create / link user ---
            user, created = User.objects.get_or_create(
                email=email,
                defaults={"username": email, "first_name": "Demo", "last_name": "User"},
            )
            if created:
                user.set_password(password)
                user.save()
                self.stdout.write(self.style.SUCCESS(f"Created user: {email}"))
            else:
                self.stdout.write(f"User {email} already exists — linking to new company.")

            UserProfile.objects.get_or_create(
                user=user,
                defaults={"company": new_company, "is_company_admin": True},
            )
            # If profile already exists, update company
            UserProfile.objects.filter(user=user).update(company=new_company, is_company_admin=True)
            self.stdout.write(f"UserProfile linked.")

            # --- 3. Clone CompanyProviders (credentials) ---
            for cp in source.company_providers.all():
                CompanyProviders.objects.create(
                    company=new_company,
                    provider=cp.provider,
                    credentials=cp.credentials,
                    primary=cp.primary,
                )
                self.stdout.write(f"  Provider: {cp.provider.name}")

            # --- 4. Clone OnboardingPreferences ---
            try:
                src_prefs = source.onboarding_preferences
                CompanyOnboardingPreferences.objects.create(
                    company=new_company,
                    preferred_distributor_ids=src_prefs.preferred_distributor_ids,
                    top_categories=src_prefs.top_categories,
                )
                self.stdout.write("  OnboardingPreferences cloned.")
            except CompanyOnboardingPreferences.DoesNotExist:
                self.stdout.write("  No OnboardingPreferences to clone.")

            # --- 5. Clone CompanyBrands + CompanyBrandDestinations ---
            cb_id_map = {}
            for cb in source.brands.all():
                new_cb = CompanyBrands.objects.create(
                    company=new_company,
                    brand=cb.brand,
                    status=cb.status,
                    status_name=cb.status_name,
                )
                cb_id_map[cb.id] = new_cb.id

            if cb_id_map:
                self.stdout.write(f"  CompanyBrands cloned: {len(cb_id_map)}")

            # --- 6. Pricing tables ---
            if skip_pricing:
                self.stdout.write(self.style.WARNING("Skipping pricing tables (--skip-pricing)."))
            else:
                self.stdout.write("Cloning pricing tables (this may take several minutes)...")

                _bulk_clone_pricing(
                    Turn14BrandPricing, "company", source, new_company, "Turn14BrandPricing", self.stdout
                )
                _bulk_clone_pricing(
                    KeystoneCompanyPricing, "company", source, new_company, "KeystoneCompanyPricing", self.stdout
                )
                _bulk_clone_pricing(
                    MeyerCompanyPricing, "company", source, new_company, "MeyerCompanyPricing", self.stdout
                )
                _bulk_clone_pricing(
                    AtechCompanyPricing, "company", source, new_company, "AtechCompanyPricing", self.stdout
                )
                _bulk_clone_pricing(
                    DlgCompanyPricing, "company", source, new_company, "DlgCompanyPricing", self.stdout
                )
                _bulk_clone_pricing(
                    WheelProsCompanyPricing, "company", source, new_company, "WheelProsCompanyPricing", self.stdout
                )
                _bulk_clone_pricing(
                    RoughCountryCompanyPricing,
                    "company",
                    source,
                    new_company,
                    "RoughCountryCompanyPricing",
                    self.stdout,
                )
                _bulk_clone_pricing(
                    ProviderPartCompanyPricing,
                    "company",
                    source,
                    new_company,
                    "ProviderPartCompanyPricing",
                    self.stdout,
                )

        self.stdout.write(self.style.SUCCESS(f"\nDone! New company id={new_company.id} ({new_company.name})"))
        self.stdout.write(f"  Login: {email} / {password}")
