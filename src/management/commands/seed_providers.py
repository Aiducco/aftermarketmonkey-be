"""
Seed providers table from PROVIDER_CATALOG constants.
Creates or updates Providers for Turn 14, Keystone, Meyer, A-Tech, DLG, SDC, Rough Country, Wheel Pros.
Run: python manage.py seed_providers
"""
from django.core.management.base import BaseCommand

from src import constants as src_constants
from src import enums as src_enums
from src import models as src_models


class Command(BaseCommand):
    help = "Seed providers table from PROVIDER_CATALOG constants."

    def handle(self, *args, **options):
        self.stdout.write("Seeding providers from PROVIDER_CATALOG...")

        created = 0
        updated = 0

        for entry in src_constants.PROVIDER_CATALOG:
            kind_value = entry["kind"].value
            kind_name = entry["kind"].name
            name = entry["name"]

            provider, was_created = src_models.Providers.objects.update_or_create(
                kind=kind_value,
                defaults={
                    "name": name,
                    "status": src_enums.BrandProviderStatus.ACTIVE.value,
                    "status_name": src_enums.BrandProviderStatus.ACTIVE.name,
                    "type": src_enums.BrandProvider.DISTRIBUTOR.value,
                    "type_name": src_enums.BrandProvider.DISTRIBUTOR.name,
                    "kind_name": kind_name,
                },
            )
            if was_created:
                created += 1
                self.stdout.write(f"  Created: {name} (kind={kind_name})")
            else:
                updated += 1
                self.stdout.write(f"  Updated: {name} (kind={kind_name})")

        self.stdout.write(
            self.style.SUCCESS(
                f"Done. Created {created}, updated {updated} providers."
            )
        )
