"""
Seed providers table from PROVIDER_CATALOG and COMING_SOON_PROVIDERS constants.
Creates or updates Providers rows; coming-soon entries are marked coming_soon=True.
Run: python manage.py seed_providers
"""
from django.core.management.base import BaseCommand

from src import constants as src_constants
from src import enums as src_enums
from src import models as src_models


class Command(BaseCommand):
    help = "Seed providers table from PROVIDER_CATALOG + COMING_SOON_PROVIDERS constants."

    def handle(self, *args, **options):
        self.stdout.write("Seeding providers...")

        created = 0
        updated = 0

        entries = [
            (entry, False) for entry in src_constants.PROVIDER_CATALOG
        ] + [
            (entry, True) for entry in src_constants.COMING_SOON_PROVIDERS
        ]

        for entry, coming_soon in entries:
            kind_value = entry["kind"].value
            kind_name = entry["kind"].name
            name = entry["name"]

            _, was_created = src_models.Providers.objects.update_or_create(
                kind=kind_value,
                defaults={
                    "name": name,
                    "status": src_enums.BrandProviderStatus.ACTIVE.value,
                    "status_name": src_enums.BrandProviderStatus.ACTIVE.name,
                    "type": src_enums.BrandProvider.DISTRIBUTOR.value,
                    "type_name": src_enums.BrandProvider.DISTRIBUTOR.name,
                    "kind_name": kind_name,
                    "coming_soon": coming_soon,
                },
            )

            tag = " (coming soon)" if coming_soon else ""
            if was_created:
                created += 1
                self.stdout.write(f"  Created{tag}: {name} (kind={kind_name})")
            else:
                updated += 1
                self.stdout.write(f"  Updated{tag}: {name} (kind={kind_name})")

        self.stdout.write(
            self.style.SUCCESS(
                f"Done. Created {created}, updated {updated} providers."
            )
        )
