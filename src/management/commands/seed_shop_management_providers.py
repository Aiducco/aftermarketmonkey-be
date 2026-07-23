"""
Seed shop_management_providers table from SHOP_MANAGEMENT_PROVIDER_CATALOG.
Creates or updates ShopManagementProviders rows.
Run: python manage.py seed_shop_management_providers
"""
from django.core.management.base import BaseCommand

from src import constants as src_constants
from src import enums as src_enums
from src import models as src_models


class Command(BaseCommand):
    help = "Seed shop_management_providers table from SHOP_MANAGEMENT_PROVIDER_CATALOG."

    def handle(self, *args, **options):
        self.stdout.write("Seeding shop management providers...")

        created = 0
        updated = 0

        for entry in src_constants.SHOP_MANAGEMENT_PROVIDER_CATALOG:
            kind_value = entry["kind"].value
            kind_name = entry["kind"].name
            name = entry["name"]

            _, was_created = src_models.ShopManagementProviders.objects.update_or_create(
                kind=kind_value,
                defaults={
                    "name": name,
                    "status": src_enums.ShopManagementProviderStatus.ACTIVE.value,
                    "status_name": src_enums.ShopManagementProviderStatus.ACTIVE.name,
                    "kind_name": kind_name,
                    "coming_soon": False,
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
                f"Done. Created {created}, updated {updated} shop management providers."
            )
        )
