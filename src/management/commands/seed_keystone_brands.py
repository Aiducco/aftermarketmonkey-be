"""
One-time utility to seed Brands, BrandProviders, and BrandKeystoneBrandMapping
for all KeystoneBrand records. Run after fetch_keystone_brands (or fetch_keystone_full_sync).

Provider Keystone is id 4. Uses bulk operations for ~1.5k brands.
"""
from django.core.management.base import BaseCommand

from src import enums as src_enums
from src import models as src_models


KEYSTONE_PROVIDER_ID = 4


class Command(BaseCommand):
    help = (
        "One-time seed: create Brands, BrandProviders, and BrandKeystoneBrandMapping "
        "for each KeystoneBrand. Provider Keystone id=4. Uses bulk operations."
    )

    def handle(self, *args, **options):
        self.stdout.write("Starting Keystone brands seed (bulk)...")

        provider = src_models.Providers.objects.filter(id=KEYSTONE_PROVIDER_ID).first()
        if not provider:
            self.stdout.write(
                self.style.ERROR("Provider with id={} not found. Aborting.".format(KEYSTONE_PROVIDER_ID))
            )
            return

        keystone_brands = list(
            src_models.KeystoneBrand.objects.all().only("id", "name", "external_id")
        )
        if not keystone_brands:
            self.stdout.write(
                self.style.WARNING(
                    "No KeystoneBrand records found. Run fetch_keystone_brands or "
                    "fetch_keystone_full_sync first."
                )
            )
            return

        names = [kb.name.strip() for kb in keystone_brands if kb.name and kb.name.strip()]
        names = list(dict.fromkeys(names))  # unique, preserve order

        # 1. Bulk upsert Brands
        existing_brands = {b.name: b for b in src_models.Brands.objects.filter(name__in=names)}
        new_brand_names = [n for n in names if n not in existing_brands]
        new_brands = [
            src_models.Brands(
                name=n,
                status=src_enums.BrandProviderStatus.ACTIVE.value,
                status_name=src_enums.BrandProviderStatus.ACTIVE.name,
            )
            for n in new_brand_names
        ]
        if new_brands:
            src_models.Brands.objects.bulk_create(new_brands)
            for b in new_brands:
                existing_brands[b.name] = b

        self.stdout.write("  Brands: {} created, {} existing".format(
            len(new_brands), len(existing_brands) - len(new_brands)
        ))

        # 2. Bulk create BrandProviders
        name_to_brand = existing_brands
        existing_bp_brand_ids = set(
            src_models.BrandProviders.objects.filter(
                provider=provider,
                brand__name__in=names,
            ).values_list("brand_id", flat=True)
        )
        bp_to_create = [
            src_models.BrandProviders(brand=name_to_brand[n], provider=provider)
            for n in names
            if n in name_to_brand and name_to_brand[n].id not in existing_bp_brand_ids
        ]
        if bp_to_create:
            src_models.BrandProviders.objects.bulk_create(bp_to_create)

        self.stdout.write("  BrandProviders: {} created".format(len(bp_to_create)))

        # 3. Bulk create BrandKeystoneBrandMapping
        existing_mapping_pairs = set(
            src_models.BrandKeystoneBrandMapping.objects.filter(
                keystone_brand__in=keystone_brands,
            ).values_list("brand_id", "keystone_brand_id")
        )
        mappings_to_create = []
        for kb in keystone_brands:
            n = kb.name.strip() if kb.name else ""
            if not n or n not in name_to_brand:
                continue
            brand = name_to_brand[n]
            if (brand.id, kb.id) not in existing_mapping_pairs:
                mappings_to_create.append(
                    src_models.BrandKeystoneBrandMapping(brand=brand, keystone_brand=kb)
                )

        if mappings_to_create:
            src_models.BrandKeystoneBrandMapping.objects.bulk_create(mappings_to_create)

        self.stdout.write("  BrandKeystoneBrandMapping: {} created".format(len(mappings_to_create)))

        self.stdout.write(
            self.style.SUCCESS(
                "Done. Brands: {} new, BrandProviders: {} new, Mappings: {} new.".format(
                    len(new_brands), len(bp_to_create), len(mappings_to_create)
                )
            )
        )
