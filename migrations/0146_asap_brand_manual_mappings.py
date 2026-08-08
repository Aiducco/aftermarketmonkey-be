"""
Manually map ASAP brands the auto-matcher in ``_match_unmapped_asap_brands`` couldn't resolve.

The cascade there (exact upper-name -> compact-key -> fuzzy word-prefix) is deliberately
conservative and never auto-creates canonical Brands, so a handful of real brands stayed
unmatched and were being skipped by product sync entirely:

  - "Yukon Gear and Axle" vs "YUKON GEAR & AXLE"   -> "and" vs "&" defeats every tier
  - "Fuel Off Road Wheels"                         -> we hold this brand under three separate
    canonical records ("FUEL OFFROAD", "FUEL WHEELS", "FUEL 1PC") sourced from different
    distributors; pointing at FUEL OFFROAD is a deliberate call, not something the matcher
    could infer
  - "Cali Offroad Wheels" / "Allied Wheel"         -> trailing "Wheels"/"Wheel" noise word
  - "ADS Racing Shocks" / "Mazzi Wheels"           -> our records carry "-INACTIVE" / "(TWG)"
    suffixes the fuzzy tier won't bridge

Only fills rows where ``brand_id`` is still NULL, so it never clobbers a mapping the matcher (or
a human) has already made, and it re-runs harmlessly. A missing ASAP brand or canonical Brand is
skipped rather than raising - these names are data, not schema, and a fresh environment may not
have every distributor loaded yet.
"""
from django.db import migrations

# ASAP brand name (AsapBrand.name) -> canonical Brands.name
ASAP_BRAND_TO_CANONICAL_BRAND = {
    "Yukon Gear and Axle": "YUKON GEAR & AXLE",
    "Fuel Off Road Wheels": "FUEL OFFROAD",
    "Cali Offroad Wheels": "CALI OFF-ROAD",
    "Allied Wheel": "ALLIED WHEELS",
    "ADS Racing Shocks": "ADS RACING SHOCKS-INACTIVE",
    "Mazzi Wheels": "MAZZI (TWG)",
}


def apply_manual_asap_brand_mappings(apps, schema_editor):
    AsapBrand = apps.get_model("src", "AsapBrand")
    Brands = apps.get_model("src", "Brands")

    for asap_name, brand_name in ASAP_BRAND_TO_CANONICAL_BRAND.items():
        asap_brand = AsapBrand.objects.filter(name=asap_name, brand__isnull=True).first()
        if asap_brand is None:
            continue
        brand = Brands.objects.filter(name=brand_name).order_by("id").first()
        if brand is None:
            continue
        asap_brand.brand = brand
        asap_brand.save(update_fields=["brand", "updated_at"])


def unapply_manual_asap_brand_mappings(apps, schema_editor):
    """Clear only the exact pairs this migration sets, so an unrelated mapping isn't dropped."""
    AsapBrand = apps.get_model("src", "AsapBrand")
    Brands = apps.get_model("src", "Brands")

    for asap_name, brand_name in ASAP_BRAND_TO_CANONICAL_BRAND.items():
        brand = Brands.objects.filter(name=brand_name).order_by("id").first()
        if brand is None:
            continue
        AsapBrand.objects.filter(name=asap_name, brand_id=brand.id).update(brand=None)


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0145_quadratec_provider"),
    ]

    operations = [
        migrations.RunPython(
            apply_manual_asap_brand_mappings,
            unapply_manual_asap_brand_mappings,
        ),
    ]
