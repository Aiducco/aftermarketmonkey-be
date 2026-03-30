"""
Auxiliary script to merge duplicate brands.

There is no ``manage.py`` subcommand; load this file from the Django shell.

**One-off merge by brand id** (e.g. keep FOX ``3017``, merge **FOX POWERSPORTS** ``3288`` into it)::

    python manage.py shell
    >>> import runpy
    >>> ns = runpy.run_path("scripts/merge_duplicate_brands.py", run_name="merge_loader")
    >>> from src import models as m
    >>> ns["merge_brands"](m.Brands.objects.get(pk=3017), m.Brands.objects.get(pk=3288))

Use ``run_name=...`` (not ``__main__``) so the script does not start the interactive ``PAIRS_TO_CHECK`` runner.

**Interactive duplicate scan** (Turn14/Keystone/WheelPros/Rough Country heuristics): run as a module::

    python scripts/merge_duplicate_brands.py

Flow per pair (``run()``):
  1. Search turn14_brands, keystone_brands, wheelpros_brands, rough_country_brands by name or code
  2. If found in at least 2 provider tables -> find Brands, ask which to keep
  3. Update mappings (wheelpros, keystone, turn14, rough_country, meyer), brand_providers
  4. Delete CompanyBrands for merge brand, then delete merge brand
  (Every action asks for confirmation)
"""
import os
import sys

if "django" not in sys.modules:
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "settings")
    import django
    django.setup()

from django.db import models as django_models
from src import models as src_models

# Pairs to check: (label, [brand names for search])
# Interactive run merges into the brand you choose to KEEP (typically FOX); FOX POWERSPORTS is merged in then deleted.
PAIRS_TO_CHECK = [
    ("FOX / FOX POWERSPORTS", ["FOX", "FOX POWERSPORTS"]),  # e.g. 3017, 3288
]


def _confirm(msg, default_no=True):
    prompt = "{} [y/N]: ".format(msg) if default_no else "{} [Y/n]: ".format(msg)
    r = input(prompt).strip().lower()
    if default_no:
        return r in ("y", "yes")
    return r not in ("n", "no")


def _search_provider_brands(name_or_code):
    """Search name/code in turn14, keystone, wheelpros, rough_country. Returns dict of provider -> list of matches."""
    q = (name_or_code or "").strip()
    if not q:
        return {}
    results = {}
    Q = django_models.Q
    t14 = list(src_models.Turn14Brand.objects.filter(
        Q(name__icontains=q) | Q(aaia_code__iexact=q) | Q(external_id__iexact=q)
    ).values("id", "name", "aaia_code", "external_id"))
    if t14:
        results["turn14"] = t14
    ks = list(src_models.KeystoneBrand.objects.filter(
        Q(name__icontains=q) | Q(aaia_code__iexact=q) | Q(external_id__iexact=q)
    ).values("id", "name", "aaia_code", "external_id"))
    if ks:
        results["keystone"] = ks
    wp = list(src_models.WheelProsBrand.objects.filter(
        Q(name__icontains=q) | Q(external_id__iexact=q)
    ).values("id", "name", "external_id"))
    if wp:
        results["wheelpros"] = wp
    rc = list(src_models.RoughCountryBrand.objects.filter(
        Q(name__icontains=q) | Q(aaia_code__iexact=q) | Q(external_id__iexact=q)
    ).values("id", "name", "aaia_code", "external_id"))
    if rc:
        results["rough_country"] = rc
    return results


def _find_brands_by_names(names):
    """Find Brands by name (case-insensitive)."""
    seen = {}
    for name in names:
        b = src_models.Brands.objects.filter(name__iexact=name).first()
        if b and b.id not in seen:
            seen[b.id] = b
        # Also try icontains for partial
        if not b:
            for b2 in src_models.Brands.objects.filter(name__icontains=name):
                if b2.id not in seen:
                    seen[b2.id] = b2
    return list(seen.values())


def _get_mappings_for_brand(brand):
    """Return which provider mappings this brand has."""
    out = []
    if src_models.BrandTurn14BrandMapping.objects.filter(brand=brand).exists():
        out.append("turn14")
    if src_models.BrandKeystoneBrandMapping.objects.filter(brand=brand).exists():
        out.append("keystone")
    if src_models.BrandWheelProsBrandMapping.objects.filter(brand=brand).exists():
        out.append("wheelpros")
    if src_models.BrandRoughCountryBrandMapping.objects.filter(brand=brand).exists():
        out.append("rough_country")
    if src_models.BrandMeyerBrandMapping.objects.filter(brand=brand).exists():
        out.append("meyer")
    return out


def merge_brands(brand_to_keep, brand_to_delete):
    """Merge brand_to_delete into brand_to_keep, then delete brand_to_delete."""
    keep_id = brand_to_keep.id
    delete_id = brand_to_delete.id

    # 1. BrandWheelProsBrandMapping
    wp_list = list(src_models.BrandWheelProsBrandMapping.objects.filter(brand_id=delete_id).select_related("wheelpros_brand"))
    print("\n--- BrandWheelProsBrandMapping: {} to process ---".format(len(wp_list)))
    for m in wp_list:
        existing = src_models.BrandWheelProsBrandMapping.objects.filter(brand_id=keep_id, wheelpros_brand=m.wheelpros_brand).first()
        if existing:
            if _confirm("  Delete mapping id={} (wheelpros_brand={})? Keep already has.".format(m.id, m.wheelpros_brand.name)):
                m.delete()
        else:
            if _confirm("  Update mapping id={} brand_id {} -> {} (wheelpros_brand={})?".format(m.id, delete_id, keep_id, m.wheelpros_brand.name)):
                m.brand_id = keep_id
                m.save()

    # 2. BrandKeystoneBrandMapping
    ks_list = list(src_models.BrandKeystoneBrandMapping.objects.filter(brand_id=delete_id).select_related("keystone_brand"))
    print("\n--- BrandKeystoneBrandMapping: {} to process ---".format(len(ks_list)))
    for m in ks_list:
        existing = src_models.BrandKeystoneBrandMapping.objects.filter(brand_id=keep_id, keystone_brand=m.keystone_brand).first()
        if existing:
            if _confirm("  Delete mapping id={} (keystone_brand={})? Keep already has.".format(m.id, m.keystone_brand.name)):
                m.delete()
        else:
            if _confirm("  Update mapping id={} brand_id {} -> {} (keystone_brand={})?".format(m.id, delete_id, keep_id, m.keystone_brand.name)):
                m.brand_id = keep_id
                m.save()

    # 3. BrandTurn14BrandMapping
    t14_list = list(src_models.BrandTurn14BrandMapping.objects.filter(brand_id=delete_id).select_related("turn14_brand"))
    print("\n--- BrandTurn14BrandMapping: {} to process ---".format(len(t14_list)))
    for m in t14_list:
        existing = src_models.BrandTurn14BrandMapping.objects.filter(brand_id=keep_id, turn14_brand=m.turn14_brand).first()
        if existing:
            if _confirm("  Delete mapping id={} (turn14_brand={})? Keep already has.".format(m.id, m.turn14_brand.name)):
                m.delete()
        else:
            if _confirm("  Update mapping id={} brand_id {} -> {} (turn14_brand={})?".format(m.id, delete_id, keep_id, m.turn14_brand.name)):
                m.brand_id = keep_id
                m.save()

    # 4. BrandRoughCountryBrandMapping
    rc_list = list(src_models.BrandRoughCountryBrandMapping.objects.filter(brand_id=delete_id).select_related("rough_country_brand"))
    print("\n--- BrandRoughCountryBrandMapping: {} to process ---".format(len(rc_list)))
    for m in rc_list:
        existing = src_models.BrandRoughCountryBrandMapping.objects.filter(brand_id=keep_id, rough_country_brand=m.rough_country_brand).first()
        if existing:
            if _confirm("  Delete mapping id={} (rough_country_brand={})? Keep already has.".format(m.id, m.rough_country_brand.name)):
                m.delete()
        else:
            if _confirm("  Update mapping id={} brand_id {} -> {} (rough_country_brand={})?".format(m.id, delete_id, keep_id, m.rough_country_brand.name)):
                m.brand_id = keep_id
                m.save()

    # 5. BrandMeyerBrandMapping
    meyer_list = list(
        src_models.BrandMeyerBrandMapping.objects.filter(brand_id=delete_id).select_related("meyer_brand")
    )
    print("\n--- BrandMeyerBrandMapping: {} to process ---".format(len(meyer_list)))
    for m in meyer_list:
        existing = src_models.BrandMeyerBrandMapping.objects.filter(
            brand_id=keep_id, meyer_brand=m.meyer_brand
        ).first()
        if existing:
            if _confirm(
                "  Delete mapping id={} (meyer_brand={})? Keep already has.".format(m.id, m.meyer_brand.name)
            ):
                m.delete()
        else:
            if _confirm(
                "  Update mapping id={} brand_id {} -> {} (meyer_brand={})?".format(
                    m.id, delete_id, keep_id, m.meyer_brand.name
                )
            ):
                m.brand_id = keep_id
                m.save()

    # 6. BrandProviders
    bp_list = list(src_models.BrandProviders.objects.filter(brand_id=delete_id).select_related("provider"))
    print("\n--- BrandProviders: {} to process ---".format(len(bp_list)))
    for bp in bp_list:
        existing = src_models.BrandProviders.objects.filter(brand_id=keep_id, provider=bp.provider).first()
        if existing:
            if _confirm("  Delete BrandProviders id={} (provider={})? Keep already has.".format(bp.id, bp.provider.kind_name)):
                bp.delete()
        else:
            if _confirm("  Update BrandProviders id={} brand_id {} -> {} (provider={})?".format(bp.id, delete_id, keep_id, bp.provider.kind_name)):
                bp.brand_id = keep_id
                bp.save()

    # 7. MasterPart (update brand_id, handle duplicates)
    mp_list = list(src_models.MasterPart.objects.filter(brand_id=delete_id))
    print("\n--- MasterPart: {} to process ---".format(len(mp_list)))
    if mp_list:
        conflicts = sum(1 for mp in mp_list if src_models.MasterPart.objects.filter(brand_id=keep_id, part_number=mp.part_number).exists())
        print("  ({} conflicts with keep brand)".format(conflicts))
        if _confirm("  Process {} MasterPart records?".format(len(mp_list))):
            from django.db import transaction
            with transaction.atomic():
                for mp in mp_list:
                    existing = src_models.MasterPart.objects.filter(brand_id=keep_id, part_number=mp.part_number).first()
                    if existing:
                        for pp in src_models.ProviderPart.objects.filter(master_part=mp):
                            kp = src_models.ProviderPart.objects.filter(master_part=existing, provider=pp.provider).first()
                            if kp:
                                pp.delete()
                            else:
                                pp.master_part = existing
                                pp.save()
                        mp.delete()
                    else:
                        mp.brand_id = keep_id
                        mp.save()

    # 8. CompanyDestinationParts
    cdp_count = src_models.CompanyDestinationParts.objects.filter(brand_id=delete_id).count()
    if cdp_count:
        if _confirm("  Update {} CompanyDestinationParts brand_id {} -> {}?".format(cdp_count, delete_id, keep_id)):
            src_models.CompanyDestinationParts.objects.filter(brand_id=delete_id).update(brand_id=keep_id)

    # 9. CompanyBrands - delete for merge brand
    cb_count = src_models.CompanyBrands.objects.filter(brand_id=delete_id).count()
    print("\n--- CompanyBrands: {} to delete (brand_id={}) ---".format(cb_count, delete_id))
    if cb_count:
        if _confirm("  Delete {} CompanyBrands for '{}'?".format(cb_count, brand_to_delete.name)):
            src_models.CompanyBrands.objects.filter(brand_id=delete_id).delete()

    # 10. BigCommerceBrands, BrandSDCBrandMapping if any
    bbc = src_models.BigCommerceBrands.objects.filter(brand_id=delete_id)
    if bbc.exists():
        if _confirm("  Update {} BigCommerceBrands?".format(bbc.count())):
            bbc.update(brand_id=keep_id)
    sdc = src_models.BrandSDCBrandMapping.objects.filter(brand_id=delete_id)
    if sdc.exists():
        for m in sdc:
            ex = src_models.BrandSDCBrandMapping.objects.filter(brand_id=keep_id, sdc_brand=m.sdc_brand).first()
            if ex:
                if _confirm("  Delete BrandSDCBrandMapping id={}?".format(m.id)):
                    m.delete()
            else:
                if _confirm("  Update BrandSDCBrandMapping id={}?".format(m.id)):
                    m.brand_id = keep_id
                    m.save()

    # 11. Delete brand
    print("\n--- Delete brand '{}' (id={}) ---".format(brand_to_delete.name, delete_id))
    if _confirm("  Confirm DELETE brand '{}'?".format(brand_to_delete.name)):
        brand_to_delete.delete()
        print("  [OK] Done.")
    else:
        print("  [SKIP] Brand NOT deleted.")


def process_pair(label, names):
    """Process one pair: search providers, find brands, ask which to keep, merge."""
    print("\n" + "=" * 60)
    print("Processing: {}".format(label))
    print("  Names: {}".format(names))
    print("=" * 60)

    provider_hits = {}
    all_providers_with_hits = set()
    for name in names:
        hits = _search_provider_brands(name)
        provider_hits[name] = hits
        for k in hits:
            all_providers_with_hits.add(k)

    print("\n[1] Provider brand tables search:")
    for name in names:
        h = provider_hits.get(name, {})
        print("  '{}' -> {}".format(name, list(h.keys()) if h else "(none)"))
    print("  Found in {} provider tables (need >= 2): {}".format(len(all_providers_with_hits), sorted(all_providers_with_hits)))

    if len(all_providers_with_hits) < 2:
        print("  [SKIP] Found in fewer than 2 provider tables. Skipping.")
        return

    brands_found = _find_brands_by_names(names)
    print("\n[2] Brands found: {}".format(len(brands_found)))
    for b in brands_found:
        mappings = _get_mappings_for_brand(b)
        print("    - id={} name='{}' aaia={} mappings={}".format(b.id, b.name, b.aaia_code or "(none)", mappings))

    if len(brands_found) < 2:
        print("  [SKIP] Need at least 2 Brands to merge. Found {}. Skipping.".format(len(brands_found)))
        return

    print("\n[3] Which brand to KEEP? (merge the others into this one)")
    for i, b in enumerate(brands_found, 1):
        print("    {} = id={} name='{}'".format(i, b.id, b.name))
    choice = input("    Enter number (1-{}): ".format(len(brands_found))).strip()
    try:
        idx = int(choice)
        if 1 <= idx <= len(brands_found):
            brand_to_keep = brands_found[idx - 1]
            brands_to_delete = [b for i, b in enumerate(brands_found, 1) if i != idx]
        else:
            print("  [SKIP] Invalid choice.")
            return
    except ValueError:
        print("  [SKIP] Invalid input.")
        return

    if len(brands_to_delete) > 1:
        print("  Multiple brands to merge. Will merge one at a time.")
    for brand_to_delete in brands_to_delete:
        print("\n  KEEP: id={} name='{}'".format(brand_to_keep.id, brand_to_keep.name))
        print("  MERGE INTO KEEP (then delete): id={} name='{}'".format(brand_to_delete.id, brand_to_delete.name))
        if not _confirm("  Proceed with this merge?", default_no=True):
            print("  [SKIP] User declined.")
            continue
        merge_brands(brand_to_keep, brand_to_delete)


def run():
    print("Duplicate brand merge script")
    print("For each pair: search turn14/keystone/wheelpros/rough_country; if found in >=2, find Brands, choose keep, merge.")
    print("")

    for label, names in PAIRS_TO_CHECK:
        try:
            process_pair(label, names)
        except Exception as e:
            print("\n[ERROR] {}: {}".format(label, e))
            import traceback
            traceback.print_exc()
            if not _confirm("  Continue to next pair?"):
                break

    print("\n" + "=" * 60)
    print("Done.")

#
# if __name__ == "__main__":
#     import pathlib
#     root = pathlib.Path(__file__).resolve().parent.parent
#     if str(root) not in sys.path:
#         sys.path.insert(0, str(root))
# run()
