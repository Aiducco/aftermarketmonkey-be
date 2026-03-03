"""
One-time script to merge duplicate brands.
Paste into Python interpreter (with Django env activated, from project root).

Steps per pair:
  1. CompanyBrands - delete records for brand being removed
  2. BrandProviders - swap brand_id or delete if duplicate
  3. BrandTurn14BrandMapping - swap brand_id or delete if duplicate
  4. BrandKeystoneBrandMapping - swap brand_id or delete if duplicate
  5. MasterPart - update brand_id
  6. CompanyDestinationParts - update brand_id
  7. BigCommerceBrands, BrandSDCBrandMapping - if any
  8. Delete the merged brand

Usage:
  cd /path/to/aftermarketmonkey-be
  python manage.py shell
  >>> exec(open("scripts/merge_duplicate_brands.py").read())
  # or paste the entire script
"""
import os
import sys

# Django setup (if not already in shell)
if "django" not in sys.modules:
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "settings")
    import django
    django.setup()

from src import models as src_models

# Duplicate brand pairs: (display_name, [name1, name2, ...]) - names to search in brands/turn14/keystone
DUPLICATE_PAIRS = [
    ("Accel", ["ACCEL", "ACCEL PLUGS"]),
    ("Alcon", ["Alcon", "ALCON BRAKE"]),
    ("Brembo", ["BREMBO", "Brembo OE", "Brembo OE Powersports"]),
    ("Camco", ["CAMCO", "CAMCO MARINE"]),
    ("Coleman", ["COLEMAN CO.", "COLEMAN RVP"]),
    ("Dexter", ["DEXTER AXLE", "DEXTER GROUP", "DEXTR MARINE"]),
    ("Dometic", ["DOMETIC", "DOMETIC CPV", "DOMETIC OUTD"]),
    ("EBC", ["EBC BRAKES", "EBC Powersports"]),
    ("Edelbrock", ["EDELBROCK", "EDEL.CYLHEAD"]),
    ("FOX", ["FOX SHOX", "FOX Powersports"]),
    ("Garmin", ["GARMIN CARTO", "GARMIN ELEC."]),
    ("Hella", ["HELLA", "HELLA LIGHTS"]),
    ("King", ["KING", "KING SHOCKS", "KING BEARING"]),
    ("KYB", ["KYB SHOCKS", "KYB Powersports"]),
    ("Mahle", ["Mahle", "Mahle OE"]),
    ("Method", ["Method Wheels", "METHOD RACE"]),
    ("Mickey Thompson", ["M.T. DRAG", "M.T. STREET", "M.T. WHEEL"]),
    ("Peterson", ["PETERSON MFG", "PETERSN MOLD", "Peterson Fluid Systems"]),
    ("Progress", ["Progress Technology", "Progress LT"]),
    ("Ranch Hand", ["RANCH HAND", "RANCH HAND M"]),
    ("Rockford Fosgate", ["Rockford Fosgate", "Rockford Fosgate UTV"]),
    ("Turn 14", ["Turn 14 Distribution", "Turn 14 HR"]),
    ("Walker", ["WALKER EXHST", "WALKER PROD."]),
    ("Wix Filters", ["WIX FILTR HD", "WIX FILTR LD"]),
]


def _confirm(msg: str, default_no: bool = True) -> bool:
    """Ask for confirmation. Returns True if user confirms."""
    prompt = "{} [y/N]: ".format(msg) if default_no else "{} [Y/n]: ".format(msg)
    r = input(prompt).strip().lower()
    if default_no:
        return r in ("y", "yes")
    return r not in ("n", "no")


def _find_brands_by_names(names: list) -> list:
    """Find Brands records by exact name match. Returns unique brands by id."""
    seen = {}
    for name in names:
        b = src_models.Brands.objects.filter(name__iexact=name).first()
        if b and b.id not in seen:
            seen[b.id] = b
    return list(seen.values())


def _find_turn14_brand_by_name(name: str):
    return src_models.Turn14Brand.objects.filter(name__iexact=name).first()


def _find_keystone_brand_by_name(name: str):
    return src_models.KeystoneBrand.objects.filter(name__iexact=name).first()


def _get_brand_provider_source(brand) -> str:
    """Return 'turn14', 'keystone', or None."""
    t14 = src_models.BrandTurn14BrandMapping.objects.filter(brand=brand).first()
    if t14:
        return "turn14"
    ks = src_models.BrandKeystoneBrandMapping.objects.filter(brand=brand).first()
    if ks:
        return "keystone"
    return None


def merge_brands(brand_to_keep, brand_to_delete):
    """Merge brand_to_delete into brand_to_keep, then delete brand_to_delete."""
    keep_id = brand_to_keep.id
    delete_id = brand_to_delete.id

    print("\n--- Step 1: CompanyBrands (delete records for brand to delete) ---")
    cb_count = src_models.CompanyBrands.objects.filter(brand_id=delete_id).count()
    print("  CompanyBrands to delete (brand_id={}): {}".format(delete_id, cb_count))
    if cb_count:
        if _confirm("  Confirm DELETE {} CompanyBrands for '{}'?".format(cb_count, brand_to_delete.name)):
            src_models.CompanyBrands.objects.filter(brand_id=delete_id).delete()
            print("  [OK] Deleted {} CompanyBrands.".format(cb_count))
        else:
            print("  [SKIP] User declined.")
            return False
    else:
        print("  (none to delete)")

    print("\n--- Step 2: BrandProviders (swap or delete) ---")
    bp_list = list(src_models.BrandProviders.objects.filter(brand_id=delete_id).select_related("provider"))
    print("  BrandProviders to process (brand_id={}): {}".format(delete_id, len(bp_list)))
    for bp in bp_list:
        existing = src_models.BrandProviders.objects.filter(brand_id=keep_id, provider=bp.provider).first()
        if existing:
            if _confirm("    Delete BrandProviders id={} (brand={}, provider={})? Duplicate exists for keep brand.".format(bp.id, brand_to_delete.name, bp.provider.kind_name)):
                bp.delete()
                print("    [OK] Deleted.")
        else:
            if _confirm("    Update BrandProviders id={} brand_id {} -> {}?".format(bp.id, delete_id, keep_id)):
                bp.brand_id = keep_id
                bp.save()
                print("    [OK] Updated.")

    print("\n--- Step 3: BrandTurn14BrandMapping (swap or delete) ---")
    t14_list = list(src_models.BrandTurn14BrandMapping.objects.filter(brand_id=delete_id).select_related("turn14_brand"))
    print("  BrandTurn14BrandMapping to process: {}".format(len(t14_list)))
    for m in t14_list:
        existing = src_models.BrandTurn14BrandMapping.objects.filter(brand_id=keep_id, turn14_brand=m.turn14_brand).first()
        if existing:
            if _confirm("    Delete mapping id={} (turn14_brand={})? Keep brand already has mapping.".format(m.id, m.turn14_brand.name)):
                m.delete()
                print("    [OK] Deleted.")
        else:
            if _confirm("    Update mapping id={} brand_id {} -> {} (turn14_brand={})?".format(m.id, delete_id, keep_id, m.turn14_brand.name)):
                m.brand_id = keep_id
                m.save()
                print("    [OK] Updated.")

    print("\n--- Step 4: BrandKeystoneBrandMapping (swap or delete) ---")
    ks_list = list(src_models.BrandKeystoneBrandMapping.objects.filter(brand_id=delete_id).select_related("keystone_brand"))
    print("  BrandKeystoneBrandMapping to process: {}".format(len(ks_list)))
    for m in ks_list:
        existing = src_models.BrandKeystoneBrandMapping.objects.filter(brand_id=keep_id, keystone_brand=m.keystone_brand).first()
        if existing:
            if _confirm("    Delete mapping id={} (keystone_brand={})? Keep brand already has mapping.".format(m.id, m.keystone_brand.name)):
                m.delete()
                print("    [OK] Deleted.")
        else:
            if _confirm("    Update mapping id={} brand_id {} -> {} (keystone_brand={})?".format(m.id, delete_id, keep_id, m.keystone_brand.name)):
                m.brand_id = keep_id
                m.save()
                print("    [OK] Updated.")

    print("\n--- Step 5: MasterPart (swap brand, handle duplicates) ---")
    mp_list = list(src_models.MasterPart.objects.filter(brand_id=delete_id).select_related("brand"))
    mp_count = len(mp_list)
    print("  MasterPart to process (brand_id={} -> {}): {}".format(delete_id, keep_id, mp_count))
    if mp_count:
        conflicts = 0
        for mp in mp_list:
            existing = src_models.MasterPart.objects.filter(brand_id=keep_id, part_number=mp.part_number).first()
            if existing:
                conflicts += 1
        if conflicts:
            print("  WARNING: {} conflicts (keep brand already has same part_number). Will merge/delete.".format(conflicts))
        if _confirm("  Confirm process {} MasterPart records ({} simple updates, {} merge/delete)?".format(mp_count, mp_count - conflicts, conflicts)):
            from django.db import transaction
            updated = 0
            merged = 0
            with transaction.atomic():
                for mp in mp_list:
                    existing = src_models.MasterPart.objects.filter(brand_id=keep_id, part_number=mp.part_number).first()
                    if existing:
                        # Keep brand already has this part_number: reassign ProviderParts, then delete duplicate
                        for pp in src_models.ProviderPart.objects.filter(master_part=mp).select_related("provider"):
                            keep_pp = src_models.ProviderPart.objects.filter(master_part=existing, provider=pp.provider).first()
                            if keep_pp:
                                pp.delete()  # duplicate provider_part, keep the one on existing
                            else:
                                pp.master_part = existing
                                pp.save()
                        mp.delete()
                        merged += 1
                    else:
                        mp.brand_id = keep_id
                        mp.save()
                        updated += 1
            print("  [OK] Updated {} MasterPart, merged/deleted {} duplicates.".format(updated, merged))
        else:
            print("  [SKIP] User declined.")
            return False
    else:
        print("  (none to update)")

    print("\n--- Step 6: CompanyDestinationParts (swap brand) ---")
    cdp_count = src_models.CompanyDestinationParts.objects.filter(brand_id=delete_id).count()
    print("  CompanyDestinationParts to update: {}".format(cdp_count))
    if cdp_count:
        if _confirm("  Confirm UPDATE {} CompanyDestinationParts?".format(cdp_count)):
            src_models.CompanyDestinationParts.objects.filter(brand_id=delete_id).update(brand_id=keep_id)
            print("  [OK] Updated {} CompanyDestinationParts.".format(cdp_count))
        else:
            print("  [SKIP] User declined.")
            return False
    else:
        print("  (none to update)")

    # BigCommerceBrands, BrandSDCBrandMapping if they exist
    bbc_count = src_models.BigCommerceBrands.objects.filter(brand_id=delete_id).count()
    if bbc_count:
        print("\n--- BigCommerceBrands to update: {} ---".format(bbc_count))
        if _confirm("  Confirm UPDATE {} BigCommerceBrands?".format(bbc_count)):
            src_models.BigCommerceBrands.objects.filter(brand_id=delete_id).update(brand_id=keep_id)
            print("  [OK] Updated.")

    sdc_count = src_models.BrandSDCBrandMapping.objects.filter(brand_id=delete_id).count()
    if sdc_count:
        print("\n--- BrandSDCBrandMapping to process: {} ---".format(sdc_count))
        sdc_list = list(src_models.BrandSDCBrandMapping.objects.filter(brand_id=delete_id).select_related("sdc_brand"))
        for m in sdc_list:
            existing = src_models.BrandSDCBrandMapping.objects.filter(brand_id=keep_id, sdc_brand=m.sdc_brand).first()
            if existing:
                if _confirm("    Delete BrandSDCBrandMapping id={}?".format(m.id)):
                    m.delete()
            else:
                if _confirm("    Update BrandSDCBrandMapping id={} brand_id -> {}?".format(m.id, keep_id)):
                    m.brand_id = keep_id
                    m.save()

    print("\n--- Step 7: Delete brand '{}' (id={}) ---".format(brand_to_delete.name, delete_id))
    if _confirm("  Confirm DELETE brand '{}' (id={})?".format(brand_to_delete.name, delete_id)):
        brand_to_delete.delete()
        print("  [OK] Brand deleted.")
        return True
    else:
        print("  [SKIP] User declined. Brand NOT deleted.")
        return False


def process_pair(display_name: str, names: list):
    """Process one duplicate pair."""
    print("\n" + "=" * 60)
    print("Processing: {} -> names: {}".format(display_name, names))
    print("=" * 60)

    brands_found = _find_brands_by_names(names)
    print("\n[1] Brands found: {} (need exactly 2)".format(len(brands_found)))
    for b in brands_found:
        src = _get_brand_provider_source(b)
        print("    - id={} name='{}' source={}".format(b.id, b.name, src or "none"))

    if len(brands_found) != 2:
        print("  [SKIP] Need exactly 2 brands. Found {}. Skipping pair.".format(len(brands_found)))
        return

    # Check one in Turn14, one in Keystone
    sources = [_get_brand_provider_source(b) for b in brands_found]
    has_t14 = "turn14" in sources
    has_ks = "keystone" in sources
    if not (has_t14 and has_ks):
        print("  [SKIP] Must have one brand from Turn14 and one from Keystone. Sources: {}. Skipping.".format(sources))
        return

    # Determine which is Turn14 and which is Keystone
    b_t14 = next((b for b in brands_found if _get_brand_provider_source(b) == "turn14"), None)
    b_ks = next((b for b in brands_found if _get_brand_provider_source(b) == "keystone"), None)

    print("\n[2] Turn14 brand: id={} name='{}'".format(b_t14.id, b_t14.name))
    print("    Keystone brand: id={} name='{}'".format(b_ks.id, b_ks.name))

    print("\n[3] Which brand to KEEP? (merge the other into this one)")
    print("    1 = {} (id={}) [Turn14]".format(b_t14.name, b_t14.id))
    print("    2 = {} (id={}) [Keystone]".format(b_ks.name, b_ks.id))
    choice = input("    Enter 1 or 2: ").strip()
    if choice == "1":
        brand_to_keep = b_t14
        brand_to_delete = b_ks
    elif choice == "2":
        brand_to_keep = b_ks
        brand_to_delete = b_t14
    else:
        print("  [SKIP] Invalid choice. Skipping pair.")
        return

    print("\n  KEEP: id={} name='{}'".format(brand_to_keep.id, brand_to_keep.name))
    print("  DELETE (merge into keep): id={} name='{}'".format(brand_to_delete.id, brand_to_delete.name))

    if not _confirm("\n  Confirm merge/delete for this pair?", default_no=True):
        print("  [SKIP] User declined.")
        return

    merge_brands(brand_to_keep, brand_to_delete)


def run():
    """Run the merge script for all pairs."""
    print("Duplicate brand merge script")
    print("Each pair will prompt for confirmation at each step.")
    print("")

    for display_name, names in DUPLICATE_PAIRS:
        try:
            process_pair(display_name, names)
        except Exception as e:
            print("\n[ERROR] Exception for {}: {}".format(display_name, e))
            import traceback
            traceback.print_exc()
            if not _confirm("  Continue to next pair?"):
                break

    print("\n" + "=" * 60)
    print("Done.")


# Run when pasted or executed
if __name__ == "__main__":
    import pathlib
    root = pathlib.Path(__file__).resolve().parent.parent
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
run()
