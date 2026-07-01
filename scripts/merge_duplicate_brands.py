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

**Batch merge from a known (keep_id, delete_id) list** (no per-row confirmation, still fully logged;
each pair runs in its own transaction so one bad pair can't affect the others)::

    python manage.py shell
    >>> import runpy
    >>> ns = runpy.run_path("scripts/merge_duplicate_brands.py", run_name="merge_loader")
    >>> pairs = [(3312, 4090), (2994, 5542)]  # (keep_id, delete_id)
    >>> ns["merge_batch"](pairs, dry_run=True)   # preview first
    >>> ns["merge_batch"](pairs)                 # then actually merge

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


def _auto_confirm(msg, default_no=True):
    """Non-interactive stand-in for ``_confirm`` used by batch mode: always proceeds, just logs."""
    print("{} [auto-yes]".format(msg))
    return True


_IN_CLAUSE_CHUNK = 2000


def _chunked(items, size=_IN_CLAUSE_CHUNK):
    items = list(items)
    for i in range(0, len(items), size):
        yield items[i : i + size]


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
    if src_models.BrandAtechBrandMapping.objects.filter(brand=brand).exists():
        out.append("atech")
    if src_models.BrandDlgBrandMapping.objects.filter(brand=brand).exists():
        out.append("dlg")
    return out


def merge_brands(brand_to_keep, brand_to_delete, confirm=_confirm):
    """Merge brand_to_delete into brand_to_keep, then delete brand_to_delete.

    ``confirm`` defaults to the interactive y/N prompt; pass ``_auto_confirm`` for batch mode
    (still prints every action, just doesn't block on input).
    """
    keep_id = brand_to_keep.id
    delete_id = brand_to_delete.id

    # 1. BrandWheelProsBrandMapping
    wp_list = list(src_models.BrandWheelProsBrandMapping.objects.filter(brand_id=delete_id).select_related("wheelpros_brand"))
    print("\n--- BrandWheelProsBrandMapping: {} to process ---".format(len(wp_list)))
    for m in wp_list:
        existing = src_models.BrandWheelProsBrandMapping.objects.filter(brand_id=keep_id, wheelpros_brand=m.wheelpros_brand).first()
        if existing:
            if confirm("  Delete mapping id={} (wheelpros_brand={})? Keep already has.".format(m.id, m.wheelpros_brand.name)):
                m.delete()
        else:
            if confirm("  Update mapping id={} brand_id {} -> {} (wheelpros_brand={})?".format(m.id, delete_id, keep_id, m.wheelpros_brand.name)):
                m.brand_id = keep_id
                m.save()

    # 2. BrandKeystoneBrandMapping
    ks_list = list(src_models.BrandKeystoneBrandMapping.objects.filter(brand_id=delete_id).select_related("keystone_brand"))
    print("\n--- BrandKeystoneBrandMapping: {} to process ---".format(len(ks_list)))
    for m in ks_list:
        existing = src_models.BrandKeystoneBrandMapping.objects.filter(brand_id=keep_id, keystone_brand=m.keystone_brand).first()
        if existing:
            if confirm("  Delete mapping id={} (keystone_brand={})? Keep already has.".format(m.id, m.keystone_brand.name)):
                m.delete()
        else:
            if confirm("  Update mapping id={} brand_id {} -> {} (keystone_brand={})?".format(m.id, delete_id, keep_id, m.keystone_brand.name)):
                m.brand_id = keep_id
                m.save()

    # 3. BrandTurn14BrandMapping
    t14_list = list(src_models.BrandTurn14BrandMapping.objects.filter(brand_id=delete_id).select_related("turn14_brand"))
    print("\n--- BrandTurn14BrandMapping: {} to process ---".format(len(t14_list)))
    for m in t14_list:
        existing = src_models.BrandTurn14BrandMapping.objects.filter(brand_id=keep_id, turn14_brand=m.turn14_brand).first()
        if existing:
            if confirm("  Delete mapping id={} (turn14_brand={})? Keep already has.".format(m.id, m.turn14_brand.name)):
                m.delete()
        else:
            if confirm("  Update mapping id={} brand_id {} -> {} (turn14_brand={})?".format(m.id, delete_id, keep_id, m.turn14_brand.name)):
                m.brand_id = keep_id
                m.save()

    # 4. BrandRoughCountryBrandMapping
    rc_list = list(src_models.BrandRoughCountryBrandMapping.objects.filter(brand_id=delete_id).select_related("rough_country_brand"))
    print("\n--- BrandRoughCountryBrandMapping: {} to process ---".format(len(rc_list)))
    for m in rc_list:
        existing = src_models.BrandRoughCountryBrandMapping.objects.filter(brand_id=keep_id, rough_country_brand=m.rough_country_brand).first()
        if existing:
            if confirm("  Delete mapping id={} (rough_country_brand={})? Keep already has.".format(m.id, m.rough_country_brand.name)):
                m.delete()
        else:
            if confirm("  Update mapping id={} brand_id {} -> {} (rough_country_brand={})?".format(m.id, delete_id, keep_id, m.rough_country_brand.name)):
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
            if confirm(
                "  Delete mapping id={} (meyer_brand={})? Keep already has.".format(m.id, m.meyer_brand.name)
            ):
                m.delete()
        else:
            if confirm(
                "  Update mapping id={} brand_id {} -> {} (meyer_brand={})?".format(
                    m.id, delete_id, keep_id, m.meyer_brand.name
                )
            ):
                m.brand_id = keep_id
                m.save()

    # 5a. BrandAtechBrandMapping
    atech_list = list(
        src_models.BrandAtechBrandMapping.objects.filter(brand_id=delete_id).select_related("atech_brand")
    )
    print("\n--- BrandAtechBrandMapping: {} to process ---".format(len(atech_list)))
    for m in atech_list:
        existing = src_models.BrandAtechBrandMapping.objects.filter(
            brand_id=keep_id, atech_brand=m.atech_brand
        ).first()
        if existing:
            if confirm(
                "  Delete mapping id={} (atech_brand={})? Keep already has.".format(m.id, m.atech_brand.name)
            ):
                m.delete()
        else:
            if confirm(
                "  Update mapping id={} brand_id {} -> {} (atech_brand={})?".format(
                    m.id, delete_id, keep_id, m.atech_brand.name
                )
            ):
                m.brand_id = keep_id
                m.save()

    # 5b. BrandDlgBrandMapping
    dlg_list = list(
        src_models.BrandDlgBrandMapping.objects.filter(brand_id=delete_id).select_related("dlg_brand")
    )
    print("\n--- BrandDlgBrandMapping: {} to process ---".format(len(dlg_list)))
    for m in dlg_list:
        existing = src_models.BrandDlgBrandMapping.objects.filter(
            brand_id=keep_id, dlg_brand=m.dlg_brand
        ).first()
        if existing:
            if confirm(
                "  Delete mapping id={} (dlg_brand={})? Keep already has.".format(m.id, m.dlg_brand.name)
            ):
                m.delete()
        else:
            if confirm(
                "  Update mapping id={} brand_id {} -> {} (dlg_brand={})?".format(
                    m.id, delete_id, keep_id, m.dlg_brand.name
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
            if confirm("  Delete BrandProviders id={} (provider={})? Keep already has.".format(bp.id, bp.provider.kind_name)):
                bp.delete()
        else:
            if confirm("  Update BrandProviders id={} brand_id {} -> {} (provider={})?".format(bp.id, delete_id, keep_id, bp.provider.kind_name)):
                bp.brand_id = keep_id
                bp.save()

    # 7. MasterPart (update brand_id, handle duplicates) - bulk, not per-row.
    mp_list = list(src_models.MasterPart.objects.filter(brand_id=delete_id).only("id", "part_number"))
    print("\n--- MasterPart: {} to process ---".format(len(mp_list)))
    if mp_list:
        part_numbers = [mp.part_number for mp in mp_list]
        keep_mp_by_pn = {}
        for chunk in _chunked(part_numbers):
            for keep_mp in src_models.MasterPart.objects.filter(
                brand_id=keep_id, part_number__in=chunk
            ).only("id", "part_number"):
                keep_mp_by_pn[keep_mp.part_number] = keep_mp

        conflict_mps = [mp for mp in mp_list if mp.part_number in keep_mp_by_pn]
        non_conflict_ids = [mp.id for mp in mp_list if mp.part_number not in keep_mp_by_pn]
        print("  ({} conflicts with keep brand, {} simple reassigns)".format(len(conflict_mps), len(non_conflict_ids)))

        if confirm("  Process {} MasterPart records?".format(len(mp_list))):
            from django.db import transaction

            with transaction.atomic():
                # Non-conflicting: just repoint brand_id, in batched bulk UPDATEs.
                for chunk in _chunked(non_conflict_ids):
                    src_models.MasterPart.objects.filter(id__in=chunk).update(brand_id=keep_id)

                if conflict_mps:
                    conflict_mp_ids = [mp.id for mp in conflict_mps]
                    mp_id_to_keep_mp_id = {mp.id: keep_mp_by_pn[mp.part_number].id for mp in conflict_mps}
                    keep_mp_ids = list({v for v in mp_id_to_keep_mp_id.values()})

                    delete_pps = []
                    for chunk in _chunked(conflict_mp_ids):
                        delete_pps.extend(
                            src_models.ProviderPart.objects.filter(master_part_id__in=chunk).only(
                                "id", "master_part_id", "provider_id"
                            )
                        )

                    existing_keep_pps = set()
                    for chunk in _chunked(keep_mp_ids):
                        existing_keep_pps.update(
                            src_models.ProviderPart.objects.filter(master_part_id__in=chunk).values_list(
                                "master_part_id", "provider_id"
                            )
                        )

                    reassign_objs = []
                    delete_pp_ids = []
                    for pp in delete_pps:
                        new_master_id = mp_id_to_keep_mp_id[pp.master_part_id]
                        if (new_master_id, pp.provider_id) in existing_keep_pps:
                            delete_pp_ids.append(pp.id)
                        else:
                            pp.master_part_id = new_master_id
                            reassign_objs.append(pp)

                    for chunk in _chunked(delete_pp_ids):
                        src_models.ProviderPart.objects.filter(id__in=chunk).delete()

                    if reassign_objs:
                        src_models.ProviderPart.objects.bulk_update(
                            reassign_objs, ["master_part_id"], batch_size=_IN_CLAUSE_CHUNK
                        )

                    for chunk in _chunked(conflict_mp_ids):
                        src_models.MasterPart.objects.filter(id__in=chunk).delete()

    # 8. CompanyDestinationParts
    cdp_count = src_models.CompanyDestinationParts.objects.filter(brand_id=delete_id).count()
    if cdp_count:
        if confirm("  Update {} CompanyDestinationParts brand_id {} -> {}?".format(cdp_count, delete_id, keep_id)):
            src_models.CompanyDestinationParts.objects.filter(brand_id=delete_id).update(brand_id=keep_id)

    # 9. CompanyBrands - delete for merge brand
    cb_count = src_models.CompanyBrands.objects.filter(brand_id=delete_id).count()
    print("\n--- CompanyBrands: {} to delete (brand_id={}) ---".format(cb_count, delete_id))
    if cb_count:
        if confirm("  Delete {} CompanyBrands for '{}'?".format(cb_count, brand_to_delete.name)):
            src_models.CompanyBrands.objects.filter(brand_id=delete_id).delete()

    # 10. BigCommerceBrands, BrandSDCBrandMapping if any
    bbc = src_models.BigCommerceBrands.objects.filter(brand_id=delete_id)
    if bbc.exists():
        if confirm("  Update {} BigCommerceBrands?".format(bbc.count())):
            bbc.update(brand_id=keep_id)
    sdc = src_models.BrandSDCBrandMapping.objects.filter(brand_id=delete_id)
    if sdc.exists():
        for m in sdc:
            ex = src_models.BrandSDCBrandMapping.objects.filter(brand_id=keep_id, sdc_brand=m.sdc_brand).first()
            if ex:
                if confirm("  Delete BrandSDCBrandMapping id={}?".format(m.id)):
                    m.delete()
            else:
                if confirm("  Update BrandSDCBrandMapping id={}?".format(m.id)):
                    m.brand_id = keep_id
                    m.save()

    # 11. Delete brand
    print("\n--- Delete brand '{}' (id={}) ---".format(brand_to_delete.name, delete_id))
    if confirm("  Confirm DELETE brand '{}'?".format(brand_to_delete.name)):
        brand_to_delete.delete()
        print("  [OK] Done.")
    else:
        print("  [SKIP] Brand NOT deleted.")


def merge_batch(pairs, dry_run=False):
    """
    Batch-merge an explicit list of ``(keep_id, delete_id)`` tuples, no per-row confirmation.

    Every action is still printed (via ``_auto_confirm``) so the log is a full audit trail.
    Pass ``dry_run=True`` to only print what *would* happen (fetches + prints ids/names, no merge_brands call).
    Skips a pair with a warning if either id doesn't exist, or if keep_id == delete_id.
    Wraps each pair in its own transaction so one bad pair can't corrupt others.
    """
    from django.db import transaction

    results = {"merged": [], "skipped": []}
    for keep_id, delete_id in pairs:
        if keep_id == delete_id:
            print("[SKIP] keep_id == delete_id == {}".format(keep_id))
            results["skipped"].append((keep_id, delete_id, "same id"))
            continue
        keep = src_models.Brands.objects.filter(pk=keep_id).first()
        delete = src_models.Brands.objects.filter(pk=delete_id).first()
        if not keep or not delete:
            print("[SKIP] pair ({}, {}): missing Brands row (keep={}, delete={})".format(
                keep_id, delete_id, bool(keep), bool(delete)
            ))
            results["skipped"].append((keep_id, delete_id, "missing brand row"))
            continue

        print("\n" + "#" * 60)
        print("KEEP: id={} name='{}'  <-  MERGE+DELETE: id={} name='{}'".format(
            keep.id, keep.name, delete.id, delete.name
        ))
        print("#" * 60)

        if dry_run:
            print("[DRY-RUN] Would merge id={} into id={}.".format(delete.id, keep.id))
            continue

        try:
            with transaction.atomic():
                merge_brands(keep, delete, confirm=_auto_confirm)
            results["merged"].append((keep_id, delete_id))
        except Exception as e:
            print("[ERROR] pair ({}, {}): {}".format(keep_id, delete_id, e))
            import traceback
            traceback.print_exc()
            results["skipped"].append((keep_id, delete_id, str(e)))

    print("\n" + "=" * 60)
    print("Batch done. Merged: {}, Skipped: {}".format(len(results["merged"]), len(results["skipped"])))
    if results["skipped"]:
        for keep_id, delete_id, reason in results["skipped"]:
            print("  SKIPPED ({}, {}): {}".format(keep_id, delete_id, reason))
    return results


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
