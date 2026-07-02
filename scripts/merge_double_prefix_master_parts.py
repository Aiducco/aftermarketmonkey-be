"""
Merge duplicate MasterPart rows created by the Turn14 double-prefix sku bug.

Background: Turn14's own internal "part_number" field (stored as MasterPart.sku, see
``_ingest_turn14_items_for_mapped_brands`` in master_parts.py) sometimes has the brand/vendor
prefix baked in twice for certain brands -- a Turn14-side data defect, not something our
ingest generates (e.g. sku="FPEFPE-HSC-4-S" for mfr_part_number/part_number="FPE-HSC-4-S").
That garbled sku can never match another provider's correctly single-prefixed sku or
part_number for the same real part, so ingest creates a brand-new duplicate MasterPart
instead of finding the existing Turn14 row (e.g. Meyer creates part_number="-HSC-4-S",
sku="FPE-HSC-4-S" alongside it). This is fixed going forward by
``_collapse_doubled_sku_prefix`` in master_parts.py; this script cleans up the rows that
were already duplicated before that fix landed.

This script finds every MasterPart whose sku is exactly "<prefix><part_number>" where
part_number itself already starts with that same prefix (the doubled-prefix fingerprint),
then looks for a sibling row in the same brand whose part_number or sku equals the
collapsed (de-duplicated) value -- the row created by another provider that couldn't
match the garbled Turn14 row. Canonical selection prefers more ProviderPart links, then
the row whose part_number is NOT a plain suffix of the other's (i.e. keeps the fuller,
correctly-prefixed part_number rather than a provider's prefix-stripped convention), then
lower id. The kept row's sku is normalized to the collapsed value as part of the merge.
ProviderParts are reassigned from the duplicate onto the canonical row (deleting the loser
on a per-provider conflict, keeping whichever has the more recent ``distributor_refreshed_at``),
blank description/image_url/aaia_code are backfilled, then the duplicate MasterPart is deleted.

There is no ``manage.py`` subcommand; load this file from the Django shell.

**Preview all pairs (no changes)**::

    python manage.py shell
    >>> import runpy
    >>> ns = runpy.run_path("scripts/merge_double_prefix_master_parts.py", run_name="merge_loader")
    >>> pairs = ns["find_duplicate_pairs"]()
    >>> len(pairs)
    >>> ns["merge_batch"](pairs, dry_run=True)

**Merge everything**::

    >>> ns["merge_batch"](pairs)

**Merge one pair by id** (e.g. a FLEECE PERFORMANCE FPE-HSC-4-S example)::

    >>> ns["merge_pair"](37207520, 38283813)

Use ``run_name=...`` (not ``__main__``) so the script does not start the interactive runner.
"""
import os
import sys

if "django" not in sys.modules:
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "settings")
    import django
    django.setup()

from django.db import connection, transaction

from src import models as src_models

_IN_CLAUSE_CHUNK = 2000


def _chunked(items, size=_IN_CLAUSE_CHUNK):
    items = list(items)
    for i in range(0, len(items), size):
        yield items[i : i + size]


def _collapse_doubled_sku_prefix(sku, part_number):
    """Mirrors master_parts._collapse_doubled_sku_prefix; kept local so this script has no
    import-order dependency on the ingest module."""
    if not sku or not part_number or sku == part_number:
        return sku
    if not sku.upper().endswith(part_number.upper()):
        return sku
    prefix = sku[: len(sku) - len(part_number)]
    if prefix and part_number.upper().startswith(prefix.upper()):
        return part_number
    return sku


def find_duplicate_pairs():
    """
    Return a list of ``(id_doubled, id_clean)`` MasterPart id pairs matching the doubled-prefix
    bug's fingerprint: one row's sku is "<prefix><part_number>" where part_number already starts
    with that prefix, and a sibling row in the same brand has part_number or sku equal to the
    collapsed value.
    """
    with connection.cursor() as cur:
        cur.execute(
            r"""
            WITH doubled AS (
                SELECT mp.id, mp.brand_id, mp.part_number, mp.sku,
                       regexp_replace(mp.sku, '^([A-Za-z]{2,6})\1', E'\\1') AS collapsed_sku
                FROM master_parts mp
                WHERE mp.sku ~ '^([A-Za-z]{2,6})\1'
            )
            SELECT DISTINCT d.id, mp2.id
            FROM doubled d
            JOIN master_parts mp2
                ON mp2.brand_id = d.brand_id
                AND (mp2.sku = d.collapsed_sku OR mp2.part_number = d.collapsed_sku)
            WHERE mp2.id != d.id
            """
        )
        rows = cur.fetchall()
    return [(a, b) for a, b in rows]


def _pick_canonical(mp_a, mp_b, pp_counts):
    """
    Prefer more ProviderPart links. Tie-break: prefer the row whose part_number is NOT a plain
    suffix of the other's (i.e. keep the fuller, correctly brand-prefixed part_number rather than
    a provider's prefix-stripped convention, e.g. keep "FPE-HSC-4-S" over "-HSC-4-S"). Final
    tie-break: lower id.
    """
    ca = pp_counts.get(mp_a.id, 0)
    cb = pp_counts.get(mp_b.id, 0)
    if ca != cb:
        return (mp_a, mp_b) if ca > cb else (mp_b, mp_a)

    pn_a = (mp_a.part_number or "").upper()
    pn_b = (mp_b.part_number or "").upper()
    if pn_a != pn_b:
        a_is_suffix_of_b = pn_b.endswith(pn_a) and len(pn_a) < len(pn_b)
        b_is_suffix_of_a = pn_a.endswith(pn_b) and len(pn_b) < len(pn_a)
        if a_is_suffix_of_b and not b_is_suffix_of_a:
            return (mp_b, mp_a)
        if b_is_suffix_of_a and not a_is_suffix_of_b:
            return (mp_a, mp_b)

    return (mp_a, mp_b) if mp_a.id < mp_b.id else (mp_b, mp_a)


def merge_pair(id_a, id_b, confirm=None):
    """
    Merge two MasterPart rows known to be duplicates of each other under the double-prefix
    bug. Picks the canonical row automatically, normalizes its sku (collapsing any doubled
    prefix), reassigns/reconciles ProviderParts, backfills blank descriptive fields, then
    deletes the loser. Prints every action.
    """
    mp_a = src_models.MasterPart.objects.filter(pk=id_a).first()
    mp_b = src_models.MasterPart.objects.filter(pk=id_b).first()
    if not mp_a or not mp_b:
        print("[SKIP] pair ({}, {}): missing MasterPart row (a={}, b={})".format(id_a, id_b, bool(mp_a), bool(mp_b)))
        return False
    if mp_a.brand_id != mp_b.brand_id:
        print("[SKIP] pair ({}, {}): different brand_id ({} vs {}), not a real duplicate".format(
            id_a, id_b, mp_a.brand_id, mp_b.brand_id
        ))
        return False

    from django.db.models import Count
    pp_counts = dict(
        src_models.ProviderPart.objects.filter(master_part_id__in=[id_a, id_b])
        .values_list("master_part_id")
        .annotate(n=Count("id"))
        .values_list("master_part_id", "n")
    )

    keep, dup = _pick_canonical(mp_a, mp_b, pp_counts)
    print("\n{}".format("#" * 60))
    print("KEEP: id={} part_number='{}' sku='{}' ({} providers)".format(
        keep.id, keep.part_number, keep.sku, pp_counts.get(keep.id, 0)
    ))
    print("MERGE+DELETE: id={} part_number='{}' sku='{}' ({} providers)".format(
        dup.id, dup.part_number, dup.sku, pp_counts.get(dup.id, 0)
    ))
    print("#" * 60)

    if confirm and not confirm("  Proceed with this merge?"):
        print("  [SKIP] declined.")
        return False

    with transaction.atomic():
        # Normalize keep's sku, collapsing a doubled prefix if present.
        corrected_sku = _collapse_doubled_sku_prefix(keep.sku or "", keep.part_number or "")
        changed = []
        if corrected_sku != keep.sku:
            print("  Normalizing keep.sku: '{}' -> '{}'.".format(keep.sku, corrected_sku))
            keep.sku = corrected_sku
            changed.append("sku")

        # Backfill blank descriptive fields on keep from dup.
        for field in ("description", "image_url", "aaia_code"):
            if not getattr(keep, field) and getattr(dup, field):
                setattr(keep, field, getattr(dup, field))
                changed.append(field)
        if changed:
            keep.save(update_fields=changed)
            print("  Updated {} on keep.".format(changed))

        # Reassign ProviderParts, resolving per-provider conflicts by freshest distributor_refreshed_at.
        keep_pps = {
            pp.provider_id: pp
            for pp in src_models.ProviderPart.objects.filter(master_part_id=keep.id)
        }
        dup_pps = list(src_models.ProviderPart.objects.filter(master_part_id=dup.id))
        for pp in dup_pps:
            existing = keep_pps.get(pp.provider_id)
            if existing is None:
                pp.master_part_id = keep.id
                pp.save(update_fields=["master_part_id"])
                print("  Reassigned ProviderPart id={} (provider_id={}) -> keep.".format(pp.id, pp.provider_id))
            else:
                dup_ts = pp.distributor_refreshed_at
                keep_ts = existing.distributor_refreshed_at
                dup_is_fresher = (dup_ts is not None) and (keep_ts is None or dup_ts > keep_ts)
                if dup_is_fresher:
                    print("  ProviderPart conflict provider_id={}: dup (id={}) is fresher, keeping it, deleting keep's (id={}).".format(
                        pp.provider_id, pp.id, existing.id
                    ))
                    existing.delete()
                    pp.master_part_id = keep.id
                    pp.save(update_fields=["master_part_id"])
                else:
                    print("  ProviderPart conflict provider_id={}: keep's (id={}) is fresher/equal, deleting dup's (id={}).".format(
                        pp.provider_id, existing.id, pp.id
                    ))
                    pp.delete()

        remaining = src_models.ProviderPart.objects.filter(master_part_id=dup.id).count()
        if remaining:
            raise RuntimeError(
                "Refusing to delete MasterPart id={}: {} ProviderPart rows still attached.".format(dup.id, remaining)
            )

        dup_id = dup.id
        dup.delete()
        print("  [OK] Deleted duplicate MasterPart id={}.".format(dup_id))

    return True


def merge_batch(pairs, dry_run=False):
    """
    Batch-merge an explicit list of ``(id_a, id_b)`` MasterPart id pairs (order doesn't matter,
    the canonical row is picked automatically inside ``merge_pair``). Every action is printed.
    Pass ``dry_run=True`` to only print which row would be kept/merged, no writes.
    Each pair runs in its own transaction so one bad pair can't affect the others.
    """
    results = {"merged": [], "skipped": []}
    for id_a, id_b in pairs:
        if id_a == id_b:
            results["skipped"].append((id_a, id_b, "same id"))
            continue

        if dry_run:
            mp_a = src_models.MasterPart.objects.filter(pk=id_a).first()
            mp_b = src_models.MasterPart.objects.filter(pk=id_b).first()
            if not mp_a or not mp_b:
                print("[SKIP] pair ({}, {}): missing MasterPart row".format(id_a, id_b))
                results["skipped"].append((id_a, id_b, "missing row"))
                continue
            from django.db.models import Count
            pp_counts = dict(
                src_models.ProviderPart.objects.filter(master_part_id__in=[id_a, id_b])
                .values_list("master_part_id")
                .annotate(n=Count("id"))
                .values_list("master_part_id", "n")
            )
            keep, dup = _pick_canonical(mp_a, mp_b, pp_counts)
            corrected_sku = _collapse_doubled_sku_prefix(keep.sku or "", keep.part_number or "")
            print("[DRY-RUN] would KEEP id={} ('{}', sku='{}'{}) <- MERGE id={} ('{}', sku='{}')".format(
                keep.id, keep.part_number, keep.sku,
                " -> '{}'".format(corrected_sku) if corrected_sku != keep.sku else "",
                dup.id, dup.part_number, dup.sku,
            ))
            continue

        try:
            ok = merge_pair(id_a, id_b)
            if ok:
                results["merged"].append((id_a, id_b))
            else:
                results["skipped"].append((id_a, id_b, "merge_pair returned False"))
        except Exception as e:
            print("[ERROR] pair ({}, {}): {}".format(id_a, id_b, e))
            import traceback
            traceback.print_exc()
            results["skipped"].append((id_a, id_b, str(e)))

    print("\n" + "=" * 60)
    print("Batch done. Merged: {}, Skipped: {}".format(len(results["merged"]), len(results["skipped"])))
    if results["skipped"]:
        for id_a, id_b, reason in results["skipped"]:
            print("  SKIPPED ({}, {}): {}".format(id_a, id_b, reason))
    return results
