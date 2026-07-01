"""
Merge duplicate MasterPart rows created by the sku-overwrite flip-flop bug.

Background: before the Phase-2 sku-overwrite fix in master_parts.py, providers with a
brand-prefixed part number (WheelPros, sometimes Meyer/A-Tech/Rough Country/DLG) could fail
to match an already-existing MasterPart if another provider's sync had since overwritten its
sku away from the bridging value, creating a second MasterPart row for the same real part
(e.g. brand=BAK: part_number="448329" vs part_number="BAK448329", same sku "BAK448329").

This script finds every ``(brand_id, sku)`` group with more than one distinct ``part_number``
still in ``master_parts`` -- the exact fingerprint of that bug -- and merges each pair: keeps
the row with more ProviderPart links (falling back to shorter part_number, then lower id),
reassigns ProviderParts from the duplicate onto the canonical row (deleting the loser on a
per-provider conflict, keeping whichever has the more recent ``distributor_refreshed_at``),
backfills any blank description/image_url/aaia_code on the canonical row, then deletes the
duplicate MasterPart.

There is no ``manage.py`` subcommand; load this file from the Django shell.

**Preview all pairs (no changes)**::

    python manage.py shell
    >>> import runpy
    >>> ns = runpy.run_path("scripts/merge_flip_flop_master_parts.py", run_name="merge_loader")
    >>> pairs = ns["find_duplicate_pairs"]()
    >>> len(pairs)
    >>> ns["merge_batch"](pairs, dry_run=True)

**Merge everything**::

    >>> ns["merge_batch"](pairs)

**Merge one pair by id** (e.g. the BAK448329 example, keep the row with more links)::

    >>> from src import models as m
    >>> ns["merge_pair"](37424805, 72549853)

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


def find_duplicate_pairs():
    """
    Return a list of ``(id_a, id_b)`` MasterPart id pairs sharing ``(brand_id, sku)`` but with
    different ``part_number`` -- the flip-flop bug's fingerprint. Every group currently has
    exactly 2 rows (verified against production data), but this doesn't assume that: it emits
    all pairwise combinations within a group just in case.
    """
    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT brand_id, sku, array_agg(id ORDER BY id)
            FROM master_parts
            WHERE sku IS NOT NULL AND sku != ''
            GROUP BY brand_id, sku
            HAVING COUNT(DISTINCT part_number) > 1
            """
        )
        rows = cur.fetchall()

    pairs = []
    for brand_id, sku, ids in rows:
        for i in range(len(ids)):
            for j in range(i + 1, len(ids)):
                pairs.append((ids[i], ids[j]))
    return pairs


def _pick_canonical(mp_a, mp_b, pp_counts):
    """Prefer more ProviderPart links, then shorter part_number, then lower id."""
    ca = pp_counts.get(mp_a.id, 0)
    cb = pp_counts.get(mp_b.id, 0)
    if ca != cb:
        return (mp_a, mp_b) if ca > cb else (mp_b, mp_a)
    la = len(mp_a.part_number or "")
    lb = len(mp_b.part_number or "")
    if la != lb:
        return (mp_a, mp_b) if la < lb else (mp_b, mp_a)
    return (mp_a, mp_b) if mp_a.id < mp_b.id else (mp_b, mp_a)


def merge_pair(id_a, id_b, confirm=None):
    """
    Merge two MasterPart rows known to be duplicates of each other. Picks the canonical row
    automatically (more ProviderPart links wins), reassigns/reconciles ProviderParts, backfills
    blank descriptive fields, then deletes the loser. Prints every action; no confirmation
    prompt (this is meant to run in a batch after ``find_duplicate_pairs`` has already scoped
    the exact bug signature -- pass ``confirm`` if you want a gate anyway).
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
        # Backfill blank descriptive fields on keep from dup.
        changed = []
        for field in ("description", "image_url", "aaia_code"):
            if not getattr(keep, field) and getattr(dup, field):
                setattr(keep, field, getattr(dup, field))
                changed.append(field)
        if changed:
            keep.save(update_fields=changed)
            print("  Backfilled {} on keep from dup.".format(changed))

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

        dup.delete()
        print("  [OK] Deleted duplicate MasterPart id={}.".format(dup.id))

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
            print("[DRY-RUN] would KEEP id={} ('{}', sku='{}') <- MERGE id={} ('{}', sku='{}')".format(
                keep.id, keep.part_number, keep.sku, dup.id, dup.part_number, dup.sku
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
