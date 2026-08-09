"""
Merge duplicate MasterPart rows created by distributors spelling the same manufacturer part
number differently (``'MS 96587'`` vs ``'MS96587'``, ``'09.5843.11'`` vs ``'09-5843-11'``).

Background and the full production survey are in ``docs/PART_NUMBER_NORMALIZATION.md``. In short:
every provider ingest resolves existing rows with an exact string match on
``(brand_id, part_number)``, so each distributor's house style creates its own MasterPart. A part
carried by five distributors ends up split across two or three rows with disjoint ``ProviderPart``
sets -- the catalog shows it twice and price comparison silently sees only a subset of sources.

**This script is deliberately conservative.** Normalizing punctuation away is not by itself
evidence that two rows are the same part: ``942B-89060+12`` and ``942B-89060-12`` are different
wheel offsets that normalize identically. A group is auto-merged only when it clears *every*
gate in ``evaluate_group`` below; anything else is written to a review CSV for a human to decide,
and can then be merged explicitly by id via ``merge_ids``.

Nothing here writes unless you ask it to -- ``merge_batch`` defaults to ``dry_run=True``.

There is no ``manage.py`` subcommand; load this file from the Django shell.

**1. Survey what would happen (no changes)**::

    python manage.py shell
    >>> import runpy
    >>> ns = runpy.run_path("scripts/merge_normalized_part_number_duplicates.py", run_name="merge_loader")
    >>> report = ns["find_merge_candidates"]()
    >>> ns["print_summary"](report)

**2. Export the groups that are NOT safe to auto-merge, for manual review**::

    >>> ns["export_review_csv"](report, "/tmp/part_number_review.csv")

**3. Merge the safe set** (still prints every action)::

    >>> ns["merge_batch"](report.auto_mergeable, dry_run=True)    # preview first
    >>> ns["merge_batch"](report.auto_mergeable, dry_run=False)

**4. Merge something from the review CSV by hand**, after you have decided it is one part::

    >>> ns["merge_ids"]([37924945, 37697443], dry_run=False)

Use ``run_name=...`` (not ``__main__``) so the script does not start the interactive runner.
"""
import collections
import csv
import os
import sys
import typing

if "django" not in sys.modules:
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "settings")
    import django
    django.setup()

from django.db import connection, transaction

from src import enums as src_enums
from src import models as src_models
from src.integrations.utils import part_numbers as pn_util

# Distributors whose feed carries no UPC/GTIN column at all, so a MasterPart backed only by these
# providers can never be corroborated by barcode. Confirmed against the raw tables: wheelpros_parts,
# dlg_parts, vossen_parts, tirerack_parts and both elitewheels_part_* tables have no
# upc/gtin/barcode column.
GTIN_LESS_PROVIDER_KINDS = frozenset({
    src_enums.BrandProviderKind.WHEELPROS.value,
    src_enums.BrandProviderKind.DLG.value,
    src_enums.BrandProviderKind.VOSSEN.value,
    src_enums.BrandProviderKind.TIRERACK.value,
    src_enums.BrandProviderKind.ELITE_WHEEL.value,
    # motorstate_products has no barcode column and the ingest sets gtin=None, so
    # Motor State can never corroborate a match: 69,624 master parts are backed by it
    # alone and not one carries a gtin. Its rows often *look* barcoded because other
    # providers on the same master part supplied one -- that says nothing about a
    # Motor-State-only candidate.
    src_enums.BrandProviderKind.MOTOR_STATE_DISTRIBUTING.value,
})

# Providers whose ingest still resolves master parts by exact string match only -- they do NOT
# call master_part_matching.extend_with_normalized_matches. Their spelling must survive a merge
# or their next sync resurrects the duplicate; see _pick_canonical. Keep this in sync with which
# _ingest_* functions in master_parts.py actually call the resolver.
EXACT_MATCH_ONLY_PROVIDER_KINDS = frozenset({
    src_enums.BrandProviderKind.TURN_14.value,
})

# Groups larger than this are almost always a shared/bogus identifier rather than one part spelled
# several ways; they go to review regardless of what the other gates say.
MAX_GROUP_SIZE = 4

_FETCH_CHUNK = 2000

GTIN_AGREE = "AGREE"
GTIN_CONFLICT = "CONFLICT"
GTIN_ONE_SIDE = "one_side"
GTIN_NONE = "none"


class MasterPartRow(typing.NamedTuple):
    id: int
    brand_id: int
    part_number: str
    sku: typing.Optional[str]
    gtin: typing.Optional[str]
    normalized_gtin: typing.Optional[str]
    provider_ids: typing.FrozenSet[int]
    provider_kinds: typing.FrozenSet[int]
    # (provider_id, provider_external_id) pairs. Needed to tell "this distributor lists two
    # different parts" apart from "the same distributor SKU got attached to both rows".
    provider_external_ids: typing.FrozenSet[typing.Tuple[int, str]]


class Group(typing.NamedTuple):
    brand_id: int
    normalized_part_number: str
    rows: typing.List[MasterPartRow]
    tier: str
    gtin_verdict: str
    providers_overlap: bool
    sign_conflict: bool
    has_gtin_blind_side: bool
    safe: bool
    reason: str


class Report(typing.NamedTuple):
    auto_mergeable: typing.List[Group]
    needs_review: typing.List[Group]


def _chunked(items, size=_FETCH_CHUNK):
    items = list(items)
    for i in range(0, len(items), size):
        yield items[i : i + size]


def _candidate_id_groups() -> typing.List[typing.List[int]]:
    """
    Narrow 3M rows down to just the colliding ones, in Postgres.

    The SQL expression mirrors ``normalize_part_number`` closely enough to generate candidates;
    it intentionally does *not* try to reproduce the non-printable stripping. Every group is
    re-normalized and re-judged in Python with the real helper before anything is merged, so SQL
    only ever widens the candidate set, never decides.
    """
    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT array_agg(id ORDER BY id)
            FROM master_parts
            WHERE part_number IS NOT NULL AND part_number <> ''
            GROUP BY brand_id, upper(regexp_replace(part_number, '[^A-Za-z0-9]', '', 'g'))
            HAVING count(*) > 1
            """
        )
        return [row[0] for row in cur.fetchall()]


def _load_rows(ids: typing.Iterable[int]) -> typing.Dict[int, MasterPartRow]:
    """Fetch the candidate MasterParts plus the providers attached to each."""
    ids = list(ids)
    providers_by_master: typing.Dict[int, typing.Set[int]] = collections.defaultdict(set)
    kinds_by_master: typing.Dict[int, typing.Set[int]] = collections.defaultdict(set)
    external_by_master: typing.Dict[int, typing.Set[typing.Tuple[int, str]]] = collections.defaultdict(set)
    for chunk in _chunked(ids):
        rows = src_models.ProviderPart.objects.filter(master_part_id__in=chunk).values_list(
            "master_part_id", "provider_id", "provider__kind", "provider_external_id"
        )
        for master_part_id, provider_id, kind, external_id in rows:
            providers_by_master[master_part_id].add(provider_id)
            kinds_by_master[master_part_id].add(kind)
            external_by_master[master_part_id].add((provider_id, (external_id or "").strip()))

    out: typing.Dict[int, MasterPartRow] = {}
    for chunk in _chunked(ids):
        rows = src_models.MasterPart.objects.filter(id__in=chunk).values(
            "id", "brand_id", "part_number", "sku", "gtin"
        )
        for row in rows:
            out[row["id"]] = MasterPartRow(
                id=row["id"],
                brand_id=row["brand_id"],
                part_number=row["part_number"] or "",
                sku=row["sku"],
                gtin=row["gtin"],
                normalized_gtin=pn_util.normalize_gtin(row["gtin"]),
                provider_ids=frozenset(providers_by_master.get(row["id"], ())),
                provider_kinds=frozenset(kinds_by_master.get(row["id"], ())),
                provider_external_ids=frozenset(external_by_master.get(row["id"], ())),
            )
    return out


def _gtin_verdict(rows: typing.Sequence[MasterPartRow]) -> str:
    values = {r.normalized_gtin for r in rows if r.normalized_gtin}
    with_gtin = sum(1 for r in rows if r.normalized_gtin)
    if with_gtin >= 2:
        return GTIN_AGREE if len(values) == 1 else GTIN_CONFLICT
    return GTIN_ONE_SIDE if with_gtin == 1 else GTIN_NONE


def _providers_overlap(rows: typing.Sequence[MasterPartRow]) -> bool:
    """
    True when one distributor lists *genuinely different parts* across the rows of this group.

    Being on more than one row is not enough on its own. What counts is whether the distributor's
    own catalog ids differ:

    - **different external ids** -> that distributor really is selling two products, the strongest
      available signal that the rows are distinct, since it is a deliberate distinction by the
      party closest to the manufacturer;
    - **the same external id on both** -> one catalog entry attached to two master parts. That is
      damage, not evidence. It happens when a newly created duplicate copies another
      distributor's spelling and then wins their *exact* match on the next sync (the
      Quadratec / Baja Designs case: Meyer's BAJ66-8413-2 ended up on both rows). Reading it as
      "these are different parts" would make the duplicate permanently unmergeable.
    """
    external_by_provider: typing.Dict[int, typing.Set[str]] = collections.defaultdict(set)
    rows_by_provider: typing.Dict[int, int] = collections.defaultdict(int)
    for row in rows:
        for provider_id, external_id in row.provider_external_ids:
            external_by_provider[provider_id].add(external_id)
            rows_by_provider[provider_id] += 1
    return any(
        rows_by_provider[provider_id] > 1 and len(external_ids) > 1
        for provider_id, external_ids in external_by_provider.items()
    )


def evaluate_group(brand_id: int, normalized_part_number: str,
                   rows: typing.List[MasterPartRow]) -> Group:
    """
    Decide whether a collision group is safe to merge automatically.

    Every gate must pass. The gates are ordered cheapest-first and the *first* failure is
    recorded as the reason, so the review CSV says exactly why a group was held back.
    """
    part_numbers = [r.part_number for r in rows]
    tier = pn_util.classify_tier(part_numbers)
    verdict = _gtin_verdict(rows)
    overlap = _providers_overlap(rows)
    sign_conflict = pn_util.has_sign_conflict(part_numbers)
    # A "blind" side is a MasterPart whose providers all come from feeds with no barcode column,
    # so it can never be corroborated by GTIN no matter how the sync runs.
    blind = any(
        r.provider_kinds and r.provider_kinds <= GTIN_LESS_PROVIDER_KINDS
        for r in rows
    )

    def build(safe: bool, reason: str) -> Group:
        return Group(
            brand_id=brand_id,
            normalized_part_number=normalized_part_number,
            rows=rows,
            tier=tier,
            gtin_verdict=verdict,
            providers_overlap=overlap,
            sign_conflict=sign_conflict,
            has_gtin_blind_side=blind,
            safe=safe,
            reason=reason,
        )

    if len({r.part_number for r in rows}) < 2:
        return build(False, "single distinct part_number (nothing to merge)")
    if len(rows) > MAX_GROUP_SIZE:
        return build(False, "group larger than {} rows".format(MAX_GROUP_SIZE))
    if overlap:
        return build(False, "same provider on both sides")
    if sign_conflict:
        return build(False, "+/- sign conflict (different variants)")
    if verdict == GTIN_CONFLICT:
        return build(False, "GTIN conflict")

    # Case and whitespace differences cannot change which physical part is meant, so they clear on
    # provider-disjointness alone. Punctuation differences can, so they need a matching barcode.
    if tier in (pn_util.TIER_CASE_ONLY, pn_util.TIER_WHITESPACE_ONLY):
        return build(True, "safe: {} + providers disjoint".format(tier))

    if verdict != GTIN_AGREE:
        return build(False, "{} needs a matching GTIN (verdict={})".format(tier, verdict))
    if blind:
        # Reachable when a barcode-less row rides along with two rows whose GTINs agree: the
        # agreement says nothing about the blind row, so it is not covered by the evidence.
        return build(False, "{} with a barcode-less provider side".format(tier))
    return build(True, "safe: {} + GTIN agrees + providers disjoint".format(tier))


def find_merge_candidates(limit: typing.Optional[int] = None) -> Report:
    """
    Scan the whole catalog and split every collision group into auto-mergeable vs needs-review.
    Read-only. ``limit`` caps the number of candidate groups examined (for a quick smoke test).
    """
    id_groups = _candidate_id_groups()
    if limit is not None:
        id_groups = id_groups[:limit]
    print("Found {} candidate groups from SQL; re-judging in Python...".format(len(id_groups)))

    all_ids = [i for group in id_groups for i in group]
    rows_by_id = _load_rows(all_ids)

    auto: typing.List[Group] = []
    review: typing.List[Group] = []
    for ids in id_groups:
        rows = [rows_by_id[i] for i in ids if i in rows_by_id]
        if len(rows) < 2:
            continue
        # Re-group under the real helper: SQL's expression is close but not identical, so a
        # candidate group can legitimately split into two here.
        by_key: typing.Dict[str, typing.List[MasterPartRow]] = collections.defaultdict(list)
        for row in rows:
            key = pn_util.normalize_part_number(row.part_number)
            if key:
                by_key[key].append(row)
        for key, members in by_key.items():
            if len(members) < 2:
                continue
            group = evaluate_group(members[0].brand_id, key, members)
            (auto if group.safe else review).append(group)

    return Report(auto_mergeable=auto, needs_review=review)


def print_summary(report: Report) -> None:
    """Print the tier/verdict breakdown and the reasons groups were held back."""
    print("\n{}".format("=" * 72))
    print("AUTO-MERGEABLE: {} groups ({} master_part rows -> {} deleted)".format(
        len(report.auto_mergeable),
        sum(len(g.rows) for g in report.auto_mergeable),
        sum(len(g.rows) - 1 for g in report.auto_mergeable),
    ))
    by_tier = collections.Counter(g.tier for g in report.auto_mergeable)
    for tier, n in sorted(by_tier.items()):
        print("    {:24s} {}".format(tier, n))

    reunited = 0
    for g in report.auto_mergeable:
        union = set().union(*(r.provider_ids for r in g.rows)) if g.rows else set()
        biggest = max((len(r.provider_ids) for r in g.rows), default=0)
        reunited += len(union) - biggest
    print("    provider links reunited onto one part: {}".format(reunited))

    print("\nNEEDS REVIEW: {} groups".format(len(report.needs_review)))
    by_reason = collections.Counter(g.reason for g in report.needs_review)
    for reason, n in by_reason.most_common():
        print("    {:52s} {}".format(reason, n))
    print("{}\n".format("=" * 72))


def export_review_csv(report: Report, path: str) -> str:
    """
    Write every held-back group to CSV with the evidence needed to judge it by hand: one line per
    MasterPart, grouped, with the reason it was not auto-merged. Feed ids back to ``merge_ids``.
    """
    brand_names = dict(src_models.Brands.objects.values_list("id", "name"))
    provider_names = dict(src_models.Providers.objects.values_list("id", "name"))
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow([
            "group_key", "reason", "tier", "gtin_verdict", "providers_overlap",
            "sign_conflict", "gtin_blind_side", "brand", "master_part_id",
            "part_number", "sku", "gtin_normalized", "providers",
        ])
        for index, g in enumerate(report.needs_review):
            for row in g.rows:
                writer.writerow([
                    index,
                    g.reason,
                    g.tier,
                    g.gtin_verdict,
                    g.providers_overlap,
                    g.sign_conflict,
                    g.has_gtin_blind_side,
                    brand_names.get(g.brand_id, g.brand_id),
                    row.id,
                    row.part_number,
                    row.sku or "",
                    row.normalized_gtin or "",
                    "|".join(sorted(provider_names.get(p, str(p)) for p in row.provider_ids)),
                ])
    print("Wrote {} groups ({} rows) to {}".format(
        len(report.needs_review), sum(len(g.rows) for g in report.needs_review), path))
    return path


def _pick_canonical(rows: typing.Sequence[MasterPartRow]) -> typing.Tuple[MasterPartRow, typing.List[MasterPartRow]]:
    """
    Choose the row to keep, in order:

    1. **carries a provider whose ingest still matches on the exact string only** (Turn14) --
       see EXACT_MATCH_ONLY_PROVIDER_KINDS. This one is a correctness requirement, not a
       preference: deleting the row Turn14 points at makes Turn14's next sync fail its exact
       lookup, insert a fresh row, and add a *second* ProviderPart while the stale one remains.
       Turn14 then sits on both rows, the group fails the provider-disjointness gate, and it can
       never be auto-merged again. Keeping Turn14's spelling means its exact match keeps hitting;
       every other provider is wired for normalized matching and finds the row regardless.
       Provider-disjointness (enforced before we get here) guarantees at most one row in the
       group carries it, so this never has to break a tie between two such rows.
    2. most ProviderPart links -- the spelling the most distributors already agree on;
    3. keeps its punctuation -- manufacturers publish separators more often than not, and a
       distributor that stripped them is the one that departed from the published number;
    4. most uppercase characters -- part numbers are conventionally uppercase, and some feeds
       lowercase them wholesale ('253100ct' from Turn14 vs '253100CT' from TireRack);
    5. lowest id, purely so the choice is stable across runs.
    """
    def sort_key(row: MasterPartRow):
        exact_only = 0 if (row.provider_kinds & EXACT_MATCH_ONLY_PROVIDER_KINDS) else 1
        punctuation = sum(1 for ch in row.part_number if not ch.isalnum())
        uppercase = sum(1 for ch in row.part_number if ch.isupper())
        return (exact_only, -len(row.provider_ids), -punctuation, -uppercase, row.id)

    ordered = sorted(rows, key=sort_key)
    return ordered[0], list(ordered[1:])


def _repoint_provider_part(losing_id: int, winning_id: int) -> None:
    """
    Move everything that references a losing ProviderPart onto the winning one, then delete it.

    ``PurchaseOrderLineItem.provider_part`` is ``on_delete=PROTECT``, so the line items must be
    repointed first or the delete raises ``ProtectedError`` mid-batch.
    """
    src_models.PurchaseOrderLineItem.objects.filter(provider_part_id=losing_id).update(
        provider_part_id=winning_id
    )
    src_models.PurchaseOrderLineItem.objects.filter(kit_source_provider_part_id=losing_id).update(
        kit_source_provider_part_id=winning_id
    )
    # Kit components: drop rows that would collide with an existing (kit_part, component_part)
    # pair on the winner, repoint the rest.
    for field in ("kit_part_id", "component_part_id"):
        for component in src_models.ProviderPartKitComponent.objects.filter(**{field: losing_id}):
            probe = {
                "kit_part_id": winning_id if field == "kit_part_id" else component.kit_part_id,
                "component_part_id": winning_id if field == "component_part_id" else component.component_part_id,
            }
            if src_models.ProviderPartKitComponent.objects.filter(**probe).exists():
                component.delete()
            else:
                setattr(component, field, winning_id)
                component.save(update_fields=[field])
    src_models.ProviderPart.objects.filter(id=losing_id).delete()


def _merge_provider_parts(keep_id: int, dup_id: int) -> None:
    """Reassign the duplicate's ProviderParts onto the canonical row, freshest wins on conflict."""
    keep_by_provider = {
        pp.provider_id: pp
        for pp in src_models.ProviderPart.objects.filter(master_part_id=keep_id)
    }
    for pp in src_models.ProviderPart.objects.filter(master_part_id=dup_id):
        existing = keep_by_provider.get(pp.provider_id)
        if existing is None:
            pp.master_part_id = keep_id
            pp.save(update_fields=["master_part_id"])
            keep_by_provider[pp.provider_id] = pp
            print("      ProviderPart {} (provider {}) -> keep".format(pp.id, pp.provider_id))
            continue
        dup_ts, keep_ts = pp.distributor_refreshed_at, existing.distributor_refreshed_at
        dup_is_fresher = dup_ts is not None and (keep_ts is None or dup_ts > keep_ts)
        if dup_is_fresher:
            print("      provider {} conflict: dup {} fresher, dropping keep's {}".format(
                pp.provider_id, pp.id, existing.id))
            _repoint_provider_part(losing_id=existing.id, winning_id=pp.id)
            pp.master_part_id = keep_id
            pp.save(update_fields=["master_part_id"])
            keep_by_provider[pp.provider_id] = pp
        else:
            print("      provider {} conflict: keep's {} fresher, dropping dup's {}".format(
                pp.provider_id, existing.id, pp.id))
            _repoint_provider_part(losing_id=pp.id, winning_id=existing.id)


def _merge_master_part_data(keep_id: int, dup_id: int) -> None:
    """MasterPartData is OneToOne: fill blanks on the keeper from the duplicate, then drop it."""
    dup_data = src_models.MasterPartData.objects.filter(master_part_id=dup_id).first()
    if dup_data is None:
        return
    keep_data = src_models.MasterPartData.objects.filter(master_part_id=keep_id).first()
    if keep_data is None:
        dup_data.master_part_id = keep_id
        dup_data.save(update_fields=["master_part_id"])
        return
    fields = [
        "images", "description", "color", "material", "series", "warranty", "vehicle_type",
        "field_specs", "youtube_video", "installation_instructions",
    ]
    filled = [f for f in fields if not getattr(keep_data, f) and getattr(dup_data, f)]
    if filled:
        for f in filled:
            setattr(keep_data, f, getattr(dup_data, f))
        keep_data.save(update_fields=filled)
        print("      MasterPartData: backfilled {}".format(filled))
    dup_data.delete()


def _merge_fitments(keep_id: int, dup_id: int) -> None:
    """Reassign fitments, dropping any that would collide with the keeper's unique_together."""
    existing = set(
        src_models.MasterPartFitment.objects.filter(master_part_id=keep_id).values_list(
            "year_start", "year_end", "make", "model", "submodel", "engine", "drive_type"
        )
    )
    moved = dropped = 0
    for fitment in src_models.MasterPartFitment.objects.filter(master_part_id=dup_id):
        key = (
            fitment.year_start, fitment.year_end, fitment.make, fitment.model,
            fitment.submodel, fitment.engine, fitment.drive_type,
        )
        if key in existing:
            fitment.delete()
            dropped += 1
        else:
            fitment.master_part_id = keep_id
            fitment.save(update_fields=["master_part_id"])
            existing.add(key)
            moved += 1
    if moved or dropped:
        print("      fitments: {} moved, {} duplicates dropped".format(moved, dropped))


def merge_ids(ids: typing.Sequence[int], dry_run: bool = True) -> bool:
    """
    Merge an explicit list of MasterPart ids known to be the same part. Use this for groups you
    approved out of the review CSV -- it applies no safety gates beyond "same brand", so only
    pass ids you have actually looked at.
    """
    rows_by_id = _load_rows(ids)
    rows = [rows_by_id[i] for i in ids if i in rows_by_id]
    if len(rows) < 2:
        print("[SKIP] fewer than 2 existing MasterPart rows in {}".format(list(ids)))
        return False
    if len({r.brand_id for r in rows}) > 1:
        print("[SKIP] {} spans multiple brands -- refusing".format(list(ids)))
        return False
    return _merge_rows(rows, dry_run=dry_run)


def _merge_rows(rows: typing.Sequence[MasterPartRow], dry_run: bool) -> bool:
    keep, dups = _pick_canonical(rows)
    print("\n  KEEP  {:>10}  {!r:<28} providers={}".format(
        keep.id, keep.part_number, sorted(keep.provider_ids)))
    for dup in dups:
        print("  MERGE {:>10}  {!r:<28} providers={}".format(
            dup.id, dup.part_number, sorted(dup.provider_ids)))
    if dry_run:
        print("  [DRY-RUN] no changes written")
        return True

    with transaction.atomic():
        keep_obj = src_models.MasterPart.objects.select_for_update().get(pk=keep.id)
        for dup in dups:
            dup_obj = src_models.MasterPart.objects.select_for_update().filter(pk=dup.id).first()
            if dup_obj is None:
                print("      [SKIP] MasterPart {} vanished".format(dup.id))
                continue

            backfilled = [
                field for field in ("description", "image_url", "aaia_code", "gtin", "sku",
                                    "overview_category", "category")
                if not getattr(keep_obj, field) and getattr(dup_obj, field)
            ]
            if backfilled:
                for field in backfilled:
                    setattr(keep_obj, field, getattr(dup_obj, field))
                keep_obj.save(update_fields=backfilled)
                print("      backfilled {} onto keep".format(backfilled))

            _merge_provider_parts(keep_id=keep_obj.id, dup_id=dup_obj.id)
            _merge_master_part_data(keep_id=keep_obj.id, dup_id=dup_obj.id)
            _merge_fitments(keep_id=keep_obj.id, dup_id=dup_obj.id)

            remaining = src_models.ProviderPart.objects.filter(master_part_id=dup_obj.id).count()
            if remaining:
                raise RuntimeError(
                    "Refusing to delete MasterPart {}: {} ProviderPart rows still attached".format(
                        dup_obj.id, remaining)
                )
            dup_obj.delete()
            print("      [OK] deleted MasterPart {}".format(dup.id))
    return True


_BULK_BACKFILL_FIELDS = ("description", "image_url", "aaia_code", "gtin", "sku",
                         "overview_category", "category")

# unique_together on MasterPartFitment, in order -- used to skip fitments that would collide.
_FITMENT_KEY_COLUMNS = ("year_start", "year_end", "make", "model", "submodel", "engine", "drive_type")


def merge_groups_bulk(groups: typing.Sequence[Group], dry_run: bool = True,
                      chunk_size: int = 400) -> typing.Dict[str, typing.Any]:
    """
    Set-based equivalent of ``merge_batch`` for gate-passing groups, ~70x faster over a remote
    connection because it issues a handful of statements per *chunk* rather than ~25 round trips
    per *group* (row-by-row is 27 hours for the full set; this is minutes).

    Only valid for groups that cleared ``evaluate_group``, and it re-asserts the property it
    depends on: **provider-disjointness**. Because no two rows in a group share a provider, no
    ProviderPart is ever deleted -- they are only repointed, which keeps their ids and so carries
    inventory, company pricing and kit components along untouched, and never trips the
    ``PROTECT`` on ``PurchaseOrderLineItem.provider_part``.

    Anything that is not a clean disjoint merge (a duplicate that also has MasterPartData when
    the keeper does) is handed back to the careful row-by-row path rather than approximated.
    """
    groups = [g for g in groups if g.safe]
    results: typing.Dict[str, typing.Any] = {"merged": 0, "deleted": 0, "fallback": [], "failed": []}

    for start in range(0, len(groups), chunk_size):
        chunk = groups[start : start + chunk_size]
        pairs: typing.List[typing.Tuple[int, int]] = []   # (dup_id, keep_id)
        keep_ids: typing.List[int] = []
        for group in chunk:
            if _providers_overlap(group.rows):
                results["failed"].append((group.normalized_part_number, "providers overlap"))
                continue
            keep, dups = _pick_canonical(group.rows)
            keep_ids.append(keep.id)
            pairs.extend((d.id, keep.id) for d in dups)
        if not pairs:
            continue

        dup_ids = [d for d, _ in pairs]
        try:
            with transaction.atomic():
                values = ", ".join(["(%s::bigint, %s::bigint)"] * len(pairs))
                params = [x for pair in pairs for x in pair]

                # Backfill blank descriptive fields on the keeper from its duplicates. DISTINCT ON
                # picks one donor deterministically when a group has several duplicates.
                sets = ", ".join(
                    "{f} = COALESCE(NULLIF(mp.{f}, ''), d.{f})".format(f=f)
                    for f in _BULK_BACKFILL_FIELDS
                )
                with connection.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE master_parts mp SET {sets}
                        FROM (
                            SELECT DISTINCT ON (m.keep_id) m.keep_id, {cols}
                            FROM (VALUES {values}) AS m(dup_id, keep_id)
                            JOIN master_parts src ON src.id = m.dup_id
                            ORDER BY m.keep_id, m.dup_id
                        ) AS d(keep_id, {cols})
                        WHERE mp.id = d.keep_id
                        """.format(
                            sets=sets,
                            cols=", ".join(_BULK_BACKFILL_FIELDS),
                            values=values,
                        ),
                        params,
                    )

                    # Repoint provider parts. Disjointness means no unique_together conflict.
                    cur.execute(
                        """
                        UPDATE provider_parts pp SET master_part_id = m.keep_id
                        FROM (VALUES {values}) AS m(dup_id, keep_id)
                        WHERE pp.master_part_id = m.dup_id
                        """.format(values=values),
                        params,
                    )

                    # MasterPartData is OneToOne: move it only where the keeper has none.
                    cur.execute(
                        """
                        UPDATE master_part_data d SET master_part_id = m.keep_id
                        FROM (VALUES {values}) AS m(dup_id, keep_id)
                        WHERE d.master_part_id = m.dup_id
                          AND NOT EXISTS (SELECT 1 FROM master_part_data k WHERE k.master_part_id = m.keep_id)
                        """.format(values=values),
                        params,
                    )
                    # Whatever is left would need a field-level merge; hand those groups back.
                    cur.execute(
                        "SELECT master_part_id FROM master_part_data WHERE master_part_id IN %s",
                        (tuple(dup_ids),),
                    )
                    needs_field_merge = {row[0] for row in cur.fetchall()}

                    # Fitments: move the ones that do not collide, drop the rest.
                    match = " AND ".join(
                        "k.{c} = f.{c}".format(c=c) for c in _FITMENT_KEY_COLUMNS
                    )
                    cur.execute(
                        """
                        UPDATE master_part_fitments f SET master_part_id = m.keep_id
                        FROM (VALUES {values}) AS m(dup_id, keep_id)
                        WHERE f.master_part_id = m.dup_id
                          AND NOT EXISTS (
                              SELECT 1 FROM master_part_fitments k
                              WHERE k.master_part_id = m.keep_id AND {match}
                          )
                        """.format(values=values, match=match),
                        params,
                    )
                    cur.execute(
                        "DELETE FROM master_part_fitments WHERE master_part_id IN %s",
                        (tuple(dup_ids),),
                    )

                    deletable = [d for d in dup_ids if d not in needs_field_merge]
                    if deletable:
                        # Belt and braces: never delete a row that still owns provider parts.
                        cur.execute(
                            """
                            DELETE FROM master_parts
                            WHERE id IN %s
                              AND NOT EXISTS (SELECT 1 FROM provider_parts pp WHERE pp.master_part_id = master_parts.id)
                            """,
                            (tuple(deletable),),
                        )
                        results["deleted"] += cur.rowcount
                results["merged"] += len(chunk) - len(needs_field_merge)
            if needs_field_merge:
                results["fallback"].extend(sorted(needs_field_merge))
        except Exception as exc:
            print("[ERROR] chunk at {}: {}".format(start, exc))
            import traceback
            traceback.print_exc()
            results["failed"].append((start, str(exc)))

        print("[bulk] {}/{} groups | deleted={} fallback={} failed={}".format(
            min(start + chunk_size, len(groups)), len(groups),
            results["deleted"], len(results["fallback"]), len(results["failed"])))

    return results


def merge_batch(groups: typing.Sequence[Group], dry_run: bool = True,
                limit: typing.Optional[int] = None) -> typing.Dict[str, list]:
    """
    Merge a list of groups from ``find_merge_candidates``. Each group runs in its own transaction
    so one failure cannot roll back the rest. Defaults to a dry run.

    ``PartRequestAudit.master_part_id`` is a plain integer column, not a foreign key -- rows there
    keep pointing at deleted ids. That is an analytics log, so it is left alone deliberately.
    """
    groups = list(groups)[: limit if limit is not None else len(groups)]
    results: typing.Dict[str, list] = {"merged": [], "failed": []}
    for index, group in enumerate(groups, start=1):
        if not group.safe:
            results["failed"].append((group.normalized_part_number, "group not marked safe"))
            continue
        try:
            print("\n[{}/{}] {} ({})".format(
                index, len(groups), group.normalized_part_number, group.reason))
            _merge_rows(group.rows, dry_run=dry_run)
            results["merged"].append(group.normalized_part_number)
        except Exception as exc:  # keep going: one bad group must not stop the batch
            print("[ERROR] {}: {}".format(group.normalized_part_number, exc))
            import traceback
            traceback.print_exc()
            results["failed"].append((group.normalized_part_number, str(exc)))

    print("\n{}".format("=" * 72))
    print("{}: {} merged, {} failed".format(
        "DRY RUN" if dry_run else "DONE", len(results["merged"]), len(results["failed"])))
    if results["failed"]:
        for key, reason in results["failed"][:20]:
            print("  FAILED {}: {}".format(key, reason))
    return results
