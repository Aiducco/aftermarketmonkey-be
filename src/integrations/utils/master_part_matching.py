"""
Resolve an incoming distributor part to an existing ``MasterPart`` whose ``part_number`` differs
only in formatting.

Every provider ingest in ``master_parts.py`` looks up existing rows with an exact string match on
``(brand_id, part_number)``. Distributors do not agree on spelling -- Keystone ships
``'MS 96587'`` where Turn14, Meyer and A-Tech all ship ``'MS96587'`` -- so each house style
creates its own MasterPart, splitting one physical part across several rows with disjoint
provider sets. See ``docs/PART_NUMBER_NORMALIZATION.md`` for the production survey.

This module is the *second* rung of each ingest's lookup ladder, tried only for feed rows that
the exact match already failed to place:

1. exact ``(brand_id, part_number)``            -- unchanged, always wins
2. ``resolve_normalized_matches`` (this module) -- formatting-only differences, heavily guarded
3. otherwise                                    -- create a new MasterPart, as before

**The guards are the point, not the normalization.** Punctuation is load-bearing in this domain:
``942B-89060+12`` and ``942B-89060-12`` are different wheel offsets that normalize to the same
key. A normalized match is accepted only when it is corroborated, using the same rules the
cleanup script applies (``scripts/merge_normalized_part_number_duplicates.py``), so a row matched
here and a row merged there can never disagree. When in doubt this returns nothing, and the
ingest falls through to creating a new MasterPart -- the pre-existing behavior. A missed match
costs a duplicate row; a wrong match puts the wrong physical part in a customer's cart.
"""
import collections
import logging
import typing

from django.db import connection

from src import enums as src_enums
from src.integrations.utils import part_numbers as pn_util

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[MASTER_PART_MATCHING]"

# Must stay identical to the expression in migration 0143, or Postgres will not use the index.
_NORMALIZED_PART_NUMBER_SQL = "upper(regexp_replace(part_number, '[^A-Za-z0-9]', '', 'g'))"

# Distributors whose feed has no UPC/GTIN column at all (confirmed against the raw tables), so a
# MasterPart backed only by these can never be corroborated by barcode.
GTIN_LESS_PROVIDER_KINDS = frozenset({
    src_enums.BrandProviderKind.WHEELPROS.value,
    src_enums.BrandProviderKind.DLG.value,
    src_enums.BrandProviderKind.VOSSEN.value,
    src_enums.BrandProviderKind.TIRERACK.value,
})

_LOOKUP_CHUNK = 2000


class FeedPart(typing.NamedTuple):
    """One unplaced row from a distributor feed, as the ingest already has it in hand."""
    brand_id: int
    part_number: str
    gtin: typing.Optional[str] = None
    # This distributor's own id for the part (vcpn, meyer_part, ...). Required to tell "the
    # distributor lists these as two different parts" apart from "a previous merge already
    # attached this very feed row to the candidate" -- see resolve_normalized_matches.
    provider_external_id: typing.Optional[str] = None


class _Candidate(typing.NamedTuple):
    master_part_id: int
    part_number: str
    gtin: typing.Optional[str]


def _chunked(items, size=_LOOKUP_CHUNK):
    items = list(items)
    for i in range(0, len(items), size):
        yield items[i : i + size]


def _fetch_candidates(
    normalized_keys: typing.Set[typing.Tuple[int, str]],
) -> typing.Dict[typing.Tuple[int, str], typing.List[_Candidate]]:
    """Load every existing MasterPart sharing a normalized key with one of the feed rows."""
    found: typing.Dict[typing.Tuple[int, str], typing.List[_Candidate]] = collections.defaultdict(list)
    for chunk in _chunked(sorted(normalized_keys)):
        with connection.cursor() as cur:
            cur.execute(
                """
                SELECT id, brand_id, part_number, gtin
                FROM master_parts
                WHERE (brand_id, {expr}) IN %s
                """.format(expr=_NORMALIZED_PART_NUMBER_SQL),
                (tuple(chunk),),
            )
            for master_part_id, brand_id, part_number, gtin in cur.fetchall():
                key = (brand_id, pn_util.normalize_part_number(part_number))
                found[key].append(
                    _Candidate(master_part_id=master_part_id, part_number=part_number or "", gtin=gtin)
                )
    return found


def _provider_external_ids_on(
    master_part_ids: typing.Set[int], provider_id: int
) -> typing.Dict[int, typing.Set[str]]:
    """
    For each candidate MasterPart, the provider_external_ids this provider already has on it.

    Not just "does a ProviderPart exist": after the duplicate cleanup
    (scripts/merge_normalized_part_number_duplicates.py) the provider is *expected* to sit on the
    canonical row, because the merge moved its ProviderPart there. Distinguishing "this is the
    same feed row we already attached" from "this distributor genuinely lists two parts" needs
    the external id, not just presence.
    """
    found: typing.Dict[int, typing.Set[str]] = collections.defaultdict(set)
    for chunk in _chunked(sorted(master_part_ids)):
        with connection.cursor() as cur:
            cur.execute(
                """
                SELECT master_part_id, provider_external_id
                FROM provider_parts
                WHERE provider_id = %s AND master_part_id IN %s
                """,
                (provider_id, tuple(chunk)),
            )
            for master_part_id, external_id in cur.fetchall():
                found[master_part_id].add((external_id or "").strip())
    return found


def _blind_master_parts(master_part_ids: typing.Set[int]) -> typing.Set[int]:
    """
    MasterParts whose providers *all* come from barcode-less feeds. Such a row can never be
    corroborated by GTIN, so a punctuation-level match against it has no second opinion.
    """
    kinds: typing.Dict[int, typing.Set[int]] = collections.defaultdict(set)
    for chunk in _chunked(sorted(master_part_ids)):
        with connection.cursor() as cur:
            cur.execute(
                """
                SELECT pp.master_part_id, p.kind
                FROM provider_parts pp
                JOIN providers p ON p.id = pp.provider_id
                WHERE pp.master_part_id IN %s
                """,
                (tuple(chunk),),
            )
            for master_part_id, kind in cur.fetchall():
                kinds[master_part_id].add(kind)
    return {
        master_part_id
        for master_part_id, kind_set in kinds.items()
        if kind_set and kind_set <= GTIN_LESS_PROVIDER_KINDS
    }


def resolve_normalized_matches(
    feed_parts: typing.Sequence[FeedPart],
    provider_id: int,
    provider_kind: typing.Optional[int] = None,
) -> typing.Dict[typing.Tuple[int, str], int]:
    """
    Map ``(brand_id, part_number)`` -> existing ``MasterPart.id`` for feed rows that safely match
    an existing row on formatting alone.

    Pass only the rows the exact-match lookup failed to place. Rows that do not clear every guard
    are simply absent from the result, and the caller creates a new MasterPart as it does today.

    Guards, all of which must pass:

    - **Exactly one candidate.** More than one existing row shares the normalized key, so the
      feed's spelling is genuinely ambiguous (this is what keeps the ``+12`` / ``-12`` pair apart).
    - **The candidate carries no *other* part from this provider.** If this provider already has a
      ProviderPart on the candidate under a *different* ``provider_external_id``, the distributor
      is listing both spellings as separate parts in its own catalog -- a deliberate distinction
      by the party closest to the manufacturer, not a formatting accident. A ProviderPart with the
      *same* external id is this very feed row, already attached by an earlier run or by the
      duplicate-merge script, and must match rather than be refused; refusing it would re-create
      the duplicate the merge just removed. Callers that do not pass ``provider_external_id`` get
      the old conservative behaviour (any existing link refuses the match).
    - **No ``+``/``-`` sign conflict** between the feed spelling and the candidate's.
    - **Tier.** Case-only and whitespace-only differences cannot change which physical part is
      meant, so they pass here. Hyphen/dot and other punctuation differences additionally require
      a **matching validated GTIN on both sides**, and are refused when the candidate is backed
      only by barcode-less providers.
    """
    if not feed_parts:
        return {}

    by_normalized: typing.Dict[typing.Tuple[int, str], typing.List[FeedPart]] = collections.defaultdict(list)
    for feed_part in feed_parts:
        key = pn_util.normalize_part_number(feed_part.part_number)
        if key:
            by_normalized[(feed_part.brand_id, key)].append(feed_part)

    if not by_normalized:
        return {}

    candidates = _fetch_candidates(set(by_normalized))
    candidate_ids = {c.master_part_id for group in candidates.values() for c in group}
    if not candidate_ids:
        return {}

    external_ids_on_candidate = _provider_external_ids_on(candidate_ids, provider_id=provider_id)
    # Only needed for the punctuation tiers; skipped entirely when the provider itself has no
    # barcodes, since those rows can never reach the GTIN-gated branch anyway.
    provider_is_blind = provider_kind in GTIN_LESS_PROVIDER_KINDS
    blind = set() if provider_is_blind else _blind_master_parts(candidate_ids)

    resolved: typing.Dict[typing.Tuple[int, str], int] = {}
    skipped = collections.Counter()

    for normalized_key, feed_rows in by_normalized.items():
        found = candidates.get(normalized_key) or []
        if not found:
            skipped["no_candidate"] += len(feed_rows)
            continue

        # Strongest possible evidence, checked before anything else: this provider already has a
        # ProviderPart on a candidate under the *same* provider_external_id. That is not an
        # inference from spelling -- the distributor's own catalog id says it is this part, and
        # the link already exists (an earlier sync made it, or the merge script moved it here).
        # It therefore outranks both the ambiguity check and the GTIN corroboration the
        # punctuation tiers otherwise require: re-deriving identity we already hold would refuse
        # the row and re-create the duplicate the merge just removed. This is exactly how a
        # T3/T4 part with a missing GTIN on either side used to leak back in.
        # Lowest id first: if a duplicate has already been created (this provider ends up linked
        # under the same external id on both rows), the older row is the canonical one, and
        # collapsing onto it lets the next cleanup run remove the newer copy. Without this the
        # choice would follow query order and could flip between syncs.
        already_linked_by_external_id = {}
        for cand in sorted(found, key=lambda c: c.master_part_id):
            for external_id in external_ids_on_candidate.get(cand.master_part_id) or ():
                already_linked_by_external_id.setdefault(external_id, cand)

        unresolved_rows = []
        for feed_part in feed_rows:
            own = (feed_part.provider_external_id or "").strip()
            linked = already_linked_by_external_id.get(own) if own else None
            if linked is not None:
                resolved[(feed_part.brand_id, feed_part.part_number)] = linked.master_part_id
                skipped["relinked_by_external_id"] += 1
            else:
                unresolved_rows.append(feed_part)
        if not unresolved_rows:
            continue

        if len(found) != 1:
            skipped["ambiguous"] += len(unresolved_rows)
            continue
        candidate = found[0]
        existing_external_ids = external_ids_on_candidate.get(candidate.master_part_id) or set()

        candidate_gtin = pn_util.normalize_gtin(candidate.gtin)
        for feed_part in unresolved_rows:
            if existing_external_ids:
                # Reaching here means this feed row's own id is NOT among them, so the provider
                # lists a genuinely different part on this master part.
                skipped["provider_lists_other_part_here"] += 1
                continue
            spellings = [feed_part.part_number, candidate.part_number]
            if pn_util.has_sign_conflict(spellings):
                skipped["sign_conflict"] += 1
                continue
            tier = pn_util.classify_tier(spellings)
            if tier not in (pn_util.TIER_CASE_ONLY, pn_util.TIER_WHITESPACE_ONLY):
                if candidate.master_part_id in blind:
                    skipped["candidate_has_no_barcode_source"] += 1
                    continue
                feed_gtin = pn_util.normalize_gtin(feed_part.gtin)
                if not feed_gtin or feed_gtin != candidate_gtin:
                    skipped["gtin_missing_or_mismatched"] += 1
                    continue
            resolved[(feed_part.brand_id, feed_part.part_number)] = candidate.master_part_id

    if resolved or skipped:
        logger.info(
            "{} provider_id={}: {} feed rows -> {} matched to existing master parts; skipped {}".format(
                _LOG_PREFIX, provider_id, len(feed_parts), len(resolved), dict(skipped)
            )
        )
    return resolved


def extend_with_normalized_matches(
    existing_by_key: typing.Dict[typing.Tuple[int, str], int],
    pairs: typing.Sequence[typing.Tuple[int, str]],
    gtin_by_key: typing.Mapping[typing.Tuple[int, str], typing.Optional[str]],
    provider_id: int,
    provider_kind: typing.Optional[int] = None,
    external_id_by_key: typing.Optional[
        typing.Mapping[typing.Tuple[int, str], typing.Optional[str]]
    ] = None,
) -> int:
    """
    Ingest-facing wrapper: fill in the ``(brand_id, part_number) -> master_part_id`` entries that
    the exact-match lookup could not place, mutating ``existing_by_key`` in place and returning
    how many were added.

    Call this immediately after an ingest's exact ``(brand_id, part_number)`` lookup and before it
    splits rows into new-vs-existing. Everything downstream then treats a formatting-only match
    exactly like an exact match.

    A key is "unplaced" when it is absent *or* mapped to ``None`` -- Meyer and WheelPros resolve
    via sku first and record a ``None`` for the misses rather than leaving the key out.
    """
    unplaced = [key for key in pairs if existing_by_key.get(key) is None]
    if not unplaced:
        return 0
    external_id_by_key = external_id_by_key or {}
    matches = resolve_normalized_matches(
        [
            FeedPart(
                brand_id=brand_id,
                part_number=part_number,
                gtin=gtin_by_key.get((brand_id, part_number)),
                provider_external_id=external_id_by_key.get((brand_id, part_number)),
            )
            for brand_id, part_number in unplaced
        ],
        provider_id=provider_id,
        provider_kind=provider_kind,
    )
    existing_by_key.update(matches)
    return len(matches)


def master_part_stubs_for(
    existing_by_key: typing.Mapping[typing.Tuple[int, str], int],
) -> typing.Dict[typing.Tuple[int, str], typing.Any]:
    """
    Build the unsaved ``MasterPart`` stubs the ingests use to attach ``ProviderPart`` rows, keyed
    by the *feed's* ``(brand_id, part_number)``.

    Ingests otherwise rebuild this by re-querying ``master_parts`` on the exact part number, which
    by definition cannot find a row matched on formatting. Only ``id`` is ever read downstream
    (it becomes ``ProviderPart.master_part_id``), so the stub carries the resolved id and the
    brand, and deliberately does not pretend to know the canonical row's own spelling.
    """
    from src import models as src_models  # local import: keeps this module importable standalone

    stubs = {}
    for (brand_id, part_number), master_part_id in existing_by_key.items():
        stub = src_models.MasterPart()
        stub.id = master_part_id
        stub.brand_id = brand_id
        stubs[(brand_id, part_number)] = stub
    return stubs
