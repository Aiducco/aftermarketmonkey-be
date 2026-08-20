"""
Resolve an incoming distributor part to an existing ``MasterPart`` whose ``part_number`` differs
only in formatting.

Every provider ingest in ``master_parts.py`` looks up existing rows with an exact string match on
``(brand_id, part_number)``. Distributors do not agree on spelling -- Keystone ships
``'MS 96587'`` where Turn14, Meyer and A-Tech all ship ``'MS96587'`` -- so each house style
creates its own MasterPart, splitting one physical part across several rows with disjoint
provider sets. See ``docs/PART_NUMBER_NORMALIZATION.md`` for the production survey.

This module supplies rungs 2-4 of each ingest's lookup ladder, tried in order for feed rows the
exact match failed to place:

1. exact ``(brand_id, part_number)``   -- unchanged, always wins
2. ``resolve_normalized_matches``      -- formatting-only differences ('MS 96587' / 'MS96587')
3. ``resolve_sku_matches``             -- same sku within the brand; the bridging value
                                          distributors agree on when part numbers differ, and the
                                          only rung that reaches the ~706,000 barcode-less rows
4. ``resolve_gtin_matches``            -- same validated barcode; the only rung that can relate
                                          part numbers with no string similarity at all, such as
                                          Premier's 'TOY357280' for Toyo's '357280' or A-Tech's
                                          'S0845' placeholders
   otherwise                           -- create a new MasterPart, as before

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
import re
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
    src_enums.BrandProviderKind.ELITE_WHEEL.value,
    # motorstate_products has no barcode column and the ingest sets gtin=None, so
    # Motor State can never corroborate a match: 69,624 master parts are backed by it
    # alone and not one carries a gtin. Its rows often *look* barcoded because other
    # providers on the same master part supplied one -- that says nothing about a
    # Motor-State-only candidate.
    src_enums.BrandProviderKind.MOTOR_STATE_DISTRIBUTING.value,
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
    # The bridging value distributors tend to agree on even when part numbers differ; see
    # resolve_sku_matches. Optional -- ingests that do not set a sku simply skip that rung.
    sku: typing.Optional[str] = None


class _Candidate(typing.NamedTuple):
    master_part_id: int
    part_number: str
    gtin: typing.Optional[str]
    sku: typing.Optional[str] = None


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


# Must stay identical to the expression in migration 0168, or Postgres will not use the index.
# Digits only, leading zeros stripped -- the first half of pn_util.normalize_gtin. The check
# digit is validated in Python, so this groups slightly more loosely and only ever widens the
# candidate set.
_GTIN_CORE_SQL = r"ltrim(regexp_replace(coalesce(gtin, ''), '\D', '', 'g'), '0')"

_NON_DIGIT_RE = re.compile(r"\D")

# Weakest evidence grade the ingest will act on. Tier 5 (barcode is the only link) is refused:
# a single wrong barcode in one feed would silently attach a part to the wrong master part, and
# nothing in the data would reveal it. The cleanup script defaults to the same ceiling.
MAX_GTIN_EVIDENCE_TIER = pn_util.GTIN_EVIDENCE_PREFIX_SUFFIX

# More than this many rows on one barcode means a shared or bogus barcode, not one part.
_MAX_GTIN_CANDIDATES = 4


def _gtin_cores(raw: typing.Optional[str]) -> typing.Set[str]:
    """
    Index keys to probe for a feed barcode.

    Two, because ``normalize_gtin`` repairs values whose check digit the feed dropped: such a row
    may be stored either as shipped or as repaired, and both forms must be found.
    """
    validated = pn_util.normalize_gtin(raw)
    if not validated:
        return set()
    cores = {validated.lstrip("0")}
    as_shipped = _NON_DIGIT_RE.sub("", pn_util.strip_non_printable(raw)).lstrip("0")
    if as_shipped:
        cores.add(as_shipped)
    return {c for c in cores if c}


def resolve_sku_matches(
    feed_parts: typing.Sequence[FeedPart],
    provider_id: int,
) -> typing.Dict[typing.Tuple[int, str], int]:
    """
    Map ``(brand_id, part_number)`` -> existing ``MasterPart.id`` where an existing row carries
    the *same sku* within the brand.

    ``sku`` is the bridging value distributors already agree on even when their part numbers
    differ: A-Tech ships BDS's ``BDS123263`` while Premier ships ``123263``, and *both* rows carry
    ``sku='BDS123263'``. Meyer and WheelPros have always resolved on ``(brand_id, sku)`` first;
    the other ingests never did, which is why those pairs split.

    This rung matters most for the ~706,000 master parts with no barcode at all, which
    ``resolve_gtin_matches`` can never help. Uses the existing ``master_parts_brand_sku_idx``.

    **A conflicting barcode vetoes the match.** In production 32 groups share a brand and sku, and
    look like a clean prefix pair, yet carry different validated barcodes (BDS ``BDS55382`` vs
    ``55382`` on two different BDS barcode ranges). Those would be silent wrong merges, so a sku
    match is refused whenever both sides have a barcode and the barcodes disagree.
    """
    if not feed_parts:
        return {}

    probes: typing.Dict[typing.Tuple[int, str], typing.List[FeedPart]] = collections.defaultdict(list)
    for feed_part in feed_parts:
        sku_key = pn_util.normalize_part_number(feed_part.sku or "")
        if sku_key:
            probes[(feed_part.brand_id, sku_key)].append(feed_part)
    if not probes:
        return {}

    candidates: typing.Dict[typing.Tuple[int, str], typing.List[_Candidate]] = collections.defaultdict(list)
    for chunk in _chunked(sorted(probes)):
        brand_ids = {b for b, _ in chunk}
        with connection.cursor() as cur:
            cur.execute(
                """
                SELECT id, brand_id, part_number, sku, gtin
                FROM master_parts
                WHERE brand_id IN %s AND sku IS NOT NULL AND sku <> ''
                """,
                (tuple(sorted(brand_ids)),),
            )
            wanted = set(chunk)
            for master_part_id, brand_id, part_number, sku, gtin in cur.fetchall():
                key = (brand_id, pn_util.normalize_part_number(sku))
                if key in wanted:
                    candidates[key].append(
                        _Candidate(master_part_id=master_part_id, part_number=part_number or "",
                                   gtin=gtin, sku=sku)
                    )

    candidate_ids = {c.master_part_id for group in candidates.values() for c in group}
    if not candidate_ids:
        return {}
    external_ids_on_candidate = _provider_external_ids_on(candidate_ids, provider_id=provider_id)

    resolved: typing.Dict[typing.Tuple[int, str], int] = {}
    skipped = collections.Counter()
    for (brand_id, sku_key), feed_rows in probes.items():
        found = candidates.get((brand_id, sku_key)) or []
        for feed_part in feed_rows:
            feed_key = pn_util.normalize_part_number(feed_part.part_number)
            # A row already under this part number is the exact-match case, not ours.
            usable = [c for c in found
                      if pn_util.normalize_part_number(c.part_number) != feed_key]
            if not usable:
                skipped["no_sku_candidate"] += 1
                continue
            if len({c.master_part_id for c in usable}) > 1:
                skipped["ambiguous_sku"] += 1
                continue
            candidate = usable[0]

            existing = external_ids_on_candidate.get(candidate.master_part_id) or set()
            own = (feed_part.provider_external_id or "").strip()
            if existing and own not in existing:
                skipped["provider_lists_other_part_here"] += 1
                continue

            feed_gtin = pn_util.normalize_gtin(feed_part.gtin)
            candidate_gtin = pn_util.normalize_gtin(candidate.gtin)
            if feed_gtin and candidate_gtin and feed_gtin != candidate_gtin:
                skipped["barcode_conflict"] += 1
                continue

            resolved[(feed_part.brand_id, feed_part.part_number)] = candidate.master_part_id
            skipped["matched"] += 1

    if resolved or skipped:
        logger.info(
            "{} provider_id={}: sku rung, {} rows -> {} matched; {}".format(
                _LOG_PREFIX, provider_id, len(feed_parts), len(resolved), dict(skipped)
            )
        )
    return resolved


def resolve_gtin_matches(
    feed_parts: typing.Sequence[FeedPart],
    provider_id: int,
    max_evidence_tier: int = MAX_GTIN_EVIDENCE_TIER,
) -> typing.Dict[typing.Tuple[int, str], int]:
    """
    Map ``(brand_id, part_number)`` -> existing ``MasterPart.id`` for feed rows whose *barcode*
    identifies an existing row, even though the part numbers are not string-related.

    This is the third and last rung of the ladder, for rows the exact and normalized lookups both
    failed to place. It exists because those two can never see:

    - a distributor that prefixes the manufacturer number (Premier's ``TOY357280`` for Toyo's
      ``357280``; Turn14, Meyer, A-Tech and WheelPros do the same for other brands);
    - A-Tech's ~41,000 ``S0845``-style placeholder part numbers, which its feed supplies because
      it has no manufacturer part number for those rows at all.

    Guarded the same way as the normalized rung, plus an evidence grade: a shared barcode on its
    own is refused (see ``pn_util.classify_gtin_evidence``), because production has 7,185 groups
    where one distributor sells two different products under one barcode.
    """
    if not feed_parts:
        return {}

    probes: typing.Dict[typing.Tuple[int, str], typing.List[FeedPart]] = collections.defaultdict(list)
    for feed_part in feed_parts:
        for core in _gtin_cores(feed_part.gtin):
            probes[(feed_part.brand_id, core)].append(feed_part)
    if not probes:
        return {}

    candidates: typing.Dict[typing.Tuple[int, str], typing.List[_Candidate]] = collections.defaultdict(list)
    for chunk in _chunked(sorted(probes)):
        with connection.cursor() as cur:
            cur.execute(
                """
                SELECT id, brand_id, part_number, sku, gtin
                FROM master_parts
                WHERE gtin IS NOT NULL AND gtin <> '' AND (brand_id, {core}) IN %s
                """.format(core=_GTIN_CORE_SQL),
                (tuple(chunk),),
            )
            for master_part_id, brand_id, part_number, sku, gtin in cur.fetchall():
                for core in _gtin_cores(gtin):
                    candidates[(brand_id, core)].append(
                        _Candidate(master_part_id=master_part_id, part_number=part_number or "",
                                   gtin=gtin, sku=sku)
                    )

    candidate_ids = {c.master_part_id for group in candidates.values() for c in group}
    if not candidate_ids:
        return {}
    external_ids_on_candidate = _provider_external_ids_on(candidate_ids, provider_id=provider_id)

    resolved: typing.Dict[typing.Tuple[int, str], int] = {}
    skipped = collections.Counter()
    for feed_part in feed_parts:
        feed_gtin = pn_util.normalize_gtin(feed_part.gtin)
        if not feed_gtin:
            continue
        found = {}
        for core in _gtin_cores(feed_part.gtin):
            for cand in candidates.get((feed_part.brand_id, core), ()):
                if pn_util.normalize_gtin(cand.gtin) == feed_gtin:
                    found[cand.master_part_id] = cand
        # A row already carrying this part number is the exact-match case, not ours to touch.
        found = {
            k: c for k, c in found.items()
            if pn_util.normalize_part_number(c.part_number)
            != pn_util.normalize_part_number(feed_part.part_number)
        }
        if not found:
            skipped["no_barcode_candidate"] += 1
            continue
        if len(found) > 1 or len(found) > _MAX_GTIN_CANDIDATES:
            skipped["ambiguous_barcode"] += 1
            continue
        candidate = next(iter(found.values()))

        existing = external_ids_on_candidate.get(candidate.master_part_id) or set()
        own = (feed_part.provider_external_id or "").strip()
        if existing and own not in existing:
            # This provider already sells something else off that master part.
            skipped["provider_lists_other_part_here"] += 1
            continue

        tier = pn_util.classify_gtin_evidence([
            (feed_part.part_number, None),
            (candidate.part_number, candidate.sku),
        ])
        if tier > max_evidence_tier:
            skipped["evidence_tier_{}".format(tier)] += 1
            continue
        resolved[(feed_part.brand_id, feed_part.part_number)] = candidate.master_part_id
        skipped["matched_tier_{}".format(tier)] += 1

    if resolved or skipped:
        logger.info(
            "{} provider_id={}: barcode rung, {} rows -> {} matched; {}".format(
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
    sku_by_key: typing.Optional[
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
    sku_by_key = sku_by_key or {}
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

    # Third rung: rows the exact and normalized lookups both failed to place may still be
    # identifiable by barcode alone -- a prefixed part number, or a placeholder the feed supplies
    # because it has no manufacturer part number. Refused unless something beyond the barcode
    # corroborates; see resolve_gtin_matches.
    still_unplaced = [key for key in pairs if existing_by_key.get(key) is None]
    if still_unplaced and sku_by_key:
        by_sku = resolve_sku_matches(
            [
                FeedPart(
                    brand_id=brand_id,
                    part_number=part_number,
                    gtin=gtin_by_key.get((brand_id, part_number)),
                    provider_external_id=external_id_by_key.get((brand_id, part_number)),
                    sku=sku_by_key.get((brand_id, part_number)),
                )
                for brand_id, part_number in still_unplaced
            ],
            provider_id=provider_id,
        )
        existing_by_key.update(by_sku)
        matches = dict(matches, **by_sku)

    still_unplaced = [key for key in pairs if existing_by_key.get(key) is None]
    if still_unplaced:
        by_barcode = resolve_gtin_matches(
            [
                FeedPart(
                    brand_id=brand_id,
                    part_number=part_number,
                    gtin=gtin_by_key.get((brand_id, part_number)),
                    provider_external_id=external_id_by_key.get((brand_id, part_number)),
                )
                for brand_id, part_number in still_unplaced
            ],
            provider_id=provider_id,
        )
        existing_by_key.update(by_barcode)
        matches = dict(matches, **by_barcode)
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
