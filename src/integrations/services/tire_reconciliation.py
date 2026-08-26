"""
Reconcile per-SKU tire enrichment answers across the sizes of one model.

One LLM call per tire means the same model in 40 sizes gets 40 independent answers, and some
disagree. Measured on the first full NITTO run: **9 of 33 models (27%) came back with a split
tread category**, covering 363 of 1,574 SKUs. Nothing reports that as a bug -- the facet counts
are simply, quietly wrong, and "Mud Terrain" shows 158 tires when it should show 161.

Two passes, and they are **not** symmetrical in how aggressive they are allowed to be.

``reconcile_categories`` -- majority vote per (brand, model name), ties broken by mean
confidence. Safe, because the vote is scoped to rows that already agree on the model name: the
question being settled is "what category is *this model*", and the only inputs are answers about
that same model.

``canonicalize_model_names`` -- **case and punctuation only**. The plan this implements called
for taking the longest variant per brand, so that "TER GRAP G3" becomes "Terra Grappler G3"
everywhere. Checked against the real enrichment output before implementing, that rule would have
merged genuinely different products:

    Terra Grappler G3 (224)  vs  Terra Grappler G2 (53)   -- different generations
    Motivo 365 (73)          vs  Motivo (54)              -- all-weather vs UHP all-season
    Trail Grappler M/T (129) vs  Trail Grappler SXS (3)   -- light truck vs UTV
    NT05 (36)                vs  NT05R (1)                -- street vs R-compound

Every one of those is a real, separately-purchasable tire. The abbreviation-expansion the rule was
aimed at never materialised, because the model already returns expanded names -- 35 spellings
across 1,574 SKUs, of which exactly one pair ("Invo"/"INVO") differs only in case. So this pass
does the one merge that is provably safe and leaves the rest alone. If distributor abbreviations
ever *do* survive into ``model_name``, that is a prompt problem, not something to paper over here.
"""
import collections
import dataclasses
import logging
import re
import typing

from django.db import connection, transaction

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[TIRE-RECONCILE]"

# Case- and punctuation-insensitive key. "Recon Grappler A/T" and "recon grappler at" collapse;
# "Terra Grappler G2" and "Terra Grappler G3" do not, which is the point.
_NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")


def model_key(name: typing.Optional[str]) -> str:
    return _NON_ALNUM_RE.sub("", (name or "").lower())


def brandless_key(name: typing.Optional[str], brand_name: str) -> str:
    """
    ``model_key`` with a redundant leading brand name removed.

    The enrichment prompt already says model_name is "the manufacturer's product name with the
    brand ... removed", and it usually is -- but not always, which leaves "One" and "Nokian One"
    as two models of the same tire in the brand facet. Collapsing on this key is safe precisely
    because it is the rule the model was given: it can only merge a name with its own brand
    prefix, never two different products.

    Only a leading brand is stripped, and only when something is left: a model genuinely named
    after its brand keeps its name rather than reducing to nothing.
    """
    key = model_key(name)
    brand = model_key(brand_name)
    if brand and key.startswith(brand) and len(key) > len(brand):
        return key[len(brand) :]
    return key


@dataclasses.dataclass
class CategoryVote:
    brand_id: int
    model_name: str
    counts: typing.Dict[str, int]
    mean_confidence: typing.Dict[str, float]

    @property
    def total(self) -> int:
        return sum(self.counts.values())

    @property
    def is_split(self) -> bool:
        return len(self.counts) > 1

    @property
    def winner(self) -> str:
        """Most votes; ties broken by mean confidence, then by code so the result is stable."""
        return max(
            self.counts,
            key=lambda code: (self.counts[code], self.mean_confidence.get(code, 0.0), code),
        )

    @property
    def winner_share(self) -> float:
        return self.counts[self.winner] / self.total if self.total else 0.0


# Below this share of the vote, the winner is not a majority worth acting on -- it is a coin
# flip. Measured on TOYO: "Proxes R {TRACK: 6, UHP: 6}" resolved to TRACK on a tiebreaker and
# would have been written as fact across twelve SKUs. Those go to the review queue instead.
DEFAULT_MIN_AGREEMENT = 0.60


@dataclasses.dataclass
class ReconciliationReport:
    votes: typing.List[CategoryVote] = dataclasses.field(default_factory=list)
    categories_changed: int = 0
    names_changed: int = 0
    touched_master_part_ids: typing.List[int] = dataclasses.field(default_factory=list)
    # Split models whose winner did not clear the agreement bar. Left exactly as the model
    # answered them, per SKU, and surfaced for a human.
    undecided: typing.List[CategoryVote] = dataclasses.field(default_factory=list)

    @property
    def split_votes(self) -> typing.List[CategoryVote]:
        return [vote for vote in self.votes if vote.is_split]

    @property
    def split_rate(self) -> float:
        return len(self.split_votes) / len(self.votes) if self.votes else 0.0


def collect_votes(brand_ids: typing.Optional[typing.Sequence[int]] = None) -> typing.List[CategoryVote]:
    """
    One vote tally per (brand, model). Grouped on the case-insensitive model name, which keeps
    ``Terra Grappler G2`` and ``G3`` as separate models -- they are separate products and must
    not vote on each other's category.
    """
    sql = """
        SELECT mp.brand_id,
               lower(ts.model_name)            AS model_key,
               min(ts.model_name)              AS model_name,
               ts.tread_category,
               count(*)                        AS n,
               avg(ts.llm_confidence)          AS avg_conf
        FROM tire_specs ts
        JOIN master_parts mp ON mp.id = ts.master_part_id
        WHERE ts.model_name IS NOT NULL
          AND ts.tread_category IS NOT NULL
          {brand_clause}
        GROUP BY 1, 2, 4
        ORDER BY 1, 2
    """
    params: typing.List[typing.Any] = []
    brand_clause = ""
    if brand_ids:
        brand_clause = "AND mp.brand_id = ANY(%s)"
        params.append(list(brand_ids))

    grouped: typing.Dict[typing.Tuple[int, str], CategoryVote] = {}
    with connection.cursor() as cursor:
        cursor.execute(sql.format(brand_clause=brand_clause), params)
        for brand_id, key, model_name, category, count, avg_conf in cursor.fetchall():
            vote = grouped.get((brand_id, key))
            if vote is None:
                vote = CategoryVote(brand_id=brand_id, model_name=model_name, counts={}, mean_confidence={})
                grouped[(brand_id, key)] = vote
            vote.counts[category] = count
            vote.mean_confidence[category] = float(avg_conf or 0)
    return list(grouped.values())


@transaction.atomic
def reconcile_categories(
    votes: typing.Sequence[CategoryVote],
    report: ReconciliationReport,
    *,
    apply_changes: bool,
    min_agreement: float = DEFAULT_MIN_AGREEMENT,
) -> None:
    """
    Overwrite each SKU's category with its model's winning category, where they differ.

    A model whose winner falls below ``min_agreement`` is **left alone**. A 50/50 split is not a
    majority, and resolving it by tiebreaker would stamp a coin flip across every size of that
    model -- worse than the inconsistency it replaces, because the inconsistency is at least
    visible. Those land in ``report.undecided`` for review.
    """
    for vote in votes:
        if not vote.is_split:
            continue
        if vote.winner_share < min_agreement:
            report.undecided.append(vote)
            logger.info(
                "%s %s: %s -- winner has only %.0f%%, below the %.0f%% bar; left for review",
                _LOG_PREFIX,
                vote.model_name,
                dict(vote.counts),
                100 * vote.winner_share,
                100 * min_agreement,
            )
            continue
        winner = vote.winner
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT ts.master_part_id FROM tire_specs ts "
                "JOIN master_parts mp ON mp.id = ts.master_part_id "
                "WHERE mp.brand_id = %s AND lower(ts.model_name) = lower(%s) "
                "  AND ts.tread_category IS DISTINCT FROM %s AND ts.tread_category IS NOT NULL",
                [vote.brand_id, vote.model_name, winner],
            )
            ids = [row[0] for row in cursor.fetchall()]
        if not ids:
            continue
        report.categories_changed += len(ids)
        report.touched_master_part_ids.extend(ids)
        logger.info(
            "%s %s: %s -> %s for %s SKU(s) (%.0f%% agreed)",
            _LOG_PREFIX,
            vote.model_name,
            dict(vote.counts),
            winner,
            len(ids),
            100 * vote.winner_share,
        )
        if apply_changes:
            with connection.cursor() as cursor:
                cursor.execute(
                    "UPDATE tire_specs SET tread_category = %s, category_reconciled = TRUE, "
                    "updated_at = now() WHERE master_part_id = ANY(%s)",
                    [winner, ids],
                )


@transaction.atomic
def canonicalize_model_names(
    report: ReconciliationReport,
    *,
    brand_ids: typing.Optional[typing.Sequence[int]] = None,
    apply_changes: bool,
) -> None:
    """
    Collapse spellings that differ only in case, punctuation, or a redundant brand prefix.

    Deliberately narrow -- see the module docstring for the four real product pairs that a
    looser rule would have merged. In particular this does NOT merge on a shared prefix: there
    are 1,005 such pairs in the catalog and they are product families, not duplicates. Merging
    "P Zero" into "P Zero Winter" would put a summer tire in the winter facet.

    The winner is the most frequent spelling rather than the longest, because frequency is
    evidence and length is not: "INVO" appearing 8 times against "Invo" 63 times is a minority
    spelling, not a more complete one. The one exception is a spelling that still carries its
    brand, which loses regardless of frequency -- see the comment on the sort key.
    """
    sql = """
        SELECT mp.brand_id, b.name AS brand_name, ts.model_name, count(*) AS n
        FROM tire_specs ts
        JOIN master_parts mp ON mp.id = ts.master_part_id
        JOIN brands b ON b.id = mp.brand_id
        WHERE ts.model_name IS NOT NULL {brand_clause}
        GROUP BY 1, 2, 3
    """
    params: typing.List[typing.Any] = []
    brand_clause = ""
    if brand_ids:
        brand_clause = "AND mp.brand_id = ANY(%s)"
        params.append(list(brand_ids))

    spellings: typing.Dict[typing.Tuple[int, str], collections.Counter] = collections.defaultdict(collections.Counter)
    with connection.cursor() as cursor:
        cursor.execute(sql.format(brand_clause=brand_clause), params)
        brand_names_by_id = {}
        for brand_id, brand_name, name, count in cursor.fetchall():
            brand_names_by_id[brand_id] = brand_name
            spellings[(brand_id, brandless_key(name, brand_name))][name] += count

    for (brand_id, _key), counter in spellings.items():
        if len(counter) < 2:
            continue
        brand_key = model_key(brand_names_by_id.get(brand_id, ""))

        def _still_has_brand(name: str, brand_key: str = brand_key) -> bool:
            key = model_key(name)
            return bool(brand_key) and key.startswith(brand_key) and key != brand_key

        # Most common wins, EXCEPT that a spelling still carrying its brand always loses: "Nokian
        # One" appears twice against "One" sixty times, but even reversed, the brandless form is
        # the one the enrichment prompt asks for. Then longer, then alphabetical, so a rerun is
        # deterministic.
        canonical = max(counter, key=lambda name: (not _still_has_brand(name), counter[name], len(name), name))
        losers = [name for name in counter if name != canonical]
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT ts.master_part_id FROM tire_specs ts "
                "JOIN master_parts mp ON mp.id = ts.master_part_id "
                "WHERE mp.brand_id = %s AND ts.model_name = ANY(%s)",
                [brand_id, losers],
            )
            ids = [row[0] for row in cursor.fetchall()]
        if not ids:
            continue
        report.names_changed += len(ids)
        report.touched_master_part_ids.extend(ids)
        logger.info("%s model name %s -> %r for %s SKU(s)", _LOG_PREFIX, losers, canonical, len(ids))
        if apply_changes:
            with connection.cursor() as cursor:
                cursor.execute(
                    "UPDATE tire_specs SET model_name = %s, updated_at = now() " "WHERE master_part_id = ANY(%s)",
                    [canonical, ids],
                )


def run(
    *,
    brand_ids: typing.Optional[typing.Sequence[int]] = None,
    apply_changes: bool = False,
    min_agreement: float = DEFAULT_MIN_AGREEMENT,
) -> ReconciliationReport:
    """
    Full pass. Names are canonicalised **before** the category vote, so two spellings of one
    model contribute to a single tally instead of two.
    """
    report = ReconciliationReport()
    canonicalize_model_names(report, brand_ids=brand_ids, apply_changes=apply_changes)
    report.votes = collect_votes(brand_ids)
    reconcile_categories(report.votes, report, apply_changes=apply_changes, min_agreement=min_agreement)
    report.touched_master_part_ids = sorted(set(report.touched_master_part_ids))
    return report
