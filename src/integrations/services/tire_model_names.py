"""
Consolidate spellings of the same tire model within a brand.

``model_name`` reaches ``tire_specs`` from two places -- an LLM reading distributor titles, and
(since the SimpleTire merge) a manufacturer catalog -- and distributors do not agree with each
other on punctuation. The same Yokohama tire arrives as ``Geolandar A/T G015`` and
``Geolandar AT G015``; the same Pirelli as ``P Zero (PZ4)`` and ``P Zero PZ4``. Every spelling
becomes its own facet value in search and its own bucket in the per-model reconciliation vote, so
the split is not cosmetic: it fragments both.

The whole design problem is that the difference between a spelling variant and a different product
is a few characters, and getting it wrong merges two real tires into one. So the key below is
deliberately narrow:

* punctuation, spacing and case are noise -- ``Renegade AT`` / ``Renegade A/T``, ``Eagle LS2`` /
  ``Eagle LS-2``, ``Blizzak Icepeak`` / ``Blizzak IcePeak``
* a roman numeral is the same generation as its arabic twin -- ``Crosstek II`` / ``Crosstek 2``
* the brand glued onto the front is noise -- ``Kanati Mud Hog M/T`` under brand Kanati
* **``+`` is not noise.** Goodyear sells both ``Ultra Grip Performance`` and
  ``UltraGrip Performance+``; stripping the plus merged them, and the first version of this key
  did exactly that on 3 groups before it was caught.
* a trailing generation marker is never noise. ``Terra Grappler`` and ``Terra Grappler G2`` are
  different tires, so keys must match *exactly* -- containment is not enough.

Canonical spelling is the one a matched SimpleTire row uses, because that is the manufacturer's
own; failing that, the most common. The losing spellings are kept in ``search_aliases`` so a
customer typing the distributor's version still finds the tire.

Read-only unless the caller passes ``apply_changes``.
"""
import collections
import dataclasses
import logging
import re
import typing

from django.db import transaction
from django.db.models import Count

from src import models as src_models

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[TIRE-MODEL-NAMES]"

# Longest first, so VIII is not matched as V followed by III.
_ROMAN = (
    ("VIII", "8"),
    ("VII", "7"),
    ("III", "3"),
    ("IX", "9"),
    ("IV", "4"),
    ("VI", "6"),
    ("II", "2"),
    ("V", "5"),
    ("X", "10"),
)

# Words that appear inside brand names but are far too generic to strip out of a model name.
_BRAND_STOPWORDS = frozenset(["TIRE", "TIRES", "TYRE", "USA", "CORPORATION", "COMPANY", "AND", "GROUP", "INC"])

_NOISE_WORDS_RE = re.compile(r"\b(TIRE|TIRES|TYRE)\b")


def normalise(model_name: typing.Optional[str], brand_name: typing.Optional[str]) -> str:
    """The comparison key. Two names sharing one are the same product; see the module docstring."""
    text = " " + (model_name or "").upper() + " "
    for token in re.split(r"[^A-Z0-9]+", (brand_name or "").upper()):
        if len(token) > 2 and token not in _BRAND_STOPWORDS:
            text = text.replace(" " + token + " ", " ")
    text = _NOISE_WORDS_RE.sub(" ", text)
    for roman, arabic in _ROMAN:
        text = re.sub(r"(?<![A-Z0-9])" + roman + r"(?![A-Z0-9])", arabic, text)
    # '+' survives: it distinguishes real products from one another.
    return re.sub(r"[^A-Z0-9+]", "", text)


@dataclasses.dataclass
class MergeGroup:
    brand_id: int
    brand_name: str
    canonical: str
    variants: typing.Dict[str, int]  # losing spelling -> row count

    @property
    def rows(self) -> int:
        return sum(self.variants.values())


@dataclasses.dataclass
class ConsolidationStats:
    distinct_before: int = 0
    distinct_after: int = 0
    groups: int = 0
    rows_changed: int = 0
    aliases_added: int = 0
    written: int = 0


def plan(brand_ids: typing.Optional[typing.Sequence[int]] = None) -> typing.List[MergeGroup]:
    """Group every (brand, model_name) pair by its key and return the groups with more than one."""
    qs = src_models.TireSpec.objects.exclude(model_name__isnull=True).exclude(model_name="")
    if brand_ids:
        qs = qs.filter(master_part__brand_id__in=list(brand_ids))
    rows = qs.values_list("master_part__brand_id", "master_part__brand__name", "model_name", "spec_source").annotate(
        n=Count("id")
    )

    buckets: typing.Dict[tuple, typing.Dict[str, typing.List[int]]] = collections.defaultdict(
        lambda: collections.defaultdict(lambda: [0, 0])
    )
    names: typing.Dict[int, str] = {}
    for brand_id, brand_name, model_name, spec_source, count in rows:
        names[brand_id] = brand_name
        slot = buckets[(brand_id, normalise(model_name, brand_name))][model_name]
        slot[0] += count
        if spec_source == src_models.TireSpec.SPEC_SOURCE_SIMPLETIRE:
            slot[1] += count

    groups = []
    for (brand_id, _key), variants in buckets.items():
        if len(variants) < 2:
            continue
        # The catalog's spelling wins; then the most common; then the shortest, as a stable
        # tie-break rather than a meaningful preference.
        canonical = max(variants.items(), key=lambda kv: (kv[1][1], kv[1][0], -len(kv[0])))[0]
        groups.append(
            MergeGroup(
                brand_id=brand_id,
                brand_name=names[brand_id],
                canonical=canonical,
                variants={name: counts[0] for name, counts in variants.items() if name != canonical},
            )
        )
    groups.sort(key=lambda g: -g.rows)
    return groups


def run(
    *,
    brand_ids: typing.Optional[typing.Sequence[int]] = None,
    apply_changes: bool = False,
) -> typing.Tuple[ConsolidationStats, typing.List[MergeGroup]]:
    groups = plan(brand_ids)
    stats = ConsolidationStats(groups=len(groups), rows_changed=sum(g.rows for g in groups))

    qs = src_models.TireSpec.objects.exclude(model_name__isnull=True).exclude(model_name="")
    if brand_ids:
        qs = qs.filter(master_part__brand_id__in=list(brand_ids))
    distinct = set(qs.values_list("master_part__brand_id", "model_name"))
    stats.distinct_before = len(distinct)
    stats.distinct_after = len(distinct) - sum(len(g.variants) for g in groups)

    if not apply_changes:
        return stats, groups

    for group in groups:
        stats.written += _apply(group)
    return stats, groups


@transaction.atomic
def _apply(group: MergeGroup) -> int:
    """
    Rename one group's losing spellings, keeping each as a search alias.

    The alias matters: a customer who types the distributor's spelling should still find the tire
    after we have relabelled it to the manufacturer's. In practice the enrichment already wrote the
    old ``model_name`` into ``search_aliases`` on 95.7% of rows, so this usually finds nothing to
    add -- it exists for the rest.
    """
    specs = list(
        src_models.TireSpec.objects.filter(master_part__brand_id=group.brand_id, model_name__in=list(group.variants))
    )
    for spec in specs:
        previous = spec.model_name
        aliases = list(spec.search_aliases or [])
        if not any((a or "").strip().casefold() == previous.casefold() for a in aliases):
            aliases.append(previous)
        spec.model_name = group.canonical
        spec.search_aliases = aliases
    src_models.TireSpec.objects.bulk_update(specs, ["model_name", "search_aliases"], batch_size=500)
    logger.info(
        "%s %s: %s <- %s (%d rows)",
        _LOG_PREFIX,
        group.brand_name,
        group.canonical,
        ", ".join(sorted(group.variants)),
        len(specs),
    )
    return len(specs)
