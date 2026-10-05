"""
Per-part PCdb terminology classification, two stages, one LLM.

The in-repo port of ``scripts/qwen_classify_parts.py``. That script was written Django-free so it
could run on a GPU box with no checkout of this codebase, and it talks to the database over raw
psycopg2. This module is the same pipeline against the ORM, so it can be run as a management
command from a machine that already has settings, models and credentials -- see
``manage.py classify_part_terminology``. Both write the same table
(:class:`src.models.MLPartTerminologyClassification`); keep the two in step if you change the
prompts.

WHAT IT DOES, per part:
  Stage 1: batch-classify N parts at once into a (category, subcategory) pair from the PCdb
           taxonomy, read from ``pcdb_terminology_flat``.
  Stage 2: group parts by their assigned pair, then batch-classify each group into the single
           best-matching part_terminology_id from EVERY terminology sharing that subcategory's
           NAME -- pooled across every category that has one, not just the specific pair Stage 1
           picked. No shortlisting, so the correct term is never excluded by a retrieval step
           missing it. The tradeoff is a much bigger prompt for the largest pooled subcategory
           names, which is what ``batch_size`` is for.

Both stages fan out over an LLM endpoint with a thread pool. The threads only make HTTP calls --
every database read happens before the pool starts and every write after it finishes -- so no
Django connection is ever touched from a worker thread.

WHY A PROVIDER HIERARCHY: a MasterPart can have several ProviderPart rows (one per distributor
carrying it), and description quality varies enormously by distributor. :data:`PROVIDER_PRIORITY`
is not a guess -- the ATECH/KEYSTONE/MOTOR_STATE_DISTRIBUTING/QUADRATEC/DLG "no real signal" tier
comes from ``src/integrations/utils/product_type.py`` ("1.65M of 3.2M master parts reach us only
through distributors that ship no type signal at all"), and TURN_14/PREMIER_PERFORMANCE/MEYER's
high placement is confirmed by that same file's closed category vocab / real PCdb terminology
hints. For each part this walks the priority list and uses the highest-ranked available provider's
data -- but skips a candidate whose raw data is only a redundant sku/brand/description echo and
falls through to the next, rather than trusting rank alone.

FOUR FIXES CONFIRMED AGAINST PRODUCTION DATA, in the order they were found necessary. All four are
load-bearing; none is a stylistic preference:

1. Subcategory-name POOLING (Stage 2 sees every terminology sharing the assigned subcategory's
   NAME, across every category that has one). Roughly 62% of PCdb subcategory names exist under
   more than one category with a different, non-overlapping term list (e.g. "Fuel Injection System
   and Related Components" under both "Engine" and "Air and Fuel Delivery"). Confirmed fix: a FUEL
   INJECTOR routed to "Engine" still matched correctly, because the real term also exists under
   "Air and Fuel Delivery" and pooling meant Stage 2 saw it anyway.
2. TAXONOMY HINTING (each subcategory in the taxonomy text carries 2-3 real example terminology
   names, not just its bare name). A bare subcategory name can be a poor semantic match for how a
   product is actually described. Confirmed fix: WeatherTech "Cargo Liner" parts were consistently
   routed to the generic "Interior" instead of the real "Trunk Lid and Compartment" subcategory
   until its taxonomy line carried examples like "Cargo Box, Cargo Net" -- with that hint, 22/22
   test parts routed correctly on the first try. A cheaper "ask Stage 1 for its top 2-3 guesses
   and retry" alternative was tried first and did NOT work: the model's own alternates never
   included the right one either, so this was never simply "not enough guesses".
3. RECURSIVE retry-on-truncation (:func:`complete_json_with_retry` halves the batch and recurses,
   not just once). Batches of ~30 parts can genuinely truncate mid-JSON, and a single
   non-recursive retry sometimes still wasn't small enough. Confirmed fix: a 300-part real run
   went from 15 hard errors to 0 once retries could recurse past one level.
4. Per-candidate CATEGORY field in Stage 2's payload (each candidate shows the real category it
   belongs to, not just its name/description). Pooling by subcategory name (#1) means Stage 2's
   candidate list can mix in a term scoped to a specific, unrelated system. Confirmed fix: a
   generic Dorman rubber expansion plug was confidently but wrongly matched to "Drum Brake Plug"
   -- a real term, but drum-brake-specific -- pooled in purely because the word "Plug" overlapped.
   A text-only warning in the prompt did NOT stop this; showing each candidate's actual category
   (so "Brake" visibly stands out among otherwise-generic candidates) did.

ONE LIMITATION STILL OPEN: the same class of problem fix #2 targets -- Stage 1 choosing a
plausible but wrong subcategory -- still recurs when two subcategories under the SAME category
both sound like generic catch-alls and 2-3 examples aren't enough to tell them apart. Confirmed on
a second, independent case: Dorman exhaust hardware kits (real terms exist, e.g. "Exhaust Manifold
Stud Kit", both under Exhaust / "Hardware, Fasteners and Fittings") were instead routed to the
sibling "Brackets, Flanges and Hangers" subcategory under the same "Exhaust" category -- correctly
refused rather than force-matched, but a real miss. The untried, likely next step: more examples
per subcategory (5-6 instead of 3) where two subcategories in one category compete.

"unclassifiable" here never means a wrong answer got forced -- it means no answer was confident
enough, which is the deliberately safer failure mode. Rows with status='unclassifiable' are
exactly where a human review pass adds the most value; start there.
"""
import collections
import dataclasses
import decimal
import json
import logging
import math
import re
import time
import typing
from concurrent.futures import ThreadPoolExecutor, as_completed

import pgbulk

from src import enums
from src import models as src_models
from src.integrations.llm import qwen_llm

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[PART-TERM]"

# Best-to-worst. Anything not listed (the small/unverified providers) sorts after this list but
# before the confirmed-worst tier below -- see _provider_rank().
#
# Ordering below the top entries was re-measured against live provider_parts on 2026-09-02, over a
# 300-row sample per distributor, scoring each on _is_informative (does the row say anything beyond
# a redundant sku/brand/description echo) and average flattened length. The percentages in the
# trailing comments are that measurement. Re-run it before trusting this order again -- feeds are
# improved distributor by distributor and this list goes stale quietly.
PROVIDER_PRIORITY = [
    "TURN_14",              # 80% informative, 118 chars -- rich when it is rich, a bare echo when
                            # it is not; kept first because _is_informative now correctly falls
                            # through the echoes instead of being fooled by them.
    "PREMIER_PERFORMANCE",  # 100%, 556 chars. product_type.py: closed vocab + real PCdb hints
    "MEYER",                # 100%, 176 chars. product_type.py: closed vocab + real PCdb hints
    "WHEELPROS",            # 100%, 341 chars. product_type.py: structural feed signal
    "TIRERACK",             # 100%, 158 chars. Confirmed rich real tire titles
    "ROUGH_COUNTRY",        # 100%, 216 chars
    "MOTOR_STATE_DISTRIBUTING",  # 100%, 204 chars -- see the note below; promoted out of the
                                 # no-signal tier, it carries a dedicated long_description
    "THE_WHEEL_GROUP",      # 100%, 198 chars. product_type.py: structural feed signal
    "KEYSTONE",             # 100%, 192 chars -- also promoted out of the no-signal tier
    "ELITE_WHEEL",          # 100%, 123 chars. product_type.py: structural feed signal
    "HELMHOUSE",            # 100%, 132 chars. product_type.py: closed category vocab
    "DLG",                  # 100%, 112 chars -- promoted out of the no-signal tier
    "QUADRATEC",            # 100%, 109 chars -- promoted out of the no-signal tier
    "WESTERN_POWER_SPORTS", # 100%, 96 chars. product_type.py: closed category vocab
    "VOSSEN",               # 100%, 86 chars. product_type.py: structural feed signal
]

# The one distributor still confirmed to ship no usable descriptive signal: 63% informative at 69
# average characters, the worst of both measures by a wide margin, and no field beyond a redundant
# sku/brand/description echo.
#
# The other four that used to sit here -- KEYSTONE, MOTOR_STATE_DISTRIBUTING, QUADRATEC, DLG -- were
# demoted on the strength of src/integrations/utils/product_type.py's "1.65M of 3.2M master parts
# reach us only through distributors that ship no type signal at all". That finding is about
# PRODUCT TYPE (is this a wheel, a tire or a part), which is a different question from part
# terminology: a feed can be useless for the first and excellent for the second. Motor State is the
# clearest case -- it ships a long_description that names the part type outright ("Transmission Pan
# Gasket - Perm-Align - 0.060 in Thick", "Valve Lash Cap - 0.120 in Thick - 11/32 in Valve Stems")
# and its feed was moved onto FTP with categories and images since that tiering was written. All
# four now measure 100% informative, so keeping them below every unranked distributor was costing
# real signal.
PROVIDER_LOW_SIGNAL = ["ATECH"]

_UNRANKED_RANK = 500  # better than confirmed-bad, worse than confirmed-good

STAGE1_MAX_TOKENS_PER_PART = 60  # includes the reasoning field
STAGE2_MAX_TOKENS_PER_PART = 60

# Flat allowance added to every token budget on top of the per-part figures above, for output that
# does not scale with the batch: the JSON envelope, and -- the reason this is as large as it is --
# a reasoning model's hidden chain of thought, which is charged to completion_tokens and spent
# BEFORE the first character of JSON.
#
# Measured on gpt-oss:120b via ollama.com: 240 reasoning tokens for a single part, ~820 for ten,
# against a per-part JSON cost of only ~48. Without this allowance a batch of 5 got 500 tokens,
# spent 447 of them reasoning, and truncated mid-JSON.
#
# This also fixes a subtler failure. complete_json_with_retry halves a failed batch on the theory
# that the output was too big -- true when output scales with parts, false for a fixed reasoning
# cost that halving does not reduce at all. Recomputing the child budget from parts alone made it
# SMALLER at every level, so a truncation caused by reasoning could never be recovered: the retry
# burned 2^6 calls and still failed. Keeping this floor intact through the recursion is what makes
# halving converge instead of chasing a cost it cannot shrink.
#
# max_tokens is a ceiling, not a spend -- a model that stops early is not billed for the headroom
# -- so this is deliberately generous rather than tuned to the measurements above.
_RESPONSE_OVERHEAD_TOKENS = 1_500

# Reasoning models spend completion tokens on hidden reasoning before answering. Measured on
# gpt-oss:120b at ten parts: "low" spends 67 reasoning tokens, "medium" 407, the model's own
# default 820, and "high" ran to 9,300 and blew the budget without ever emitting JSON. "low"
# returned all ten answers correctly, so more thinking bought nothing here -- both stages are
# structured pick-from-a-list tasks, not open problems. Sent only when set; see qwen_llm.
DEFAULT_REASONING_EFFORT = "low"

# Stage 2 normally sends the whole pooled candidate list (see fix #1), and a few PCdb subcategory
# names pool to more terms than any context window holds. Measured against gpt-oss:120b (131,072
# tokens) via ollama.com on 2026-09-02:
#
#   Hardware, Fasteners and Fittings  11,601 candidates  ~488k tokens  hard 400, prompt too long
#   Brackets, Flanges and Hangers      1,919 candidates   ~75k tokens  fits, but returns nothing
#   Electrical Connectors              1,590 candidates   ~65k tokens  works
#
# A pool over this ceiling falls back to the single (category, subcategory) pair Stage 1 actually
# chose -- which is what a reader expects the second stage to use anyway, and which very nearly
# always fits: 4 of 240 pooled names exceed the ceiling, against 1 of 852 individual pairs. Of the
# 96 pairs sitting underneath an oversized pool, 95 fit on their own. The single exception is
# ('Body', 'Hardware, Fasteners and Fittings') at 3,522 terms, and parts routed there get an error
# row naming it rather than a request the endpoint rejects.
#
# What the fallback gives up is precisely what pooling bought: for those parts, a Stage 1 that
# picked the right subcategory name under the wrong category is no longer recoverable. That is a
# narrower loss than it sounds -- it applies only to the four biggest catch-all names -- but it is
# a real one, so the rows say so (see assemble_rows) instead of looking like ordinary results.
#
# The ceiling cannot be something the retry recovers from: complete_json_with_retry halves the PART
# batch, and the candidate list is the same size for one part as for fifty. A batch of eight routed
# to a too-big pool would halve its way through fifteen separate rejected requests.
MAX_STAGE2_CANDIDATES = 1_500

# How deep complete_json_with_retry may halve. 6 levels takes a batch of 64 down to size 1 with
# room to spare for any realistic batch_size; the cap exists to bound worst-case latency on a
# pathological input as much as to stop runaway recursion.
_MAX_RETRY_DEPTH = 6

# Chunk size for the provider_parts read. Purely to keep a single IN (...) from carrying a
# six-figure id list; unrelated to the LLM batch size.
_PROVIDER_FETCH_CHUNK = 5_000

_TRAILING_PAREN_RE = re.compile(r"\s*\([^)]*\)\s*$")


def _token_budget(part_count: int, per_part: int) -> int:
    """Per-part output plus the flat allowance. See :data:`_RESPONSE_OVERHEAD_TOKENS`."""
    return _RESPONSE_OVERHEAD_TOKENS + part_count * per_part

UPSERT_FIELDS = [
    "status",
    "category",
    "subcategory",
    "stage1_confidence",
    "part_terminology_id",
    "stage2_confidence",
    "reasoning",
    "model_used",
    "updated_at",
]


# ================================================================================================
# Provider hierarchy and part context
# ================================================================================================

def _provider_kind_name(kind: typing.Any) -> str:
    try:
        return enums.BrandProviderKind(kind).name
    except ValueError:
        return "UNKNOWN_{}".format(kind)


def _provider_rank(name: str) -> int:
    if name in PROVIDER_PRIORITY:
        return PROVIDER_PRIORITY.index(name)
    if name in PROVIDER_LOW_SIGNAL:
        return 1000 + PROVIDER_LOW_SIGNAL.index(name)
    return _UNRANKED_RANK


def _strip_example_hint(value: typing.Optional[str]) -> typing.Optional[str]:
    """
    The taxonomy given to Stage 1 decorates each subcategory with "(e.g. ...)" example terminology
    names. The prompt asks for the bare name back; this strips the parenthetical defensively in
    case the model echoes the whole decorated label instead.
    """
    if not value:
        return value
    return _TRAILING_PAREN_RE.sub("", value).strip()


def _flatten_product_details(product_details: typing.Any) -> str:
    if not product_details:
        return ""
    parts = []
    for item in product_details:
        if not isinstance(item, dict):
            continue
        value = item.get("value")
        if value in (None, "", False):
            continue
        parts.append("{}: {}".format(item.get("label"), value))
    return "; ".join(parts)


def _is_informative(raw_text: str, description: str, brand: str) -> bool:
    """
    True if raw_text says something beyond a redundant echo of sku/brand/description.

    Case-insensitive, and that is load-bearing rather than tidiness. Brands are stored upper-case
    ("WISECO") and echoed by distributors in title case ("Brand: Wiseco"), so a case-sensitive
    strip left the brand sitting in the residual and scored a pure echo as informative. That
    silently disabled the whole fall-through this function exists for: Wiseco piston ring sets
    took Turn 14's "SKU: wis3810A; Brand: Wiseco; Description: <the description again>" over Motor
    State's "Single Cyl. Piston Ring Set 3.810 Bore", and Stage 2 then refused them because
    nothing in the text said "piston".
    """
    if not raw_text:
        return False
    residual = raw_text
    for known in (description, brand):
        if known:
            residual = re.sub(re.escape(known), "", residual, flags=re.IGNORECASE)
    residual = re.sub(r"\b(sku|brand|description)\s*:\s*", "", residual, flags=re.IGNORECASE)
    return len(residual.strip(" ;:")) > 10


def build_part_context(brand: str, description: str, provider_rows: typing.Sequence[tuple]) -> str:
    """
    provider_rows: ``(provider_kind, product_details)`` for one MasterPart, from every ProviderPart
    row it has. Walks PROVIDER_PRIORITY best-first and uses the first provider whose data is
    genuinely informative; falls back to the best-ranked available provider's data even if thin
    when none qualify.
    """
    candidates = [
        (_provider_kind_name(kind), _flatten_product_details(product_details))
        for kind, product_details in provider_rows
    ]
    candidates.sort(key=lambda candidate: _provider_rank(candidate[0]))

    for name, raw_text in candidates:
        if _is_informative(raw_text, description, brand):
            return "{} {} [{}: {}]".format(brand, description, name, raw_text).strip()

    if candidates:
        name, raw_text = candidates[0]
        if raw_text:
            return "{} {} [{}: {}]".format(brand, description, name, raw_text).strip()
    return "{} {}".format(brand, description).strip()


# ================================================================================================
# Prompts.
#
# Deliberately free of hardcoded "this exact title means this exact terminology" examples: there is
# no formally verified ground truth for this task, so baking in specific answers would teach the
# model our own guesses rather than let it reason from the real candidate list. What IS included is
# STRUCTURAL fact about the taxonomy and the data (verified against the real PCdb corpus, not
# opinions about specific parts), plus instructions aimed at failure modes found in production
# testing -- see fixes #1, #2 and #4 in the module docstring.
# ================================================================================================

STAGE1_SYSTEM = """You route each auto-parts product to the single best-matching (category, subcategory)
pair from the AutoCare PCdb taxonomy given below. Format: "Category: subcategory1 (e.g. example terms
in that subcategory); subcategory2 (e.g. ...); ...". The parenthetical examples are real terminology
names that live in that subcategory -- use them as your main signal for whether a subcategory is the
right fit, since a subcategory's own NAME is often a poor match for how a product is actually
described (e.g. a "cargo liner" product might belong under a subcategory named something like "Trunk
Lid and Compartment" that doesn't read as a liner at all -- the examples are what would reveal that).

Titles are often terse, abbreviated, or (for wheels and tires especially) mostly size/spec codes with
few or no descriptive words -- e.g. a wheel title commonly looks like "diameter X width, bolt pattern,
backspacing/offset" with no word "wheel" anywhere. Use brand context, general automotive domain
knowledge, and the formatting of the text itself to infer the part type; don't require an exact
keyword match before committing to an answer.

IMPORTANT: the same subcategory NAME can exist under more than one category, with a different and
non-overlapping list of terminologies under each -- e.g. "Fuel Injection System and Related
Components" exists as its own subcategory under both "Engine" and "Air and Fuel Delivery". Picking
the right subcategory name is not enough on its own: read the full taxonomy line for the category
you're considering and make sure that specific category is the right domain for this part, not just
that the subcategory name sounds plausible.

For each part, briefly state why you chose that category/subcategory (one short phrase is enough) --
this is for auditing your own routing, not a formality. Only return null for a part when the text
truly gives no signal at all about what kind of product it is.

Return strict JSON: {"parts": [{"id": <int>, "category": "<exact category>", "subcategory":
"<exact subcategory name only, WITHOUT the parenthetical examples>", "confidence": <0.0-1.0>,
"reasoning": "<short phrase>"} or {"id": <int>, "category": null, "subcategory": null, "confidence":
0.0, "reasoning": "<why nothing fits>"}, ...]} -- one entry per part given, in any order.
category/subcategory MUST be copied verbatim from the taxonomy list given (minus the "(e.g. ...)"
part)."""

STAGE2_SYSTEM = """You classify each auto-parts product into the single best-matching PCdb part
terminology from the candidate list given (scoped to one category/subcategory chosen by an earlier
routing step -- that earlier step can be wrong, see below).

Pick the candidate whose name/description most precisely describes what the product actually IS.
PCdb terminology for wheels/tires is typically generic (just "Wheel" or "Tire"), not size/finish/
style-specific -- that detail lives in fitment/application data, not the terminology name, so don't
expect or require a size match in the candidate name.

The category/subcategory this candidate list came from was chosen by a separate step that can be
wrong (the same subcategory name can exist under a different, unrelated category). If NONE of the
given candidates are even plausibly the same kind of product -- not just an imperfect match, but
genuinely a different domain -- say so explicitly in your reasoning and return terminology_id: null.
Do not force a match to the least-bad candidate just because the list isn't empty; a wrong routing
upstream means every candidate here can legitimately be wrong.

WATCH FOR THIS SPECIFIC TRAP: each candidate carries its own "category" field, which can differ
from the routed category/subcategory given above -- the list is pooled from every category that
happens to share this subcategory name, so it may mix candidates from genuinely unrelated systems
(e.g. a generic-sounding subcategory that pulls in both a universal hardware part and something
scoped to one specific system, like a candidate whose own "category" is "Brake" or "Transmission",
mixed in among otherwise-generic candidates). Before picking a candidate, check whether its
"category" matches a system the product is actually part of -- if a candidate's category ties it
to a specific system/vehicle area the product has no real connection to, that is a genuinely
different domain and disqualifies it the same as if it weren't in the list at all, even if its name
is the closest word-match among the given options. When most candidates share one category and one
candidate's category stands out as different, that mismatch itself is a signal to distrust it.

Return strict JSON: {"parts": [{"id": <int>, "terminology_id": <int or null>, "confidence": <0.0-1.0>,
"reasoning": "<one sentence>"}, ...]} -- one entry per part given. terminology_id MUST be one of the
candidate ids given, or null if none fit."""


# ================================================================================================
# Taxonomy
# ================================================================================================

@dataclasses.dataclass(frozen=True)
class Taxonomy:
    """
    The active PCdb corpus in the three shapes the pipeline needs: the (category, subcategory)
    pairs Stage 1 is allowed to return, the by-subcategory-NAME pooling Stage 2 draws candidates
    from (fix #1), and the rendered text handed to Stage 1.
    """

    by_pair: typing.Dict[typing.Tuple[str, str], typing.List[dict]]
    by_subcategory_name: typing.Dict[str, typing.List[dict]]
    text: str

    @property
    def terminology_count(self) -> int:
        return sum(len(rows) for rows in self.by_pair.values())


def load_terminology_rows() -> typing.List[dict]:
    return list(
        src_models.PcdbTerminologyFlat.objects.filter(is_active=True).values(
            "part_terminology_id", "name", "category_name", "subcategory_name", "description"
        )
    )


def build_taxonomy(rows: typing.Optional[typing.Sequence[dict]] = None, examples_per_subcategory: int = 3) -> Taxonomy:
    """
    Rows default to the active ``pcdb_terminology_flat`` corpus; the argument exists so the shape
    of this can be tested without a database.

    Pooling is by subcategory NAME alone, across every category that has one -- ~62% of PCdb
    subcategory names exist under more than one category (e.g. "Fuel Injection System and Related
    Components" under both "Engine" and "Air and Fuel Delivery", each with a different,
    non-overlapping term list). Stage 1 picking the right subcategory name but the wrong category
    is a real, confirmed failure mode (FUEL INJECTOR routed to "Engine" instead of "Air and Fuel
    Delivery"); pooling means Stage 2 sees the term either way. It does NOT fix Stage 1 picking the
    wrong subcategory NAME entirely -- a different, harder failure mode, confirmed separately on
    SPEAKER, routed to "Electronic Accessories" when the real term lives under the unrelated
    "Mobile Multi-Media". Only a same-name cross-category collision is recoverable this way.
    """
    if rows is None:
        rows = load_terminology_rows()

    by_pair = collections.defaultdict(list)
    by_subcategory_name = collections.defaultdict(list)
    for row in rows:
        category, subcategory = row["category_name"], row["subcategory_name"]
        if not category or not subcategory:
            # A pair Stage 1 could never name is a pair Stage 2 could never be routed to. Dropping
            # these keeps the rendered taxonomy joinable rather than tripping over a None label.
            continue
        by_pair[(category, subcategory)].append(row)
        by_subcategory_name[subcategory].append(row)

    # Each subcategory label carries a few real example terminology names, shortest first as a
    # proxy for "most generic/representative" rather than a deeply specific hardware term. See fix
    # #2 in the module docstring for why the bare name alone is not enough.
    by_category = collections.defaultdict(list)
    for (category, subcategory), pair_rows in by_pair.items():
        examples = [r["name"] for r in sorted(pair_rows, key=lambda r: len(r["name"]))[:examples_per_subcategory]]
        label = "{} (e.g. {})".format(subcategory, ", ".join(examples)) if examples else subcategory
        by_category[category].append(label)

    text = "\n".join(
        "{}: {}".format(category, "; ".join(sorted(labels)))
        for category, labels in sorted(by_category.items(), key=lambda item: item[0] or "")
    )
    return Taxonomy(by_pair=dict(by_pair), by_subcategory_name=dict(by_subcategory_name), text=text)


# ================================================================================================
# Reads
# ================================================================================================

def select_parts(
    brand_ids: typing.Optional[typing.Sequence[int]] = None,
    limit: typing.Optional[int] = None,
    include_classified: bool = False,
) -> typing.List[dict]:
    """
    Returns ``[{"id", "description", "brand"}, ...]``.

    With more than one brand and a limit, samples evenly across brands rather than taking the
    lowest master_part ids overall -- a caller who names five brands and asks for 100 parts wants
    twenty of each, not a hundred of whichever brand happens to sort first.
    """
    queryset = src_models.MasterPart.objects.exclude(description__isnull=True).exclude(description="")
    if brand_ids:
        queryset = queryset.filter(brand_id__in=brand_ids)
    if not include_classified:
        queryset = queryset.filter(ml_terminology_classification__isnull=True)

    if brand_ids and len(brand_ids) > 1 and limit:
        per_brand = math.ceil(limit / len(brand_ids))
        rows: typing.List[tuple] = []
        for brand_id in brand_ids:
            rows.extend(
                queryset.filter(brand_id=brand_id)
                .order_by("id")
                .values_list("id", "description", "brand__name")[:per_brand]
            )
        rows.sort(key=lambda row: (row[2] or "", row[0]))
        rows = rows[:limit]
    else:
        queryset = queryset.order_by("id").values_list("id", "description", "brand__name")
        rows = list(queryset[:limit] if limit else queryset)

    return [{"id": row[0], "description": row[1], "brand": row[2]} for row in rows]


def provider_rows_by_part(master_part_ids: typing.Sequence[int]) -> typing.Dict[int, typing.List[tuple]]:
    """Returns ``{master_part_id: [(provider_kind, product_details), ...]}``."""
    out = collections.defaultdict(list)
    ids = list(master_part_ids)
    for start in range(0, len(ids), _PROVIDER_FETCH_CHUNK):
        chunk = ids[start:start + _PROVIDER_FETCH_CHUNK]
        rows = src_models.ProviderPart.objects.filter(master_part_id__in=chunk).values_list(
            "master_part_id", "provider__kind", "product_details"
        )
        for master_part_id, kind, product_details in rows:
            out[master_part_id].append((kind, product_details))
    return dict(out)


# ================================================================================================
# LLM calls
# ================================================================================================

def complete_json_with_retry(
    client: typing.Any,
    model: str,
    system: str,
    user_payload: dict,
    per_part_tokens: int,
    parts_key: str = "parts",
    reasoning_effort: typing.Optional[str] = DEFAULT_REASONING_EFFORT,
    _depth: int = 0,
) -> typing.Tuple[typing.Optional[dict], typing.Optional[str]]:
    """
    Recursive retry on parse failure, halving the batch each time. A parse failure is usually the
    response getting cut off, and the fix is less output to generate, not a blind retry of the same
    oversized request -- the same truncation-recovery pattern already validated in this project's
    grouped classification pipeline.

    Recurses all the way down to single-part calls rather than giving up after one halving. That is
    confirmed necessary on real data: a single level left some persistently-oversized batches
    unrecovered (15 parts hard-failed in one production test run before this was made recursive),
    and per-part output size is what is actually oversized, not the batch as a whole.
    """
    parts = user_payload.get(parts_key, [])
    parsed, err = qwen_llm.complete_json(
        client,
        system,
        json.dumps(user_payload),
        max_tokens=_token_budget(len(parts), per_part_tokens),
        model=model,
        reasoning_effort=reasoning_effort,
    )
    if parsed is not None:
        return parsed, None

    if len(parts) <= 1 or _depth >= _MAX_RETRY_DEPTH:
        return None, err

    logger.warning(
        "%s Batch of %d failed (%s) at retry depth %d, retrying as two halves",
        _LOG_PREFIX, len(parts), err, _depth,
    )
    half = len(parts) // 2
    merged: typing.Dict[str, list] = {parts_key: []}
    any_failed = False
    for sub_parts in (parts[:half], parts[half:]):
        sub_payload = dict(user_payload)
        sub_payload[parts_key] = sub_parts
        sub_parsed, sub_err = complete_json_with_retry(
            client,
            model,
            system,
            sub_payload,
            per_part_tokens=per_part_tokens,
            parts_key=parts_key,
            reasoning_effort=reasoning_effort,
            _depth=_depth + 1,
        )
        if sub_parsed is None:
            any_failed = True
            logger.warning("%s Retry half-batch of %d exhausted retries: %s", _LOG_PREFIX, len(sub_parts), sub_err)
            continue
        merged[parts_key].extend(sub_parsed.get(parts_key, []))

    if any_failed and not merged[parts_key]:
        return None, err
    return merged, None


def _entry_id(entry: dict) -> typing.Any:
    """
    Response ids come back as whatever the model felt like emitting. Self-hosted models are
    noticeably looser about this than a hosted API -- "id": "101" instead of 101 is common -- and
    a string id would miss the lookup below and turn a perfectly good answer into a "missing from
    response" unclassifiable. Coerce what can be coerced and leave the rest to that fallback.
    """
    value = entry.get("id")
    try:
        return int(value)
    except (TypeError, ValueError):
        return value


def run_stage1_batch(
    client,
    model: str,
    taxonomy_text: str,
    batch: typing.Sequence[dict],
    reasoning_effort: typing.Optional[str] = DEFAULT_REASONING_EFFORT,
) -> typing.Dict[int, dict]:
    """batch: ``{id, brand, description, context}`` dicts. Returns ``{part_id: entry}``."""
    payload = {"taxonomy": taxonomy_text, "parts": [{"id": p["id"], "text": p["context"]} for p in batch]}
    parsed, err = complete_json_with_retry(
        client, model, STAGE1_SYSTEM, payload,
        per_part_tokens=STAGE1_MAX_TOKENS_PER_PART, reasoning_effort=reasoning_effort,
    )
    if err or not parsed:
        logger.warning("%s Stage 1 batch of %d failed: %s", _LOG_PREFIX, len(batch), err)
        return {
            p["id"]: {"category": None, "subcategory": None, "confidence": 0.0, "error": err}
            for p in batch
        }

    result = {_entry_id(entry): entry for entry in parsed.get("parts", []) if isinstance(entry, dict)}
    for p in batch:
        if p["id"] not in result:
            result[p["id"]] = {
                "category": None, "subcategory": None, "confidence": 0.0, "error": "missing from response",
            }
    return result


def run_stage2_batch(
    client,
    model: str,
    category: str,
    subcategory: str,
    candidates: typing.Sequence[dict],
    batch: typing.Sequence[dict],
    reasoning_effort: typing.Optional[str] = DEFAULT_REASONING_EFFORT,
) -> typing.Dict[int, dict]:
    """
    Each candidate carries its OWN real category, which can differ from the routed ``category``
    since candidates are pooled across every category sharing this subcategory name. Showing it as
    its own field, not buried in free-text description, is what lets Stage 2 actually see a domain
    mismatch instead of having to infer it -- confirmed necessary: without it a generic rubber
    engine expansion plug got confidently matched to "Drum Brake Plug" (pooled in from the
    unrelated "Brake" category) purely because the word "Plug" overlapped, even after the system
    prompt was strengthened with a warning about exactly this trap. Prompt wording alone did not
    override the superficial word-match.
    """
    payload = {
        "category": category,
        "subcategory": subcategory,
        "candidates": [
            {
                "id": t["part_terminology_id"],
                "name": t["name"],
                "category": t["category_name"],
                "description": t["description"] or t["name"],
            }
            for t in candidates
        ],
        "parts": [{"id": p["id"], "text": p["context"]} for p in batch],
    }
    parsed, err = complete_json_with_retry(
        client, model, STAGE2_SYSTEM, payload,
        per_part_tokens=STAGE2_MAX_TOKENS_PER_PART, reasoning_effort=reasoning_effort,
    )
    if err or not parsed:
        logger.warning(
            "%s Stage 2 batch [%s/%s] of %d failed: %s", _LOG_PREFIX, category, subcategory, len(batch), err
        )
        return {
            p["id"]: {"terminology_id": None, "confidence": 0.0, "reasoning": err, "error": err}
            for p in batch
        }

    result = {_entry_id(entry): entry for entry in parsed.get("parts", []) if isinstance(entry, dict)}
    for p in batch:
        if p["id"] not in result:
            result[p["id"]] = {
                "terminology_id": None, "confidence": 0.0,
                "reasoning": "missing from response", "error": "missing",
            }
    return result


# ================================================================================================
# Result assembly and persistence
# ================================================================================================

def _coerce_confidence(value: typing.Any) -> typing.Optional[decimal.Decimal]:
    """
    The confidence columns are ``DecimalField(max_digits=3, decimal_places=2)``, so anything the
    model returns has to land in [0, 1] at two decimal places. A model that answers 0.923, "0.9" or
    a stray 1.5 should cost us the precision, not the whole row -- the confidence is advisory, the
    classification is the payload.
    """
    if value is None:
        return None
    try:
        parsed = decimal.Decimal(str(value))
    except (decimal.InvalidOperation, TypeError, ValueError):
        return None
    if not parsed.is_finite():
        return None
    parsed = min(max(parsed, decimal.Decimal("0")), decimal.Decimal("1"))
    return parsed.quantize(decimal.Decimal("0.01"), rounding=decimal.ROUND_HALF_UP)


def assemble_rows(
    routed: typing.Dict[typing.Tuple[str, str], typing.List[dict]],
    unroutable: typing.Sequence[dict],
    stage2_results: typing.Dict[int, dict],
    model: str,
) -> typing.List[dict]:
    """
    EVERY part gets a row, including failures, unroutables and nulls.

    That is the point of the ``status`` column: "the model looked and found nothing"
    (unclassifiable, with reasoning) is a real and useful answer, and "the pipeline itself failed"
    (error, with the exception or parse-failure text) needs to stay queryable rather than showing
    up as an absence indistinguishable from "not processed yet".
    """
    statuses = src_models.MLPartTerminologyClassification
    rows = []

    for part in unroutable:
        stage1_error = part.get("stage1_error")
        rows.append({
            "master_part_id": part["id"],
            "status": statuses.STATUS_ERROR if stage1_error else statuses.STATUS_UNCLASSIFIABLE,
            "category": None,
            "subcategory": None,
            "stage1_confidence": _coerce_confidence(part.get("stage1_confidence")),
            "part_terminology_id": None,
            "stage2_confidence": None,
            "reasoning": "Stage 1: {}".format(
                stage1_error or part.get("stage1_reasoning") or "no matching (category, subcategory) found"
            ),
            "model_used": model,
        })

    for (category, subcategory), group_parts in routed.items():
        for part in group_parts:
            stage2 = stage2_results.get(part["id"], {})
            terminology_id = stage2.get("terminology_id")
            if stage2.get("error"):
                status = statuses.STATUS_ERROR
            elif terminology_id is None:
                status = statuses.STATUS_UNCLASSIFIABLE
            else:
                status = statuses.STATUS_CLASSIFIED

            reasoning = []
            if part.get("stage1_reasoning"):
                reasoning.append("Stage 1: {}".format(part["stage1_reasoning"]))
            if part.get("stage2_narrowed"):
                # Worth carrying into the row: these parts were judged against one category's
                # terms rather than the pooled list, so a cross-category miss upstream was not
                # recoverable for them the way it is everywhere else.
                reasoning.append(
                    "Stage 2 saw only this category's {} terms (pool too big to send)".format(
                        part["stage2_narrowed"]
                    )
                )
            if stage2.get("reasoning") or stage2.get("error"):
                reasoning.append("Stage 2: {}".format(stage2.get("reasoning") or stage2.get("error")))

            rows.append({
                "master_part_id": part["id"],
                "status": status,
                "category": category,
                "subcategory": subcategory,
                "stage1_confidence": _coerce_confidence(part.get("stage1_confidence")),
                "part_terminology_id": terminology_id,
                "stage2_confidence": _coerce_confidence(stage2.get("confidence")),
                "reasoning": " | ".join(reasoning) or None,
                "model_used": model,
            })

    return rows


def persist(rows: typing.Sequence[dict]) -> int:
    if not rows:
        return 0
    pgbulk.upsert(
        src_models.MLPartTerminologyClassification,
        [src_models.MLPartTerminologyClassification(**row) for row in rows],
        unique_fields=["master_part"],
        update_fields=UPSERT_FIELDS,
    )
    logger.info("%s Wrote %d row(s) to ml_part_terminology_classification", _LOG_PREFIX, len(rows))
    return len(rows)


def _chunked(items: typing.Sequence, size: int):
    for start in range(0, len(items), size):
        yield items[start:start + size]


# ================================================================================================
# Orchestration
# ================================================================================================

def run(
    brand_ids: typing.Optional[typing.Sequence[int]] = None,
    limit: typing.Optional[int] = None,
    batch_size: int = 50,
    max_workers: int = 8,
    include_classified: bool = False,
    apply_changes: bool = False,
    base_url: typing.Optional[str] = None,
    api_key: typing.Optional[str] = None,
    model: typing.Optional[str] = None,
    reasoning_effort: typing.Optional[str] = DEFAULT_REASONING_EFFORT,
    examples_per_subcategory: int = 3,
    max_candidates: int = MAX_STAGE2_CANDIDATES,
) -> dict:
    """
    Read-only unless ``apply_changes`` is passed, matching the other bulk classification commands
    in this project -- a bare run classifies everything and reports what it would write.

    base_url/api_key/model default to the QWEN_* environment (see
    :mod:`src.integrations.llm.qwen_llm`); pass them to point one run at a different endpoint
    without touching the environment.
    """
    started = time.monotonic()

    logger.info("%s Loading PCdb terminology corpus...", _LOG_PREFIX)
    taxonomy = build_taxonomy(examples_per_subcategory=examples_per_subcategory)
    logger.info(
        "%s Loaded %d active terminologies across %d (category, subcategory) pair(s)",
        _LOG_PREFIX, taxonomy.terminology_count, len(taxonomy.by_pair),
    )

    logger.info(
        "%s Selecting parts (brand_ids=%s, limit=%s, include_classified=%s)...",
        _LOG_PREFIX, brand_ids, limit, include_classified,
    )
    parts = select_parts(brand_ids=brand_ids, limit=limit, include_classified=include_classified)
    logger.info("%s Found %d part(s) to classify", _LOG_PREFIX, len(parts))
    if not parts:
        return {
            "parts": 0, "routed": 0, "unroutable": 0, "pairs": 0, "oversized_pool": 0, "narrowed_pool": 0,
            "classified": 0, "unclassifiable": 0, "errors": 0,
            "written": 0, "applied": apply_changes, "rows": [], "model": model, "elapsed": 0.0,
        }

    logger.info("%s Fetching provider data for %d part(s)...", _LOG_PREFIX, len(parts))
    by_part = provider_rows_by_part([p["id"] for p in parts])
    for part in parts:
        part["context"] = build_part_context(part["brand"], part["description"], by_part.get(part["id"], []))

    client = qwen_llm.client(base_url=base_url, api_key=api_key)
    model = model or qwen_llm.model_name()

    # ---- Stage 1 ----
    stage1_batches = list(_chunked(parts, batch_size))
    logger.info(
        "%s Stage 1: %d batch(es) of up to %d part(s), %d worker(s)",
        _LOG_PREFIX, len(stage1_batches), batch_size, max_workers,
    )
    stage1_results: typing.Dict[int, dict] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = [
            pool.submit(run_stage1_batch, client, model, taxonomy.text, batch, reasoning_effort)
            for batch in stage1_batches
        ]
        for done, future in enumerate(as_completed(futures), start=1):
            stage1_results.update(future.result())
            logger.info("%s Stage 1 progress: %d/%d batches", _LOG_PREFIX, done, len(stage1_batches))

    routed = collections.defaultdict(list)
    unroutable = []
    for part in parts:
        result = stage1_results.get(part["id"], {})
        category = _strip_example_hint(result.get("category"))
        subcategory = _strip_example_hint(result.get("subcategory"))
        part["stage1_confidence"] = result.get("confidence")
        part["stage1_reasoning"] = result.get("reasoning")
        part["stage1_error"] = result.get("error")
        if category and subcategory and (category, subcategory) in taxonomy.by_pair:
            routed[(category, subcategory)].append(part)
        else:
            unroutable.append(part)
    logger.info(
        "%s Stage 1 done in %.1fs: %d part(s) routed across %d pair(s), %d unroutable",
        _LOG_PREFIX, time.monotonic() - started, sum(len(v) for v in routed.values()), len(routed), len(unroutable),
    )

    # ---- Stage 2 ----
    # Every terminology sharing the assigned subcategory NAME is shown, pooled across categories
    # (see build_taxonomy) -- no narrowing, so the correct term can never be excluded by a
    # retrieval step missing it. Confirmed failure mode: a narrowed top-40 shortlist excluded the
    # correct "Manual Transmission Shifter Block Off Plate" from a 365-term pair. The tradeoff is a
    # much bigger prompt for the largest pooled names, so a small batch_size may be needed if the
    # model's context window is tight.
    jobs = []
    stage2_results: typing.Dict[int, dict] = {}
    narrowed_parts = 0
    oversized = 0
    for (category, subcategory), group_parts in routed.items():
        candidates = taxonomy.by_subcategory_name[subcategory]
        narrowed = len(candidates) > max_candidates
        if narrowed:
            # Fall back to the pair Stage 1 chose. See MAX_STAGE2_CANDIDATES for what this costs.
            candidates = taxonomy.by_pair[(category, subcategory)]
            logger.warning(
                "%s Pool '%s' has %d candidates (ceiling %d); falling back to the '%s' pair alone "
                "(%d candidates) for %d part(s) -- no cross-category recovery for these",
                _LOG_PREFIX, subcategory, len(taxonomy.by_subcategory_name[subcategory]),
                max_candidates, category, len(candidates), len(group_parts),
            )

        if len(candidates) > max_candidates:
            oversized += len(group_parts)
            logger.error(
                "%s Pair '%s' / '%s' is %d candidates, over the %d ceiling even unpooled; "
                "%d part(s) skipped rather than sent as a request the endpoint will reject",
                _LOG_PREFIX, category, subcategory, len(candidates), max_candidates, len(group_parts),
            )
            for part in group_parts:
                stage2_results[part["id"]] = {
                    "terminology_id": None,
                    "confidence": 0.0,
                    "error": "'{}' / '{}' has {} terms, over the {} ceiling even unpooled".format(
                        category, subcategory, len(candidates), max_candidates
                    ),
                }
            continue

        if narrowed:
            narrowed_parts += len(group_parts)
            for part in group_parts:
                part["stage2_narrowed"] = len(candidates)
        for batch in _chunked(group_parts, batch_size):
            jobs.append((category, subcategory, candidates, batch))

    logger.info("%s Stage 2: %d batch job(s), %d worker(s)", _LOG_PREFIX, len(jobs), max_workers)
    stage2_started = time.monotonic()
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = [
            pool.submit(run_stage2_batch, client, model, category, subcategory, candidates, batch, reasoning_effort)
            for category, subcategory, candidates, batch in jobs
        ]
        for done, future in enumerate(as_completed(futures), start=1):
            stage2_results.update(future.result())
            logger.info("%s Stage 2 progress: %d/%d batch jobs", _LOG_PREFIX, done, len(jobs))
    logger.info("%s Stage 2 done in %.1fs", _LOG_PREFIX, time.monotonic() - stage2_started)

    rows = assemble_rows(routed, unroutable, stage2_results, model)
    counts = collections.Counter(row["status"] for row in rows)
    statuses = src_models.MLPartTerminologyClassification
    elapsed = time.monotonic() - started
    logger.info(
        "%s SUMMARY: %d classified, %d unclassifiable, %d error(s) (total %d, elapsed %.1fs)",
        _LOG_PREFIX,
        counts[statuses.STATUS_CLASSIFIED],
        counts[statuses.STATUS_UNCLASSIFIABLE],
        counts[statuses.STATUS_ERROR],
        len(rows),
        elapsed,
    )

    return {
        "parts": len(parts),
        "routed": sum(len(v) for v in routed.values()),
        "unroutable": len(unroutable),
        "oversized_pool": oversized,
        "narrowed_pool": narrowed_parts,
        "pairs": len(routed),
        "classified": counts[statuses.STATUS_CLASSIFIED],
        "unclassifiable": counts[statuses.STATUS_UNCLASSIFIABLE],
        "errors": counts[statuses.STATUS_ERROR],
        "written": persist(rows) if apply_changes else 0,
        "applied": apply_changes,
        "rows": rows,
        "model": model,
        "elapsed": elapsed,
    }
