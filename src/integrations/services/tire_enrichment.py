"""
Per-tire LLM enrichment: one call per master part, producing one ``TireSpec`` row.

Three sources feed a tire spec and the split between them is the whole design:

  ``src.domain.tire_size``   everything encoded in the sidewall string -- size, service type,
                             load index, speed rating, load range. Deterministic, and the
                             **source of truth**. The model is forbidden from returning any of
                             it (system prompt rule 5, enforced by ``_check_size_leak``).
  the LLM                    everything that needs knowing the tire market: what "TER GRAP G3"
                             actually is, what tread category it belongs to, what a customer
                             would type to find it.
  distributor feeds          structured specs where a feed happens to ship them (tread depth,
                             UTQG, max PSI). See ``_STRUCTURED_FIELD_KEYS``.

Precedence when writing is **distributor structured field -> parser -> LLM**; the model only
fills what the other two cannot.

**The parser is also the gate.** Candidate selection does not trust ``product_type`` -- as of the
2026-08-25 survey all 1,593 NITTO master parts are unclassified, so waiting for the type would
mean enriching nothing. Instead, any master part whose titles decode to a tire size is offered to
the model, which confirms or denies with ``is_tire``. That gate was measured against production
before being relied on: over 61,715 Wheel Pros wheel and accessory rows it produced **zero** false
positives, and it recovered 98.7% of that distributor's 5,807 known tire rows.

Validation **rejects, it does not repair** (see ``validate``). A response carrying a dimension is
treated as prompt drift and the whole response is dropped, not patched -- if the model has started
inventing sizes, the rest of what it said is suspect too.

Nothing here is Nitto-specific. ``--brands`` scopes a run; the pipeline itself is brand-agnostic.
"""
import concurrent.futures
import dataclasses
import decimal
import json
import logging
import re
import typing

from django.db import connection, transaction
from django.utils import timezone

from src import enums as src_enums
from src import models as src_models
from src.domain import tire_size
from src.integrations.llm import azure_llm
from src.integrations.utils import product_type as product_type_utils

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[TIRE-ENRICHMENT]"

# Keyset page over master_parts. Small relative to the enrichment cost -- the LLM call dominates,
# so there is nothing to gain from a bigger page and a smaller one keeps memory flat.
CANDIDATE_BATCH_SIZE = 500

# Which master parts a run considers.
MODE_MISSING = "missing"  # no tire_specs row at all
MODE_INCOMPLETE = "incomplete"  # row exists but the model failed to identify it
MODE_ALL = "all"
MODES = (MODE_MISSING, MODE_INCOMPLETE, MODE_ALL)

# Recorded on MasterPart.product_type_source when the model says a row is not a tire. product_type
# itself stays NULL -- "not a tire" is not a classification, and this module has no business
# deciding between wheel and part. The marker exists so a re-run doesn't pay for the same verdict
# twice; ``--include-rejected`` overrides it.
NOT_A_TIRE_SOURCE = "llm:tire_enrichment:not-a-tire"
TIRE_SOURCE = "llm:tire_enrichment"

# Below this, an is_3pmsf=true is stored as NULL. Severe-snow certification has legal weight in
# some jurisdictions, so a maybe is worth less than an unknown.
IS_3PMSF_MIN_CONFIDENCE = decimal.Decimal("0.80")

# Prompt budget. Titles past the first few are near-duplicates of each other, and every extra one
# is tokens on every call in the catalog.
MAX_TITLES = 8
MAX_PART_NUMBERS = 8
MAX_CATEGORIES = 6
LLM_MAX_TOKENS = 700

# The canary. Any of these appearing in a response means the model has drifted off rule 5 and the
# whole response is discarded. Kept deliberately wide -- a near-miss key name is still drift.
FORBIDDEN_RESPONSE_KEYS = frozenset(
    {
        "section_width",
        "section_width_mm",
        "section_width_in",
        "width",
        "aspect_ratio",
        "aspect",
        "rim_diameter",
        "rim_diameter_in",
        "rim_size",
        "wheel_diameter",
        "diameter",
        "overall_diameter",
        "overall_diameter_in",
        "load_index",
        "load_index_dual",
        "speed_rating",
        "load_range",
        "ply_rating",
        "max_load",
        "max_load_lb",
        "max_psi",
        "max_pressure",
        "tread_depth",
        "tread_depth_32nds",
        "utqg",
        "utqg_treadwear",
        "utqg_traction",
        "utqg_temperature",
        "weight",
        "weight_lbs",
        "price",
        "msrp",
        "sizes",
        "available_sizes",
        "size",
        "size_display",
        "rim_width",
        "rim_width_min_in",
        "rim_width_max_in",
    }
)

VEHICLE_CLASSES = frozenset(dict(src_models.TireSpec.VEHICLE_CLASS_CHOICES))
TIERS = frozenset(dict(src_models.TireSpec.TIER_CHOICES))
NOISE_LEVELS = frozenset(dict(src_models.TireSpec.NOISE_CHOICES))

# Detail keys that carry a human-readable product title, in the shape provider_parts stores
# (a list of {key, label, value}). Surveyed across the whole catalog: "description" is the one
# every distributor writes, the rest are defensive.
_TITLE_KEYS = ("description", "part_description", "title", "name", "product_name")

# Detail keys that carry a part number a dealer might search by.
_PART_NUMBER_KEYS = ("sku", "mpn", "upc", "quadratec_pn", "motorstate_pn", "wps_sku", "helmet_house_pn")

# Detail keys that carry catalog taxonomy, which tells the model how the distributor filed the
# product ("Tire and Wheel > Tire").
_CATEGORY_KEYS = ("part_category", "part_subcategory", "part_terminology", "category", "subcategory")

# Distributor structured specs -> TireSpec column. **No feed we currently ingest supplies any of
# these** (verified by surveying every distinct product_details key in the catalog on 2026-08-25),
# so this map is empty of matches today and every one of these columns comes out NULL. It is here,
# rather than absent, because the precedence rule says a distributor's own number beats anything
# derived -- the day a feed ships UTQG, wiring it up is one line, not a new tier.
_STRUCTURED_FIELD_KEYS = {
    "tread_depth_32nds": ("tread_depth_32nds", "tread_depth"),
    "max_psi": ("max_psi", "max_inflation_pressure", "max_pressure"),
    "rim_width_min_in": ("rim_width_min_in", "rim_width_min"),
    "rim_width_max_in": ("rim_width_max_in", "rim_width_max"),
    "utqg_treadwear": ("utqg_treadwear", "treadwear"),
    "utqg_traction": ("utqg_traction", "traction"),
    "utqg_temperature": ("utqg_temperature", "temperature"),
}
_STRUCTURED_INT_FIELDS = frozenset(["tread_depth_32nds", "max_psi", "utqg_treadwear"])
_STRUCTURED_DECIMAL_FIELDS = frozenset(["rim_width_min_in", "rim_width_max_in"])

# ``provider_parts.provider_external_id`` is a composite of our own brand row id and the
# distributor part number ("617_WPRN205-730", "121_N205-730"). The prefix is internal
# plumbing -- sending it to the model wastes tokens and, worse, crowds real part numbers out
# of the capped list.
_EXTERNAL_ID_PREFIX_RE = re.compile(r"^\d+_")


@dataclasses.dataclass
class TireCandidate:
    """One master part, everything we hold about it, and the size the parser decoded."""

    master_part_id: int
    brand_id: int
    brand_name: str
    product_type: typing.Optional[str]
    product_type_source: typing.Optional[str]
    titles: typing.List[str]
    part_numbers: typing.List[str]
    categories: typing.List[str]
    structured: typing.Dict[str, typing.Any]
    parsed: tire_size.ParsedSize
    # More than one distinct size across the titles: the providers linked to this master part
    # describe different tires, which is a merge problem upstream, not a parse failure.
    size_variants: typing.List[str]


@dataclasses.dataclass
class RunStats:
    scanned: int = 0
    no_size: int = 0
    size_conflict: int = 0
    called: int = 0
    llm_errors: int = 0
    rejected: typing.Dict[str, int] = dataclasses.field(default_factory=dict)
    not_a_tire: int = 0
    written: int = 0
    product_type_set: int = 0
    product_type_conflict: int = 0
    with_category: int = 0
    with_model_name: int = 0

    def reject(self, reason: str) -> None:
        self.rejected[reason] = self.rejected.get(reason, 0) + 1


# ==========================================================================================
# Reading what we hold about a master part
# ==========================================================================================


def _detail_pairs(product_details: typing.Any) -> typing.Iterator[typing.Tuple[str, typing.Any]]:
    """
    ``(key, value)`` pairs out of a ``ProviderPart.product_details``.

    The column is a JSONField but the value is not uniformly shaped: most rows hold a list of
    ``{key, label, value}`` dicts, and some hold that list *as a JSON string* (double-encoded by
    an older sync). Both are handled here so no caller has to know.
    """
    if isinstance(product_details, str):
        try:
            product_details = json.loads(product_details)
        except (ValueError, TypeError):
            return
    if isinstance(product_details, dict):
        for key, value in product_details.items():
            yield str(key), value
        return
    if not isinstance(product_details, list):
        return
    for item in product_details:
        if isinstance(item, dict) and "key" in item:
            yield str(item["key"]), item.get("value")


def _clean(value: typing.Any) -> typing.Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _append_unique(target: typing.List[str], value: typing.Optional[str], limit: int) -> None:
    """Case-insensitive dedupe that keeps the first spelling seen, so the ordering a run produces
    is stable and the prompt is cacheable across re-runs."""
    if value is None or len(target) >= limit:
        return
    lowered = value.lower()
    if any(existing.lower() == lowered for existing in target):
        return
    target.append(value)


def build_candidate(
    *,
    master_part: typing.Dict[str, typing.Any],
    provider_rows: typing.Sequence[typing.Dict[str, typing.Any]],
) -> typing.Optional[TireCandidate]:
    """
    Assemble one candidate, or ``None`` if no title decodes to a tire size.

    Every provider's own title is collected, not just the master's -- that is where the spelled-out
    model name lives. TireRack ships ``285/70R17~~ NI RIDGE GRAPPLER`` and Rough Country ships
    ``285/70R17 Nitto Ridge Grappler`` for the same SKU; only the second one names the product.
    """
    titles: typing.List[str] = []
    part_numbers: typing.List[str] = []
    categories: typing.List[str] = []
    structured: typing.Dict[str, typing.Any] = {}

    _append_unique(titles, _clean(master_part.get("description")), MAX_TITLES)
    _append_unique(part_numbers, _clean(master_part.get("part_number")), MAX_PART_NUMBERS)
    _append_unique(part_numbers, _clean(master_part.get("sku")), MAX_PART_NUMBERS)
    for column in ("overview_category", "category"):
        _append_unique(categories, _clean(master_part.get(column)), MAX_CATEGORIES)

    # provider_rows arrive ordered by provider_id, so "first provider that supplies a value wins"
    # is deterministic across runs rather than dependent on row order.
    for row in provider_rows:
        taxonomy = " > ".join(
            part
            for part in (
                _clean(row.get("overview_category")),
                _clean(row.get("category")),
                _clean(row.get("subcategory")),
            )
            if part
        )
        _append_unique(categories, taxonomy or None, MAX_CATEGORIES)
        external_id = _clean(row.get("provider_external_id"))
        if external_id:
            _append_unique(part_numbers, _EXTERNAL_ID_PREFIX_RE.sub("", external_id) or None, MAX_PART_NUMBERS)

        details = dict(_detail_pairs(row.get("product_details")))
        for key in _TITLE_KEYS:
            _append_unique(titles, _clean(details.get(key)), MAX_TITLES)
        for key in _PART_NUMBER_KEYS:
            _append_unique(part_numbers, _clean(details.get(key)), MAX_PART_NUMBERS)
        for key in _CATEGORY_KEYS:
            _append_unique(categories, _clean(details.get(key)), MAX_CATEGORIES)
        for field, keys in _STRUCTURED_FIELD_KEYS.items():
            if field in structured:
                continue
            for key in keys:
                coerced = _coerce_structured(field, details.get(key))
                if coerced is not None:
                    structured[field] = coerced
                    break

    parsed = tire_size.parse_best(titles)
    if parsed is None:
        return None

    return TireCandidate(
        master_part_id=master_part["id"],
        brand_id=master_part["brand_id"],
        brand_name=master_part.get("brand_name") or "",
        product_type=master_part.get("product_type"),
        product_type_source=master_part.get("product_type_source"),
        titles=titles,
        part_numbers=part_numbers,
        categories=categories,
        structured=structured,
        parsed=parsed,
        size_variants=tire_size.disagreements(titles),
    )


def _coerce_structured(field: str, value: typing.Any) -> typing.Any:
    """Distributor values arrive as strings as often as numbers. A value that will not coerce is
    dropped rather than stored as text -- a spec column holding "N/A" is worse than a NULL."""
    text = _clean(value)
    if text is None:
        return None
    if field in _STRUCTURED_INT_FIELDS:
        try:
            return int(round(float(text)))
        except ValueError:
            return None
    if field in _STRUCTURED_DECIMAL_FIELDS:
        try:
            return decimal.Decimal(text).quantize(decimal.Decimal("0.1"))
        except (decimal.InvalidOperation, ValueError):
            return None
    return text[:4] if field.startswith("utqg_") else text


# ==========================================================================================
# The prompt
# ==========================================================================================

_SYSTEM_PREAMBLE = """You extract tire product information from messy automotive distributor \
catalogue data.

Distributors abbreviate inconsistently. "TER GRAP G3", "NI TERRA GRAP G3" and
"Terra Grappler G3" are the same Nitto product. Your job is to recognise the product,
give its canonical name, and classify it.

You are shown ONE tire (one size of one model), with every distributor title we hold for it.

The size has already been parsed deterministically and is supplied to you. Do not re-derive
it. Echo back size_matches_input=false ONLY if the titles clearly contradict the parsed size.

Return only JSON matching the schema. No prose, no markdown fences.

RULES

1. model_name is the manufacturer's product name with the brand, size, service description
   and part number removed. Decode abbreviations using your knowledge of the tire market.
   If you cannot identify the product, set model_name=null rather than echoing the
   abbreviation.

2. search_aliases are what a CUSTOMER would type -- short forms, common misspellings,
   spacing variants ("KO2", "K02", "BFG KO2", "Duratrac"). Also include the distributor
   abbreviations seen in the titles, so dealers who know those catalogues can find it.

3. tread_category MUST be one of the codes listed below. Never invent a code. If the titles
   do not let you identify the model, return null -- do not guess from the brand alone.

4. Capability flags must be null unless you are confident. is_3pmsf is a certification with
   legal weight in some jurisdictions -- return true ONLY if you know this model carries the
   Three-Peak Mountain Snowflake marking. Never infer it from "winter capable" or
   "all weather" marketing language. When unsure, null.

5. NEVER return section width, aspect ratio, rim diameter, load index, speed rating, load
   range, overall diameter, max PSI, tread depth, UTQG, weight, price, or available sizes.
   Those come from the parser and the distributor feed.

6. If this is not a tire -- a wheel, an accessory, a mounted package -- set is_tire=false and
   leave everything else null.

7. Powersports tires are real tires: motorcycle, scooter, ATV, UTV, side-by-side, and turf.
   They have their own tread_category codes (MC_*, ATV_*, TURF) and their own vehicle_class
   values ("motorcycle", "atv_utv"). NEVER classify one with a light-vehicle code -- a motocross
   knobby is MC_OFFROAD, not MT, and a scooter tire is MC_STREET, not SUMMER. If you can tell it
   is a powersports tire but not which kind, return tread_category=null and set the
   vehicle_class; a null category is recoverable, a wrong one is not.

SCHEMA
{
  "is_tire": bool,
  "size_matches_input": bool,
  "model_name": string or null,
  "sub_model": string or null,
  "brand_name_corrected": string or null,
  "search_aliases": [string],
  "tread_category": one of the codes below, or null,
  "vehicle_class": "passenger" | "light_truck" | "trailer" | "commercial" | "motorcycle" | "atv_utv" | null,
  "is_3pmsf": bool or null,
  "is_ms": bool or null,
  "is_run_flat": bool or null,
  "is_studdable": bool or null,
  "has_reinforced_sidewall": bool or null,
  "tier": "budget" | "mid" | "premium" | "flagship" | null,
  "noise_level": "quiet" | "moderate" | "loud" | null,
  "use_case_tags": [string],
  "confidence": number between 0 and 1,
  "reason": "one sentence"
}

TREAD CATEGORY CODES
"""


def build_system_prompt() -> str:
    """
    The system message, with the tread vocabulary read from ``tread_category`` rather than
    hard-coded -- the table is the constraint the response is validated against, so building the
    prompt from anything else would let the two drift apart silently.

    Fixed for the whole run and identical between runs, which is what makes it cacheable.
    """
    lines = [_SYSTEM_PREAMBLE]
    for category in src_models.TreadCategory.objects.all():
        lines.append(
            "  {code:<12} {label} -- {description}".format(
                code=category.code,
                label=category.label,
                description=category.description,
            )
        )
    return "\n".join(lines)


def build_user_payload(candidate: TireCandidate) -> typing.Dict[str, typing.Any]:
    """
    What one call sees. The parsed size is included on purpose: it costs about 30 tokens and it
    stops the model guessing at dimensions -- it can see them and spend its attention on
    identifying the product instead.
    """
    payload: typing.Dict[str, typing.Any] = {
        "master_part_id": candidate.master_part_id,
        "brand_string": candidate.brand_name,
        "titles": candidate.titles,
        "part_numbers": candidate.part_numbers,
        "parsed_size": candidate.parsed.as_llm_payload(),
    }
    if candidate.categories:
        payload["distributor_categories"] = candidate.categories
    return payload


# ==========================================================================================
# Validation -- reject, do not repair
# ==========================================================================================


@dataclasses.dataclass
class ValidatedResponse:
    is_tire: bool
    size_matches_input: bool
    model_name: typing.Optional[str] = None
    sub_model: typing.Optional[str] = None
    tread_category: typing.Optional[str] = None
    vehicle_class: typing.Optional[str] = None
    search_aliases: typing.List[str] = dataclasses.field(default_factory=list)
    use_case_tags: typing.List[str] = dataclasses.field(default_factory=list)
    tier: typing.Optional[str] = None
    noise_level: typing.Optional[str] = None
    is_3pmsf: typing.Optional[bool] = None
    is_ms: typing.Optional[bool] = None
    is_run_flat: typing.Optional[bool] = None
    is_studdable: typing.Optional[bool] = None
    has_reinforced_sidewall: typing.Optional[bool] = None
    confidence: typing.Optional[decimal.Decimal] = None
    reason: typing.Optional[str] = None


def _check_size_leak(response: typing.Dict[str, typing.Any]) -> typing.Optional[str]:
    """
    The canary from rule 5. A dimension in the response means the prompt has drifted, and a model
    that has started inventing sizes cannot be trusted on the fields we *did* ask for either --
    so the finding rejects the whole response rather than dropping the offending key.
    """
    leaked = sorted(key for key in response if key.lower() in FORBIDDEN_RESPONSE_KEYS and response[key] is not None)
    return ", ".join(leaked) if leaked else None


def _as_bool(value: typing.Any) -> typing.Optional[bool]:
    return value if isinstance(value, bool) else None


def _as_string_list(value: typing.Any, limit: int = 25) -> typing.List[str]:
    if not isinstance(value, list):
        return []
    cleaned: typing.List[str] = []
    for item in value:
        text = _clean(item)
        if text is not None and len(text) <= 120:
            _append_unique(cleaned, text, limit)
    return cleaned


def validate(
    response: typing.Any,
    candidate: TireCandidate,
    valid_categories: typing.AbstractSet[str],
) -> typing.Tuple[typing.Optional[ValidatedResponse], typing.Optional[str]]:
    """
    Turn a raw response into a ``ValidatedResponse``, or return the reason it was rejected.

    Rejection is per the plan's table and is never a repair. The two silent downgrades below --
    a low-confidence 3PMSF and a model_name that is just a title echoed back -- are the exceptions,
    and both downgrade to NULL rather than to a guess.
    """
    if not isinstance(response, dict):
        return None, "not-an-object"

    leak = _check_size_leak(response)
    if leak:
        return None, "size-leak:{}".format(leak)

    is_tire = response.get("is_tire")
    if not isinstance(is_tire, bool):
        return None, "missing-is_tire"
    if not is_tire:
        return ValidatedResponse(is_tire=False, size_matches_input=True), None

    category = _clean(response.get("tread_category"))
    if category is not None:
        category = category.upper()
        if category not in valid_categories:
            # Not repaired to a near-match: an invented code means the model was not working from
            # the vocabulary it was given, and the category it meant is a guess.
            return None, "unknown-tread-category:{}".format(category)

    model_name = _clean(response.get("model_name"))
    if model_name is not None:
        lowered = model_name.lower()
        if any(lowered == title.lower() for title in candidate.titles):
            # It handed a raw distributor title back. That is not an identification.
            model_name = None
        elif len(model_name) > 255:
            model_name = model_name[:255]

    confidence: typing.Optional[decimal.Decimal] = None
    raw_confidence = response.get("confidence")
    if isinstance(raw_confidence, (int, float)) and not isinstance(raw_confidence, bool):
        confidence = decimal.Decimal(str(raw_confidence)).quantize(decimal.Decimal("0.01"))
        confidence = min(max(confidence, decimal.Decimal("0")), decimal.Decimal("1"))

    is_3pmsf = _as_bool(response.get("is_3pmsf"))
    if is_3pmsf and (confidence is None or confidence < IS_3PMSF_MIN_CONFIDENCE):
        # Downgraded, not rejected: the rest of the answer is still usable, and an unknown
        # certification is safer than a maybe.
        is_3pmsf = None

    vehicle_class = _clean(response.get("vehicle_class"))
    if vehicle_class is not None:
        vehicle_class = vehicle_class.lower()
        if vehicle_class not in VEHICLE_CLASSES:
            return None, "unknown-vehicle-class:{}".format(vehicle_class)

    # tier and noise_level are soft marketing metadata, so an out-of-vocabulary value drops the
    # field rather than the response. Measured on the first full NITTO run: 8 of 1,581 responses
    # answered tier="performance" -- confusing the price tier with the tread category -- and
    # rejecting outright threw away 8 otherwise-correct identifications over a field nothing
    # depends on. tread_category and vehicle_class stay hard rejections: those are FK-constrained
    # and drive facets, so a wrong one is worse than a missing one.
    tier = _clean(response.get("tier"))
    if tier is not None:
        tier = tier.lower()
        if tier not in TIERS:
            tier = None

    noise_level = _clean(response.get("noise_level"))
    if noise_level is not None:
        noise_level = noise_level.lower()
        if noise_level not in NOISE_LEVELS:
            noise_level = None

    sub_model = _clean(response.get("sub_model"))
    reason = _clean(response.get("reason"))
    size_matches = response.get("size_matches_input")

    return (
        ValidatedResponse(
            is_tire=True,
            size_matches_input=size_matches if isinstance(size_matches, bool) else True,
            model_name=model_name,
            sub_model=sub_model[:255] if sub_model else None,
            tread_category=category,
            vehicle_class=vehicle_class,
            search_aliases=_as_string_list(response.get("search_aliases")),
            use_case_tags=_as_string_list(response.get("use_case_tags")),
            tier=tier,
            noise_level=noise_level,
            is_3pmsf=is_3pmsf,
            is_ms=_as_bool(response.get("is_ms")),
            is_run_flat=_as_bool(response.get("is_run_flat")),
            is_studdable=_as_bool(response.get("is_studdable")),
            has_reinforced_sidewall=_as_bool(response.get("has_reinforced_sidewall")),
            confidence=confidence,
            reason=reason,
        ),
        None,
    )


# ==========================================================================================
# Lookup resolution
# ==========================================================================================


class LookupTables:
    """
    The three standards tables, read once per run.

    Load range is keyed by ``(code, applies_to)``: the LT/ST letters and the passenger SL/XL
    designations are two vocabularies in one table, and a lookup that ignores which one applies
    will happily match a passenger tire's "E" against a light-truck row (see the model docstring).
    """

    def __init__(self):
        self.load_index = {row.load_index: row.max_load_lb for row in src_models.TireLoadIndex.objects.all()}
        self.speed_rating = {row.code: row.max_speed_mph for row in src_models.TireSpeedRating.objects.all()}
        self.load_range = {
            (row.load_range, row.applies_to): row.ply_rating for row in src_models.TireLoadRange.objects.all()
        }
        self.load_range_alias = {
            (row.alias, row.applies_to): row.ply_rating for row in src_models.TireLoadRange.objects.all() if row.alias
        }
        self.tread_categories = frozenset(src_models.TreadCategory.objects.values_list("code", flat=True))

    def resolve(self, parsed: tire_size.ParsedSize) -> typing.Dict[str, typing.Any]:
        max_load_lb = self.load_index.get(parsed.load_index) if parsed.load_index else None
        max_speed_mph = self.speed_rating.get(parsed.speed_rating) if parsed.speed_rating else None

        ply_rating = None
        if parsed.load_range:
            # SL/XL/RF are the passenger vocabulary; every letter code belongs to LT/ST. The
            # service type is not consulted, because a passenger tire is often written with no
            # prefix at all and the designation itself is unambiguous.
            applies_to = (
                src_models.TireLoadRange.APPLIES_TO_PASSENGER
                if parsed.load_range in ("SL", "XL", "RF")
                else src_models.TireLoadRange.APPLIES_TO_LT_ST
            )
            key = (parsed.load_range, applies_to)
            ply_rating = self.load_range.get(key) or self.load_range_alias.get(key)

        return {
            "max_load_lb": max_load_lb,
            "max_speed_mph": max_speed_mph,
            "ply_rating": ply_rating,
        }


# ==========================================================================================
# Candidate selection
# ==========================================================================================

_MODE_CLAUSES = {
    MODE_MISSING: "NOT EXISTS (SELECT 1 FROM tire_specs ts WHERE ts.master_part_id = mp.id)",
    MODE_INCOMPLETE: (
        "EXISTS (SELECT 1 FROM tire_specs ts WHERE ts.master_part_id = mp.id"
        "        AND (ts.model_name IS NULL OR ts.tread_category IS NULL))"
    ),
    MODE_ALL: "TRUE",
}


def resolve_brand_ids(brand_names: typing.Sequence[str]) -> typing.Dict[str, int]:
    """Brand name -> id, matched case-insensitively. Callers report the misses themselves so the
    command can fail loudly on a typo instead of silently enriching nothing."""
    resolved = {}
    for name in brand_names:
        brand = src_models.Brands.objects.filter(name__iexact=name.strip()).first()
        if brand is not None:
            resolved[name] = brand.id
    return resolved


def iter_candidates(
    *,
    brand_ids: typing.Optional[typing.Sequence[int]] = None,
    exclude_brand_ids: typing.Optional[typing.Sequence[int]] = None,
    mode: str = MODE_MISSING,
    limit: typing.Optional[int] = None,
    include_rejected: bool = False,
    stats: typing.Optional[RunStats] = None,
) -> typing.Iterator[TireCandidate]:
    """
    Yield candidates in ``master_parts.id`` order, keyset-paginated.

    Deliberately not filtered on ``product_type``: see the module docstring for why the parser is
    the gate instead. ``limit`` counts *candidates yielded*, not rows scanned, so a small pilot run
    gets a full batch of real work rather than stopping early on unparseable rows.
    """
    if mode not in MODES:
        raise ValueError("Unknown mode {!r}; expected one of {}".format(mode, ", ".join(MODES)))
    stats = stats if stats is not None else RunStats()

    where = ["mp.description IS NOT NULL", _MODE_CLAUSES[mode]]
    params: typing.List[typing.Any] = []
    if brand_ids:
        where.append("mp.brand_id = ANY(%s)")
        params.append(list(brand_ids))
    if exclude_brand_ids:
        # Used to hold back brands that are known duplicates of each other: reconciliation votes
        # per brand_id, so enriching both halves of a split brand means the same model is voted
        # on twice and can resolve differently in each. Cheaper to skip them than to re-run.
        where.append("mp.brand_id <> ALL(%s)")
        params.append(list(exclude_brand_ids))
    if not include_rejected:
        where.append("mp.product_type_source IS DISTINCT FROM %s")
        params.append(NOT_A_TIRE_SOURCE)

    sql = (
        "SELECT mp.id, mp.brand_id, b.name AS brand_name, mp.part_number, mp.sku, mp.description,"
        "       mp.overview_category, mp.category, mp.product_type, mp.product_type_source "
        "FROM master_parts mp "
        "JOIN brands b ON b.id = mp.brand_id "
        "WHERE {where} AND mp.id > %s "
        "ORDER BY mp.id "
        "LIMIT %s".format(where=" AND ".join(where))
    )

    yielded = 0
    last_id = 0
    while True:
        with connection.cursor() as cursor:
            cursor.execute(sql, params + [last_id, CANDIDATE_BATCH_SIZE])
            columns = [column[0] for column in cursor.description]
            rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
        if not rows:
            return

        last_id = rows[-1]["id"]
        provider_rows = _provider_rows_for([row["id"] for row in rows])
        for row in rows:
            stats.scanned += 1
            candidate = build_candidate(master_part=row, provider_rows=provider_rows.get(row["id"], []))
            if candidate is None:
                stats.no_size += 1
                continue
            if len(candidate.size_variants) > 1:
                stats.size_conflict += 1
            yield candidate
            yielded += 1
            if limit is not None and yielded >= limit:
                return


def _provider_rows_for(
    master_part_ids: typing.Sequence[int],
) -> typing.Dict[int, typing.List[typing.Dict[str, typing.Any]]]:
    """All provider rows for a page of master parts, in one query, ordered by provider so the
    first-provider-wins rule in ``build_candidate`` is deterministic."""
    if not master_part_ids:
        return {}
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT pp.master_part_id, pp.provider_id, pp.provider_external_id,"
            "       pp.overview_category, pp.category, pp.subcategory, pp.product_details "
            "FROM provider_parts pp "
            "WHERE pp.master_part_id = ANY(%s) "
            "ORDER BY pp.master_part_id, pp.provider_id",
            [list(master_part_ids)],
        )
        columns = [column[0] for column in cursor.description]
        grouped: typing.Dict[int, typing.List[typing.Dict[str, typing.Any]]] = {}
        for row in cursor.fetchall():
            record = dict(zip(columns, row))
            grouped.setdefault(record["master_part_id"], []).append(record)
    return grouped


# ==========================================================================================
# Writing
# ==========================================================================================


def build_tire_spec(
    *,
    candidate: TireCandidate,
    validated: ValidatedResponse,
    lookups: LookupTables,
    model_used: str,
) -> src_models.TireSpec:
    """
    Compose the row. Precedence is enforced here and nowhere else: the parser block is copied
    verbatim from ``candidate.parsed``, the LLM block from ``validated``, and the distributor
    block is applied last **only over columns neither of the other two owns**.
    """
    parsed = candidate.parsed
    spec = src_models.TireSpec(
        master_part_id=candidate.master_part_id,
        notation=parsed.notation,
        service_type=parsed.service_type,
        section_width_mm=parsed.section_width_mm,
        aspect_ratio=parsed.aspect_ratio,
        section_width_in=parsed.section_width_in,
        overall_diameter_in=parsed.overall_diameter_in,
        construction=parsed.construction,
        rim_diameter_in=parsed.rim_diameter_in,
        load_index=parsed.load_index,
        load_index_dual=parsed.load_index_dual,
        speed_rating=parsed.speed_rating,
        load_range=parsed.load_range,
        size_display=parsed.size_display,
        model_name=validated.model_name,
        sub_model=validated.sub_model,
        tread_category_id=validated.tread_category,
        vehicle_class=validated.vehicle_class,
        search_aliases=validated.search_aliases,
        use_case_tags=validated.use_case_tags,
        tier=validated.tier,
        noise_level=validated.noise_level,
        is_3pmsf=validated.is_3pmsf,
        is_ms=validated.is_ms,
        is_run_flat=validated.is_run_flat,
        is_studdable=validated.is_studdable,
        has_reinforced_sidewall=validated.has_reinforced_sidewall,
        llm_confidence=validated.confidence,
        llm_reason=validated.reason,
        llm_model_used=model_used,
        # Either the model contradicted the parser, or two providers on this master part describe
        # different tires. Specs are still written -- the flag is a review queue, not a veto.
        size_disputed=(not validated.size_matches_input) or len(candidate.size_variants) > 1,
        category_reconciled=False,
        enriched_at=timezone.now(),
    )
    for field, value in lookups.resolve(parsed).items():
        setattr(spec, field, value)
    for field, value in candidate.structured.items():
        setattr(spec, field, value)
    return spec


SPEC_UPDATE_FIELDS = [
    "notation",
    "service_type",
    "section_width_mm",
    "aspect_ratio",
    "section_width_in",
    "overall_diameter_in",
    "construction",
    "rim_diameter_in",
    "load_index",
    "load_index_dual",
    "speed_rating",
    "load_range",
    "size_display",
    "max_load_lb",
    "max_speed_mph",
    "ply_rating",
    "model_name",
    "sub_model",
    "tread_category",
    "vehicle_class",
    "search_aliases",
    "use_case_tags",
    "tier",
    "noise_level",
    "is_3pmsf",
    "is_ms",
    "is_run_flat",
    "is_studdable",
    "has_reinforced_sidewall",
    "tread_depth_32nds",
    "max_psi",
    "rim_width_min_in",
    "rim_width_max_in",
    "utqg_treadwear",
    "utqg_traction",
    "utqg_temperature",
    "llm_confidence",
    "llm_reason",
    "llm_model_used",
    "size_disputed",
    "enriched_at",
    "updated_at",
]


@transaction.atomic
def write_batch(
    specs: typing.Sequence[src_models.TireSpec],
    not_a_tire_ids: typing.Sequence[int],
    stats: RunStats,
) -> None:
    """
    Upsert a batch of specs and stamp ``MasterPart.product_type``.

    ``category_reconciled`` is deliberately absent from the update list: it belongs to the
    reconciliation pass that runs after enrichment, and re-enriching one SKU must not silently
    clear the fact that its category came from a per-model vote.
    """
    if specs:
        # Django's own ON CONFLICT DO UPDATE rather than pgbulk (which the rest of this package
        # uses): pgbulk 3.2.4 reaches for a psycopg3-only escaping API and raises on this repo's
        # psycopg2 driver. bulk_create still fires auto_now, so updated_at stays honest.
        src_models.TireSpec.objects.bulk_create(
            list(specs),
            update_conflicts=True,
            unique_fields=["master_part"],
            update_fields=SPEC_UPDATE_FIELDS,
        )
        stats.written += len(specs)
        _stamp_product_type([spec.master_part_id for spec in specs], stats)

    if not_a_tire_ids:
        # product_type stays NULL -- "not a tire" says nothing about whether it is a wheel or a
        # part, and this module does not guess. Only the provenance marker is written, so the
        # next run skips these instead of paying for the same verdict again.
        src_models.MasterPart.objects.filter(id__in=list(not_a_tire_ids)).update(
            product_type_source=NOT_A_TIRE_SOURCE,
            updated_at=timezone.now(),
        )


def _stamp_product_type(master_part_ids: typing.Sequence[int], stats: RunStats) -> None:
    """
    Set ``product_type='tire'`` -- but never over a distributor's own assertion.

    ``src.integrations.utils.product_type`` ranks its sources in tiers, T1-T3 being things a
    distributor actually told us and T4-T5 being inference. This module's verdict is inference
    over titles, so it fills a NULL and it overwrites another inference, but it leaves a feed-level
    signal alone and reports the disagreement instead. A distributor calling something a wheel
    while the model calls it a tire is worth a human look, not a silent overwrite.
    """
    rows = src_models.MasterPart.objects.filter(id__in=list(master_part_ids)).values(
        "id", "product_type", "product_type_source"
    )
    to_stamp = []
    for row in rows:
        current = row["product_type"]
        if current == src_enums.ProductType.TIRE.value and row["product_type_source"] == TIRE_SOURCE:
            continue
        if current is not None and current != src_enums.ProductType.TIRE.value:
            source = row["product_type_source"] or ""
            if product_type_utils.tier_for_source(source) <= 3:
                stats.product_type_conflict += 1
                logger.warning(
                    "%s master_part=%s: distributor says %s (%s) but the model says tire; leaving it alone",
                    _LOG_PREFIX,
                    row["id"],
                    current,
                    source or "unknown source",
                )
                continue
        to_stamp.append(row["id"])

    if to_stamp:
        src_models.MasterPart.objects.filter(id__in=to_stamp).update(
            product_type=src_enums.ProductType.TIRE.value,
            product_type_source=TIRE_SOURCE,
            updated_at=timezone.now(),
        )
        stats.product_type_set += len(to_stamp)


# ==========================================================================================
# The run
# ==========================================================================================


def run(
    *,
    brand_names: typing.Optional[typing.Sequence[str]] = None,
    exclude_brand_names: typing.Optional[typing.Sequence[str]] = None,
    mode: str = MODE_MISSING,
    limit: typing.Optional[int] = None,
    max_workers: int = 4,
    apply_changes: bool = False,
    include_rejected: bool = False,
    write_batch_size: int = 100,
    on_result: typing.Optional[
        typing.Callable[[TireCandidate, typing.Optional[ValidatedResponse], typing.Optional[str]], None]
    ] = None,
) -> RunStats:
    """
    Enrich candidates and, with ``apply_changes``, write them.

    Without ``apply_changes`` the LLM is still called -- this is a dry run of the *write*, not of
    the spend, and the distinction matters: what a bare run is for is inspecting real responses
    before letting them touch the catalog. Use ``--limit`` to control cost.

    ``on_result`` is called for every candidate with its validated response (or the rejection
    reason), which is how the command writes its review CSV without this function knowing about
    files.
    """
    stats = RunStats()
    brand_ids = None
    if brand_names:
        resolved = resolve_brand_ids(brand_names)
        missing = sorted(set(brand_names) - set(resolved))
        if missing:
            raise ValueError("Unknown brand(s): {}".format(", ".join(missing)))
        brand_ids = list(resolved.values())

    exclude_brand_ids = None
    if exclude_brand_names:
        resolved = resolve_brand_ids(exclude_brand_names)
        missing = sorted(set(exclude_brand_names) - set(resolved))
        if missing:
            raise ValueError("Unknown brand(s) to exclude: {}".format(", ".join(missing)))
        exclude_brand_ids = list(resolved.values())

    lookups = LookupTables()
    system_prompt = build_system_prompt()
    model_used = azure_llm.deployment()
    client = azure_llm.client()

    pending_specs: typing.List[src_models.TireSpec] = []
    pending_not_tire: typing.List[int] = []

    def flush() -> None:
        if not apply_changes:
            pending_specs.clear()
            pending_not_tire.clear()
            return
        write_batch(pending_specs, pending_not_tire, stats)
        pending_specs.clear()
        pending_not_tire.clear()

    def call(candidate: TireCandidate):
        payload = json.dumps(build_user_payload(candidate), separators=(",", ":"))
        response, error = azure_llm.complete_json(
            client, system_prompt, payload, max_tokens=LLM_MAX_TOKENS, model=model_used
        )
        return candidate, response, error

    candidates = iter_candidates(
        brand_ids=brand_ids,
        mode=mode,
        limit=limit,
        include_rejected=include_rejected,
        stats=stats,
    )

    # Bounded submission, NOT ``pool.map``. ``Executor.map`` drains its whole iterable up front
    # before yielding a single result, which on a full-catalog run means: nothing is written until
    # the entire 3.17M-row scan finishes, every future and its result is held in memory until
    # then, and a crash at any point loses the lot. Measured -- 12 minutes into such a run, zero
    # rows had been written. Keeping only a few batches in flight makes writes incremental again,
    # so an interrupted run keeps everything it already paid for and ``--mode missing`` resumes.
    window = max(max_workers * 4, write_batch_size)
    candidate_iter = iter(candidates)
    exhausted = False

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as pool:
        in_flight: typing.Dict[concurrent.futures.Future, TireCandidate] = {}
        while True:
            while not exhausted and len(in_flight) < window:
                try:
                    in_flight[pool.submit(call, next(candidate_iter))] = None
                except StopIteration:
                    exhausted = True
            if not in_flight:
                break
            done, _pending = concurrent.futures.wait(in_flight, return_when=concurrent.futures.FIRST_COMPLETED)
            for future in done:
                in_flight.pop(future)
                candidate, response, error = future.result()
                stats.called += 1
                if error is not None:
                    stats.llm_errors += 1
                    logger.warning("%s master_part=%s LLM error: %s", _LOG_PREFIX, candidate.master_part_id, error)
                    if on_result:
                        on_result(candidate, None, "llm-error")
                    continue

                validated, reason = validate(response, candidate, lookups.tread_categories)
                if validated is None:
                    stats.reject(reason.split(":", 1)[0])
                    logger.warning("%s master_part=%s rejected: %s", _LOG_PREFIX, candidate.master_part_id, reason)
                    if on_result:
                        on_result(candidate, None, reason)
                    continue

                if not validated.is_tire:
                    stats.not_a_tire += 1
                    pending_not_tire.append(candidate.master_part_id)
                else:
                    pending_specs.append(
                        build_tire_spec(
                            candidate=candidate,
                            validated=validated,
                            lookups=lookups,
                            model_used=model_used,
                        )
                    )
                    if validated.tread_category:
                        stats.with_category += 1
                    if validated.model_name:
                        stats.with_model_name += 1

                if on_result:
                    on_result(candidate, validated, None)

                if len(pending_specs) + len(pending_not_tire) >= write_batch_size:
                    flush()

    flush()
    return stats
