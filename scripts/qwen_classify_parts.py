#!/usr/bin/env python3
"""
Standalone (no Django, no internal `src.*` imports) two-stage PCdb terminology classifier for
MasterPart rows. Reads directly from and writes directly to the production Postgres database via
psycopg2 -- there is no export/import step, this script owns the whole read -> classify -> write
loop by itself, so it can run on a machine with no access to this codebase.

WHAT IT DOES, per part:
  Stage 1: batch-classify N parts at once into a (category, subcategory) pair from the PCdb
           taxonomy (852 real pairs, loaded from pcdb_terminology_flat).
  Stage 2: group parts by their assigned pair, then batch-classify each group into the single
           best-matching terminology_id from EVERY terminology sharing that subcategory's NAME --
           pooled across every category that has one, not just the specific pair Stage 1 picked
           (see KNOWN LIMITATION below for why). No shortlisting or narrowing, so the correct term
           is never excluded by a retrieval step missing it. The tradeoff is a much bigger prompt
           for the largest pooled subcategory names -- see --batch-size below.
Both stages run many batches concurrently (ThreadPoolExecutor) against the LLM endpoint.

WHY A PROVIDER HIERARCHY: a MasterPart can have several ProviderPart rows (one per distributor
carrying it), and description quality varies enormously by distributor. PROVIDER_PRIORITY below
is not a guess -- the ATECH/KEYSTONE/MOTOR_STATE_DISTRIBUTING/QUADRATEC/DLG "no real signal" tier
is lifted directly from this codebase's own src/integrations/utils/product_type.py ("1.65M of
3.2M master parts reach us only through distributors that ship no type signal at all"), and
TURN_14/PREMIER_PERFORMANCE/MEYER's high placement is confirmed by that same file's closed
category vocab / real PCdb terminology hints. For each part, this script walks the priority list
and uses the highest-ranked available provider's data -- but skips a candidate whose raw data is
uninformative (no fields beyond a redundant sku/brand/description echo) and falls through to the
next, rather than trusting rank alone.

FOUR REAL FIXES CONFIRMED WORKING, in the order they were found necessary against production data:

1. Subcategory-name POOLING (Stage 2 sees every terminology sharing the assigned subcategory's
   NAME, pooled across every category that has one, not just the one pair Stage 1 picked) --
   roughly 62% of PCdb subcategory names exist under more than one category with a different,
   non-overlapping term list (e.g. "Fuel Injection System and Related Components" under both
   "Engine" and "Air and Fuel Delivery"). Confirmed fix: a FUEL INJECTOR routed to "Engine" was
   still correctly matched, because the real term also exists under "Air and Fuel Delivery" and
   pooling meant Stage 2 saw it anyway.
2. TAXONOMY HINTING (each subcategory in the taxonomy text carries 2-3 real example terminology
   names, not just its bare name) -- a bare subcategory name can be a poor semantic match for how
   a product is actually described. Confirmed fix: WeatherTech "Cargo Liner" parts were
   consistently routed to the generic "Interior" instead of the real "Trunk Lid and Compartment"
   subcategory until its taxonomy line carried examples like "Cargo Box, Cargo Net" -- with that
   hint, 22/22 test parts routed correctly on the first try. (A cheaper "ask Stage 1 for its top
   2-3 subcategory guesses and retry" alternative was tried first and did NOT work: the model's
   own alternates never included the right one either -- this wasn't simply "not enough guesses.")
3. RECURSIVE retry-on-truncation (complete_json_with_retry halves the batch and recurses on JSON
   parse failure, not just once) -- batches of ~30 parts can genuinely truncate mid-JSON, and a
   single non-recursive retry sometimes still wasn't small enough. Confirmed fix: a 300-part real
   run went from 15 hard errors to 0 once retries could recurse past one level.
4. Per-candidate CATEGORY field in Stage 2's payload (each candidate shows the real category it
   belongs to, not just its name/description) -- pooling by subcategory name (#1) means Stage 2's
   candidate list can mix in a term scoped to a specific, unrelated system. Confirmed fix: a
   generic Dorman rubber expansion plug was confidently (but wrongly) matched to "Drum Brake
   Plug" -- a real term, but drum-brake-specific -- pooled in purely because the word "Plug"
   overlapped; a text-only warning in the prompt did NOT stop this, but showing each candidate's
   actual category (so "Brake" visibly stands out among otherwise-generic candidates) did.

ONE REAL LIMITATION STILL CONFIRMED OPEN: the same class of problem fix #2 targets -- Stage 1
choosing a plausible but wrong subcategory -- still recurs when two subcategories under the SAME
category both sound like generic catch-alls and 2-3 examples aren't enough to tell them apart.
Confirmed on a second, independent case: Dorman exhaust hardware kits (real terms exist, e.g.
"Exhaust Manifold Stud Kit", "Exhaust Manifold Hardware Kit", both under Exhaust / "Hardware,
Fasteners and Fittings") were instead routed to the sibling "Brackets, Flanges and Hangers"
subcategory under the same "Exhaust" category -- correctly refused rather than force-matched, but
a real miss nonetheless. The untried, likely next step: more examples per subcategory (5-6 instead
of 3) for more disambiguating signal specifically where two subcategories in one category compete.

Whatever the underlying cause, "unclassifiable" here never means a wrong answer got forced -- it
means no answer was confident enough, which is the deliberately safer failure mode (see this
project's own history for why "never wrong" outranks "always answers" in this design). Rows with
status='unclassifiable' are exactly where a human review pass adds the most value -- start there.

SETUP (on whatever machine runs this):
    pip install openai psycopg2-binary

REQUIRED ENV VARS (or pass the equivalent --flags):
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD   -- Postgres connection
    LLM_BASE_URL                                      -- your OpenAI-compatible endpoint, e.g.
                                                          http://localhost:8000/v1 for a local vLLM
                                                          server, or whatever your Qwen deployment
                                                          exposes
    LLM_API_KEY                                       -- optional; most self-hosted servers don't
                                                          check it, but the client requires a
                                                          non-empty string (defaults to "not-needed")
    LLM_MODEL                                          -- the model name your server expects, e.g.
                                                          "qwen2.5-72b-instruct"

USAGE:
    python qwen_classify_parts.py --limit 50 --batch-size 25 --max-workers 4
    python qwen_classify_parts.py --brand-ids 2951,3564,5914 --batch-size 50 --max-workers 8
    python qwen_classify_parts.py --dry-run --limit 10          # classify but don't write
"""
import argparse
import json
import logging
import math
import os
import re
import sys
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

# ============================================================================================
# Logging
# ============================================================================================

logger = logging.getLogger("qwen_classify_parts")


def setup_logging(log_file: str | None):
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(threadName)s %(message)s")
    stream = logging.StreamHandler(sys.stdout)
    stream.setFormatter(fmt)
    logger.addHandler(stream)
    if log_file:
        fh = logging.FileHandler(log_file)
        fh.setFormatter(fmt)
        logger.addHandler(fh)


# ============================================================================================
# Provider hierarchy -- see module docstring for the evidence behind this ordering. Names must
# match enums.BrandProviderKind in the Django project (kept as a plain int->name map here since
# this script can't import that enum).
# ============================================================================================

PROVIDER_KIND_NAMES = {
    1: "TURN_14", 2: "SDC", 3: "KEYSTONE", 4: "ROUGH_COUNTRY", 5: "WHEELPROS", 6: "MEYER",
    7: "ATECH", 8: "DLG", 9: "ATD", 10: "ALLPRO_DISTRIBUTING", 11: "AUTOMATIC_DISTRIBUTORS",
    12: "CTP_DISTRIBUTORS", 13: "CROWN_AUTOMOTIVE", 14: "DIX_PERF_NORTH", 15: "EARL_OWEN",
    16: "ELITE_WHEEL", 17: "FASTCO", 18: "GRANDWEST_ENTERPRISES", 19: "HELMHOUSE",
    20: "HOLLEY_PERFORMANCE", 21: "THIBAULT", 22: "MARCOR", 23: "MOTOR_STATE_DISTRIBUTING",
    24: "OVERLAND_VEHICLE_SYSTEMS", 25: "PARTS_AUTHORITY", 26: "PARTS_CANADA",
    27: "PARTS_UNLIMITED", 28: "PREMIER_PERFORMANCE", 29: "SSF_IMPORTED_AUTO_PARTS",
    30: "THE_WHEEL_GROUP", 31: "THIBERT", 32: "WESTERN_POWER_SPORTS", 33: "XDP",
    34: "ASAP_NETWORK", 35: "VOSSEN", 36: "TIRERACK", 37: "QUADRATEC",
}

# Best-to-worst. Anything not listed (the small/unverified providers) sorts after this list but
# before the confirmed-worst tier at the bottom -- see _provider_rank().
PROVIDER_PRIORITY = [
    "TURN_14",              # confirmed rich, correctly-classified descriptions in production testing
    "PREMIER_PERFORMANCE",  # product_type.py: closed category vocab + real PCdb terminology hints
    "MEYER",                # product_type.py: closed category vocab + real PCdb terminology hints
    "WHEELPROS",            # product_type.py: structural feed signal, ~99.2% clean size data
    "TIRERACK",             # confirmed rich real tire titles in production testing
    "ELITE_WHEEL", "THE_WHEEL_GROUP", "VOSSEN",  # product_type.py: structural feed signal
    "ROUGH_COUNTRY", "WESTERN_POWER_SPORTS", "HELMHOUSE",  # product_type.py: closed category vocab
]
# Confirmed NO real descriptive signal -- src/integrations/utils/product_type.py: "1.65M of 3.2M
# master parts reach us only through distributors that ship no type signal at all". ATECH last
# specifically per direct testing (18.8-char avg descriptions, zero extra fields beyond a
# redundant sku/brand/description echo).
PROVIDER_LOW_SIGNAL = ["KEYSTONE", "MOTOR_STATE_DISTRIBUTING", "QUADRATEC", "DLG", "ATECH"]


def _provider_rank(name: str) -> int:
    if name in PROVIDER_PRIORITY:
        return PROVIDER_PRIORITY.index(name)
    if name in PROVIDER_LOW_SIGNAL:
        return 1000 + PROVIDER_LOW_SIGNAL.index(name)
    return 500  # unranked/unverified providers: better than confirmed-bad, worse than confirmed-good


_TRAILING_PAREN_RE = re.compile(r"\s*\([^)]*\)\s*$")


def _strip_example_hint(s: str | None) -> str | None:
    """The taxonomy given to Stage 1 decorates each subcategory with '(e.g. ...)' example
    terminology names -- the prompt asks for the bare name back, but this strips the
    parenthetical defensively in case the model echoes the whole decorated label instead."""
    if not s:
        return s
    return _TRAILING_PAREN_RE.sub("", s).strip()


def _flatten_product_details(product_details) -> str:
    if not product_details:
        return ""
    parts = []
    for item in product_details:
        label, value = item.get("label"), item.get("value")
        if value in (None, "", False):
            continue
        parts.append(f"{label}: {value}")
    return "; ".join(parts)


def _is_informative(raw_text: str, description: str, brand: str) -> bool:
    """True if raw_text says something beyond a redundant echo of sku/brand/description."""
    if not raw_text:
        return False
    residual = raw_text
    for known in (description, brand):
        if known:
            residual = residual.replace(known, "")
    residual = re.sub(r"\b(sku|brand|description)\s*:\s*", "", residual, flags=re.IGNORECASE)
    return len(residual.strip(" ;:")) > 10


def build_part_context(brand: str, description: str, provider_rows: list) -> str:
    """
    provider_rows: list of (provider_kind_int, product_details) for one MasterPart, from every
    ProviderPart row it has. Walks PROVIDER_PRIORITY best-first, using the first available
    provider whose data is genuinely informative; falls back to the best-ranked available
    provider's data even if thin if none qualify as informative.
    """
    candidates = []
    for kind_int, product_details in provider_rows:
        name = PROVIDER_KIND_NAMES.get(kind_int, f"UNKNOWN_{kind_int}")
        raw_text = _flatten_product_details(product_details)
        candidates.append((name, raw_text))
    candidates.sort(key=lambda c: _provider_rank(c[0]))

    for name, raw_text in candidates:
        if _is_informative(raw_text, description, brand):
            return f"{brand} {description} [{name}: {raw_text}]".strip()

    if candidates:
        name, raw_text = candidates[0]
        return f"{brand} {description} [{name}: {raw_text}]".strip() if raw_text else f"{brand} {description}".strip()
    return f"{brand} {description}".strip()


# ============================================================================================
# LLM client -- generic OpenAI-compatible endpoint (vLLM, Ollama, text-generation-webui, etc. all
# implement this API shape). Point LLM_BASE_URL at whatever your Qwen deployment exposes.
# ============================================================================================

_FENCE_RE = re.compile(r"^```(?:json)?\s*|\s*```$")


def get_llm_client(base_url: str, api_key: str):
    from openai import OpenAI
    if not base_url:
        raise RuntimeError("LLM_BASE_URL (or --llm-base-url) is not set -- point it at your OpenAI-compatible endpoint")
    return OpenAI(base_url=base_url, api_key=api_key or "not-needed", max_retries=3, timeout=180.0)


def complete_json(cli, model: str, system: str, user: str, max_tokens: int) -> tuple[dict | None, str | None]:
    try:
        resp = cli.chat.completions.create(
            model=model,
            messages=[{"role": "system", "content": system}, {"role": "user", "content": user}],
            max_tokens=max_tokens,
            temperature=0,
            response_format={"type": "json_object"},
        )
        if getattr(resp, "usage", None):
            logger.info("LLM usage: prompt_tokens=%s completion_tokens=%s (budget was %s)",
                        resp.usage.prompt_tokens, resp.usage.completion_tokens, max_tokens)
        raw = _FENCE_RE.sub("", (resp.choices[0].message.content or "").strip())
        return json.loads(raw), None
    except json.JSONDecodeError as e:
        return None, f"JSON parse error: {e}"
    except Exception as e:
        return None, f"{type(e).__name__}: {e}"[:250]


def complete_json_with_retry(cli, model: str, system: str, user_payload: dict, max_tokens: int,
                              parts_key: str = "parts", _depth: int = 0) -> tuple[dict | None, str | None]:
    """Recursive retry on parse failure, halving the batch each time -- mirrors the
    truncation-recovery pattern already validated in this project's grouped classification
    pipeline: a parse failure is usually the response getting cut off, and the fix is less output
    to generate, not a blind retry of the same oversized request. Recurses all the way down to
    single-part calls if needed rather than giving up after one halving -- confirmed necessary on
    real data: a single level of halving left some persistently-oversized batches unrecovered
    (15 parts hard-failed in one production test run before this was made recursive), whereas
    continuing to halve resolves it since per-part output size is what's actually oversized, not
    the batch as a whole. _depth is a safety cap (partly for accidental infinite recursion, partly
    to bound worst-case latency on a truly pathological input) -- 6 levels takes a batch of 64 all
    the way down to size 1 with room to spare for any realistic --batch-size."""
    user_text = json.dumps(user_payload)
    parsed, err = complete_json(cli, model, system, user_text, max_tokens)
    if parsed is not None:
        return parsed, None
    parts = user_payload.get(parts_key, [])
    if len(parts) <= 1 or _depth >= 6:
        return None, err
    logger.warning("Batch of %d failed (%s) at retry depth %d, retrying as two halves", len(parts), err, _depth)
    half = len(parts) // 2
    per_part_tokens = max(100, max_tokens // len(parts))
    merged = {"parts": []}
    any_failed = False
    for sub_parts in (parts[:half], parts[half:]):
        sub_payload = dict(user_payload)
        sub_payload[parts_key] = sub_parts
        sub_max_tokens = max(300, per_part_tokens * len(sub_parts) + 200)
        sub_parsed, sub_err = complete_json_with_retry(cli, model, system, sub_payload, sub_max_tokens,
                                                         parts_key=parts_key, _depth=_depth + 1)
        if sub_parsed is None:
            any_failed = True
            logger.warning("Retry half-batch of %d exhausted retries: %s", len(sub_parts), sub_err)
            continue
        merged["parts"].extend(sub_parsed.get(parts_key, []))
    if any_failed and not merged["parts"]:
        return None, err
    return merged, None


# ============================================================================================
# Classification prompts.
#
# Deliberately free of hardcoded "this exact title means this exact terminology" examples: we
# have no formally verified ground truth for this task, so baking in specific answers risks
# teaching the model our own guesses rather than letting it reason from the real candidate list.
# What IS included below are STRUCTURAL facts about the taxonomy and data (verified against the
# real PCdb corpus, not opinions about specific parts), and instructions aimed directly at real
# failure modes found in production testing -- see the two callouts inline.
# ============================================================================================

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

STAGE1_MAX_TOKENS_PER_PART = 60   # includes the new reasoning field
STAGE2_MAX_TOKENS_PER_PART = 60


# ============================================================================================
# DB layer
# ============================================================================================

def get_db_conn(args):
    import psycopg2
    return psycopg2.connect(
        host=args.db_host, port=args.db_port, dbname=args.db_name,
        user=args.db_user, password=args.db_password,
    )


def fetch_terminology_corpus(conn) -> list:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT part_terminology_id, name, aliases, is_active, category_name, subcategory_name,
                   subcategory_id, description
            FROM pcdb_terminology_flat WHERE is_active = true
        """)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def fetch_parts_to_classify(conn, brand_ids, limit, only_unclassified) -> list:
    """When brand_ids has more than one id, samples evenly across brands (not just the lowest
    master_part_id overall) so a --limit run actually gets the diversity a caller asked for."""
    where = ["mp.description IS NOT NULL", "mp.description != ''"]
    params = []
    if brand_ids:
        where.append("mp.brand_id = ANY(%s)")
        params.append(brand_ids)
    if only_unclassified:
        where.append("NOT EXISTS (SELECT 1 FROM ml_part_terminology_classification x WHERE x.master_part_id = mp.id)")
    where_sql = " AND ".join(where)

    if brand_ids and len(brand_ids) > 1 and limit:
        # Round-robin across brands using ROW_NUMBER() per brand, then take the first
        # ceil(limit/len(brand_ids)) rows of each -- even coverage instead of one brand dominating.
        per_brand = math.ceil(limit / len(brand_ids))
        sql = f"""
            SELECT id, description, brand_name FROM (
                SELECT mp.id, mp.description, b.name AS brand_name,
                       ROW_NUMBER() OVER (PARTITION BY mp.brand_id ORDER BY mp.id) AS rn
                FROM master_parts mp
                JOIN brands b ON b.id = mp.brand_id
                WHERE {where_sql}
            ) ranked
            WHERE rn <= %s
            ORDER BY brand_name, id
            LIMIT %s
        """
        params.extend([per_brand, limit])
    else:
        sql = f"""
            SELECT mp.id, mp.description, b.name AS brand_name
            FROM master_parts mp
            JOIN brands b ON b.id = mp.brand_id
            WHERE {where_sql}
            ORDER BY mp.id
        """
        if limit:
            sql += " LIMIT %s"
            params.append(limit)

    with conn.cursor() as cur:
        cur.execute(sql, params)
        return [{"id": r[0], "description": r[1], "brand": r[2]} for r in cur.fetchall()]


def fetch_provider_rows_for_parts(conn, master_part_ids: list) -> dict:
    """Returns {master_part_id: [(provider_kind_int, product_details), ...]}."""
    if not master_part_ids:
        return {}
    with conn.cursor() as cur:
        cur.execute("""
            SELECT pp.master_part_id, p.kind, pp.product_details
            FROM provider_parts pp
            JOIN providers p ON p.id = pp.provider_id
            WHERE pp.master_part_id = ANY(%s)
        """, (master_part_ids,))
        out = defaultdict(list)
        for mp_id, kind, product_details in cur.fetchall():
            out[mp_id].append((kind, product_details))
        return out


def upsert_results(conn, rows: list, dry_run: bool):
    """rows: list of dicts with keys matching ml_part_terminology_classification columns."""
    if dry_run:
        for r in rows:
            logger.info("[DRY RUN] would write: %s", r)
        return
    if not rows:
        return
    from psycopg2.extras import execute_values
    values = [
        (r["master_part_id"], r["status"], r.get("category"), r.get("subcategory"),
         r.get("stage1_confidence"), r.get("part_terminology_id"), r.get("stage2_confidence"),
         r.get("reasoning"), r.get("model_used"))
        for r in rows
    ]
    sql = """
        INSERT INTO ml_part_terminology_classification
            (master_part_id, status, category, subcategory, stage1_confidence,
             part_terminology_id, stage2_confidence, reasoning, model_used, created_at, updated_at)
        VALUES %s
        ON CONFLICT (master_part_id) DO UPDATE SET
            status = EXCLUDED.status, category = EXCLUDED.category, subcategory = EXCLUDED.subcategory,
            stage1_confidence = EXCLUDED.stage1_confidence, part_terminology_id = EXCLUDED.part_terminology_id,
            stage2_confidence = EXCLUDED.stage2_confidence, reasoning = EXCLUDED.reasoning,
            model_used = EXCLUDED.model_used, updated_at = now()
    """
    template = "(%s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now())"
    with conn.cursor() as cur:
        execute_values(cur, sql, values, template=template)
    conn.commit()
    logger.info("Wrote %d row(s) to ml_part_terminology_classification", len(rows))


# ============================================================================================
# Classification
# ============================================================================================

def chunked(items: list, size: int):
    for i in range(0, len(items), size):
        yield items[i:i + size]


def run_stage1_batch(cli, model: str, taxonomy_text: str, batch: list) -> dict:
    """batch: list of {id, brand, description, context}. Returns {part_id: {category, subcategory, confidence, reasoning}}."""
    payload = {"taxonomy": taxonomy_text, "parts": [{"id": p["id"], "text": p["context"]} for p in batch]}
    max_tokens = max(500, len(batch) * STAGE1_MAX_TOKENS_PER_PART)
    parsed, err = complete_json_with_retry(cli, model, STAGE1_SYSTEM, payload, max_tokens)
    if err or not parsed:
        logger.warning("Stage 1 batch of %d failed: %s", len(batch), err)
        return {p["id"]: {"category": None, "subcategory": None, "confidence": 0.0, "error": err} for p in batch}
    result = {entry.get("id"): entry for entry in parsed.get("parts", [])}
    for p in batch:
        if p["id"] not in result:
            result[p["id"]] = {"category": None, "subcategory": None, "confidence": 0.0, "error": "missing from response"}
    return result


def run_stage2_batch(cli, model: str, cat: str, subcat: str, candidates: list, batch: list) -> dict:
    """batch: list of {id, brand, description, context}. Returns {part_id: {terminology_id, confidence, reasoning}}.

    Each candidate carries its OWN real category (t["category_name"]), which can differ from the
    routed `cat` above since candidates are pooled across every category sharing this subcategory
    name (see classify_all's by_subcat_name). Showing it explicitly, not just in free-text
    description, is what lets Stage 2 actually see a domain mismatch instead of having to infer it
    -- confirmed necessary: without this, a generic rubber engine expansion plug got confidently
    matched to "Drum Brake Plug" (pooled in from the unrelated "Brake" category) purely because
    the word "Plug" overlapped, even after the system prompt was strengthened with a warning about
    exactly this trap -- prompt wording alone didn't override the superficial word-match."""
    payload = {
        "category": cat, "subcategory": subcat,
        "candidates": [{"id": t["part_terminology_id"], "name": t["name"], "category": t["category_name"],
                         "description": t["description"] or t["name"]}
                        for t in candidates],
        "parts": [{"id": p["id"], "text": p["context"]} for p in batch],
    }
    max_tokens = max(500, len(batch) * STAGE2_MAX_TOKENS_PER_PART)
    parsed, err = complete_json_with_retry(cli, model, STAGE2_SYSTEM, payload, max_tokens)
    if err or not parsed:
        logger.warning("Stage 2 batch [%s/%s] of %d failed: %s", cat, subcat, len(batch), err)
        return {p["id"]: {"terminology_id": None, "confidence": 0.0, "reasoning": err, "error": err} for p in batch}
    result = {entry.get("id"): entry for entry in parsed.get("parts", [])}
    for p in batch:
        if p["id"] not in result:
            result[p["id"]] = {"terminology_id": None, "confidence": 0.0, "reasoning": "missing from response", "error": "missing"}
    return result


def classify_all(conn, args):
    logger.info("Loading PCdb terminology corpus...")
    term_rows = fetch_terminology_corpus(conn)
    active_rows = [r for r in term_rows if r["is_active"]]
    by_pair = defaultdict(list)
    # Pooled by subcategory NAME alone, across every category that has one -- ~62% of PCdb
    # subcategory names exist under more than one category (e.g. "Fuel Injection System and
    # Related Components" under both "Engine" and "Air and Fuel Delivery", each with a different,
    # non-overlapping term list). Stage 1 picking the right subcategory name but the wrong
    # category is a real, confirmed failure mode (FUEL INJECTOR routed to "Engine" instead of
    # "Air and Fuel Delivery") -- pooling means Stage 2 sees the term either way. This does NOT
    # fix Stage 1 picking the wrong subcategory NAME entirely (a different, harder failure mode,
    # confirmed separately on SPEAKER -- routed to "Electronic Accessories" when the real term
    # lives under the unrelated "Mobile Multi-Media"); only a same-name cross-category collision
    # is recoverable this way.
    by_subcat_name = defaultdict(list)
    for r in active_rows:
        by_pair[(r["category_name"], r["subcategory_name"])].append(r)
        by_subcat_name[r["subcategory_name"]].append(r)

    # Each subcategory label carries a few real example terminology names (shortest names first,
    # as a proxy for "most generic/representative" rather than a deeply specific hardware term) --
    # a bare subcategory name can be a poor semantic match for how a product is actually described
    # (confirmed real miss: WeatherTech "Cargo Liner" parts never got routed to the real correct
    # subcategory, "Trunk Lid and Compartment", because that name alone doesn't read as "cargo
    # liner" -- a hint like "e.g. Cargo Floor Liner, Trunk Lid, ..." gives the model something to
    # actually match against). Stage 1 is told to return the bare name only, but the parenthetical
    # is stripped from its response defensively too (see _strip_example_hint) in case it echoes
    # the whole decorated label back instead.
    by_category = defaultdict(list)
    for (cat, subcat), rows in by_pair.items():
        examples = [r["name"] for r in sorted(rows, key=lambda r: len(r["name"]))[:3]]
        label = f"{subcat} (e.g. {', '.join(examples)})" if examples else subcat
        by_category[cat].append(label)
    taxonomy_text = "\n".join(f"{cat}: {'; '.join(sorted(subcats))}" for cat, subcats in sorted(by_category.items()))
    logger.info("Loaded %d active terminologies across %d (category, subcategory) pairs", len(active_rows), len(by_pair))

    brand_ids = [int(x) for x in args.brand_ids.split(",")] if args.brand_ids else None
    logger.info("Fetching parts to classify (brand_ids=%s, limit=%s, only_unclassified=%s)...",
                brand_ids, args.limit, not args.reclassify)
    parts = fetch_parts_to_classify(conn, brand_ids, args.limit, only_unclassified=not args.reclassify)
    logger.info("Found %d part(s) to classify", len(parts))
    if not parts:
        return

    logger.info("Fetching provider data for %d part(s)...", len(parts))
    provider_rows_by_part = fetch_provider_rows_for_parts(conn, [p["id"] for p in parts])
    for p in parts:
        p["context"] = build_part_context(p["brand"], p["description"], provider_rows_by_part.get(p["id"], []))

    cli = get_llm_client(args.llm_base_url, args.llm_api_key)
    model = args.llm_model
    t0 = time.monotonic()

    # ---- Stage 1: batched, parallel ----
    stage1_batches = list(chunked(parts, args.batch_size))
    logger.info("Stage 1: %d batch(es) of up to %d parts, %d worker(s)", len(stage1_batches), args.batch_size, args.max_workers)
    stage1_results = {}
    with ThreadPoolExecutor(max_workers=args.max_workers) as pool:
        futures = [pool.submit(run_stage1_batch, cli, model, taxonomy_text, batch) for batch in stage1_batches]
        done = 0
        for fut in as_completed(futures):
            stage1_results.update(fut.result())
            done += 1
            logger.info("Stage 1 progress: %d/%d batches done", done, len(stage1_batches))

    # ---- Group by assigned pair for Stage 2 ----
    groups = defaultdict(list)
    unroutable = []
    for p in parts:
        r = stage1_results.get(p["id"], {})
        cat, subcat = _strip_example_hint(r.get("category")), _strip_example_hint(r.get("subcategory"))
        p["stage1_confidence"] = r.get("confidence")
        p["stage1_reasoning"] = r.get("reasoning")
        p["stage1_error"] = r.get("error")
        if cat and subcat and (cat, subcat) in by_pair:
            groups[(cat, subcat)].append(p)
        else:
            unroutable.append(p)
    logger.info("Stage 1 done in %.1fs: %d part(s) routed across %d pair(s), %d unroutable",
                time.monotonic() - t0, sum(len(v) for v in groups.values()), len(groups), len(unroutable))

    # ---- Stage 2: batched per pair, parallel. Every terminology sharing the assigned
    # subcategory NAME is shown (pooled across categories, see by_subcat_name above) -- no
    # narrowing/shortlisting, so the correct term can never be excluded by a retrieval step
    # missing it (confirmed real failure mode: a narrowed top-40 shortlist excluded the correct
    # "Manual Transmission Shifter Block Off Plate" term from a 365-term pair) and a same-name
    # cross-category collision doesn't lose the term either (confirmed: FUEL INJECTOR recovered
    # via pooling despite Stage 1 picking the "wrong" category). The tradeoff is a much bigger
    # prompt for the largest pooled names -- with no per-token API cost this is a straightforward
    # completeness-over-size tradeoff, but it does mean a smaller --batch-size may be needed if
    # your model's context window is limited.
    stage2_jobs = []  # (cat, subcat, candidates, batch)
    for (cat, subcat), group_parts in groups.items():
        candidates = by_subcat_name[subcat]
        for batch in chunked(group_parts, args.batch_size):
            stage2_jobs.append((cat, subcat, candidates, batch))

    logger.info("Stage 2: %d batch job(s), %d worker(s)", len(stage2_jobs), args.max_workers)
    t1 = time.monotonic()
    stage2_results = {}
    with ThreadPoolExecutor(max_workers=args.max_workers) as pool:
        futures = {pool.submit(run_stage2_batch, cli, model, cat, subcat, candidates, batch): (cat, subcat)
                   for cat, subcat, candidates, batch in stage2_jobs}
        done = 0
        for fut in as_completed(futures):
            stage2_results.update(fut.result())
            done += 1
            logger.info("Stage 2 progress: %d/%d batch jobs done", done, len(stage2_jobs))
    logger.info("Stage 2 done in %.1fs", time.monotonic() - t1)

    # ---- Assemble rows: EVERY part gets a row, even failures/unroutables/nulls ----
    out_rows = []
    for p in unroutable:
        status = "error" if p.get("stage1_error") else "unclassifiable"
        out_rows.append({
            "master_part_id": p["id"], "status": status, "category": None, "subcategory": None,
            "stage1_confidence": p.get("stage1_confidence"), "part_terminology_id": None,
            "stage2_confidence": None,
            "reasoning": f"Stage 1: {p.get('stage1_error') or p.get('stage1_reasoning') or 'no matching (category, subcategory) found'}",
            "model_used": model,
        })
    for (cat, subcat), group_parts in groups.items():
        for p in group_parts:
            r2 = stage2_results.get(p["id"], {})
            tid = r2.get("terminology_id")
            if r2.get("error"):
                status = "error"
            elif tid is None:
                status = "unclassifiable"
            else:
                status = "classified"
            reasoning_parts = []
            if p.get("stage1_reasoning"):
                reasoning_parts.append(f"Stage 1: {p['stage1_reasoning']}")
            if r2.get("reasoning") or r2.get("error"):
                reasoning_parts.append(f"Stage 2: {r2.get('reasoning') or r2.get('error')}")
            out_rows.append({
                "master_part_id": p["id"], "status": status, "category": cat, "subcategory": subcat,
                "stage1_confidence": p.get("stage1_confidence"), "part_terminology_id": tid,
                "stage2_confidence": r2.get("confidence"),
                "reasoning": " | ".join(reasoning_parts) or None,
                "model_used": model,
            })

    classified = sum(1 for r in out_rows if r["status"] == "classified")
    unclassifiable = sum(1 for r in out_rows if r["status"] == "unclassifiable")
    errors = sum(1 for r in out_rows if r["status"] == "error")
    logger.info("=== SUMMARY: %d classified, %d unclassifiable, %d errors (total %d, elapsed %.1fs) ===",
                classified, unclassifiable, errors, len(out_rows), time.monotonic() - t0)

    upsert_results(conn, out_rows, dry_run=args.dry_run)


# ============================================================================================
# CLI
# ============================================================================================

def parse_args():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    # No credential defaults on purpose -- this script is shared externally and must never carry
    # real production secrets in its source (or in git history). Supply via --db-* flags or
    # DB_HOST/DB_PORT/DB_NAME/DB_USER/DB_PASSWORD env vars.
    p.add_argument("--db-host", default=os.environ.get("DB_HOST"))
    p.add_argument("--db-port", default=os.environ.get("DB_PORT", "5432"))
    p.add_argument("--db-name", default=os.environ.get("DB_NAME"))
    p.add_argument("--db-user", default=os.environ.get("DB_USER"))
    p.add_argument("--db-password", default=os.environ.get("DB_PASSWORD"))
    p.add_argument("--llm-base-url", default=os.environ.get("LLM_BASE_URL"))
    p.add_argument("--llm-api-key", default=os.environ.get("LLM_API_KEY", "not-needed"))
    p.add_argument("--llm-model", default=os.environ.get("LLM_MODEL"))
    p.add_argument("--brand-ids", default=None, help="comma-separated MasterPart.brand_id filter")
    p.add_argument("--limit", type=int, default=None, help="cap total parts processed")
    p.add_argument("--batch-size", type=int, default=50, help="parts per LLM batch call (Stage 1 and Stage 2)")
    p.add_argument("--max-workers", type=int, default=8, help="concurrent batch calls")
    p.add_argument("--reclassify", action="store_true", help="include parts that already have a row (default: skip them)")
    p.add_argument("--dry-run", action="store_true", help="classify but don't write to the database")
    p.add_argument("--log-file", default=None)
    return p.parse_args()


def main():
    args = parse_args()
    setup_logging(args.log_file)
    missing = [f"--{n.replace('_', '-')}" for n in ("db_host", "db_name", "db_user", "db_password") if not getattr(args, n)]
    if not args.llm_base_url:
        missing.append("--llm-base-url")
    if not args.llm_model:
        missing.append("--llm-model")
    if missing:
        logger.error("Missing required config (flag or its env var): %s", ", ".join(missing))
        sys.exit(1)

    conn = get_db_conn(args)
    try:
        classify_all(conn, args)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
