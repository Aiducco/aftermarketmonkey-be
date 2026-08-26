"""
Independent audit of the LLM-derived fields on ``tire_specs``.

The size fields are already verified against evidence we did not produce -- distributors append
their own overall diameter and 99.8% of them agree with ours. **The LLM fields have no such
check.** ``model_name``, ``tread_category``, ``vehicle_class`` and ``is_3pmsf`` rest entirely on
one model's recall, and self-reported confidence is not evidence: 44,000 of 47,312 specs sit at
>= 0.95, which says the model is rarely uncertain, not that it is rarely wrong.

**Blind re-derivation by a different vendor's model.** Two design choices carry the weight:

  *Different model family.* Re-running the same prompt through the same model measures
  reproducibility. Its errors are correlated with themselves, so a confidently-wrong answer comes
  back confidently wrong and the agreement rate reads as 100%. The auditor here is Anthropic's
  Claude against Azure's GPT-4.1, so a disagreement is informative.

  *Blind, not adversarial.* The auditor is never shown the existing answer. Asking a model to
  check an answer it can see biases it toward ratification; asking it the original question and
  comparing is a cleaner measurement.

Agreement is a **lower bound on correctness, not a proof of it**: both models can share a
misconception, and neither has seen the tire. A disagreement rate of N% means at least N% of rows
have one wrong answer -- it does not mean 100-N% are right.
"""
import collections
import dataclasses
import json
import random
import re
import typing

from django.db import connection

from src.integrations.llm import anthropic_llm
from src.integrations.services import tire_enrichment

_LOG_PREFIX = "[TIRE-AUDIT]"

# Confidence bands to sample across. A flat random sample would be ~93% high-confidence rows and
# would say almost nothing about the tail, which is where errors concentrate.
CONFIDENCE_BANDS = (
    ("high", "ts.llm_confidence >= 0.95"),
    ("mid", "ts.llm_confidence >= 0.80 AND ts.llm_confidence < 0.95"),
    ("low", "ts.llm_confidence < 0.80 OR ts.llm_confidence IS NULL"),
)

_PUNCT_RE = re.compile(r"[^a-z0-9]+")


def normalize_model_name(name: typing.Optional[str]) -> str:
    """Case- and punctuation-insensitive. "Pilot Sport A/S 4" and "Pilot Sport AS 4" are the same
    answer written two ways, and counting that as a disagreement would drown the real ones."""
    return _PUNCT_RE.sub("", (name or "").lower())


@dataclasses.dataclass
class AuditRow:
    master_part_id: int
    brand_name: str
    size_display: str
    titles: typing.List[str]
    band: str
    ours: typing.Dict[str, typing.Any]
    theirs: typing.Optional[typing.Dict[str, typing.Any]] = None
    error: typing.Optional[str] = None

    def agrees_on(self, field: str) -> typing.Optional[bool]:
        """
        True / False / ``None`` for "not a meaningful comparison".

        **NULL is not a wrong answer, it is an abstention.** Every one of these fields treats
        NULL as "unknown", and the enrichment prompt actively rewards declining over guessing. So
        a row where one side answered and the other abstained is not a disagreement -- counting
        it as one put is_3pmsf agreement at 10% when the two models never once contradicted each
        other on it. Those land in ``not_comparable`` and are reported separately, because "we
        abstain far more than the auditor" is a real and useful finding, just not an error rate.
        """
        if self.theirs is None:
            return None
        ours, theirs = self.ours.get(field), self.theirs.get(field)
        if field == "model_name":
            ours, theirs = normalize_model_name(ours) or None, normalize_model_name(theirs) or None
        if ours is None or theirs is None:
            return None
        return ours == theirs

    def abstention(self, field: str) -> typing.Optional[str]:
        """Which side declined, when exactly one did."""
        if self.theirs is None:
            return None
        ours, theirs = self.ours.get(field), self.theirs.get(field)
        if field == "model_name":
            ours, theirs = normalize_model_name(ours) or None, normalize_model_name(theirs) or None
        if ours is None and theirs is not None:
            return "ours"
        if theirs is None and ours is not None:
            return "auditor"
        return None


@dataclasses.dataclass
class FieldResult:
    field: str
    agree: int = 0
    disagree: int = 0
    not_comparable: int = 0
    we_abstained: int = 0
    auditor_abstained: int = 0

    @property
    def compared(self) -> int:
        return self.agree + self.disagree

    @property
    def agreement(self) -> float:
        return self.agree / self.compared if self.compared else 0.0


AUDITED_FIELDS = ("tread_category", "model_name", "vehicle_class", "is_3pmsf")


def sample(
    *,
    per_band: int,
    brand_ids: typing.Optional[typing.Sequence[int]] = None,
    only_3pmsf: bool = False,
    seed: int = 1,
) -> typing.List[typing.Dict[str, typing.Any]]:
    """
    Stratified sample across confidence bands.

    Ordered by a seeded hash rather than ``random()`` so a re-run audits the same rows and two
    runs are comparable. ``only_3pmsf`` narrows to rows asserting severe-snow certification --
    the field with legal exposure and the one worth over-sampling.
    """
    where = ["TRUE"]
    params: typing.List[typing.Any] = []
    if brand_ids:
        where.append("mp.brand_id = ANY(%s)")
        params.append(list(brand_ids))
    if only_3pmsf:
        where.append("ts.is_3pmsf IS TRUE")

    rows: typing.List[typing.Dict[str, typing.Any]] = []
    for band, predicate in CONFIDENCE_BANDS:
        sql = """
            SELECT ts.master_part_id, b.name, ts.size_display, ts.model_name, ts.tread_category,
                   ts.vehicle_class, ts.is_3pmsf, ts.llm_confidence
            FROM tire_specs ts
            JOIN master_parts mp ON mp.id = ts.master_part_id
            JOIN brands b ON b.id = mp.brand_id
            WHERE {where} AND ({band})
            ORDER BY md5(ts.master_part_id::text || %s)
            LIMIT %s
        """.format(
            where=" AND ".join(where), band=predicate
        )
        with connection.cursor() as cursor:
            cursor.execute(sql, params + [str(seed), per_band])
            for row in cursor.fetchall():
                rows.append(
                    {
                        "master_part_id": row[0],
                        "brand_name": row[1],
                        "size_display": row[2],
                        "band": band,
                        "ours": {
                            "model_name": row[3],
                            "tread_category": row[4],
                            "vehicle_class": row[5],
                            "is_3pmsf": row[6],
                        },
                    }
                )
    random.Random(seed).shuffle(rows)
    return rows


def build_audit_rows(sampled: typing.Sequence[typing.Dict[str, typing.Any]]) -> typing.List[AuditRow]:
    """Rebuild each sampled spec's original evidence -- the same titles the enrichment model saw."""
    ids = [row["master_part_id"] for row in sampled]
    if not ids:
        return []
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT mp.id, mp.brand_id, b.name AS brand_name, mp.part_number, mp.sku, mp.description,"
            "       mp.overview_category, mp.category, mp.product_type, mp.product_type_source "
            "FROM master_parts mp JOIN brands b ON b.id = mp.brand_id WHERE mp.id = ANY(%s)",
            [ids],
        )
        columns = [c[0] for c in cursor.description]
        masters = {r[0]: dict(zip(columns, r)) for r in cursor.fetchall()}
    provider_rows = tire_enrichment._provider_rows_for(ids)

    out = []
    for row in sampled:
        master = masters.get(row["master_part_id"])
        if master is None:
            continue
        candidate = tire_enrichment.build_candidate(
            master_part=master, provider_rows=provider_rows.get(row["master_part_id"], [])
        )
        if candidate is None:
            continue
        out.append(
            AuditRow(
                master_part_id=row["master_part_id"],
                brand_name=row["brand_name"],
                size_display=row["size_display"],
                titles=candidate.titles,
                band=row["band"],
                ours=row["ours"],
            )
        )
    return out


def audit_one(cli, row: AuditRow, system_prompt: str, model: typing.Optional[str] = None) -> AuditRow:
    payload = json.dumps(
        {"brand": row.brand_name, "titles": row.titles, "size": row.size_display},
        separators=(",", ":"),
    )
    response, error = anthropic_llm.complete_json(cli, system_prompt, payload, max_tokens=400, model=model)
    if error is not None:
        row.error = error
        return row
    if not isinstance(response, dict):
        row.error = "not-an-object"
        return row
    row.theirs = {
        "model_name": response.get("model_name"),
        "tread_category": (response.get("tread_category") or None),
        "vehicle_class": (response.get("vehicle_class") or None),
        "is_3pmsf": response.get("is_3pmsf") if isinstance(response.get("is_3pmsf"), bool) else None,
    }
    return row


def tally(rows: typing.Sequence[AuditRow]) -> typing.Dict[str, FieldResult]:
    results = {field: FieldResult(field) for field in AUDITED_FIELDS}
    for row in rows:
        if row.theirs is None:
            continue
        for field in AUDITED_FIELDS:
            verdict = row.agrees_on(field)
            if verdict is None:
                results[field].not_comparable += 1
                who = row.abstention(field)
                if who == "ours":
                    results[field].we_abstained += 1
                elif who == "auditor":
                    results[field].auditor_abstained += 1
            elif verdict:
                results[field].agree += 1
            else:
                results[field].disagree += 1
    return results


def tally_by_band(rows: typing.Sequence[AuditRow], field: str) -> typing.Dict[str, FieldResult]:
    by_band: typing.Dict[str, FieldResult] = collections.defaultdict(lambda: FieldResult(field))
    for row in rows:
        verdict = row.agrees_on(field)
        if verdict is None:
            by_band[row.band].not_comparable += 1
        elif verdict:
            by_band[row.band].agree += 1
        else:
            by_band[row.band].disagree += 1
    return dict(by_band)
