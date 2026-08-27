"""
Experiment: can the LLM do the parser's job?

``src.domain.tire_size`` currently owns every dimension, and it is the one part of the pipeline
with outside verification -- 99.8% agreement with distributor-stated diameters over 3,431 rows.
This module asks the model the same question and measures how close it gets, **writing nothing**.
The parser stays the source of truth regardless of the outcome; the point is a number, not a
migration.

Two rules make the comparison mean something:

  **Read, do not derive.** The model is told to transcribe what the string says and return null
  otherwise. If it were allowed to compute overall diameter from width and aspect it would score
  well on arithmetic and tell us nothing about extraction -- and a model that quietly computes is
  exactly the failure the parser exists to prevent, because it will also quietly compute when the
  string is ambiguous.

  **Null is a correct answer.** Every field the string omits should come back null. Scoring
  counts a null-vs-null as agreement and a null-vs-value as an abstention, not an error, so a
  model that declines appropriately is not punished for it.

The traps are deliberately included in the sample. Model-year ranges, wheel sizes and bolt
patterns are what cost the parser the most iterations; if the model walks into them, that is the
finding.
"""
import dataclasses
import decimal
import json
import typing

from django.db import connection

from src.domain import tire_size
from src.integrations.services import tire_enrichment

_LOG_PREFIX = "[SIZE-EXPERIMENT]"

# Fields compared against the parser. overall_diameter_in is deliberately absent from the scored
# set: the prompt forbids deriving it, so the model should return it only when a distributor
# printed it, which is a different question from "did you read the size correctly".
COMPARED_FIELDS = (
    "notation",
    "service_type",
    "section_width_mm",
    "aspect_ratio",
    "rim_diameter_in",
    "construction",
    "load_index",
    "load_index_dual",
    "speed_rating",
    "load_range",
    "size_display",
)

NUMERIC_FIELDS = frozenset(["section_width_mm", "aspect_ratio", "load_index", "load_index_dual"])
DECIMAL_FIELDS = frozenset(["rim_diameter_in", "section_width_in", "overall_diameter_in"])

SYSTEM_PROMPT = """You transcribe tire size information from distributor catalogue text.

You are given the raw titles a distributor wrote for ONE product. Read the tire size out of them
and report exactly what is written.

THE ONE RULE THAT MATTERS: transcribe, never derive.

- Report only what the text states. If the text does not state a field, return null for it.
- Do NOT calculate. Do not compute overall diameter from width and aspect. Do not infer load
  range from load index, or a speed rating from a model's reputation, or an aspect ratio that is
  simply absent. A null is a correct and expected answer; an invented value is not.
- If the text contains no tire size at all, return {"is_size_present": false} and nothing else.

READING THE NOTATION

metric      205/55R16, LT275/70R18, 275/40ZR20, 245/70R19.5
            <section width in mm>/<aspect ratio %><construction><rim diameter in inches>
flotation   33X12.50R15LT, 31x10.50-15, 35X12.50R20
            <overall diameter in>X<section width in><construction><rim diameter in>
numeric     7.50-16, 8.75R16.5
            <section width in><construction><rim diameter in>   (states NO aspect ratio)
motorcycle  120/100-18, 90/90-21   metric shape, but a 2-digit width or an aspect above 95

construction is the letter or mark between aspect and rim: R radial, ZR high-speed radial
marker, B belted bias, D or a bare hyphen for diagonal/bias ply.

SERVICE DESCRIPTION -- this is where mistakes happen

After the size comes the service description, then sometimes a load range.

  "LT225/75R16 115/Q E"     load_index 115, speed_rating Q, load_range E
  "275/60R20 116S XL"       load_index 116, speed_rating S, load_range XL
  "LT265/70R17 121/118S"    load_index 121, load_index_dual 118, speed_rating S
  "LT265/70R17 121/118S LRE" same, plus load_range E written with an LR prefix
  "275/35R19XL 100W"        load_range XL glued to the rim, load_index 100, speed W
  "35x12.50R17 128/R F"     load_index 128, speed_rating R, load_range F

Two traps in that block:

  A slash between load index and speed is a SEPARATOR, not a dual load. "115/Q" is load index
  115 and speed Q. A dual load has TWO NUMBERS: "121/118S".

  The load range letters (A-N) and the speed symbols share letters. Position decides: the letter
  attached to the load index is the SPEED RATING; a letter standing alone after it is the LOAD
  RANGE. In "115/Q E", Q is the speed and E is the load range. Never the other way round.

Load range vocabulary: SL, XL, RF, and the single letters A B C D E F G H J L M N.
(I, K and O are not used -- they read as 1 and 0 on a sidewall.)
Speed vocabulary: A1-A8, B C D E F G J K L M N P Q R S T U H V W Y and (Y).
ZR is NOT a speed rating. It is a construction marker; the real speed is the letter in the
service description.

FIELD FORMAT -- report the value, not the raw token

These are the mistakes to avoid. Each one is a real answer that was wrong only in its shape.

1. load_range is the DESIGNATION ALONE. Distributors often append the ply rating after a slash:
   "E/10" is Load Range E with a 10-ply equivalence. Report load_range "E", never "E/10".
   Likewise "G/14" -> "G", "C/6" -> "C", "L/20" -> "L", "(F/12)" -> "F". The ply number is
   derived from the designation elsewhere and must not appear in this field.

2. size_display carries ONLY the size. Strip any ply suffix: "285/70R17/10" -> "285/70R17",
   "LT235/85R16/10" -> "LT235/85R16". Strip the service description and everything after it.

3. A decimal-less commercial rim is normalised WITH the decimal. "285/75R245" is a 24.5 inch
   rim: report rim_diameter_in 24.5 and size_display "285/75R24.5". Same for R195 -> 19.5,
   R225 -> 22.5, R175 -> 17.5. This is the one place you rewrite what is written, because the
   two spellings are the same rim and must produce the same size_display.

4. NEVER add a service type that is not in the text. "275/55R20 113H" has no LT, so
   service_type is null and size_display is "275/55R20" -- not "LT275/55R20". A tire's service
   type changes what it fits; inferring one from the model name or the load index is exactly
   the kind of guess this task forbids.

5. C is the European commercial marker and is written AFTER the rim: "225/75R16C". Report
   service_type "C" and size_display "225/75R16C", not "C225/75R16". LT, ST, P and T prefix.

6. Parentheses around a whole service description do NOT make it the open-ended (Y).
   "285/35R22 (106Y) XL" is load_index 106, speed_rating "Y". Only a bare "(Y)" standing where
   the speed symbol goes -- "295/30ZR19 100(Y)" -- is the open-ended symbol.

7. Transcribe the load index digit for digit. "37X12.50R20LT 128Q" is 128. Do not adjust it to
   a value that looks more usual for the size.

8. Report the construction character that is WRITTEN, even when it is obviously sloppy.
   "LT325/50-22 122R" has a bare hyphen, so construction is "D" -- report D. Many distributors
   type a hyphen where the tire is really a radial; correcting that is a judgement, and this
   task is transcription.

WHAT IS NOT A TIRE SIZE -- return is_size_present false

  "Belltech LOWERING KIT 16.5-17 Chevy Silverado"   16.5-17 is a model-year range
  "South Bend Clutch 05.5-13 Dodge"                 05.5-13 is a model-year range
  "WeatherTech 21-24 Ford F-150 / 23-24 F-250"      F-150 is a vehicle, not a 150mm tire
  "ARROW 20X10.5 5X112 66.5 RBL +40"                a wheel: 20x10.5 rim, 5x112 bolt pattern
  "XD811 FINS 20X9 -12MM RED"                       a wheel accessory, -12mm is offset
  "Fork Springs - Prog. 4.5-10.5 N/mm"              a spring rate
  "4981910571360"                                   a barcode

A trailing number after the service description is usually the distributor's own overall
diameter ("116S XL 32.9") -- report it as stated_overall_diameter_in ONLY if it is clearly that.
If the trailing text is part of the model name ("BS TUR ER33", "MI SCORCHER 31"), it is not.

WORKED EXAMPLES

Titles: ["TER GRAP G3 LT225/75R16 115/Q E 29.53"]
{"is_size_present": true, "notation": "metric", "service_type": "LT", "section_width_mm": 225,
 "aspect_ratio": 75, "section_width_in": null, "rim_diameter_in": 16, "construction": "R",
 "load_index": 115, "load_index_dual": null, "speed_rating": "Q", "load_range": "E",
 "stated_overall_diameter_in": 29.53, "size_display": "LT225/75R16"}

Titles: ["RECON GRAP 33x11.50R16LT 124R 32.5"]
{"is_size_present": true, "notation": "flotation", "service_type": "LT", "section_width_mm": null,
 "aspect_ratio": null, "section_width_in": 11.50, "rim_diameter_in": 16, "construction": "R",
 "load_index": 124, "load_index_dual": null, "speed_rating": "R", "load_range": null,
 "stated_overall_diameter_in": 32.5, "size_display": "33X11.50R16LT"}

Titles: ["NT555 G2 275/40ZR20 106W XL"]
{"is_size_present": true, "notation": "metric", "service_type": null, "section_width_mm": 275,
 "aspect_ratio": 40, "section_width_in": null, "rim_diameter_in": 20, "construction": "ZR",
 "load_index": 106, "load_index_dual": null, "speed_rating": "W", "load_range": "XL",
 "stated_overall_diameter_in": null, "size_display": "275/40ZR20"}

Titles: ["Kenda K772 Parker DT Rear Tire - 120/100-18 6PR 68M TT"]
{"is_size_present": true, "notation": "motorcycle", "service_type": null, "section_width_mm": 120,
 "aspect_ratio": 100, "section_width_in": null, "rim_diameter_in": 18, "construction": "D",
 "load_index": 68, "load_index_dual": null, "speed_rating": "M", "load_range": null,
 "stated_overall_diameter_in": null, "size_display": "120/100-18"}

Titles: ["Toyo Open Country M/T Tire - LT275/70R18 125P E/10 (1.32 FET Inc.)"]
{"is_size_present": true, "notation": "metric", "service_type": "LT", "section_width_mm": 275,
 "aspect_ratio": 70, "section_width_in": null, "rim_diameter_in": 18, "construction": "R",
 "load_index": 125, "load_index_dual": null, "speed_rating": "P", "load_range": "E",
 "stated_overall_diameter_in": null, "size_display": "LT275/70R18"}
   (load_range is "E", not "E/10". The 1.32 is a federal excise tax, not a diameter.)

Titles: ["Toyo M122 - 285/75R245 144L (G/14) M122 TL"]
{"is_size_present": true, "notation": "metric", "service_type": null, "section_width_mm": 285,
 "aspect_ratio": 75, "section_width_in": null, "rim_diameter_in": 24.5, "construction": "R",
 "load_index": 144, "load_index_dual": null, "speed_rating": "L", "load_range": "G",
 "stated_overall_diameter_in": null, "size_display": "285/75R24.5"}
   (R245 is a 24.5 inch rim, normalised with the decimal.)

Titles: ["275/55R20 113H OPHTD TL"]
{"is_size_present": true, "notation": "metric", "service_type": null, "section_width_mm": 275,
 "aspect_ratio": 55, "section_width_in": null, "rim_diameter_in": 20, "construction": "R",
 "load_index": 113, "load_index_dual": null, "speed_rating": "H", "load_range": null,
 "stated_overall_diameter_in": null, "size_display": "275/55R20"}
   (No LT anywhere in the text, so none is reported. TL means tubeless.)

Titles: ["Belltech LOWERING KIT 16.5-17 Chevy Silverado All Cabs 4WD"]
{"is_size_present": false}

Return only JSON. No prose, no markdown fences."""


@dataclasses.dataclass
class SizeComparison:
    master_part_id: int
    titles: typing.List[str]
    parser: typing.Dict[str, typing.Any]
    llm: typing.Optional[typing.Dict[str, typing.Any]] = None
    error: typing.Optional[str] = None

    def verdict(self, field: str) -> typing.Optional[bool]:
        """True / False / None where None means one side abstained -- not an error."""
        if self.llm is None:
            return None
        ours, theirs = self.parser.get(field), self.llm.get(field)
        if ours is None or theirs is None:
            return None
        return _normalize(field, ours) == _normalize(field, theirs)

    def abstained(self, field: str) -> typing.Optional[str]:
        if self.llm is None:
            return None
        ours, theirs = self.parser.get(field), self.llm.get(field)
        if ours is not None and theirs is None:
            return "llm"
        if theirs is not None and ours is None:
            return "parser"
        return None


def _normalize(field: str, value: typing.Any) -> typing.Any:
    if value is None:
        return None
    if field in NUMERIC_FIELDS:
        try:
            return int(value)
        except (TypeError, ValueError):
            return None
    if field in DECIMAL_FIELDS:
        try:
            return decimal.Decimal(str(value)).quantize(decimal.Decimal("0.1"))
        except (TypeError, ValueError, decimal.InvalidOperation):
            return None
    text = str(value).strip().upper()
    if field == "size_display":
        # The parser renders bias as a hyphen and normalises the X; compare on content, not
        # punctuation, or every flotation size reads as a disagreement.
        return text.replace(" ", "").replace("-", "").replace("/", "")
    if field == "notation":
        # The prompt offers "motorcycle" as its own notation because that is how a human reads
        # it; the parser files those under metric. Same answer, different label.
        return "METRIC" if text == "MOTORCYCLE" else text
    return text


def parser_fields(parsed: tire_size.ParsedSize) -> typing.Dict[str, typing.Any]:
    return {
        "notation": parsed.notation,
        "service_type": parsed.service_type,
        "section_width_mm": parsed.section_width_mm,
        "aspect_ratio": parsed.aspect_ratio,
        "section_width_in": parsed.section_width_in,
        "rim_diameter_in": parsed.rim_diameter_in,
        "construction": parsed.construction,
        "load_index": parsed.load_index,
        "load_index_dual": parsed.load_index_dual,
        "speed_rating": parsed.speed_rating,
        "load_range": parsed.load_range,
        "size_display": parsed.size_display,
    }


def sample_candidates(
    *, brand_ids: typing.Sequence[int], limit: int, seed: int = 1
) -> typing.List[tire_enrichment.TireCandidate]:
    """Master parts the parser already decoded, so every row has a reference answer."""
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT mp.id, mp.brand_id, b.name AS brand_name, mp.part_number, mp.sku, mp.description,"
            "       mp.overview_category, mp.category, mp.product_type, mp.product_type_source "
            "FROM master_parts mp JOIN brands b ON b.id = mp.brand_id "
            "JOIN tire_specs ts ON ts.master_part_id = mp.id "
            "WHERE mp.brand_id = ANY(%s) "
            "ORDER BY md5(mp.id::text || %s) LIMIT %s",
            [list(brand_ids), str(seed), limit],
        )
        columns = [c[0] for c in cursor.description]
        rows = [dict(zip(columns, r)) for r in cursor.fetchall()]

    provider_rows = tire_enrichment._provider_rows_for([r["id"] for r in rows])
    candidates = []
    for row in rows:
        candidate = tire_enrichment.build_candidate(master_part=row, provider_rows=provider_rows.get(row["id"], []))
        if candidate is not None:
            candidates.append(candidate)
    return candidates


def ask(cli, candidate: tire_enrichment.TireCandidate, llm_module, model: str) -> SizeComparison:
    """One call. The model sees only the titles -- never the parser's answer."""
    comparison = SizeComparison(
        master_part_id=candidate.master_part_id,
        titles=candidate.titles,
        parser=parser_fields(candidate.parsed),
    )
    payload = json.dumps({"titles": candidate.titles}, separators=(",", ":"))
    response, error = llm_module.complete_json(cli, SYSTEM_PROMPT, payload, max_tokens=600, model=model)
    if error is not None:
        comparison.error = error
    elif not isinstance(response, dict):
        comparison.error = "not-an-object"
    elif response.get("is_size_present") is False:
        comparison.llm = {field: None for field in COMPARED_FIELDS}
        comparison.llm["is_size_present"] = False
    else:
        comparison.llm = response
    return comparison
