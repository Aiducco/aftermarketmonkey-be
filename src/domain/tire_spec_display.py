"""
Compose the tire specification block a part detail page renders for a ``product_type == "tire"``
part -- the SIZE / RATINGS / TYPE & CAPABILITY card.

Same discipline as ``src.domain.spec_line``, one level up in detail: **structured columns in, a
payload out, never a distributor title**. Pure -- a mapping of ``tire_specs`` columns in, a plain
dict out, no Django and no DB, so the whole card is testable without a database.

Three rules the card exists to enforce:

  * **A code alone is not information.** ``97Y`` means nothing to a buyer; ``1,609 lb`` and
    ``186 mph`` do. Every coded field is emitted as a ``<field>`` / ``<field>_label`` pair (the
    same convention ``src.api.services.tire_search`` uses for ``tread_category``), so the client
    renders the resolved value and keeps the code for filtering.
  * **Unknown is NULL, and every key is always present.** A nullable column ships as ``null``,
    never as ``""`` -- an empty string reads as "the answer is blank" when the truth is "we never
    learned it", and it is indistinguishable from a genuinely empty value. The tri-state flags
    follow the same rule and are the reason it matters most: ``is_3pmsf`` is a certification with
    legal weight, so ``null`` (nobody told us) and ``false`` (checked, not rated) must stay
    distinguishable -- see ``TireSpec``'s docstring. A UI renders ``null`` as an em dash.
  * **Every column a buyer would act on ships.** The whitelist below is the API surface, so a
    column that exists but is not listed here is invisible to the product; the ones deliberately
    withheld are the LLM's own working notes (``llm_confidence``, ``llm_reason``,
    ``llm_model_used``, ``category_reconciled``, ``search_aliases`` -- that last one feeds the
    search index, it is not a fact about the tire).

``overall_diameter_in`` is nominal when ``notation == "numeric"`` (that notation carries no aspect
ratio at all), which is why ``overall_diameter_is_nominal`` ships next to it -- the client must
not present it with the same precision as a stated flotation diameter. ``revolutions_per_mile``
inherits that caveat, being derived from it.

``service_type`` and ``vehicle_class`` look contradictory on some rows and are not: the first is
the sidewall stamp (absent on a Euro-metric size, whatever the tire is for), the second is the
application the catalog assigns it. A 215/70R16 104T XL with no LT prefix, carried as a light
truck tire at 50 psi, is a normal Euro-metric XL, not a dropped prefix.
"""
import decimal
import math
import typing

MM_PER_INCH = decimal.Decimal("25.4")
INCHES_PER_MILE = 63360

# Mirrors ``src.domain.tire_size.SERVICE_TYPES``. "T" is the temporary spare, which is exactly why
# it is spelled out: a buyer seeing "T" next to a full-size tire has no way to know it is the
# donut in the trunk.
SERVICE_TYPE_LABELS = {
    "P": "P-metric",
    "LT": "Light truck",
    "ST": "Special trailer",
    "T": "Temporary spare",
    "C": "Commercial",
}

# Mirrors ``src.domain.tire_size.CONSTRUCTION_*``. ZR stays distinct from R: it is stamped for
# sizes originally rated above 240 km/h and shoppers search for it by name.
CONSTRUCTION_LABELS = {
    "R": "Radial",
    "ZR": "ZR radial",
    "D": "Bias ply",
    "B": "Belted bias",
}

# The passenger vocabulary only (``TireLoadRange.applies_to == "passenger"``). LT/ST letter codes
# have no expansion -- "E" is just Load range E -- and are labelled from the letter itself below.
# RF is the alternate stamping for XL (see ``TireLoadRange.alias``) and is labelled as what it is
# rather than silently rewritten to XL, since that is what the sidewall says.
LOAD_RANGE_LABELS = {
    "SL": "Standard load",
    "XL": "Extra load",
    "RF": "Reinforced",
    "LL": "Light load",
}

# Mirrors ``TireSpec.VEHICLE_CLASS_CHOICES``. Held here rather than read off the model so this
# module stays Django-free, the same trade ``tire_size.SPEED_RATINGS`` makes against migration
# 0178 -- ``src.domain.tests.test_tire_spec_display`` asserts the two stay in step.
VEHICLE_CLASS_LABELS = {
    "passenger": "Passenger",
    "light_truck": "Light truck",
    "trailer": "Trailer",
    "commercial": "Commercial",
    "motorcycle": "Motorcycle",
    "atv_utv": "ATV / UTV",
}

# Mirrors ``TireSpec.SPEC_SOURCE_CHOICES``. Which tier owns the size block is a quality signal, not
# an implementation detail: specs merged from a manufacturer-grade catalog are measured facts,
# specs from the parser are decoded from a distributor's title and are only as good as that title.
# ``src.domain.tests.test_tire_spec_display`` asserts this stays in step with the model.
SPEC_SOURCE_LABELS = {
    "parser": "Parsed from the distributor title",
    "simpletire": "SimpleTire catalog",
    "tdg": "TDG catalog",
}

# NULL means unknown on every one of these, which is why they ship as an explicit ``null`` rather
# than being defaulted to false -- see the module docstring. (The search index drops them instead:
# ``src.search.tires_index._TRISTATE_FLAGS``, same reason, different mechanism, because a Meili
# filter has no third state.)
TRISTATE_FLAGS = (
    "is_3pmsf",
    "is_ms",
    "is_run_flat",
    "is_studdable",
    "is_tubeless",
    "has_reinforced_sidewall",
)

# A tire is sold one at a time but bought four at a time, and "will this carry my truck" is
# answered by the axle set, not by one corner. Multiplying the resolved per-tire figure is exact:
# load index is a per-tire rating by definition.
TIRES_PER_SET = 4

# ...except on a motorcycle, which has two. A set-of-four figure for a scooter tire is not a
# rounding issue, it is a wrong answer, so it is withheld rather than computed. ATV/UTV keeps it:
# those do run four.
VEHICLE_CLASSES_WITHOUT_A_SET_OF_FOUR = frozenset(["motorcycle"])


def _decimal(value: typing.Any) -> typing.Optional[float]:
    """Decimal columns out to JSON. ``float`` matches how pricing is serialised elsewhere."""
    if value is None:
        return None
    return float(value)


def _isoformat(value: typing.Any) -> typing.Optional[str]:
    """Datetimes out to JSON, matching how every other timestamp on the detail payload ships."""
    if value is None:
        return None
    return value.isoformat() if hasattr(value, "isoformat") else str(value)


def _service_type_label(service_type: typing.Optional[str]) -> typing.Optional[str]:
    if not service_type:
        return None
    return SERVICE_TYPE_LABELS.get(service_type, service_type)


def _load_range_label(load_range: typing.Optional[str]) -> typing.Optional[str]:
    """``XL`` -> ``Extra load``; ``E`` -> ``Load range E``."""
    if not load_range:
        return None
    if load_range in LOAD_RANGE_LABELS:
        return LOAD_RANGE_LABELS[load_range]
    return "Load range {}".format(load_range)


def _section_width_in(
    section_width_in: typing.Any,
    section_width_mm: typing.Optional[int],
) -> typing.Optional[float]:
    """
    The inch width, for the ``245 mm (9.65")`` line.

    Stated by the size string on a flotation size and converted from millimetres on a metric one --
    ``src.domain.tire_size`` already writes both into the column, so this normally just passes the
    stored value through. The fallback covers a row enriched before the column existed: same
    conversion, same half-up rounding to a hundredth as the parser, so a backfilled tire and a
    freshly parsed one never disagree in the last digit.
    """
    if section_width_in is not None:
        return float(section_width_in)
    if section_width_mm is None:
        return None
    return float(
        (decimal.Decimal(section_width_mm) / MM_PER_INCH).quantize(
            decimal.Decimal("0.01"), rounding=decimal.ROUND_HALF_UP
        )
    )



def _revolutions_per_mile(overall_diameter_in: typing.Any) -> typing.Optional[float]:
    """
    How many times the tire turns in a mile, from its free diameter: ``63360 / (pi * d)``.

    This is the geometric figure for an unloaded tire. A manufacturer's published revs/mile is
    measured under load and runs roughly 3% higher, because a rolling tire deflects and its
    effective circumference is smaller than its free one -- so do not present this as the
    sidewall's own number. What it is exact for is the comparison people actually make: the ratio
    between two tires' revs/mile is the ratio of their diameters, which is precisely how far a
    speedometer will read off after a size change.
    """
    if overall_diameter_in is None:
        return None
    diameter = float(overall_diameter_in)
    if diameter <= 0:
        return None
    return round(INCHES_PER_MILE / (math.pi * diameter), 1)


def _text(value: typing.Any) -> typing.Optional[str]:
    """
    A nullable text column out to JSON: the value, or ``null``.

    Never ``""``. The columns feeding this are NULL when unknown, and blanking them out would
    hand a client one token for two different states -- see the module docstring.
    """
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def build_tire_specs(
    row: typing.Mapping[str, typing.Any],
    *,
    equivalent_sizes_count: typing.Optional[int] = None,
) -> typing.Dict[str, typing.Any]:
    """
    Render the spec card from a ``tire_specs`` row.

    ``row`` is any mapping with the column names, plus ``tread_category_label`` and
    ``season_category_label`` resolved from the ``tread_category`` table (those labels are the
    only thing a UI ever renders -- the codes are identifiers, see the ``TreadCategory``
    docstring).

    ``equivalent_sizes_count`` is the one figure this module cannot derive from a single row --
    it takes a query across the catalog, so the caller supplies it (``src.api.services.parts``).
    ``None`` means "not counted", which is why it is a keyword with a default rather than part of
    ``row``.

    Every key is always present, so a client renders the card from a fixed template and blanks out
    nulls instead of probing for keys.
    """
    max_load_lb = row.get("max_load_lb")
    tread_category = _text(row.get("tread_category"))
    vehicle_class = _text(row.get("vehicle_class"))
    overall_diameter_in = row.get("overall_diameter_in")
    set_of_four_max_load_lb = (
        max_load_lb * TIRES_PER_SET
        if max_load_lb is not None and vehicle_class not in VEHICLE_CLASSES_WITHOUT_A_SET_OF_FOUR
        else None
    )
    spec_source = _text(row.get("spec_source"))

    payload: typing.Dict[str, typing.Any] = {
        # ---- identity ------------------------------------------------------------------------
        "size_display": row.get("size_display") or "",
        "model_name": _text(row.get("model_name")),
        "sub_model": _text(row.get("sub_model")),
        # ---- size ----------------------------------------------------------------------------
        "notation": _text(row.get("notation")),
        # The sidewall stamp, absent on a Euro-metric size. Not a contradiction of vehicle_class
        # below -- see the module docstring before "fixing" a null here.
        "service_type": _text(row.get("service_type")),
        "service_type_label": _service_type_label(row.get("service_type")),
        "section_width_mm": row.get("section_width_mm"),
        "section_width_in": _section_width_in(row.get("section_width_in"), row.get("section_width_mm")),
        "aspect_ratio": row.get("aspect_ratio"),
        "construction": _text(row.get("construction")),
        "construction_label": CONSTRUCTION_LABELS.get(row.get("construction") or ""),
        "overall_diameter_in": _decimal(overall_diameter_in),
        # Nominal, not measured, for numeric sizes -- do not render it as an exact figure.
        "overall_diameter_is_nominal": row.get("notation") == "numeric",
        "rim_diameter_in": _decimal(row.get("rim_diameter_in")),
        # Unloaded, geometric. Read _revolutions_per_mile before putting it next to a
        # manufacturer's published figure.
        "revolutions_per_mile": _revolutions_per_mile(overall_diameter_in),
        # ---- ratings -------------------------------------------------------------------------
        "load_index": row.get("load_index"),
        "load_index_dual": row.get("load_index_dual"),
        "max_load_lb": max_load_lb,
        "set_of_four_max_load_lb": set_of_four_max_load_lb,
        "speed_rating": _text(row.get("speed_rating")),
        "max_speed_mph": row.get("max_speed_mph"),
        "load_range": _text(row.get("load_range")),
        "load_range_label": _load_range_label(row.get("load_range")),
        # Strength equivalence to bias construction, never a count of physical layers -- see the
        # ``TireLoadRange`` docstring before wording this in a UI. NULL on a passenger load range
        # (XL has no ply equivalent) is an answer, not a gap: render ``load_range_label``.
        "ply_rating": row.get("ply_rating"),
        "max_psi": row.get("max_psi"),
        "tread_depth_32nds": row.get("tread_depth_32nds"),
        "utqg_treadwear": row.get("utqg_treadwear"),
        "utqg_traction": _text(row.get("utqg_traction")),
        "utqg_temperature": _text(row.get("utqg_temperature")),
        # ---- type & capability ---------------------------------------------------------------
        "tread_category": tread_category,
        "tread_category_label": _text(row.get("tread_category_label")),
        # A second axis, not a second guess: a summer UHP tire is UHP on the performance axis and
        # SUMMER on the season one, and either alone is a half-answer. See the column's comment.
        "season_category": _text(row.get("season_category")),
        "season_category_label": _text(row.get("season_category_label")),
        "vehicle_class": vehicle_class,
        "vehicle_class_label": VEHICLE_CLASS_LABELS.get(vehicle_class or ""),
        "tier": _text(row.get("tier")),
        "noise_level": _text(row.get("noise_level")),
        "use_case_tags": list(row.get("use_case_tags") or []),
        # ---- construction & appearance ---------------------------------------------------------
        # Blackwall / Outlined White Lettering / Raised White Lettering, as the catalog publishes
        # it. Appearance is a real purchase decision on a truck tire.
        "sidewall_style": _text(row.get("sidewall_style")),
        # Directional and asymmetrical tires cannot be rotated freely, which is a cost-of-ownership
        # fact, not a styling one.
        "tread_design": _text(row.get("tread_design")),
        "commercial_position": _text(row.get("commercial_position")),
        # 'N0 - Porsche', 'MO - Mercedes-Benz'. Buyers search for these by name: it is the
        # difference between a tire that fits the car and one the manufacturer approved for it.
        "oe_marking": _text(row.get("oe_marking")),
        # ---- ownership ---------------------------------------------------------------------------
        "mileage_warranty_miles": row.get("mileage_warranty_miles"),
        "tire_weight_lb": _decimal(row.get("tire_weight_lb")),
        # ---- fitment to a wheel ----------------------------------------------------------------
        "rim_width_min_in": _decimal(row.get("rim_width_min_in")),
        "rim_width_max_in": _decimal(row.get("rim_width_max_in")),
        # How many other sizes in the catalog stand the same height on the same rim. Supplied by
        # the caller; ``null`` means it was not counted, ``0`` means there are none.
        "equivalent_sizes_count": equivalent_sizes_count,
        # ---- provenance ------------------------------------------------------------------------
        # Who supplied the specs. A catalog-sourced tire is worth more trust than one decoded from
        # a distributor's title, and the client is entitled to say so.
        "spec_source": spec_source,
        "spec_source_label": SPEC_SOURCE_LABELS.get(spec_source or ""),
        # How the catalog row was matched: 1 = brand + part number, 2 = part number + agreeing
        # size, 3 = brand + model + size. Lower is stronger.
        "simpletire_match_tier": row.get("simpletire_match_tier"),
        "tdg_match_tier": row.get("tdg_match_tier"),
        # Parser and model disagreed, or two providers described different sizes. The specs are
        # written anyway; a UI that surfaces sizing decisions should caveat them when this is set.
        "size_disputed": bool(row.get("size_disputed")),
        "enriched_at": _isoformat(row.get("enriched_at")),
    }

    # Tri-state: True, False, or null for "nobody has told us". Always present -- a missing key
    # and a false one are the same thing to most clients, and for is_3pmsf that conflation is a
    # claim about a certification we never verified.
    for flag in TRISTATE_FLAGS:
        value = row.get(flag)
        payload[flag] = None if value is None else bool(value)

    return payload
