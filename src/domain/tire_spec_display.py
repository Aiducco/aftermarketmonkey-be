"""
Compose the tire specification block a part detail page renders for a ``product_type == "tire"``
part -- the SIZE / RATINGS / TYPE & CAPABILITY card.

Same discipline as ``src.domain.spec_line``, one level up in detail: **structured columns in, a
payload out, never a distributor title**. Pure -- a mapping of ``tire_specs`` columns in, a plain
dict out, no Django and no DB, so the whole card is testable without a database.

Two rules the card exists to enforce:

  * **A code alone is not information.** ``97Y`` means nothing to a buyer; ``1,609 lb`` and
    ``186 mph`` do. Every coded field is emitted as a ``<field>`` / ``<field>_label`` pair (the
    same convention ``src.api.services.tire_search`` uses for ``tread_category``), so the client
    renders the resolved value and keeps the code for filtering.
  * **Unknown is not false.** The four tri-state flags are omitted from the payload entirely when
    NULL rather than serialised as false -- see ``TireSpec``'s docstring, and note that
    ``is_3pmsf`` is a certification with legal weight. An absent key means "we don't know", which
    a UI shows as an em dash; ``false`` means "checked, not rated".

``overall_diameter_in`` is nominal when ``notation == "numeric"`` (that notation carries no aspect
ratio at all), which is why ``overall_diameter_is_nominal`` ships next to it -- the client must
not present it with the same precision as a stated flotation diameter.
"""
import decimal
import typing

MM_PER_INCH = decimal.Decimal("25.4")

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

# NULL means unknown on every one of these, so they are omitted rather than defaulted -- see the
# module docstring and ``src.search.tires_index._TRISTATE_FLAGS``, which drops them for the same
# reason.
TRISTATE_FLAGS = ("is_3pmsf", "is_ms", "is_run_flat", "is_studdable")

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


def build_tire_specs(row: typing.Mapping[str, typing.Any]) -> typing.Dict[str, typing.Any]:
    """
    Render the spec card from a ``tire_specs`` row.

    ``row`` is any mapping with the column names, plus ``tread_category_label`` resolved from the
    ``tread_category`` table (that label is the only thing a UI ever renders -- the code is an
    identifier, see the ``TreadCategory`` docstring).

    Every key is always present except the four tri-state flags, so a client can render the card
    from a fixed template and blank out nulls, instead of probing for keys.
    """
    max_load_lb = row.get("max_load_lb")
    tread_category = row.get("tread_category") or ""
    vehicle_class = row.get("vehicle_class") or ""
    set_of_four_max_load_lb = (
        max_load_lb * TIRES_PER_SET
        if max_load_lb is not None and vehicle_class not in VEHICLE_CLASSES_WITHOUT_A_SET_OF_FOUR
        else None
    )

    payload: typing.Dict[str, typing.Any] = {
        # ---- identity ------------------------------------------------------------------------
        "size_display": row.get("size_display") or "",
        "model_name": row.get("model_name") or "",
        "sub_model": row.get("sub_model") or "",
        # ---- size ----------------------------------------------------------------------------
        "notation": row.get("notation") or "",
        "service_type": row.get("service_type") or "",
        "service_type_label": _service_type_label(row.get("service_type")),
        "section_width_mm": row.get("section_width_mm"),
        "section_width_in": _section_width_in(row.get("section_width_in"), row.get("section_width_mm")),
        "aspect_ratio": row.get("aspect_ratio"),
        "construction": row.get("construction") or "",
        "construction_label": CONSTRUCTION_LABELS.get(row.get("construction") or ""),
        "overall_diameter_in": _decimal(row.get("overall_diameter_in")),
        # Nominal, not measured, for numeric sizes -- do not render it as an exact figure.
        "overall_diameter_is_nominal": row.get("notation") == "numeric",
        "rim_diameter_in": _decimal(row.get("rim_diameter_in")),
        # ---- ratings -------------------------------------------------------------------------
        "load_index": row.get("load_index"),
        "load_index_dual": row.get("load_index_dual"),
        "max_load_lb": max_load_lb,
        "set_of_four_max_load_lb": set_of_four_max_load_lb,
        "speed_rating": row.get("speed_rating") or "",
        "max_speed_mph": row.get("max_speed_mph"),
        "load_range": row.get("load_range") or "",
        "load_range_label": _load_range_label(row.get("load_range")),
        # Strength equivalence to bias construction, never a count of physical layers -- see the
        # ``TireLoadRange`` docstring before wording this in a UI.
        "ply_rating": row.get("ply_rating"),
        "max_psi": row.get("max_psi"),
        "tread_depth_32nds": row.get("tread_depth_32nds"),
        "utqg_treadwear": row.get("utqg_treadwear"),
        "utqg_traction": row.get("utqg_traction") or "",
        "utqg_temperature": row.get("utqg_temperature") or "",
        # ---- type & capability ---------------------------------------------------------------
        "tread_category": tread_category,
        "tread_category_label": row.get("tread_category_label") or "",
        "vehicle_class": vehicle_class,
        "vehicle_class_label": VEHICLE_CLASS_LABELS.get(vehicle_class),
        "tier": row.get("tier") or "",
        "noise_level": row.get("noise_level") or "",
        "use_case_tags": list(row.get("use_case_tags") or []),
        # ---- fitment to a wheel ----------------------------------------------------------------
        "rim_width_min_in": _decimal(row.get("rim_width_min_in")),
        "rim_width_max_in": _decimal(row.get("rim_width_max_in")),
        # ---- provenance ------------------------------------------------------------------------
        # Parser and model disagreed, or two providers described different sizes. The specs are
        # written anyway; a UI that surfaces sizing decisions should caveat them when this is set.
        "size_disputed": bool(row.get("size_disputed")),
        "enriched_at": _isoformat(row.get("enriched_at")),
    }

    # Present only when known. An absent key is "unknown", never false.
    for flag in TRISTATE_FLAGS:
        value = row.get(flag)
        if value is not None:
            payload[flag] = bool(value)

    return payload
