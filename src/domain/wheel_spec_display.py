"""
Render the wheel specification card from a ``wheel_specs`` row.

The counterpart to ``src.domain.tire_spec_display``, and the same contract: **every key is always
present**, so a client renders the card from a fixed template and blanks nulls rather than probing
for keys. Pure -- no Django, no database -- so the shape is testable on its own.

Three things this module computes rather than reads, because they are exact arithmetic on values
we already hold and storing them would be a second copy to keep in step:

``backspacing_in``  offset and backspacing describe the same thing in two vocabularies, and both
                    ship on every card. American truck buyers think in backspacing; OE fitment data
                    is published as ET. Neither converts without the wheel's width, which is why
                    only one of them is ever in a feed and the other has to be derived.
``set_of_four_load_lb``  the figure a customer actually needs when the question is whether the
                    wheels will carry the truck.
``bolt circle in inches``  alongside millimetres, because ``6x5.5`` is what a Jeep owner searches
                    for and ``6x139.7`` is what the spec sheet says. They are the same circle.

``max_psi`` is beadlock-only and is emitted as ``None`` on anything else. A beadlock ring has a
torque and pressure limit; on a normal wheel the number is meaningless and the row should not
render at all.
"""
import decimal
import typing

MM_PER_INCH = decimal.Decimal("25.4")
WHEELS_PER_SET = 4

VEHICLE_CLASS_LABELS = {
    "passenger": "Passenger",
    "light_truck": "Light truck",
    "trailer": "Trailer",
    "commercial": "Commercial",
    "motorcycle": "Motorcycle",
    "atv_utv": "ATV / UTV",
}

CONSTRUCTION_LABELS = {
    "cast": "Cast",
    "flow_formed": "Flow formed",
    "forged": "Forged",
    "steel": "Steel",
    "multi_piece": "Multi piece",
}

TIER_LABELS = {"budget": "Budget", "mid": "Mid", "premium": "Premium", "flagship": "Flagship"}

HUB_RING_LABELS = {"included": "Included", "required": "Required", "not_needed": "Not needed"}

SPEC_SOURCE_LABELS = {
    "feed": "Distributor feed",
    "parser": "Read from the product title",
    "catalog": "Manufacturer catalog",
}

# NULL means unknown on each of these, and the card must say nothing rather than "No". A wheel
# whose feed never mentioned beadlock is not thereby a non-beadlock wheel.
TRISTATE_FLAGS = (
    "is_beadlock",
    "is_simulated_beadlock",
    "is_hub_centric",
    "is_directional",
    "is_dually",
    "tpms_compatible",
    "caps_included",
    "lugs_included",
)

# Rendered as pills under the type block, in this order. Label is what shows; the value is only
# included when it is True, because a pill saying "not directional" is noise.
PILL_FLAGS = (
    ("is_beadlock", "Beadlock"),
    ("is_simulated_beadlock", "Simulated beadlock"),
    ("is_hub_centric", "Hub centric"),
    ("is_directional", "Directional"),
    ("is_dually", "Dually"),
)


def _decimal(value: typing.Any) -> typing.Optional[float]:
    return None if value is None else float(value)


def _text(value: typing.Any) -> typing.Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


# Words a title-caser would otherwise mangle: they are abbreviations, not words.
_KEEP_UPPER = frozenset(["OE", "OEM", "TPMS", "UTV", "ATV", "RWL", "OWL", "SS", "II", "III", "XL"])


def _title_case(value: typing.Any) -> typing.Optional[str]:
    """ "GLOSS BLACK MILLED" -> "Gloss Black Milled", leaving abbreviations alone."""
    text = _text(value)
    if text is None:
        return None
    # Genuinely mixed case is the source's own styling and is left alone ("Matte Black w/ Milled
    # Accents"). All-upper and all-lower are both machine artefacts and both get cased.
    if text != text.upper() and text != text.lower():
        return text
    return " ".join(word if word in _KEEP_UPPER else word.capitalize() for word in text.split())


def _isoformat(value: typing.Any) -> typing.Optional[str]:
    return value.isoformat() if value is not None and hasattr(value, "isoformat") else None


def backspacing_in(offset_mm: typing.Any, width_in: typing.Any) -> typing.Optional[decimal.Decimal]:
    """
    Backspacing from offset and width, to two decimals.

        backspacing = width / 2 + 0.5" + offset

    The half inch is the flange, by convention. Needs the width: the same +18 mm offset is 5.5" of
    backspacing on an 8" wheel and 6.5" on a 10" one, which is why a feed that publishes one of
    these never publishes the other and why this cannot be a lookup.
    """
    if offset_mm is None or width_in is None:
        return None
    width = decimal.Decimal(str(width_in))
    offset = decimal.Decimal(str(offset_mm))
    return (width / 2 + decimal.Decimal("0.5") + offset / MM_PER_INCH).quantize(decimal.Decimal("0.01"))


def _circle_in(circle_mm: typing.Any) -> typing.Optional[float]:
    if circle_mm is None:
        return None
    return float((decimal.Decimal(str(circle_mm)) / MM_PER_INCH).quantize(decimal.Decimal("0.01")))


def _bolt_pattern(lug_count: typing.Any, circle_mm: typing.Any, display: typing.Any) -> typing.Optional[dict]:
    """One pattern, in both vocabularies. ``None`` when the wheel has no such drilling."""
    if lug_count is None or circle_mm is None:
        return None
    return {
        "lug_count": lug_count,
        "circle_mm": _decimal(circle_mm),
        "circle_in": _circle_in(circle_mm),
        "display": _text(display) or "{}x{}".format(lug_count, circle_mm),
    }


def _pills(row: typing.Mapping[str, typing.Any]) -> typing.List[str]:
    return [label for field, label in PILL_FLAGS if row.get(field) is True]


def build_wheel_specs(row: typing.Mapping[str, typing.Any]) -> typing.Dict[str, typing.Any]:
    """
    The spec card for one wheel. ``row`` is any mapping carrying the ``wheel_specs`` column names.

    Grouped the way the card is laid out -- size, ratings, type, construction, ownership, fitment --
    so the client renders sections without knowing which column belongs where.
    """
    width = row.get("width_in")
    offset = row.get("offset_mm")
    load_rating = row.get("load_rating_lb")
    is_beadlock = row.get("is_beadlock")

    # Stored backspacing wins when a feed published it; otherwise derive. Both are the same
    # quantity, and a feed that states it is more authoritative than our arithmetic.
    backspacing = row.get("backspacing_in")
    if backspacing is None:
        backspacing = backspacing_in(offset, width)

    payload: typing.Dict[str, typing.Any] = {
        # ---- identity --------------------------------------------------------------------------
        "size_display": _text(row.get("size_display")) or "",
        "model_name": _text(row.get("model_name")),
        "sub_model": _text(row.get("sub_model")),
        "style_number": _text(row.get("style_number")),
        # ---- size ------------------------------------------------------------------------------
        "diameter_in": _decimal(row.get("diameter_in")),
        "width_in": _decimal(width),
        "bolt_pattern": _bolt_pattern(
            row.get("bolt_lug_count"), row.get("bolt_circle_mm"), row.get("bolt_pattern_display")
        ),
        # Present only on a dual-drilled wheel; null is the normal case and the row should hide.
        "bolt_pattern_2": _bolt_pattern(
            row.get("bolt_lug_count_2"), row.get("bolt_circle_mm_2"), row.get("bolt_pattern_2_display")
        ),
        # True on an undrilled wheel, **None otherwise** -- deliberately not False.
        #
        # The card renders every key and a false boolean draws as a crossed-out pill, so shipping
        # False put "Blank (undrilled) ✕" underneath a perfectly good "5x114.3" and read as a
        # contradiction. There is no contradiction in the data: a check constraint forbids a blank
        # from having a circle and zero rows violate it. The fix is to say nothing rather than to
        # say "not blank", which is what the other flags on this card already do.
        #
        # The search index keeps the real boolean -- an undrilled wheel has to stay excludable.
        "is_blank_drilled": True if row.get("is_blank_drilled") else None,
        "offset_mm": offset,
        "backspacing_in": _decimal(backspacing),
        "center_bore_mm": _decimal(row.get("center_bore_mm")),
        # ---- ratings ---------------------------------------------------------------------------
        "load_rating_lb": load_rating,
        "set_of_four_load_lb": load_rating * WHEELS_PER_SET if load_rating is not None else None,
        # A beadlock ring has a pressure limit; on any other wheel the figure is meaningless.
        "max_psi": row.get("max_psi") if is_beadlock else None,
        # ---- type and capability -----------------------------------------------------------------
        "vehicle_class": _text(row.get("vehicle_class")),
        "vehicle_class_label": VEHICLE_CLASS_LABELS.get(row.get("vehicle_class") or ""),
        "tier": _text(row.get("tier")),
        "tier_label": TIER_LABELS.get(row.get("tier") or ""),
        "style_tags": list(row.get("style_tags") or []),
        "pills": _pills(row),
        # ---- construction and appearance ---------------------------------------------------------
        "material": _text(row.get("material")),
        "construction": _text(row.get("construction")),
        "construction_label": CONSTRUCTION_LABELS.get(row.get("construction") or ""),
        "piece_count": row.get("piece_count"),
        # The manufacturer's own wording, and the bucket the facet rail groups it under. Both, so
        # the card can show "Matte Black w/ Milled Accents" while the filter says "black".
        # Both, and both ready to render. The feeds shout -- "GLOSS BLACK MILLED", "CONICAL" --
        # and raw feed casing on a product page reads as unprocessed data, so the card carries a
        # display form beside the verbatim one rather than making every client title-case it.
        "finish": _text(row.get("finish")),
        "finish_display": _title_case(row.get("finish")),
        "finish_family": _text(row.get("finish_family")),
        "finish_family_label": _title_case(row.get("finish_family")),
        # ---- ownership ---------------------------------------------------------------------------
        "weight_lb": _decimal(row.get("weight_lb")),
        "structural_warranty": _text(row.get("structural_warranty")),
        "finish_warranty": _text(row.get("finish_warranty")),
        # ---- fitment -----------------------------------------------------------------------------
        "lug_seat": _text(row.get("lug_seat")),
        "lug_seat_display": _title_case(row.get("lug_seat")),
        "lug_thread_size": _text(row.get("lug_thread_size")),
        "hub_rings": _text(row.get("hub_rings")),
        "hub_rings_label": HUB_RING_LABELS.get(row.get("hub_rings") or ""),
        # ---- provenance ---------------------------------------------------------------------------
        "spec_source": _text(row.get("spec_source")),
        "spec_source_label": SPEC_SOURCE_LABELS.get(row.get("spec_source") or ""),
        "source_feed": _text(row.get("source_feed")),
        # The feed contradicts its own product title on a dimension. Shown so a buyer is not the
        # one who discovers it.
        "size_disputed": bool(row.get("size_disputed")),
        "enriched_at": _isoformat(row.get("enriched_at")),
    }

    # Tri-states last, so an unknown is absent from the card rather than rendered as "No".
    for flag in TRISTATE_FLAGS:
        value = row.get(flag)
        payload[flag] = None if value is None else bool(value)
    return payload
