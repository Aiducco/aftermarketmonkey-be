"""
Reduce a published finish string to the bucket a customer filters on.

Wheel Pros alone publishes 454 distinct finishes for 34,194 wheels -- ``GLOSS BLACK MILLED``,
``MATTE BLACK W/ GLOSS BLACK LIP``, ``GLOSS SILVER W/ MACHINED FACE``. Nobody browses 454 options,
so the raw string is kept for display and this produces the facet.

The rule is **colour first, treatment second**, and that ordering is the whole design. A shopper
looking at ``GLOSS BLACK MILLED`` wants it under black; milling is what was done to the face, not
what colour the wheel is. Only when no colour is named does the treatment become the family, which
is right for ``POLISHED`` and ``CHROME`` -- those are the finish.

Word order in the source cannot be trusted (``BLACK MACHINED`` and ``MACHINED BLACK`` both occur),
so this matches on membership rather than position, and takes the first colour in a fixed
precedence rather than the first colour in the string.
"""
import re
import typing

# Ordered. The first match wins, so the more specific name must precede the more general one:
# GUNMETAL and ANTHRACITE both contain no other colour word but are commonly paired with GRAY,
# and BLACKOUT must be found before any attempt at BLACK's substring.
_COLOUR_FAMILIES: typing.Tuple[typing.Tuple[str, typing.Tuple[str, ...]], ...] = (
    ("gunmetal", ("GUNMETAL", "GUN METAL")),
    ("anthracite", ("ANTHRACITE",)),
    ("bronze", ("BRONZE", "COPPER")),
    ("gold", ("GOLD", "BRASS")),
    ("red", ("RED", "CRIMSON")),
    ("blue", ("BLUE", "COBALT")),
    ("green", ("GREEN", "OLIVE")),
    ("orange", ("ORANGE",)),
    ("purple", ("PURPLE", "VIOLET")),
    ("white", ("WHITE",)),
    ("grey", ("GRAY", "GREY", "GRAPHITE", "TITANIUM", "PLATINUM", "PEWTER")),
    ("silver", ("SILVER",)),
    ("black", ("BLACKOUT", "BLACK")),
)

# Only reached when the string names no colour at all.
_TREATMENT_FAMILIES: typing.Tuple[typing.Tuple[str, typing.Tuple[str, ...]], ...] = (
    ("chrome", ("CHROME",)),
    ("polished", ("POLISH",)),
    ("machined", ("MACHINE", "MILLED", "BRUSHED", "CUT")),
    ("raw", ("RAW", "BARE", "AS CAST", "CLEAR")),
)

_WORD_SPLIT = re.compile(r"[^A-Z0-9]+")


def finish_family(finish: typing.Optional[str]) -> typing.Optional[str]:
    """The facet value, or ``None`` when the string names nothing we recognise."""
    if not finish:
        return None
    text = finish.upper()
    words = set(_WORD_SPLIT.split(text))
    for family, needles in _COLOUR_FAMILIES:
        for needle in needles:
            if needle in words or (" " in needle and needle in text):
                return family
    for family, needles in _TREATMENT_FAMILIES:
        for needle in needles:
            # Treatments are matched as prefixes because the source inflects them:
            # POLISH / POLISHED / POLISHD, MACHINE / MACHINED / MCH.
            if any(word.startswith(needle) for word in words) or needle in text:
                return family
    return None
