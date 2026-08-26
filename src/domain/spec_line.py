"""
Compose the one-line spec summary shown under a tire in search results.

    LT275/70R18 · 116T (2,756 lb) · 33.2" OD · Load E (10 ply)

**Built from structured fields, never from a distributor title.** The titles are the problem this
whole pipeline exists to solve -- "TER GRAP G3 LT225/75R16 115/Q E 29.53" is what a distributor
wrote for its own warehouse staff. And the resolved numbers are the point: ``116T`` means nothing
to a buyer, ``2,756 lb`` and ``118 mph`` do.

Pure: a mapping in, a string out. Segments whose inputs are missing are dropped rather than
rendered as "None" or "—", so a sparsely enriched tire produces a short line instead of a line
full of holes.
"""
import typing

SEPARATOR = " · "


def _load_segment(
    load_index: typing.Optional[int],
    speed_rating: typing.Optional[str],
    max_load_lb: typing.Optional[int],
    max_speed_mph: typing.Optional[int],
) -> typing.Optional[str]:
    """``116T (2,756 lb)`` -- the code, then what it actually means."""
    if load_index is None and not speed_rating:
        return None
    code = "{}{}".format(load_index if load_index is not None else "", speed_rating or "")
    resolved = []
    if max_load_lb is not None:
        resolved.append("{:,} lb".format(max_load_lb))
    if max_speed_mph is not None:
        resolved.append("{} mph".format(max_speed_mph))
    if resolved:
        return "{} ({})".format(code, ", ".join(resolved))
    return code


def _load_range_segment(load_range: typing.Optional[str], ply_rating: typing.Optional[int]) -> typing.Optional[str]:
    if not load_range:
        return None
    if ply_rating is not None:
        # "10 ply" is a strength equivalence to bias construction, not a count of layers -- see
        # the TireLoadRange model docstring. The wording matches how the industry prints it.
        return "Load {} ({} ply)".format(load_range, ply_rating)
    return "Load {}".format(load_range)


def build_spec_line(document: typing.Mapping[str, typing.Any]) -> str:
    """
    Render the line from an index document (or any mapping with the same keys).

    ``notation == "numeric"`` suppresses the overall diameter: that value is nominal, derived from
    a convention rather than read off the tire, and presenting it next to exact figures would
    imply a precision it does not have.
    """
    segments: typing.List[typing.Optional[str]] = [document.get("size_display") or None]

    segments.append(
        _load_segment(
            document.get("load_index"),
            document.get("speed_rating"),
            document.get("max_load_lb"),
            document.get("max_speed_mph"),
        )
    )

    overall = document.get("overall_diameter_in")
    if overall is not None and document.get("notation") != "numeric":
        segments.append('{}" OD'.format(overall))

    segments.append(_load_range_segment(document.get("load_range"), document.get("ply_rating")))

    return SEPARATOR.join(segment for segment in segments if segment)
