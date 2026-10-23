"""
Parse Summit Racing's applications tab (``/parts/<sku>/applications``) into fitment rows.

The tab is server-rendered HTML with no JSON equivalent -- ``/data/parts/fitment`` answers
Fits/NoFit for a vehicle you already know, and the ``.../expose/0/`` facet route returns each
dimension's values *separately*, which must never be cross-producted back into combinations
(that is precisely the fabrication the BaseVehicleID rule exists to prevent). The rows below are
the only place Summit publishes real combinations.

Row shape is **declared per part**, not fixed: ``POST /data/parts/<sku>/confirm-applications``
returns ``RequiredAutoLoadAttributes`` listing that part's dimensions. A bumper carries only
Make/Model/Submodel/Years because bumpers do not vary by engine; a coil spring additionally
carries Engine Type, Engine Family, Engine Size, Engine VIN Code, Liter, CID, Drivetrain,
Transmission, Body Style and Region. The parser therefore reads whatever labels a row happens to
carry rather than expecting a fixed column set.
"""

from __future__ import annotations

import dataclasses
import html
import logging
import re
import typing

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[summit-racing]"

# Each row is a flat run of label/value div pairs: the label div holds <strong>Name:</strong>,
# the value div the text. Matching the *pair* rather than walking the DOM keeps this immune to
# the surrounding grid classes changing width breakpoints (they differ between part types).
_PAIR_RE = re.compile(
    r'<div class="small-12[^"]*">\s*<strong>(?P<label>[^<]+?):\s*</strong>\s*</div>\s*'
    r'<div class="small-12[^"]*">(?P<value>.*?)</div>',
    re.S,
)

_ROW_MARKER = '<div class="attribute-container application-item'

_RESULTS_FOUND_RE = re.compile(r"Results Found:\s*</strong>\s*([\d,]+)")

_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"\s+")

# Column widths from MasterPartFitment. Summit occasionally emits a long submodel or an engine
# string with trailing qualifiers; truncating at the boundary beats a 500 from Postgres
# mid-batch, and the overflow is never the discriminating part of the value.
_MAX_MAKE = 128
_MAX_MODEL = 128
_MAX_SUBMODEL = 255
_MAX_ENGINE = 255
_MAX_DRIVE_TYPE = 64


@dataclasses.dataclass(frozen=True)
class ApplicationRow:
    """One vehicle application, already shaped like a ``MasterPartFitment``."""

    year_start: int
    year_end: int
    make: str
    model: str
    submodel: str = ""
    engine: str = ""
    drive_type: str = ""
    notes: str = ""

    @property
    def is_usable(self) -> bool:
        return bool(self.make and self.model and self.year_start and self.year_end)


def parse_results_found(page_html: str) -> typing.Optional[int]:
    """Total applications across all pages, or ``None`` when the tab is absent.

    ``None`` and ``0`` mean different things and callers depend on the difference: ``None`` is
    "this is not an applications page" (a guessed SKU that does not exist), ``0`` is "Summit
    knows this part and it has no vehicle applications" (a universal-fit part).
    """
    match = _RESULTS_FOUND_RE.search(page_html)
    if not match:
        return None
    return int(match.group(1).replace(",", ""))


def parse_applications(page_html: str) -> typing.List[ApplicationRow]:
    """Every application row on one page, skipping rows too incomplete to store."""
    rows: typing.List[ApplicationRow] = []
    malformed = 0
    for chunk in page_html.split(_ROW_MARKER)[1:]:
        fields = _parse_row_fields(chunk)
        if not fields:
            continue
        row = _to_application_row(fields)
        if row is None:
            malformed += 1
            continue
        rows.append(row)
    if malformed:
        logger.debug("%s dropped %s unusable application row(s)", _LOG_PREFIX, malformed)
    return rows


def _parse_row_fields(chunk: str) -> typing.Dict[str, str]:
    """Label -> value for one row.

    The chunk runs to the end of the document because splitting on the row marker does not
    bound it; stopping at the pagination bar keeps a trailing row from absorbing the footer.
    """
    body = chunk.split('<div class="pagination-bar')[0]
    fields: typing.Dict[str, str] = {}
    for match in _PAIR_RE.finditer(body):
        label = _clean(match.group("label"))
        value = _clean(match.group("value"))
        if label and label not in fields:
            fields[label] = value
    return fields


def _to_application_row(fields: typing.Dict[str, str]) -> typing.Optional[ApplicationRow]:
    # "Universal: Yes" rows describe a part that fits everything, which is the absence of a
    # vehicle application rather than an application to every vehicle. Storing it would assert
    # fitment against every vehicle in the catalog.
    if fields.get("Universal", "").strip().lower() == "yes":
        return None

    year_start = _to_year(fields.get("Beginning Year"))
    year_end = _to_year(fields.get("Ending Year"))
    make = fields.get("Make", "").strip()
    model = fields.get("Model", "").strip()
    if not (year_start and year_end and make and model):
        return None

    # Engine Size ("6.4L/391") is the most specific engine string Summit renders and the one
    # that discriminates between applications; Engine Type ("L6") is the fallback for part types
    # that only vary by cylinder layout.
    engine = (fields.get("Engine Size") or fields.get("Engine Type") or "").strip()

    return ApplicationRow(
        year_start=year_start,
        year_end=year_end,
        make=make[:_MAX_MAKE],
        model=model[:_MAX_MODEL],
        submodel=fields.get("Submodel", "").strip()[:_MAX_SUBMODEL],
        engine=engine[:_MAX_ENGINE],
        drive_type=fields.get("Drivetrain", "").strip()[:_MAX_DRIVE_TYPE],
        notes=fields.get("Application Notes", "").strip(),
    )


def _to_year(raw: typing.Optional[str]) -> typing.Optional[int]:
    if not raw:
        return None
    digits = raw.strip()[:4]
    if not digits.isdigit():
        return None
    year = int(digits)
    # Summit renders 1900-2027; anything outside that is a parse slip, not a vehicle.
    if not 1900 <= year <= 2100:
        return None
    return year


def _clean(raw: str) -> str:
    """Strip tags and entities and collapse the server-rendered indentation to single spaces."""
    return _WS_RE.sub(" ", html.unescape(_TAG_RE.sub("", raw))).strip()
