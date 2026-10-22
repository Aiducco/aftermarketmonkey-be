"""
One place to decide whether a feed's image value is actually an image.

Feeds signal "no photo" in two incompatible ways and neither is NULL:

- **a placeholder string in the URL column.** Premier writes the literal ``'NA'`` on 556,786 of
  its 707,843 non-blank rows; AllPro, Tri-State and SDC each ship a few hundred of the same
  family. These reached ``master_parts.image_url`` verbatim and rendered as the text "NA" on
  412,682 master parts -- 26% of every part that had an "image" at all.
- **a real, fetchable URL whose content is a placeholder graphic** --
  ``photocomingsoon.jpg``, ``ICON_IMAGECOMINGSOON.jpg``, ``placeholder-base_1.jpg``,
  ``no_image.jpg``. Only ~220 rows, but they are worse than blank: the PDP shows a confident
  "image coming soon" card and no backfill will ever revisit the part.

``master_part_enrichment`` already filtered both on the way into ``MasterPartData`` (its
``_PLACEHOLDER_VALUES`` / ``_image_urls``), but the flat-feed ingests that write
``master_parts.image_url`` directly did not, so the junk arrived by the other door and the
nightly sync rewrote it after every cleanup. Both paths now share this module.

**'blank' and 'missing' are deliberately not placeholder words.** They appear in genuine product
names -- ``Uni-Hitch-Step-Blank.jpg``, ``END7009-BLANK.jpg``, blank-drilled wheel photos -- so
matching them would discard real photography.
"""
import re
import typing
from urllib.parse import urlparse

# Values that mean "no image" rather than a URL. Compared casefolded after stripping.
PLACEHOLDER_VALUES = frozenset({
    "na", "n/a", "n.a.", "none", "null", "unknown", "-", "--", "tbd", "nan",
    "0", "false", "no image", "noimage", "not available", "#n/a", "undefined",
})

# Substrings that mark a real URL as a placeholder graphic. Matched against the whole URL,
# casefolded. Kept narrow on purpose -- see the module docstring on 'blank'/'missing'.
_PLACEHOLDER_URL_PATTERNS = (
    "noimage",
    "no-image",
    "no_image",
    "noimageavailable",
    "placeholder",
    "comingsoon",
    "coming_soon",
    "coming-soon",
    "coming soon",
    "notavailable",
    "image_not_available",
)

_NON_PRINTABLE_RE = re.compile(r"[^\x20-\x7E]")


def is_placeholder_value(value: typing.Any) -> bool:
    """True when the raw feed value is a 'no image' marker rather than a URL."""
    if value is None:
        return True
    text = str(value).strip()
    return not text or text.casefold() in PLACEHOLDER_VALUES


def is_placeholder_url(url: str) -> bool:
    """True when a syntactically valid URL points at a 'no photo' placeholder graphic."""
    lowered = url.casefold()
    return any(pattern in lowered for pattern in _PLACEHOLDER_URL_PATTERNS)


def clean_image_url(value: typing.Any) -> typing.Optional[str]:
    """
    Return a usable absolute http(s) image URL, or ``None``.

    ``None`` is the repo's blank for ``master_parts.image_url`` (2.93M rows are NULL and none are
    ''), so returning it lets every fill-if-blank backfill revisit the part later -- which is the
    whole point: a part carrying 'NA' was invisible to enrichment, because 'NA' is neither NULL
    nor ''.
    """
    if is_placeholder_value(value):
        return None
    url = _NON_PRINTABLE_RE.sub("", str(value)).strip()
    if not url or not url.lower().startswith(("http://", "https://")):
        return None
    if is_placeholder_url(url):
        return None
    # A URL with no host is not fetchable ('https:///foo.jpg' shows up in a couple of feeds).
    if not urlparse(url).netloc:
        return None
    return url
