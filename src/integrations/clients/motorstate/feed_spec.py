"""
Motor State FTP feed: one CSV per account (``<account>.csv``), header row, CRLF line endings,
UTF-8 with a BOM.

The column set is **account-dependent** -- Motor State provisions each dealer's file with the
columns that dealer is entitled to. Two live examples:

  * enriched account -- 27 columns, including ``Image URL``, ``Category Level 1/2/3`` and
    ``Long Description(150)``
  * plain account    -- 26 columns, no image/category/long-description, but carrying
    ``CanadaRestricted``, ``AcquiredDate``, ``EmissionsWarning`` and ``Oversized`` instead

22 columns are common to both. Parse by header name, never by position, and treat everything
outside :data:`REQUIRED_COLUMNS` as optional -- a missing column means "this account does not
receive that field", not a malformed feed.
"""
import re
import typing

# Default remote filename is derived from the FTP login (``853809@motorstateftp.com`` ->
# ``853809.csv``); this is only the fallback when that derivation yields nothing.
DEFAULT_REMOTE_FEED_FILENAME = "motorstate.csv"

# Canonical column name -> the header labels seen in the wild. Headers are matched on a
# normalized key (uppercased, non-alphanumerics stripped) so spacing/punctuation drift in the
# vendor's header row cannot break ingest.
_HEADER_ALIASES: typing.Dict[str, typing.Tuple[str, ...]] = {
    "part_number": ("PartNumber",),
    "description": ("Description",),
    "brand": ("Brand",),
    "suggested_retail": ("SuggestedRetail",),
    "cost": ("Cost",),
    "length": ("Length",),
    "width": ("Width",),
    "height": ("Height",),
    "weight": ("Weight",),
    "qty_avail": ("QtyAvail",),
    "upc": ("UPC",),
    "jobber": ("Jobber",),
    "aaia_code": ("AAIACode",),
    "map_price": ("MapPrice",),
    "vendor_msrp": ("VendorMSRP",),
    "air_restricted": ("AirRestricted",),
    "state_restricted": ("StateRestricted",),
    "truck_freight_only": ("TruckFrtOnly",),
    "manufacturer_part": ("ManufacturerPart",),
    "ship_alone": ("ShipAlone",),
    "status": ("Status",),
    "notes": ("MotorStateNotes",),
    "canada_restricted": ("CanadaRestricted",),
    "acquired_date": ("AcquiredDate",),
    "emissions_warning": ("EmissionsWarning",),
    "oversized": ("Oversized",),
    "image_url": ("Image URL", "ImageURL"),
    "category_level_1": ("Category Level 1", "CategoryLevel1"),
    "category_level_2": ("Category Level 2", "CategoryLevel2"),
    "category_level_3": ("Category Level 3", "CategoryLevel3"),
    # The vendor's header carries the truncation length in the label itself.
    "long_description": ("Long Description(150)", "LongDescription", "Long Description"),
}

# Without a part number a row cannot be keyed to anything, so this is the only hard requirement.
REQUIRED_COLUMNS = ("part_number",)

# Columns only the enriched accounts receive. Their absence selects pricing-only mode --
# see ``motorstate.feed_has_catalog_columns``.
ENRICHED_COLUMNS = (
    "image_url",
    "category_level_1",
    "category_level_2",
    "category_level_3",
    "long_description",
)

# Per-account price columns. Every account's file carries these, with that account's own
# numbers -- this is the whole reason pricing is pulled per company rather than once.
PRICE_COLUMNS = ("cost", "jobber", "suggested_retail", "map_price", "vendor_msrp")

# Motor State StatusType, as it appears in the feed's ``Status`` column.
STATUS_STOCKING = "S"
STATUS_ORDER_AS_NEEDED = "O"
STATUS_DISCONTINUED = "X"

_NON_ALNUM_RE = re.compile(r"[^A-Z0-9]")


def normalize_header(label: typing.Any) -> str:
    """Uppercase and strip everything but letters/digits, so ``Category Level 1``,
    ``category level 1`` and ``CategoryLevel1`` all collapse to one key."""
    return _NON_ALNUM_RE.sub("", str(label or "").strip().upper())


_ALIAS_TO_CANONICAL: typing.Dict[str, str] = {}
for _canonical, _labels in _HEADER_ALIASES.items():
    for _label in _labels:
        _ALIAS_TO_CANONICAL[normalize_header(_label)] = _canonical
    # The canonical name itself is always accepted too (``long_description`` -> LONGDESCRIPTION).
    _ALIAS_TO_CANONICAL.setdefault(normalize_header(_canonical), _canonical)


def canonical_column(label: typing.Any) -> typing.Optional[str]:
    """Canonical name for a raw header label, or None when the column is unrecognized."""
    return _ALIAS_TO_CANONICAL.get(normalize_header(label))


def map_header_row(header: typing.Sequence[typing.Any]) -> typing.Dict[int, str]:
    """Column index -> canonical name, skipping unrecognized columns.

    Index-keyed rather than name-keyed because a vendor file can repeat a header label; the
    first occurrence wins and later duplicates are dropped rather than silently overwriting.
    """
    out: typing.Dict[int, str] = {}
    claimed: typing.Set[str] = set()
    for i, label in enumerate(header or ()):
        canonical = canonical_column(label)
        if canonical and canonical not in claimed:
            claimed.add(canonical)
            out[i] = canonical
    return out
