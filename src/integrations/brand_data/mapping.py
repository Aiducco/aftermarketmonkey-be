"""
Turn one source record into one ``raw_tire_specs`` row, driven by a column map in the registry.

Every source in this department is the same job wearing different clothes: something hands us a
record -- a CSV row, a JSON object, a scraped block -- whose keys are that brand's own words, and
a row has to come out with our column names on it. The *only* thing that differs between brands
is which of their words means which of our columns, and that belongs in data, not in a module per
brand. So it lives in ``TireBrandSource.config['field_map']``:

    "field_map": {
        "part_number":   "Article No.",
        "model_name":    ["Pattern", "Tread Pattern"],
        "size_raw":      "Size",
        "service_description": "Load/Speed",
        "utqg":          "UTQG",
        "brand_name":    {"const": "Michelin"}
    }

A target may take a single source key, a list tried in order (brands rename columns between
editions of the same sheet, and both editions have to keep working), or a literal. Keys are
matched loosely -- case, spacing and punctuation are ignored -- because ``'Load Index'``,
``'LOAD INDEX'`` and ``'load_index'`` are the same column in three exports of the same file, and a
map that breaks on that is a map somebody has to re-edit every quarter.

Some of the brand's cells carry two facts at once, and the map names the cell, not the facts:
``service_description`` fills load index, dual index and speed rating; ``load_range`` fills the
letter and the ply count behind it; ``rim_width_range`` fills the min and the max; ``utqg`` fills
the printed string and its three grades. An explicitly mapped column always wins over one of
these fan-outs, so a sheet that has both a combined cell and its own load-index column keeps the
column.

Two rules hold everywhere in here:

**Published values only.** Nothing in this module fills a column from anything but the source's
own text. Our reading of the size goes to ``parsed_size``, which is JSON and clearly ours. See the
``RawTireSpec`` docstring for why that separation is the point of the table.

**Unreadable is not empty.** A cell that had text we could not parse produces a warning on the
row, never a silent NULL. A source whose columns we misidentified is then a run with thousands of
warnings, rather than a table that looks fine and is not.
"""
import dataclasses
import decimal
import re
import typing

from src.domain import tire_size
from src.integrations.brand_data import normalize
from src.integrations.services import tire_catalog


class FieldMapError(ValueError):
    """A field map naming a column that does not exist. Raised before a source runs, not during."""


# Target -> how to read it. The keys of this table are exactly the ``raw_tire_specs`` columns a
# source is allowed to write; anything else in a field map is a typo, and is rejected before the
# pull rather than discovered after it.
_TEXT = "text"
_INT = "int"
_DECIMAL = "decimal"
_BOOL = "bool"

COLUMN_KINDS: typing.Dict[str, typing.Tuple[str, typing.Optional[int]]] = {
    "external_key": (_TEXT, 255),
    "part_number": (_TEXT, 128),
    "gtin": (_TEXT, 32),
    "brand_name": (_TEXT, 128),
    "model_name": (_TEXT, 255),
    "sub_model": (_TEXT, 255),
    "size_raw": (_TEXT, 128),
    "size_display": (_TEXT, 64),
    "service_type": (_TEXT, 16),
    "load_index": (_INT, None),
    "load_index_dual": (_INT, None),
    "speed_rating": (_TEXT, 8),
    "load_range": (_TEXT, 8),
    "ply_rating": (_INT, None),
    "tread_depth_32nds": ("tread_depth", None),
    "max_load_lb": (_INT, None),
    "max_load_dual_lb": (_INT, None),
    "max_psi": (_INT, None),
    "rim_width_min_in": (_DECIMAL, None),
    "rim_width_max_in": (_DECIMAL, None),
    "measured_rim_width_in": (_DECIMAL, None),
    "overall_diameter_in": (_DECIMAL, None),
    "section_width_in": (_DECIMAL, None),
    "tire_weight_lb": (_DECIMAL, None),
    "revs_per_mile": (_INT, None),
    "utqg": ("utqg", None),
    "utqg_treadwear": (_INT, None),
    "utqg_traction": (_TEXT, 4),
    "utqg_temperature": (_TEXT, 4),
    "sidewall_style": (_TEXT, 64),
    "tread_design": (_TEXT, 32),
    "mileage_warranty_miles": ("miles", None),
    "commercial_position": (_TEXT, 32),
    "oe_marking": (_TEXT, 128),
    "season_label": (_TEXT, 64),
    "category_label": (_TEXT, 64),
    "vehicle_class_label": (_TEXT, 64),
    "is_3pmsf": (_BOOL, None),
    "is_ms": (_BOOL, None),
    "is_run_flat": (_BOOL, None),
    "is_studdable": (_BOOL, None),
    "is_tubeless": (_BOOL, None),
    "has_reinforced_sidewall": (_BOOL, None),
    "is_discontinued": (_BOOL, None),
    "product_url": (_TEXT, None),
    "image_url": (_TEXT, None),
    "spec_sheet_url": (_TEXT, None),
}

# Cells that carry more than one column's worth of fact. The value is what they fan out into, for
# error messages; the reading is done by the matching function in ``normalize``.
COMPOSITE_TARGETS: typing.Dict[str, typing.Tuple[str, ...]] = {
    "service_description": ("load_index", "load_index_dual", "speed_rating"),
    "rim_width_range": ("rim_width_min_in", "rim_width_max_in"),
    "load_range": ("load_range", "ply_rating"),
    "utqg": ("utqg", "utqg_treadwear", "utqg_traction", "utqg_temperature"),
}

VALID_TARGETS = frozenset(COLUMN_KINDS) | frozenset(COMPOSITE_TARGETS)

# The three read wholly by a fan-out reader. ``utqg`` is not among them because it is also a
# column in its own right -- the printed string is worth keeping -- so it is read in the main
# loop and fans out from there.
_COMPOSITE_READERS = {
    "service_description": normalize.service_description,
    "load_range": normalize.load_range,
    "rim_width_range": normalize.rim_width_range,
}

# Columns whose value is ours, not the source's, and which therefore cannot be mapped.
DERIVED_COLUMNS = frozenset(["brand_key", "part_number_key", "model_key", "parsed_size"])


@dataclasses.dataclass
class MappedRow:
    """One source record, read. ``values`` holds only the columns the source actually spoke to."""

    values: typing.Dict[str, typing.Any] = dataclasses.field(default_factory=dict)
    attributes: typing.Dict[str, str] = dataclasses.field(default_factory=dict)
    warnings: typing.List[str] = dataclasses.field(default_factory=list)
    used_keys: typing.Set[str] = dataclasses.field(default_factory=set)

    @property
    def identifies_a_tire(self) -> bool:
        """
        Enough to be worth keeping: a size, or a part number to hang one off later.

        Deliberately weak. A row with a part number and no size is a row whose size column we
        have not found yet, and dropping it loses the evidence of that; the size gate belongs at
        the match, where a wrong answer is expensive.
        """
        return bool(self.values.get("size_display") or self.values.get("part_number"))


def validate_field_map(field_map: typing.Mapping[str, typing.Any]) -> None:
    """Fail a source before it pulls anything, with the whole list of what is wrong at once."""
    if not isinstance(field_map, dict):
        raise FieldMapError("field_map must be an object of {our column: source key}")
    unknown = sorted(set(field_map) - VALID_TARGETS)
    if unknown:
        derived = [name for name in unknown if name in DERIVED_COLUMNS]
        message = f"field_map targets no column can take: {', '.join(unknown)}"
        if derived:
            message += f" -- {', '.join(derived)} are derived by ingest and cannot be mapped"
        raise FieldMapError(message)


# ---------------------------------------------------------------------------------------------
# Reading a record
# ---------------------------------------------------------------------------------------------
_LOOSE_RE = re.compile(r"[^a-z0-9]")


def _loose(key: str) -> str:
    return _LOOSE_RE.sub("", str(key).lower())


def flatten(payload: typing.Any, prefix: str = "") -> typing.Dict[str, str]:
    """
    Every published label -> its value as text, nested objects dotted.

    Lists of scalars are joined rather than indexed (``'sizes'``: ``'A, B'``), because a brand
    that sends a list of homologation codes means one fact with several values, not several
    facts. Lists of objects keep their index, because those are rows.
    """
    out: typing.Dict[str, str] = {}
    if isinstance(payload, dict):
        for key, value in payload.items():
            out.update(flatten(value, f"{prefix}.{key}" if prefix else str(key)))
    elif isinstance(payload, (list, tuple)):
        scalars = [item for item in payload if not isinstance(item, (dict, list, tuple))]
        if len(scalars) == len(payload):
            joined = normalize.clean(", ".join(str(item) for item in payload if item is not None))
            if joined is not None and prefix:
                out[prefix] = joined
        else:
            for index, item in enumerate(payload):
                out.update(flatten(item, f"{prefix}[{index}]"))
    else:
        cleaned = normalize.clean(payload)
        if cleaned is not None and prefix:
            out[prefix] = cleaned
    return out


class _Record:
    """A record's keys, addressable exactly, loosely, and by dotted path."""

    def __init__(self, payload: typing.Any):
        self.payload = payload
        self.flat = flatten(payload)
        self._loose = {}
        for key, value in self.flat.items():
            self._loose.setdefault(_loose(key), (key, value))
            # A dotted path's last segment is also addressable on its own, so a map written
            # against a flat CSV keeps working when the same brand later sends nested JSON.
            leaf = key.rsplit(".", 1)[-1]
            self._loose.setdefault(_loose(leaf), (key, value))

    def get(self, key: str) -> typing.Tuple[typing.Optional[str], typing.Optional[str]]:
        """Returns (value, the key it was found under), both None when the record has nothing."""
        if key in self.flat:
            return self.flat[key], key
        found = self._loose.get(_loose(key))
        if found is None:
            return None, None
        return found[1], found[0]


def _resolve(record: _Record, spec: typing.Any) -> typing.Tuple[typing.Optional[str], typing.Optional[str]]:
    if isinstance(spec, dict):
        if "const" in spec:
            return normalize.clean(spec["const"]), None
        paths = spec.get("paths") or ([spec["path"]] if spec.get("path") else [])
        default = spec.get("default")
    elif isinstance(spec, (list, tuple)):
        paths, default = list(spec), None
    else:
        paths, default = [spec], None
    for path in paths:
        value, found_under = record.get(str(path))
        if value is not None:
            return value, found_under
    return normalize.clean(default), None


def _parse(kind: str, max_length: typing.Optional[int], value: str) -> normalize.Result:
    if kind == _TEXT:
        return normalize.text(value, max_length=max_length)
    if kind == _INT:
        return normalize.integer(value)
    if kind == _DECIMAL:
        return normalize.decimal_value(value)
    if kind == _BOOL:
        return normalize.boolean(value)
    if kind == "tread_depth":
        return normalize.tread_depth(value)
    if kind == "miles":
        return normalize.miles(value)
    raise AssertionError(f"no parser for kind {kind!r}")  # unreachable: kinds come from COLUMN_KINDS


def map_record(payload: typing.Any, field_map: typing.Mapping[str, typing.Any]) -> MappedRow:
    """
    Read one source record against one field map.

    Order matters and is: explicitly mapped columns first, then the composite cells filling only
    what is still empty, then our own derivations. That way a brand that publishes both a
    ``Load Index`` column and a ``116T`` cell keeps the column, and the composite is a fallback
    rather than an override.
    """
    record = _Record(payload)
    row = MappedRow(attributes=dict(record.flat))

    for target, spec in field_map.items():
        if target in _COMPOSITE_READERS:
            continue  # a cell holding several facts; read below, once the plain columns are in
        if target not in COLUMN_KINDS:
            continue  # validate_field_map has already refused these; be inert if called directly
        value, found_under = _resolve(record, spec)
        if found_under:
            row.used_keys.add(found_under)
        if value is None:
            continue
        kind, max_length = COLUMN_KINDS[target]
        if kind == "utqg":
            printed, grades, warning = normalize.utqg(value)
            if printed is not None:
                row.values["utqg"] = printed
            for key, grade in grades.items():
                row.values.setdefault(key, grade)
            if warning:
                row.warnings.append(f"utqg: {warning}")
            continue
        parsed, warning = _parse(kind, max_length, value)
        if warning:
            row.warnings.append(f"{target}: {warning}")
        if parsed is not None:
            row.values[target] = parsed

    _apply_composites(record, field_map, row)
    _derive(row)
    return row


def _apply_composites(record: _Record, field_map: typing.Mapping[str, typing.Any], row: MappedRow) -> None:
    for target, reader in _COMPOSITE_READERS.items():
        spec = field_map.get(target)
        if spec is None:
            continue
        value, found_under = _resolve(record, spec)
        if found_under:
            row.used_keys.add(found_under)
        if value is None:
            continue
        values, warning = reader(value)
        if warning:
            row.warnings.append(f"{target}: {warning}")
        for key, parsed in values.items():
            # setdefault, not assignment: an explicitly mapped column outranks a fan-out.
            row.values.setdefault(key, parsed)


def _derive(row: MappedRow) -> None:
    """
    The three things that are ours rather than the source's: the match keys, the size string
    isolated out of whatever it arrived glued to, and our decode of that size.
    """
    size_source = row.values.get("size_display") or row.values.get("size_raw")
    parsed = tire_size.parse(size_source) if size_source else None

    if "size_display" not in row.values and size_source:
        # 'P235/55R17 XL 103W - Grip 20' is a size with a sentence around it. The parser knows
        # exactly which substring was the size; anything else here would be a second, worse
        # parser. When it cannot tell, the whole cell is kept and the warning below says so.
        row.values["size_display"] = (parsed.matched_text if parsed else size_source)[:64]

    if size_source and parsed is None:
        row.warnings.append(f"size did not parse: {size_source!r}")

    row.values["parsed_size"] = _parsed_size_json(parsed)
    row.values["brand_key"] = tire_catalog.brand_key(row.values.get("brand_name")) or None
    row.values["part_number_key"] = tire_catalog.part_key(row.values.get("part_number")) or None
    row.values["model_key"] = tire_catalog.model_key(row.values.get("model_name")) or None


def _parsed_size_json(parsed: typing.Optional[tire_size.ParsedSize]) -> typing.Optional[typing.Dict[str, typing.Any]]:
    if parsed is None:
        return None
    out: typing.Dict[str, typing.Any] = {}
    for field in dataclasses.fields(parsed):
        if field.name == "span":
            continue  # an offset into a string this table does not keep
        value = getattr(parsed, field.name)
        out[field.name] = str(value) if isinstance(value, decimal.Decimal) else value
    return out
