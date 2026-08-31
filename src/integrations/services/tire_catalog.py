"""
Shared machinery for merging an external tire catalog into ``tire_specs``.

We merge from two scraped catalogs -- SimpleTire and TDG -- and will likely add more. What differs
between them is *which fields they carry and which of those we trust*; what must not differ is how
a row is matched and what the merge is forbidden to touch. Both of those live here, so a second
source cannot quietly acquire a weaker size gate or a wider write set than the first.

The matching rule is three tiers, each gated on the size agreeing. That gate is not a nicety: 7,814
of our part numbers exist under some other-named brand in SimpleTire alone, because short numeric
MPNs collide across manufacturers -- Michelin parts colliding with Atturo and Petlas. Requiring the
dimensions to agree as well cuts that to a few hundred rows that really are the same tire.

Brand aliases are per-source (the two catalogs spell brands differently from us *and* from each
other), so each adapter supplies its own map. Each map is inferred from part-number evidence and
then verified by checking the size agrees on the rows it buys -- never by string similarity.
"""
import decimal
import re
import typing

from src.domain import tire_size


def brand_key(name: typing.Optional[str]) -> str:
    return re.sub(r"[^A-Z0-9]", "", (name or "").upper())


def part_key(part_number: typing.Optional[str]) -> str:
    """The catalogs punctuate MPNs differently; nothing else about them differs."""
    return re.sub(r"[^A-Z0-9]", "", (part_number or "").upper())


def alias_keys(
    name: typing.Optional[str], aliases: typing.Mapping[str, typing.Union[str, typing.Sequence[str]]]
) -> typing.Tuple[str, ...]:
    """
    The catalog brand keys a name of ours may be filed under.

    Usually one: we write ``YOKOHAMA TIRE`` where both catalogs write ``Yokohama``. Sometimes
    several, because the name is not a manufacturer at all. ``WHEEL PROS`` is a distributor whose
    tires are Falken's, Nitto's and Toyo's; ``GREENBALL CORPORATION/ KANATI`` is a parent company
    selling under Kanati, GBC and Centennial. Mapping those to a single brand would strand most of
    their stock, so a value may be a tuple and every entry is tried.

    Trying several brands is only safe because every match tier still requires the size to agree.
    Without that gate this would be a licence to mismatch across manufacturers.
    """
    key = brand_key(name)
    target = aliases.get(key)
    if target is None:
        return (key,)
    if isinstance(target, str):
        return (target,)
    return tuple(target)


_MODEL_NOISE_RE = re.compile(r"\b(TIRE|TIRES|TYRE)\b")


def model_key(name: typing.Optional[str]) -> str:
    return re.sub(r"[^A-Z0-9]", "", _MODEL_NOISE_RE.sub(" ", (name or "").upper()))


_size_cache: typing.Dict[typing.Optional[str], typing.Optional[tuple]] = {}


def canonical_size(display: typing.Optional[str]) -> typing.Optional[tuple]:
    """
    Reduce any catalog's size string to comparable dimensions.

    They print '10.00-15', '265/70R18' and '35X12.50R20LT' in three different house styles. Rather
    than compare typography, every side goes through our own parser -- the same one that is the
    source of truth for the size block -- and the results are compared as numbers.
    """
    if display in _size_cache:
        return _size_cache[display]
    parsed = tire_size.parse(display or "")
    value = None
    if parsed is not None and parsed.rim_diameter_in is not None:
        value = (parsed.section_width_mm, parsed.aspect_ratio, str(parsed.rim_diameter_in))
    _size_cache[display] = value
    return value


def differs(old: typing.Any, new: typing.Any) -> bool:
    """Compare as the database would, so Decimal('9') and Decimal('9.0') are not a change."""
    if old is None and new is None:
        return False
    if old is None or new is None:
        return True
    if isinstance(old, (decimal.Decimal, float, int)) and not isinstance(old, bool):
        try:
            return decimal.Decimal(str(old)) != decimal.Decimal(str(new))
        except (decimal.InvalidOperation, ValueError):
            pass
    return old != new


# Columns no catalog merge may write, whatever it claims to know. The reasons differ per field and
# are recorded where each was decided; the short version:
#
#   overall_diameter_in   ours is the diameter the size prints (35.0 for a 35X12.50R20) and drives
#                         "35 inch" search; a catalog's is measured (33.07). Same name, different
#                         quantity.
#   max_speed_mph         both sides derive it from the speed rating, but we floor 160 km/h to 99
#                         and they ceil to 100; speed_sort range filters would straddle the border.
#   the size block        a match is only accepted when both sides agree on the dimensions, so
#                         there is nothing to gain and a parser fix to lose.
#   sub_model             ours holds OE / Front / Rear, which no catalog has an equivalent for.
NEVER_WRITE = frozenset(
    [
        "overall_diameter_in",
        "max_speed_mph",
        "sub_model",
        "size_display",
        "notation",
        "service_type",
        "section_width_mm",
        "section_width_in",
        "aspect_ratio",
        "construction",
        "rim_diameter_in",
        "size_disputed",
        "llm_confidence",
        "llm_reason",
        "llm_model_used",
    ]
)


class Match(typing.NamedTuple):
    row: dict
    tier: int


class CatalogIndex:
    """
    One scraped catalog, indexed for the three match tiers.

    ``fields`` names the columns holding brand, part number, size and model on this particular
    source, because no two of them agree on that either. ``extra_part_fields`` lets a source offer
    a second identifier (TDG publishes both an item number and a manufacturer part number, and
    either may be the one our distributor filed).
    """

    def __init__(
        self,
        rows: typing.Iterable[dict],
        *,
        brand_field: str,
        part_field: str,
        size_field: str,
        model_field: str,
        extra_part_fields: typing.Sequence[str] = (),
    ):
        self.brand_field, self.size_field = brand_field, size_field
        self.by_id: typing.Dict[typing.Any, dict] = {}
        self.by_brand_part: typing.Dict[tuple, typing.List] = {}
        self.by_part: typing.Dict[str, typing.List] = {}
        self.by_brand_model_size: typing.Dict[tuple, typing.Set] = {}
        self.brands: typing.Set[str] = set()

        for row in rows:
            row_id = row["id"]
            self.by_id[row_id] = row
            bkey = brand_key(row.get(brand_field))
            self.brands.add(bkey)
            for field in (part_field, *extra_part_fields):
                pkey = part_key(row.get(field))
                if not pkey:
                    continue
                self.by_brand_part.setdefault((bkey, pkey), []).append(row_id)
                self.by_part.setdefault(pkey, []).append(row_id)
            size = canonical_size(row.get(size_field))
            if size and row.get(model_field):
                key = (bkey, model_key(row[model_field]), size)
                self.by_brand_model_size.setdefault(key, set()).add(row_id)

    def size_of(self, row_id) -> typing.Optional[tuple]:
        return canonical_size(self.by_id[row_id].get(self.size_field))

    def match(
        self,
        *,
        brand: typing.Optional[str],
        part_number: typing.Optional[str],
        size_display: typing.Optional[str],
        model_name: typing.Optional[str],
        aliases: typing.Mapping[str, typing.Union[str, typing.Sequence[str]]],
    ) -> typing.Optional[Match]:
        """Most trustworthy key first. Every tier requires the dimensions to agree."""
        bkeys = alias_keys(brand, aliases)
        pkey = part_key(part_number)
        ours = canonical_size(size_display)

        # Tier 1 is evaluated across every candidate brand before deciding, so that a
        # size-disagreeing hit under one of a distributor's manufacturers cannot veto a good hit
        # under another. A row is only refused when every candidate disagrees.
        exact = [rid for bkey in bkeys for rid in self.by_brand_part.get((bkey, pkey), ())]
        if exact:
            for row_id in exact:
                theirs = self.size_of(row_id)
                if not (ours and theirs and ours != theirs):
                    return Match(self.by_id[row_id], 1)
            # Same brand, same MPN, different tire: one catalog is wrong, so trust neither.
            return None

        if ours:
            agreeing = {i for i in self.by_part.get(pkey, ()) if self.size_of(i) == ours}
            if len(agreeing) == 1:
                return Match(self.by_id[agreeing.pop()], 2)

        if ours and model_name:
            candidates: typing.Set = set()
            for bkey in bkeys:
                if bkey in self.brands:
                    candidates |= self.by_brand_model_size.get((bkey, model_key(model_name), ours), set())
            if len(candidates) == 1:
                return Match(self.by_id[candidates.pop()], 3)

        return None
