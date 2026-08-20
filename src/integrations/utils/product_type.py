"""
Decide whether a part is a ``wheel``, a ``tire``, or a ``part`` -- the rules behind
``MasterPart.product_type`` (see ``src.enums.ProductType``).

**Only a rim is a wheel and only a tire is a tire.** Everything else is ``part``: lug nuts, center
caps, spacers, TPMS sensors, valve stems, inner tubes, tire chains, wheel covers, spare-tire
carriers. This is the single rule that most of the tables below exist to enforce, because every
distributor lumps accessories in with the thing they bolt onto -- Turn14 has a
``Wheel and Tire Accessories`` category, Wheel Pros ships an ``accessories`` feed, and PCdb files
``Wheel Cover`` under its ``Wheel`` subcategory.

**Nothing here guesses.** Every function returns ``None`` when its inputs do not decide the
question, and ``None`` means "not classified" -- it is never quietly turned into ``part``. The
distributor category tables are closed vocabularies captured from production: a value we have
never seen returns ``None`` rather than being assumed harmless, so when a distributor widens its
feed the run reports new vocabulary instead of mislabelling it. 1.65M of 3.2M master parts reach
us only through distributors that ship no type signal at all (A-Tech, Keystone, Motor State,
Quadratec, DLG), and the honest answer for those is NULL.

Tiers, applied highest-first. Each is a separate ``product_type_source`` prefix so the
inferential ones can be reverted without touching what a distributor actually told us:

  T1 ``<distributor>:<field>``  table of origin / explicit feed type -- structural, not text
  T2 ``<distributor>:category`` closed distributor category vocabulary
  T3 ``pcdb:<id>``              PCdb part terminology (Premier, Meyer)
  T4 ``brand:<rule>``           brand-level (a brand that sells nothing but wheels or tires)
  T5 ``keyword:<rule>``         size regexes over description text

T4/T5 are inferential and belong behind the classification command's ``--tiers`` flag; T1-T3 are
what a distributor asserted.

Everything in this module is pure -- plain values in, verdict out, no DB access. The batched scan
that feeds it lives in the classification service.

Production evidence for each rule is recorded next to it. Counts are from the 2026-08-20 survey.
"""
import re
import typing

from src import enums as src_enums

WHEEL = src_enums.ProductType.WHEEL.value
TIRE = src_enums.ProductType.TIRE.value
PART = src_enums.ProductType.PART.value


class Verdict(typing.NamedTuple):
    """A classification plus the rule that produced it (stored as ``product_type_source``)."""
    product_type: str
    source: str


# ==========================================================================================
# T1 -- table of origin and explicit feed type
#
# The strongest signals: the distributor separated the products for us, structurally, before
# any text was involved. Every claim below was checked against production rather than taken
# from a feed's documentation -- ``TireRackParts`` was assumed tire-only on exactly that kind
# of reasoning and turned out to be a 198-brand mixed catalog (it is handled in T5 instead).
# ==========================================================================================

def classify_elite_wheel(*, is_tire_sheet: bool) -> Verdict:
    """
    Elite Wheel ships wheels and tires as two separate worksheets, landing in two separate
    tables (``EliteWheelPartWheel`` / ``EliteWheelPartTire``), so the row's own table is the
    answer.

    Verified: 2,485 wheel rows (all carry ``size``, none has an accessory group label) and 801
    tire rows (all carry ``raw_size``). 2,485 + 801 = 3,286 = every Elite ProviderPart, so there
    is no third bucket of accessories hiding somewhere else.
    """
    if is_tire_sheet:
        return Verdict(TIRE, "elite_wheel:tire_sheet")
    return Verdict(WHEEL, "elite_wheel:wheel_sheet")


def classify_the_wheel_group() -> Verdict:
    """
    The Wheel Group's ``US Data Mastersheet`` is a wheel catalog end to end.

    Verified: all 2,072 rows carry a diameter; all 11 brands (Touren, Mayhem, Ion Alloy, Cali
    Off-Road, Ridler, Dirty Life, Kraze, American Truxx, Mazzi, TuffStuff, Ion Trailer) are wheel
    brands; zero rows mention cap / lug / screw / hardware. The 19 rows without a bolt pattern
    are undrilled ``BLANK`` wheels, still wheels. Note ``TheWheelGroupPart.wheel_cap`` and
    ``.screw`` are companion SKUs *referenced by* a wheel row, not rows of their own here.
    """
    return Verdict(WHEEL, "the_wheel_group:catalog")


def classify_wheelpros(
    *,
    feed_type: typing.Optional[str],
    size: typing.Optional[str],
) -> typing.Optional[Verdict]:
    """
    Wheel Pros delivers three separate SFTP feeds and records which one wrote the row in
    ``WheelProsPart.feed_type``.

    Verified: ``wheel`` 43,079 rows, of which exactly 1 lacks ``size`` -- the 355 whose
    description mentions cap/lug are real wheels whose fitment notes read
    ``ARROW 20X10.5 5X112 66.5 RBL +40 BMW CAP`` or ``FLUX 15X6 6X5.5 +0 G-BLK-DDT (NO CAP)``.
    ``tire`` 5,807 rows; the 1,313 that carry no metric size are drag and flotation sizes
    (``ET DRAG 28.0/10.5-15``, ``SPORTSMAN FRONT LT26X7.50-15``), still tires. ``accessories``
    18,596 -> part.

    The 1,617 rows with a blank ``feed_type`` are lift kits, coil spacers and steering wheels;
    they get no verdict here and fall through to a later tier.

    ``size`` is required for a ``wheel`` verdict as a cheap tripwire: if Wheel Pros ever starts
    routing non-wheels through that feed, those rows go unclassified instead of wrong.
    """
    normalized = (feed_type or "").strip().lower()
    if normalized == "wheel":
        if not (size or "").strip():
            return None
        return Verdict(WHEEL, "wheelpros:feed_type")
    if normalized == "tire":
        return Verdict(TIRE, "wheelpros:feed_type")
    if normalized == "accessories":
        return Verdict(PART, "wheelpros:feed_type")
    return None


# Vossen SKU prefixes that are not wheels. Vossen is its own brand and its feed is wheels plus a
# short tail of companion hardware; these two prefixes are that entire tail.
VOSSEN_ACCESSORY_SKU_PREFIXES = ("CAP-", "LUG-")


def classify_vossen(
    *,
    sku: typing.Optional[str],
    diameter: typing.Optional[str],
) -> typing.Optional[Verdict]:
    """
    Vossen's feed is wheels plus center caps and lug hardware, and the split is exact.

    Verified: of 3,263 rows, precisely 119 have no usable diameter -- 71 ``LUG-*`` and 48
    ``CAP-*``, with no other prefix among them. Every row whose diameter is not ``0``/blank also
    carries a bolt pattern, so there is no ambiguous middle. Both checks are applied here rather
    than trusting either alone.
    """
    normalized_sku = (sku or "").strip().upper()
    if normalized_sku.startswith(VOSSEN_ACCESSORY_SKU_PREFIXES):
        return Verdict(PART, "vossen:accessory_sku")
    # Non-wheel rows carry "0" or blank as a placeholder, per VossenPart's own docstring.
    if (diameter or "").strip() not in ("", "0"):
        return Verdict(WHEEL, "vossen:diameter")
    return None


# ==========================================================================================
# T2 -- closed distributor category vocabularies
#
# Captured from production. A category absent from its table returns None, which is what makes
# these safe to assert `part` from: "Brakes, Rotors & Pads" is not a guess that something is a
# part, it is the distributor telling us it is brakes. Catch-all buckets that could contain
# anything map to None instead.
# ==========================================================================================

# Turn14 ``category`` -- complete 50-value vocabulary. Only 4 categories mention wheels or tires
# and all 4 are covered; ``Wheel and Tire Accessories`` (6,986 rows: lug nuts, center caps,
# spacers, valve stems, tire chains, TPMS) is the reason this table is explicit rather than a
# substring match on "wheel".
TURN14_CATEGORY_TYPES = {
    "Air Filters": PART,
    "Air Intake Systems": PART,
    "Apparel": PART,
    "Audio, Video & Radios": PART,
    "Bags & Packs": PART,
    "Batteries, Starting & Charging": PART,
    "Body": PART,
    "Body Armor & Protection": PART,
    "Brakes, Rotors & Pads": PART,
    "Bumpers, Grilles & Guards": PART,
    "Controls": PART,
    "Cooling": PART,
    "Data Acquisition": PART,
    "Deflectors": PART,
    "Detailing & Care": PART,
    "Drivetrain": PART,
    "Engine Components": PART,
    "Exhaust, Mufflers & Tips": PART,
    "Exterior Styling": PART,
    "Fabrication": PART,
    "Fender Flares & Trim": PART,
    "Floor Mats": PART,
    "Forced Induction": PART,
    "Fuel Delivery": PART,
    "Gauges & Pods": PART,
    "Ignition": PART,
    "Implements": PART,
    "Interior Accessories": PART,
    "Lights": PART,
    "Marketing": PART,
    "Nerf Bars & Running Boards": PART,
    "Oils & Oil Filters": PART,
    "Programmers & Chips": PART,
    "Roofs & Roof Accessories": PART,
    "Safety": PART,
    "Seats": PART,
    "Services": PART,
    "Suspension": PART,
    "Tires": TIRE,
    "Tonneau Covers": PART,
    "Tools": PART,
    "Transport": PART,
    "Truck Bed Accessories": PART,
    "Wheel and Tire Accessories": PART,
    "Wheels": WHEEL,
    "Winches & Hitches": PART,
    "Windshields": PART,
    # Catch-alls: a damaged-goods or unsorted bucket can hold a wheel as easily as a bracket.
    "Misc Powersports": None,
    "Scratch & Dent": None,
    "Uncategorized": None,
}

# WPS ``product_type`` -- complete 108-value vocabulary. WPS is powersports, so most of it is
# apparel and gear. ``Rims`` is WPS's own name for a bare wheel; ``Wheel Components`` and
# ``Tire/Wheel Accessories`` are hardware; ``Tubes`` are inner tubes, which are not tires.
WPS_PRODUCT_TYPE_TYPES = {
    "Air Filters": PART,
    "Audio/Visual/Communication": PART,
    "Batteries": PART,
    "Belts": PART,
    "Bike": PART,
    "Body": PART,
    "Brakes": PART,
    "Cable/Hydraulic Control Lines": PART,
    "Chains": PART,
    "Chemicals": PART,
    "Clamps": PART,
    "Clutch": PART,
    "Cranks": PART,
    "Drive": PART,
    "Electrical": PART,
    "Engine": PART,
    "Engine Management": PART,
    "Exhaust": PART,
    "Eyewear": PART,
    "Farm/Agriculture": PART,
    "Flotation Vests": PART,
    "Food & Beverage": PART,
    "Foot Controls": PART,
    "Footwear": PART,
    "Forks": PART,
    "Fuel Containers": PART,
    "Fuel Tank": PART,
    "Gas Caps": PART,
    "Gaskets/Seals": PART,
    "Gauges/Meters": PART,
    "Gloves": PART,
    "GPS": PART,
    "Graphics/Decals": PART,
    "Grips": PART,
    "Guards/Braces": PART,
    "Hand Controls": PART,
    "Handguards": PART,
    "Handlebars": PART,
    "Hardware/Fasteners/Fittings": PART,
    "Headgear": PART,
    "Helmet Accessories": PART,
    "Helmets": PART,
    "Hoodies": PART,
    "Hyfax": PART,
    "Ice Scratchers": PART,
    "Illumination": PART,
    "Intake/Carb/Fuel System": PART,
    "Jackets": PART,
    "Jerseys": PART,
    "Jets": PART,
    "Layers": PART,
    "Levers": PART,
    "Luggage": PART,
    "Mats/Rugs": PART,
    "Mirrors": PART,
    "Mounts/Brackets": PART,
    "Oil Change Kit": PART,
    "Oil Filters": PART,
    "Onesies": PART,
    "Pants": PART,
    "Piston kits & Components": PART,
    "Plow": PART,
    "Plow Mount": PART,
    "Promotional": PART,
    "Protective/Safety": PART,
    "Racks": PART,
    "Rims": WHEEL,
    "Risers": PART,
    "Seat": PART,
    "Security": PART,
    "Shirts": PART,
    "Shoes": PART,
    "Shorts": PART,
    "Skis/Carbides/Runners": PART,
    "Socks": PART,
    "Spark Plugs": PART,
    "Sprockets": PART,
    "Stands/Lifts": PART,
    "Starters": PART,
    "Steering": PART,
    "Storage Covers": PART,
    "Straps/Tie-Downs": PART,
    "Suits": PART,
    "Suspension": PART,
    "Sweaters": PART,
    "Switches": PART,
    "Tank Tops": PART,
    "Throttle": PART,
    # A mounted wheel-and-tire package. Follows PCdb, which files "Wheel and Tire Assembly"
    # (14897) under its Wheel subcategory rather than Tire.
    "Tire And Wheel Kit": WHEEL,
    "Tires": TIRE,
    "Tire/Wheel Accessories": PART,
    "Tools": PART,
    "Track Kit": PART,
    "Tracks": PART,
    "Trailer/Towing": PART,
    "Tubes": PART,
    "Undergarments": PART,
    "Utility Containers": PART,
    "UTV Cab/Roof/Door": PART,
    "Vests": PART,
    "Watercraft Towables": PART,
    "Wheel Components": PART,
    "Wheels": WHEEL,
    "Winch": PART,
    "Winch Mount": PART,
    "Windshield/Windscreen": PART,
    # Catch-alls that can hold anything, including a wheel.
    "Accessories": None,
    "Replacement Parts": None,
}

# Rough Country ``category`` -- complete 35-value vocabulary. ``RC Wheels`` (716) is their own
# wheel line and ``Wheels`` (41) the generic bucket; ``Wheel Well Liners`` (63) is bodywork.
ROUGH_COUNTRY_CATEGORY_TYPES = {
    "Air Spring Kits": PART,
    "Apparel & Acc": PART,
    "Bed Covers": PART,
    "Bull Bars": PART,
    "Bumpers": PART,
    "Cargo Management": PART,
    "Decals": PART,
    "Driveline Acc": PART,
    "Fender Flares": PART,
    "Gift Card": PART,
    "Interior & Mats": PART,
    "Jeep Add-Ons": PART,
    "Jeep Tops": PART,
    "Leveling Kits": PART,
    "Lighting & Acc": PART,
    "Lowering Kits": PART,
    "RC Body Lift Kits": PART,
    "RC Wheels": WHEEL,
    "Shocks & Stabilizers": PART,
    "Steps & Runningboard": PART,
    "Susp Accessories": PART,
    "Susp Lift Kits 2wd": PART,
    "Susp Lift Kits 4wd": PART,
    "Susp Lift Kits UTV": PART,
    "Tires": TIRE,
    "Vertex Kits": PART,
    "Vertex Shocks": PART,
    "Wheels": WHEEL,
    "Wheel Well Liners": PART,
    "Winches & Acc": PART,
    "Windshields": PART,
    # Catch-alls -- "Misc 25%" and "Component" are pricing/ops buckets, not product classes.
    "Component": None,
    "Misc 25%": None,
    "Other Finished Goods": None,
    "Performance": None,
}

# Premier ``part_category`` -- complete 24-value vocabulary, itself PCdb's category names.
# ``Tire and Wheel`` (22,581 rows) cannot be split at this level, so it defers to the PCdb
# terminology tier; ``NA`` (420,160) is Premier's own null.
PREMIER_PART_CATEGORY_TYPES = {
    "Accessories and Fluids": PART,
    "Air and Fuel Delivery": PART,
    "Belts and Cooling": PART,
    "Body": PART,
    "Brake": PART,
    "Driveline and Axles": PART,
    "Electrical Charging and Starting": PART,
    "Electrical Lighting and Body": PART,
    "Emission Control": PART,
    "Engine": PART,
    "Entertainment and Telematics": PART,
    "Exhaust": PART,
    "Hardware and Service Supplies": PART,
    "HVAC": PART,
    "Ignition": PART,
    "Steering": PART,
    "Suspension": PART,
    "Tools and Equipment": PART,
    "Transfer Case": PART,
    "Transmission": PART,
    "Wiper and Washer": PART,
    "Multifunction Terms": None,
    "NA": None,
    "Tire and Wheel": None,
}

# Helmet House ``category`` -- complete 43-value vocabulary. It is a powersports apparel and
# helmet distributor with no wheel or tire line at all; the brand-name values (ALPINESTARS, HJC,
# SHOEI, CORTECH, NORU, TOURMASTER, 100 PERCENT) are the feed using a brand where a category
# belongs, which is untidy but still unambiguously not a wheel or a tire.
HELMET_HOUSE_CATEGORY_TYPES = {
    name: PART
    for name in (
        "100 PERCENT", "ACCESSORIES", "ALPINESTARS", "BAGS", "BASELAYER", "BASE LAYERS",
        "BEANIE", "BOOT", "BOOTS", "CASUAL", "CASUAL WEAR", "CORTECH", "FULL HEAT", "GLOVE",
        "GLOVES", "GOGGLES", "HELMET", "HELMETS", "HJC", "HOODIE", "JACKET", "JACKETS",
        "LINERS", "LUGGAGE", "MC COVER", "MISC", "MISCELLANEOUS", "NORU", "ONE PIECE SUIT",
        "PANT", "PANTS", "POP", "PROTECTOR", "RAINSUIT", "SHELF TALKER", "SHIELDS", "SHOEI",
        "SHOES", "SOCKS", "SUIT", "SUITS", "TOURMASTER", "VEST",
    )
}

# Meyer ``category``. Meyer's own values are semicolon-joined multi-values
# ("ACCESSORIESEXTERIOR;TIRE/WHEEL"), so this is membership-tested per token rather than looked
# up whole. Verified: no row whose ``sub_category`` is WHEEL or TIRE sits outside the
# ``TIRE/WHEEL`` category, so a part with no TIRE/WHEEL token cannot be either.
MEYER_TIRE_WHEEL_CATEGORY = "TIRE/WHEEL"


def _lookup_category(
    raw_category: typing.Any,
    vocabulary: typing.Dict[str, typing.Optional[str]],
    source: str,
) -> typing.Optional[Verdict]:
    """Closed-vocabulary lookup. Unknown value or a value mapped to None -> no verdict."""
    if raw_category is None:
        return None
    key = raw_category.strip() if isinstance(raw_category, str) else str(raw_category).strip()
    if not key:
        return None
    product_type = vocabulary.get(key)
    if product_type is None:
        return None
    return Verdict(product_type, source)


def classify_turn14(category: typing.Any) -> typing.Optional[Verdict]:
    """Turn14 ``category`` (794,453 of 795,946 rows carry one)."""
    return _lookup_category(category, TURN14_CATEGORY_TYPES, "turn14:category")


def classify_wps(product_type: typing.Any) -> typing.Optional[Verdict]:
    """WPS ``WpsItem.product_type``."""
    return _lookup_category(product_type, WPS_PRODUCT_TYPE_TYPES, "wps:product_type")


def classify_rough_country(category: typing.Any) -> typing.Optional[Verdict]:
    """Rough Country ``category`` (7,959 of 7,960 rows carry one)."""
    return _lookup_category(category, ROUGH_COUNTRY_CATEGORY_TYPES, "rough_country:category")


def classify_premier_category(part_category: typing.Any) -> typing.Optional[Verdict]:
    """
    Premier ``part_category``. Returns nothing for ``Tire and Wheel`` on purpose -- callers
    should try ``classify_pcdb_terminology`` on the row's ``part_terminology`` first, which
    resolves that bucket properly.
    """
    return _lookup_category(part_category, PREMIER_PART_CATEGORY_TYPES, "premier:part_category")


def classify_helmet_house(category: typing.Any) -> typing.Optional[Verdict]:
    """Helmet House ``category``."""
    return _lookup_category(category, HELMET_HOUSE_CATEGORY_TYPES, "helmet_house:category")


def split_multi_value(raw: typing.Any) -> typing.List[str]:
    """
    Meyer packs several values into one column separated by ';' -- categories
    ("ACCESSORIESEXTERIOR;TIRE/WHEEL") and terminologies ("WHEEL LUG NUT LOCK;WHEEL LOCK") alike.
    ``master_parts._meyer_first_category_token`` keeps only the first for CategoryMapping; here
    every token matters, since "TIRE/WHEEL" can be the second one.
    """
    if raw is None:
        return []
    text = raw if isinstance(raw, str) else str(raw)
    return [token.strip() for token in text.split(";") if token.strip()]


def classify_meyer_category(category: typing.Any) -> typing.Optional[Verdict]:
    """
    Meyer ``category``. Only ``TIRE/WHEEL`` can contain a wheel or a tire, so anything without
    that token is a part; a row that has it needs ``sub_category`` (PCdb terminology) to say
    which, and gets no verdict here.

    Note ``BRAKE/WHEEL HUB`` mentions "WHEEL" but is brake and hub hardware -- token equality,
    not substring matching, is what keeps its 14,631 rows out of the wheel bucket.
    """
    tokens = split_multi_value(category)
    if not tokens:
        return None
    if MEYER_TIRE_WHEEL_CATEGORY in tokens:
        return None
    return Verdict(PART, "meyer:category")


# ==========================================================================================
# T3 -- PCdb part terminology
#
# Used by Premier (``part_terminology``, 688,748 of 688,934 rows non-null) and Meyer
# (``sub_category``, which is the same vocabulary uppercased). PCdb is already mirrored locally
# by ``load_pcdb`` into ``pcdb_terminology_flat`` (40,646 terminologies).
#
# The subcategory is NOT usable as the discriminator, which is the trap here: of the 135 terms
# under category 17 "Tire and Wheel", roughly 120 are accessories, and PCdb files "Wheel Cover"
# (10026) under subcategory Wheel and "Tire Inner Tube" (7640) under subcategory Tire. So the
# wheel and tire sets are enumerated by terminology id instead.
# ==========================================================================================

PCDB_TIRE_AND_WHEEL_CATEGORY_ID = 17

# Terms that ARE a wheel. Includes mounted assemblies, which PCdb itself files under Wheel.
PCDB_WHEEL_TERMINOLOGY_IDS = frozenset({
    7644,   # Wheel
    50882,  # Wheel Set
    61780,  # Wheel Rim with Side Ring
    14897,  # Wheel and Tire Assembly
    61775,  # Disc Rim and Wheel Assembly
    67687,  # Spare Tire Assembly -- a mounted spare, i.e. wheel + tire
    # Heavy-truck wheel assemblies, sold as the wheel itself.
    61802,  # Spoke Wheel with Cup / Stud / Drum
    61804,  # Spoke Wheel with Rotor
    61806,  # Steer Axle Wheel Assembly
    61807,  # Drive Axle Wheel Assembly
    61808,  # Pusher Axle Wheel Assembly
    61809,  # Tag Axle Wheel Assembly
    61810,  # Trailer Axle Wheel Assembly
})

# Terms that ARE a tire. Deliberately short: "Tire Inner Tube", "Tire Snow Chain", every TPMS
# term and the whole Spare Tire *carrier* family are accessories and fall through to part.
PCDB_TIRE_TERMINOLOGY_IDS = frozenset({
    7636,   # Tire
    71101,  # Tire Casing -- the carcass itself, the retread trade's word for a tire
    47296,  # Spare Tire -- an unmounted spare is still a tire
})

# Terms we decline to call either way. Left unclassified rather than forced into `part`, since
# these plausibly ARE wheels and a wrong `part` here is as bad as a wrong `wheel`.
PCDB_AMBIGUOUS_TERMINOLOGY_IDS = frozenset({
    51469,  # Wheel Kit -- wheels, or wheels plus mounting hardware, or just the hardware
    61801,  # Support Equipment Caster Wheel -- a caster on shop equipment, not a vehicle wheel
    16886,  # Dual Wheel Extension -- dually adapter; sits between wheel and hardware
})


def classify_pcdb_terminology(
    *,
    part_terminology_id: typing.Optional[int],
    category_id: typing.Optional[int],
) -> typing.Optional[Verdict]:
    """
    Resolve a PCdb terminology to a product type.

    Any terminology that resolved at all and is not on the wheel or tire allow-list is a part --
    that is the whole of PCdb's 40,646 terms, not just category 17, so this is the tier that
    classifies the bulk of Premier and Meyer. ``category_id`` is required only as proof the
    terminology actually resolved against our mirror; an unresolved name gets no verdict.
    """
    if part_terminology_id is None:
        return None
    if part_terminology_id in PCDB_AMBIGUOUS_TERMINOLOGY_IDS:
        return None
    if part_terminology_id in PCDB_WHEEL_TERMINOLOGY_IDS:
        return Verdict(WHEEL, "pcdb:{}".format(part_terminology_id))
    if part_terminology_id in PCDB_TIRE_TERMINOLOGY_IDS:
        return Verdict(TIRE, "pcdb:{}".format(part_terminology_id))
    if category_id is None:
        return None
    return Verdict(PART, "pcdb:{}".format(part_terminology_id))


def normalize_terminology_name(raw: typing.Any) -> str:
    """
    Key for matching a distributor's terminology text against ``pcdb_terminology_flat.name``.

    Case and internal whitespace only -- nothing is stripped that could change meaning. Meyer
    ships the vocabulary uppercased ("WHEEL LUG NUT"), Premier in title case ("Wheel Lug Nut").
    """
    if raw is None:
        return ""
    text = raw if isinstance(raw, str) else str(raw)
    return " ".join(text.split()).casefold()


# ==========================================================================================
# T5 -- size regexes over description text
#
# Inferential: this reads a distributor's prose rather than a field it filled in deliberately.
# Reserved for catalogs with no category data at all, which today means TireRack.
# ==========================================================================================

# Tire sizes, in the four shapes TireRack's feed actually uses:
#   metric / LT-metric     265/70R17, LT275/70R18, 205/55ZR16
#   flotation              35X12.50R17
#   vintage European       175HR14, 205R16, 17R400C
#   drag / bias flotation  28.0/10.5-15, LT26X7.50-15, 31/71-19
#
# The speed-rating letter in the vintage shape is optional -- Pirelli ships "155R13 PI CINTURATO
# CA67" alongside Vredestein's "175HR14~".
#
# The ``(?<![-\d])`` guard on the drag/bias shape is load-bearing, not defensive tidying. A
# center cap that fits two bolt patterns is described as "NOMAD 5-127/6-120 DUSK CAP", and
# without the guard the tail "127/6-120" reads as a drag tire size -- 9 Wheel Accessories rows
# were being called tires because of it. A real drag size is never preceded by "<digit>-".
TIRE_SIZE_RE = re.compile(
    r"\d{2,3}/\d{2}[A-Z]*R?\d{2}"
    r"|\d{2}X\d+(?:\.\d+)?R\d{2}"
    r"|\d{2,3}[HVSRTZ]?R\d{2,3}"
    r"|(?<![-\d])\d{2,3}(?:\.\d)?/\d{1,2}(?:\.\d)?-\d{2}",
    re.IGNORECASE,
)

# Wheel sizes: diameter x width immediately followed by a bolt pattern, anchored at the start of
# the description ("20X9  8-165 ET01 FL CATALYST"). The anchor and the required bolt pattern are
# what keep this from firing on a tire's flotation size.
WHEEL_SIZE_RE = re.compile(r"^\d{2}X\d+(?:\.\d+)?\s+\d-\d{2,3}", re.IGNORECASE)


def classify_tirerack_description(description: typing.Any) -> typing.Optional[Verdict]:
    """
    TireRack ships no category field -- its feed is 12 columns and ``Description`` is the only
    type signal -- and the catalog is emphatically not tires-only: 86,224 rows across 198
    manufacturer labels, mixing tires with KYB Shocks (3,629), Bilstein (3,429), Powerstop
    (3,308), a "Wheel Accessories" label (3,285) and wheel brands (Fuel Off-Road, Enkei, BBS,
    Method, KMC, Niche).

    Measured by running this function over all 86,224 rows in production:

      tire regex   21,772 / 21,825 rows on the 14 pure tire brands = 99.8% recall
      wheel regex  13,901 / 14,378 rows on 13 pure wheel brands    = 96.7% recall
      precision    0 verdicts of any kind across 22,186 rows of KYB, Bilstein, Powerstop,
                   Brembo, Hawk, H&R, KW, Koni and "Wheel Accessories", and no crossover in
                   either direction between the tire-brand and wheel-brand sets

    Over the whole table: 27,788 tire, 24,121 wheel, 34,315 unclassified -- the remainder being
    shocks, brakes and springs, which the brand tier resolves rather than this one. The 53 tire
    misses are Michelin inner tubes, which are correctly not tires.

    Tire is tested first: a tire description can contain a wheel-diameter-looking token, but the
    wheel pattern requires a bolt pattern that no tire size produces.
    """
    if description is None:
        return None
    text = description if isinstance(description, str) else str(description)
    if not text.strip():
        return None
    if TIRE_SIZE_RE.search(text):
        return Verdict(TIRE, "keyword:tirerack-tire-size")
    if WHEEL_SIZE_RE.search(text):
        return Verdict(WHEEL, "keyword:tirerack-wheel-size")
    return None


# ==========================================================================================
# Resolution across providers
# ==========================================================================================

# Lower number wins. Ordered by how directly the distributor asserted the type: a structural
# feed split beats a category string, which beats prose. Ties within a tier are broken by
# `resolve_verdicts` preferring wheel/tire over part -- see there for why.
SOURCE_TIER = {
    "elite_wheel": 1,
    "the_wheel_group": 1,
    "wheelpros": 1,
    "vossen": 1,
    "turn14": 2,
    "wps": 2,
    "rough_country": 2,
    "helmet_house": 2,
    "meyer": 2,
    "premier": 2,
    "pcdb": 3,
    "brand": 4,
    "keyword": 5,
}


def tier_for_source(source: str) -> int:
    """Tier number for a ``product_type_source``; unknown prefixes sort last."""
    return SOURCE_TIER.get(source.split(":", 1)[0], max(SOURCE_TIER.values()) + 1)


def resolve_verdicts(
    verdicts: typing.Sequence[typing.Optional[Verdict]],
) -> typing.Optional[Verdict]:
    """
    Pick one verdict for a master part from the verdicts of all its provider rows.

    Best tier wins. Within a tier, a ``wheel``/``tire`` verdict beats ``part``: distributors
    disagree mostly by being vague rather than by being wrong -- one sells a wheel under
    "Wheels" while another files the same part under a catch-all that maps to part -- and the
    specific claim is the informative one. Genuine same-tier contradictions between wheel and
    tire are rare enough to be worth seeing, so the first is kept and the caller is expected to
    report them (that is the cross-provider disagreement check in the QA phase).
    """
    present = [verdict for verdict in verdicts if verdict is not None]
    if not present:
        return None
    return min(
        present,
        key=lambda verdict: (
            tier_for_source(verdict.source),
            verdict.product_type == PART,
        ),
    )
