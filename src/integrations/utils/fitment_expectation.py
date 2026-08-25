"""
Decide whether a part is the *kind of thing* that has vehicle fitment at all -- the difference
between "we are missing fitment for this part" and "this part has no fitment to miss".

``MasterPartFitment`` answers "what vehicles does this bolt onto". A brake rotor has an answer.
A 275/60R20 tire does not: it fits by size, and the same tire spans thousands of vehicles that
no YMM lookup would usefully enumerate. Nor does a t-shirt, a helmet, a quart of oil, a torque
wrench, or a length of raw aluminium tube. Counting all of those as "missing fitment" is what
makes the headline 14.5% coverage number meaningless -- a large part of that gap is parts that
were never supposed to have any.

**Vehicle-specific is not the same as vehicle-fitted.** A universal part can still be sold for
a vehicle (a 3" exhaust tip fits anything with a 3" pipe). This module only answers whether a
row in the fitment table is the right way to describe the part.

Three verdicts, and the third matters as much as the other two:

  ``EXPECTED``       vehicle-specific -- absent fitment is a real gap
  ``NOT_APPLICABLE`` universal / sized / non-vehicle -- absent fitment is correct
  ``None``           the signals do not decide. Never silently turned into either one.

**The thresholds here are measured, not assumed.** Turn 14 is the only large feed that ships
both a category and vehicle fitment (794k categorised parts, 429k of them fitted), so its
per-category fitment rate is direct evidence for what a category *is*: 96.8% of Floor Mats
carry fitment and 0.4% of Tires do. Every category verdict below records that observed rate.

A category is only called ``NOT_APPLICABLE`` when it is both low-rate **and** has a physical
reason to be -- the part fits by size, by bolt pattern, by body, or not to a vehicle at all. A
low rate on its own is equally consistent with "Turn 14 hasn't published fitment for this line
yet", which is a gap, not an exemption. Where the rate is mid-range the category is genuinely
mixed and the answer is ``None``: ``Wheels`` at 10.2% is not "wheels never fit", it is "wheels
fit by bolt pattern, and the 10% carrying YMM are the OE-replacement lines".

Pure module -- values in, verdict out, no DB access. The batched scan that feeds it lives in
``src.integrations.services.fitment_audit``. Counts are from the 2026-08-20 survey.
"""
import typing

EXPECTED = "expected"
NOT_APPLICABLE = "not_applicable"


class Verdict(typing.NamedTuple):
    """An expectation plus the rule that produced it, so a roll-up stays auditable."""
    expectation: str
    source: str


class _Cat(typing.NamedTuple):
    """A category verdict carrying the observed Turn 14 fitment rate that justifies it."""
    expectation: typing.Optional[str]
    observed_rate: float


# ==========================================================================================
# Turn 14 ``provider_parts.category`` -- 794,173 categorised parts, the measured vocabulary.
#
# ``observed_rate`` is the share of that category's master parts that actually carry >=1
# MasterPartFitment row. Keep it next to the verdict: when a feed changes shape, the tell is a
# rate that no longer matches the verdict it was supposed to justify.
# ==========================================================================================
TURN14_CATEGORY_EXPECTATION = {
    # -- Vehicle-specific. Fitment is the whole point of the part. -----------------------
    "Floor Mats": _Cat(EXPECTED, 96.8),
    "Body": _Cat(EXPECTED, 89.5),
    "Nerf Bars & Running Boards": _Cat(EXPECTED, 89.0),
    "Seats": _Cat(EXPECTED, 83.5),
    "Deflectors": _Cat(EXPECTED, 82.3),
    "Tonneau Covers": _Cat(EXPECTED, 79.7),
    "Windshields": _Cat(EXPECTED, 76.5),
    "Body Armor & Protection": _Cat(EXPECTED, 76.0),
    "Air Intake Systems": _Cat(EXPECTED, 68.8),
    "Brakes, Rotors & Pads": _Cat(EXPECTED, 68.1),
    "Exhaust, Mufflers & Tips": _Cat(EXPECTED, 67.7),
    "Suspension": _Cat(EXPECTED, 66.8),
    "Ignition": _Cat(EXPECTED, 66.7),
    "Bumpers, Grilles & Guards": _Cat(EXPECTED, 58.6),
    "Lights": _Cat(EXPECTED, 58.0),
    "Exterior Styling": _Cat(EXPECTED, 57.9),
    "Roofs & Roof Accessories": _Cat(EXPECTED, 57.2),
    "Programmers & Chips": _Cat(EXPECTED, 56.8),
    "Cooling": _Cat(EXPECTED, 53.4),
    "Interior Accessories": _Cat(EXPECTED, 52.1),
    "Truck Bed Accessories": _Cat(EXPECTED, 50.3),
    "Controls": _Cat(EXPECTED, 47.1),
    "Drivetrain": _Cat(EXPECTED, 46.5),
    "Engine Components": _Cat(EXPECTED, 42.7),
    "Batteries, Starting & Charging": _Cat(EXPECTED, 42.2),
    "Fuel Delivery": _Cat(EXPECTED, 35.5),
    "Air Filters": _Cat(EXPECTED, 35.2),
    "Forced Induction": _Cat(EXPECTED, 29.1),

    # -- Not applicable. Each has a reason beyond the low rate. ---------------------------
    # Sized, not fitted: a 275/60R20 is chosen from the sticker in the door jamb, and the
    # 0.4% that do carry rows are OE-fitment tires Turn 14 happens to publish VCdb for.
    "Tires": _Cat(NOT_APPLICABLE, 0.4),
    # Not a vehicle part. Shirts, hats, banners.
    "Apparel": _Cat(NOT_APPLICABLE, 0.0),
    # Head-and-neck restraints, suits, harnesses, fire bottles -- fitted to the driver or to
    # a roll cage, not to a YMM.
    "Safety": _Cat(NOT_APPLICABLE, 2.7),
    # Head units, speakers, amps: fitted by DIN slot and speaker diameter.
    "Audio, Video & Radios": _Cat(NOT_APPLICABLE, 0.8),

    # -- Genuinely mixed. Left undecided on purpose. --------------------------------------
    # Bolt-pattern/offset fitment; the 10.2% that carry YMM are OE-replacement lines.
    "Wheels": _Cat(None, 10.2),
    # Looks exempt at 14.9% and is not. A sample of the fitted rows is application tooling --
    # "ACT 1997 Acura CL Alignment Tool", "Rancho 97-06 Jeep TJ Front Bushing Installation
    # Tool", "EPI 04+ Yamaha Belt Removal Tool" -- which has genuine YMM fitment, mixed in with
    # the generic wrenches and pullers that have none. The category cannot separate the two.
    "Tools": _Cat(None, 14.9),
    # Raw stock (tube, sheet, weld-on tabs) mixed with vehicle-specific cage kits.
    "Fabrication": _Cat(None, 28.1),
    # Lug nuts and TPMS (universal, by thread pitch) mixed with vehicle-specific spacers.
    "Wheel and Tire Accessories": _Cat(None, 31.8),
    # Bulk fluids (universal) mixed with application-specific oil filters.
    "Oils & Oil Filters": _Cat(None, 33.4),
    # Gauges are universal; the pods that hold them are dash-specific.
    "Gauges & Pods": _Cat(None, 16.6),
    # Winches are universal; their mounting plates are vehicle-specific.
    "Winches & Hitches": _Cat(None, 16.8),
}


# ==========================================================================================
# WPS ``wps_items.product_type`` -- powersports. 127,130 master parts, 12,186 of them fitted
# (9.6%), and every one of those got its fitment from Turn 14 rather than from WPS: the two
# catalogs overlap and the merged MasterPart inherits Turn 14's rows.
#
# WPS is a motorcycle/ATV/snowmobile catalog and MasterPartFitment is a VCdb (car and light
# truck) table, so nothing here can be filled from WPS itself -- but the right answer still
# differs by row. Riding gear genuinely has no vehicle fitment; a sprocket for a specific bike
# does, and its absence is a gap a powersports fitment source would have to fill. That is the
# point of splitting the vocabulary: NOT_APPLICABLE must not quietly absorb the whole feed.
# ==========================================================================================
WPS_PRODUCT_TYPE_EXPECTATION = {
    # Rider-worn and rider-owned. Sized to a person, never to a vehicle.
    "Eyewear": NOT_APPLICABLE,
    "Flotation Vests": NOT_APPLICABLE,
    "Food & Beverage": NOT_APPLICABLE,
    "Footwear": NOT_APPLICABLE,
    "Gloves": NOT_APPLICABLE,
    "Headgear": NOT_APPLICABLE,
    "Helmet Accessories": NOT_APPLICABLE,
    "Helmets": NOT_APPLICABLE,
    "Hoodies": NOT_APPLICABLE,
    "Jackets": NOT_APPLICABLE,
    "Jerseys": NOT_APPLICABLE,
    "Layers": NOT_APPLICABLE,
    "Onesies": NOT_APPLICABLE,
    "Pants": NOT_APPLICABLE,
    "Protective/Safety": NOT_APPLICABLE,
    "Shirts": NOT_APPLICABLE,
    "Shoes": NOT_APPLICABLE,
    "Shorts": NOT_APPLICABLE,
    "Socks": NOT_APPLICABLE,
    "Suits": NOT_APPLICABLE,
    "Sweaters": NOT_APPLICABLE,
    "Tank Tops": NOT_APPLICABLE,
    "Undergarments": NOT_APPLICABLE,
    "Vests": NOT_APPLICABLE,
    # Consumables and shop goods.
    "Chemicals": NOT_APPLICABLE,
    "Promotional": NOT_APPLICABLE,
    "Stands/Lifts": NOT_APPLICABLE,
    "Tools": NOT_APPLICABLE,
    "Utility Containers": NOT_APPLICABLE,
    # Sized or universal hardware.
    "Clamps": NOT_APPLICABLE,
    "Fuel Containers": NOT_APPLICABLE,
    "Hardware/Fasteners/Fittings": NOT_APPLICABLE,
    "Straps/Tie-Downs": NOT_APPLICABLE,
    "Tires": NOT_APPLICABLE,
    "Tubes": NOT_APPLICABLE,
    # Not a powered vehicle in VCdb's sense.
    "Bike": NOT_APPLICABLE,
    "Farm/Agriculture": NOT_APPLICABLE,
    "Watercraft Towables": NOT_APPLICABLE,
    # Vehicle-specific powersports parts. EXPECTED, and unfillable from a VCdb-only pipeline --
    # exactly the distinction this table exists to keep visible.
    "Body": EXPECTED,
    "Brakes": EXPECTED,
    "Cable/Hydraulic Control Lines": EXPECTED,
    "Clutch": EXPECTED,
    "Drive": EXPECTED,
    "Engine": EXPECTED,
    "Exhaust": EXPECTED,
    "Foot Controls": EXPECTED,
    "Forks": EXPECTED,
    "Fuel Tank": EXPECTED,
    "Gaskets/Seals": EXPECTED,
    "Hand Controls": EXPECTED,
    "Intake/Carb/Fuel System": EXPECTED,
    "Piston kits & Components": EXPECTED,
    "Plow Mount": EXPECTED,
    "Sprockets": EXPECTED,
    "Suspension": EXPECTED,
    "Track Kit": EXPECTED,
    "UTV Cab/Roof/Door": EXPECTED,
    "Windshield/Windscreen": EXPECTED,
    "Winch Mount": EXPECTED,
    # Catch-alls that can hold anything.
    "Accessories": None,
    "Replacement Parts": None,
}


# ==========================================================================================
# Premier ``premier_parts.part_category`` -- PCdb top-level categories on 268,591 of 688,940
# rows (the other 420,163 are the literal string "NA" and decide nothing).
# ==========================================================================================
PREMIER_CATEGORY_EXPECTATION = {
    "Brake": EXPECTED,
    "Body": EXPECTED,
    "Suspension": EXPECTED,
    "Engine": EXPECTED,
    "Air and Fuel Delivery": EXPECTED,
    "Driveline and Axles": EXPECTED,
    "Exhaust": EXPECTED,
    "Electrical Lighting and Body": EXPECTED,
    "Transmission": EXPECTED,
    "Belts and Cooling": EXPECTED,
    "Steering": EXPECTED,
    "Transfer Case": EXPECTED,
    "HVAC": EXPECTED,
    "Emission Control": EXPECTED,
    "Ignition": EXPECTED,
    "Electrical Charging and Starting": EXPECTED,
    "Wiper and Washer": EXPECTED,
    # Wheels by bolt pattern, tires by size -- the same split as Turn 14's Wheels/Tires.
    "Tire and Wheel": NOT_APPLICABLE,
    "Tools and Equipment": NOT_APPLICABLE,
    "Accessories and Fluids": NOT_APPLICABLE,
    "Hardware and Service Supplies": NOT_APPLICABLE,
    "Entertainment and Telematics": NOT_APPLICABLE,
    # Feed's own null markers, and a PCdb bucket for terms that span categories.
    "NA": None,
    "None": None,
    "Multifunction Terms": None,
}


# ==========================================================================================
# Meyer ``meyer_parts.category`` -- 227,942 of 964,618 rows carry one; the other 736,676 are
# NULL and decide nothing. The vocabulary is PCdb-ish but Meyer's own (``ACCESSORIESEXTERIOR``
# as one token, ``BRAKE/WHEEL HUB`` as one bucket).
#
# A value can pack several categories semicolon-joined ("ACCESSORIESEXTERIOR;RV ACCESSORIES").
# Those ~2,100 rows are deliberately not classified here: the honest resolution is "only if
# every token agrees", and getting that right is worth more than the 0.9% of Meyer's
# categorised rows it would recover. They fall through to ``unknown``, which is the correct
# place for them.
# ==========================================================================================
MEYER_CATEGORY_EXPECTATION = {
    "ACCESSORIESEXTERIOR": EXPECTED,
    "ACCESSORIESINTERIOR": EXPECTED,
    "BELT DRIVE SYSTEM": EXPECTED,
    "BODYEXTERIOR": EXPECTED,
    "BODYINTERIOR": EXPECTED,
    "BRAKE/WHEEL HUB": EXPECTED,
    "CLUTCH": EXPECTED,
    "COOLING SYSTEM": EXPECTED,
    "DRIVETRAIN": EXPECTED,
    "ELECTRICAL": EXPECTED,
    "EMISSION": EXPECTED,
    "ENGINE": EXPECTED,
    "EXHAUST": EXPECTED,
    "FUEL": EXPECTED,
    "HVAC": EXPECTED,
    "IGNITION": EXPECTED,
    "STEERING": EXPECTED,
    "SUSPENSION": EXPECTED,
    "TRANSFER CASE": EXPECTED,
    "TRANSMISSION AND TRANSAXLE  AUTOMATIC": EXPECTED,
    "TRANSMISSION AND TRANSAXLE  MANUAL": EXPECTED,
    "WIPER": EXPECTED,
    # Sized, universal, or not a VCdb vehicle at all.
    "TIRE/WHEEL": NOT_APPLICABLE,
    "TOOLS AND EQUIPMENT": NOT_APPLICABLE,
    "HARDWARE AND SERVICE SUPPLIES": NOT_APPLICABLE,
    "MAINTENANCE": NOT_APPLICABLE,
    "APPAREL AND COLLECTIBLES": NOT_APPLICABLE,
    # RVs and boats are not in VCdb, so no fitment row can describe them. Note this is a
    # statement about our fitment table, not about the parts: an RV step absolutely fits
    # specific coaches, we simply have no vocabulary to say which.
    "RV ACCESSORIES": NOT_APPLICABLE,
    "MARINE ACCESSORIES": NOT_APPLICABLE,
    # Genuinely undecidable buckets.
    "MISCELLANEOUS": None,
    "UTILITY": None,
    # Alarms and immobilisers are vehicle-specific; fire extinguishers and straps are not.
    "SECURITY/SAFETY": None,
    # Sold as universal kits, plumbed per application.
    "NITROUS OXIDE INJECTION SYSTEM": None,
}


# ==========================================================================================
# Helmet House -- 41,475 master parts, zero fitment, and correctly so. A motorcycle riding-gear
# catalog end to end: helmets, jackets, gloves, boots, luggage. ``category`` is dirty (brand
# names such as SHOEI and HJC appear as categories, and 20,274 rows are null), which is why the
# verdict is at feed level rather than per category -- there is no product in this catalog that
# bolts to a vehicle.
# ==========================================================================================
def classify_helmet_house() -> Verdict:
    return Verdict(NOT_APPLICABLE, "helmet_house:catalog")


# ==========================================================================================
# Wheel Pros -- ``wheelpros_parts.feed_type`` separates the SFTP feeds structurally:
# 43,083 wheel / 5,807 tire / 18,596 accessories / 1,617 null.
# ==========================================================================================
def classify_wheelpros(feed_type: typing.Any) -> typing.Optional[Verdict]:
    """
    Wheels fit by bolt pattern and tires by size; neither is a YMM lookup. ``accessories``
    (caps, lugs, TPMS, hardware) is universal by thread pitch. The whole feed is
    NOT_APPLICABLE, but per feed_type so the source string still says which.
    """
    if not isinstance(feed_type, str) or not feed_type.strip():
        return None
    normalized = feed_type.strip().lower()
    if normalized in ("wheel", "tire", "accessories"):
        return Verdict(NOT_APPLICABLE, "wheelpros:{}".format(normalized))
    return None


# ==========================================================================================
# Wheel- and tire-only catalogs. Each was confirmed single-purpose in the product_type work
# (see src.integrations.utils.product_type): Elite Wheel ships wheels and tires as two
# worksheets, The Wheel Group's mastersheet is wheels end to end, Vossen is a wheel brand whose
# only non-wheel rows are CAP-/LUG- accessories. None of it is YMM-fitted.
# ==========================================================================================
WHEEL_AND_TIRE_ONLY_PROVIDERS = {
    "Elite Wheel": "elite_wheel:catalog",
    "The Wheel Group": "the_wheel_group:catalog",
    "Vossen": "vossen:catalog",
    "TireRack": "tirerack:catalog",
}


def classify_provider_catalog(provider_name: typing.Any) -> typing.Optional[Verdict]:
    """
    Feeds whose entire catalog is one non-fitted product class. TireRack is included on the
    strength of its content, not its name -- it is a 198-brand mixed wheel/tire catalog (see
    ``product_type.classify_tirerack_description``), but every one of those brands sells the
    same two sized-not-fitted things.
    """
    source = WHEEL_AND_TIRE_ONLY_PROVIDERS.get(provider_name)
    if source is None:
        return None
    return Verdict(NOT_APPLICABLE, source)


# ==========================================================================================
# Resolution. Ordered by how much the signal actually knows, best first.
# ==========================================================================================
SOURCE_TIER = {
    "turn14": 1,       # a category on the feed that also ships the fitment
    "premier": 1,      # PCdb category
    "meyer": 1,        # Meyer's own category vocabulary, same granularity as PCdb's top level
    "wps": 2,          # a product_type vocabulary, but a non-VCdb vehicle universe
    "wheelpros": 2,
    "helmet_house": 2,
    "elite_wheel": 2,
    "the_wheel_group": 2,
    "vossen": 2,
    "tirerack": 2,
}

UNKNOWN_TIER = 99


def tier_for_source(source: str) -> int:
    return SOURCE_TIER.get(source.split(":", 1)[0], UNKNOWN_TIER)


def classify_turn14(category: typing.Any) -> typing.Optional[Verdict]:
    if not isinstance(category, str):
        return None
    entry = TURN14_CATEGORY_EXPECTATION.get(category.strip())
    if entry is None or entry.expectation is None:
        return None
    return Verdict(entry.expectation, "turn14:{}".format(category.strip()))


def classify_premier(part_category: typing.Any) -> typing.Optional[Verdict]:
    if not isinstance(part_category, str):
        return None
    expectation = PREMIER_CATEGORY_EXPECTATION.get(part_category.strip())
    if expectation is None:
        return None
    return Verdict(expectation, "premier:{}".format(part_category.strip()))


def classify_meyer(category: typing.Any) -> typing.Optional[Verdict]:
    """Single-valued categories only -- see the table's note on semicolon-joined values."""
    if not isinstance(category, str) or ";" in category:
        return None
    expectation = MEYER_CATEGORY_EXPECTATION.get(category.strip())
    if expectation is None:
        return None
    return Verdict(expectation, "meyer:{}".format(category.strip()))


def classify_wps(product_type: typing.Any) -> typing.Optional[Verdict]:
    if not isinstance(product_type, str):
        return None
    expectation = WPS_PRODUCT_TYPE_EXPECTATION.get(product_type.strip())
    if expectation is None:
        return None
    return Verdict(expectation, "wps:{}".format(product_type.strip()))


def resolve(verdicts: typing.Sequence[Verdict]) -> typing.Optional[Verdict]:
    """
    Pick one answer for a master part sold by several distributors.

    Same rule as the product_type resolver, and for the same reason: keep only the best tier
    available, and if the distributors on that tier disagree, return ``None``. A part one feed
    files under Suspension and another under Tire and Wheel is either miscategorised or two
    different parts merged onto one MasterPart, and both deserve a human look rather than a
    coin flip.
    """
    if not verdicts:
        return None
    best_tier = min(tier_for_source(v.source) for v in verdicts)
    at_best = [v for v in verdicts if tier_for_source(v.source) == best_tier]
    expectations = {v.expectation for v in at_best}
    if len(expectations) != 1:
        return None
    return at_best[0]
