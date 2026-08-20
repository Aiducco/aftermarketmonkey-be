"""
Cross-distributor part-number and GTIN normalization.

Distributors do not agree on how to spell a manufacturer part number, and every provider ingest
in ``master_parts.py`` resolves existing rows with an exact string match on
``(brand_id, part_number)``. The result is one ``MasterPart`` per spelling of the same physical
part, each holding a disjoint subset of ``ProviderPart`` rows -- so the catalog shows the part
twice and price comparison only ever sees a subset of the sources. Verified against the raw feed
tables, this is the distributors' own data, not something our ingest introduces::

    Brembo rotor   A-Tech '09-5843-11'   Meyer '09.5843.11'   Keystone '09584311C02'
    FEL-PRO gasket Keystone 'MS 96587'   Turn14/Meyer/A-Tech 'MS96587'

See ``docs/PART_NUMBER_NORMALIZATION.md`` for the full survey of production data.

Everything here is a pure function so the matching rules can be reasoned about (and re-run over a
CSV dump) without touching the database. The consumers are the provider ingests in
``master_parts.py`` and ``scripts/merge_normalized_part_number_duplicates.py``.

**Normalization is deliberately not enough on its own to merge two rows.** Punctuation is
load-bearing in this domain -- ``+``/``-`` encode wheel offset and bearing sizing, so
``942B-89060+12`` and ``942B-89060-12`` are different parts that normalize identically. Callers
must apply the corroborating guards (see ``has_sign_conflict`` and the provider-disjointness /
GTIN checks in the merge script) before treating a normalized-key match as the same part.
"""
import re
import typing

_NON_ALNUM_RE = re.compile(r"[^A-Z0-9]")
_NON_PRINTABLE_RE = re.compile(r"[^\x20-\x7E]")
_NON_DIGIT_RE = re.compile(r"\D")

# Feed values that mean "no barcode" rather than a barcode. Compared after upper+strip.
_GTIN_PLACEHOLDERS = frozenset({"NA", "N/A", "NONE", "NULL", "-", "0", "00", "000000000000"})

# A GTIN carries at most 14 digits. The lower bound is a guard against short junk lucking its way
# past the check digit once zero-padded -- Walbro ships gtin='3926', which validates as
# '00000000003926' and would otherwise collide with anything else that has a bogus short value.
_GTIN_MIN_SIGNIFICANT_DIGITS = 11
_GTIN_LENGTH = 14


def strip_non_printable(value: typing.Optional[str]) -> str:
    """
    Drop non-printable/non-ASCII characters and surrounding whitespace.

    Premier ships mojibake in a handful of part numbers (``'PP-HG-6.4+010\xff \xff'``), which
    creates duplicate rows within that one provider. Only ~31 rows repo-wide are affected, but
    they are free to clean at ingest time.
    """
    if not value:
        return ""
    return _NON_PRINTABLE_RE.sub("", value).strip()


def normalize_part_number(part_number: typing.Optional[str]) -> str:
    """
    Uppercase and strip everything that is not a letter or digit.

    This is a *match candidate* key, not an identity: distinct parts can share one (see the
    module docstring). Returns ``""`` when nothing survives, which callers must treat as
    "not matchable" rather than as a key.

        'MS 96587'   -> 'MS96587'
        '09.5843.11' -> '09584311'
        '942B-89060+12' -> '942B8906012'   # same key as the '-12' part: needs has_sign_conflict
    """
    return _NON_ALNUM_RE.sub("", strip_non_printable(part_number).upper())


def _gs1_check_digit(body_13: str) -> str:
    """GS1 mod-10 check digit for the first 13 digits of a GTIN-14."""
    total = sum(int(ch) * (3 if i % 2 == 0 else 1) for i, ch in enumerate(body_13))
    return str((10 - total % 10) % 10)


def _has_valid_check_digit(gtin_14: str) -> bool:
    return _gs1_check_digit(gtin_14[:13]) == gtin_14[13]


def normalize_gtin(raw: typing.Optional[str]) -> typing.Optional[str]:
    """
    Return a validated 14-digit GTIN, or ``None`` when the value is unusable.

    ``master_parts.gtin`` is dirty in several distinct ways, all of which are handled here so the
    same physical barcode compares equal across providers:

    - zero-padding differences (``'00787765331678'`` vs ``'787765331678'``) -- normalized to 14
    - float artifacts from CSV parsing (``'840269908767.0'``)
    - placeholder junk (``'NA'``, ``'N/A'``, ...) -- rejected
    - **check digit dropped by the feed**: Dynamat ``XFOM3U`` arrives as ``769103920355`` from
      A-Tech and ``76910392035`` from Meyer, the same barcode minus its check digit. When the
      value fails validation, the check digit is recomputed from the body and re-validated;
      only a value that then checks out is accepted.

    Anything that still fails the GS1 check digit is rejected rather than returned unvalidated --
    a wrong GTIN silently merges two different parts, which is the one outcome worth being
    strict about.
    """
    if not raw:
        return None
    value = strip_non_printable(raw).upper()
    if value in _GTIN_PLACEHOLDERS:
        return None
    if value.endswith(".0"):
        value = value[:-2]
    digits = _NON_DIGIT_RE.sub("", value).lstrip("0")
    if not _GTIN_MIN_SIGNIFICANT_DIGITS <= len(digits) <= _GTIN_LENGTH:
        return None

    candidate = digits.zfill(_GTIN_LENGTH)
    if _has_valid_check_digit(candidate):
        return candidate

    # Feed stored the barcode without its check digit -> recompute and re-validate.
    if len(digits) <= _GTIN_LENGTH - 1:
        body = digits.zfill(_GTIN_LENGTH - 1)
        repaired = body + _gs1_check_digit(body)
        if _has_valid_check_digit(repaired):
            return repaired
    return None


def sign_signature(part_number: typing.Optional[str]) -> typing.Tuple[typing.Tuple[int, str], ...]:
    """
    Positions of ``+``/``-`` within a part number, measured in alphanumeric characters seen so
    far. Lets two spellings be compared for a sign disagreement at the *same* logical position
    even when they differ in other punctuation.

        'M1471880F8+42' -> ((10, '+'),)
        '942B-89060+12' -> ((4, '-'), (9, '+'))
    """
    signature: typing.List[typing.Tuple[int, str]] = []
    alnum_seen = 0
    for ch in strip_non_printable(part_number).upper():
        if ch.isalnum():
            alnum_seen += 1
        elif ch in "+-":
            signature.append((alnum_seen, ch))
    return tuple(signature)


def has_sign_conflict(part_numbers: typing.Iterable[str]) -> bool:
    """
    True when two spellings place a *different* sign at the same alphanumeric offset -- i.e. the
    group contains both a ``+N`` and a ``-N`` variant and normalization would merge parts that
    the sign deliberately distinguishes.

    This is what separates the two cases that look identical as strings::

        SAFE   'M1471880F842' (A-Tech, + stripped) vs 'M1471880F8+42' (TireRack)
        UNSAFE 'XBCPXL+5' vs 'XBCPXL-5'   -- Simpson sells both, they are different parts

    Only 41 production groups trip this, and they are exactly the ones that must not be merged.
    """
    signs_at: typing.Dict[int, typing.Set[str]] = {}
    for part_number in part_numbers:
        for position, sign in sign_signature(part_number):
            signs_at.setdefault(position, set()).add(sign)
    return any(len(signs) > 1 for signs in signs_at.values())


# Difference tiers, ordered by how much semantic risk normalizing away the difference carries.
# Callers gate on these: T1/T2 never change meaning, T3/T4 need corroboration.
TIER_CASE_ONLY = "T1_case_only"
TIER_WHITESPACE_ONLY = "T2_whitespace_only"
TIER_HYPHEN_DOT = "T3_hyphen_dot"
TIER_OTHER_PUNCTUATION = "T4_other_punctuation"

# Characters whose presence/absence is (empirically) never meaningful on its own -- as opposed to
# '+', '/', '_', which do carry meaning in wheel and bearing part numbers.
_LOW_RISK_PUNCTUATION = frozenset("-. ")


# --------------------------------------------------------------------------------------------
# Evidence grading for rows that share a brand and a validated GTIN
#
# The part-number tiers above cannot relate two spellings that are not string-similar at all --
# Premier ships Toyo's '357280' as 'TOY357280'; A-Tech numbers ~41,000 rows 'S0845'-style because
# its feed has no manufacturer part number for them (confirmed: its mfr_part_number column repeats
# the same placeholder). Only a shared barcode links those.
#
# A shared barcode alone is NOT enough: in production 7,185 groups have the same distributor on
# both sides under different SKUs -- genuinely two products -- and a single wrong barcode in one
# feed would silently merge unrelated parts. So groups are graded by whether anything *else*
# corroborates, and callers decide how weak a grade they will act on.
#
# Used by both master_part_matching (at ingest) and the cleanup script, so the two can never
# disagree about what counts as evidence.
# --------------------------------------------------------------------------------------------

GTIN_EVIDENCE_SKU_BRIDGE = 1      # one row's sku IS the other's part number -- two signals
GTIN_EVIDENCE_PLACEHOLDER = 2     # 'S####' is a catalog artifact, not a competing product
GTIN_EVIDENCE_LEADING_ZERO = 3    # '010140' vs '10140'
GTIN_EVIDENCE_PREFIX_SUFFIX = 4   # 'TOY357280' vs '357280' -- substring plus barcode
GTIN_EVIDENCE_UNRELATED = 5       # the barcode is the ONLY link

GTIN_EVIDENCE_LABELS = {
    GTIN_EVIDENCE_SKU_BRIDGE: "sku bridges the two spellings",
    GTIN_EVIDENCE_PLACEHOLDER: "placeholder part number vs real part",
    GTIN_EVIDENCE_LEADING_ZERO: "leading zeros differ",
    GTIN_EVIDENCE_PREFIX_SUFFIX: "brand prefix / suffix",
    GTIN_EVIDENCE_UNRELATED: "unrelated part numbers (barcode alone)",
}

# A-Tech's placeholder shape. Deliberately strict: a real manufacturer part number of this exact
# form would be unusual, and being wrong here means merging two real products.
_PLACEHOLDER_PART_NUMBER_RE = re.compile(r"^S\d{4}$")


def is_placeholder_part_number(part_number: typing.Optional[str]) -> bool:
    """True for catalog-artifact part numbers such as A-Tech's ``S0845``."""
    return bool(_PLACEHOLDER_PART_NUMBER_RE.match(strip_non_printable(part_number).upper()))


def classify_gtin_evidence(
    entries: typing.Sequence[typing.Tuple[str, typing.Optional[str]]],
) -> int:
    """
    Grade what corroborates a shared barcode, given ``(part_number, sku)`` for each row.

    Returns one of the ``GTIN_EVIDENCE_*`` constants, lowest (strongest) that applies. Callers
    gate on it: accepting up to ``GTIN_EVIDENCE_PREFIX_SUFFIX`` acts only where something beyond
    the barcode agrees, while ``GTIN_EVIDENCE_UNRELATED`` should normally be refused.
    """
    part_numbers = [p for p, _ in entries]
    keys = {normalize_part_number(p) for p in part_numbers}

    for part_number, sku in entries:
        sku_key = normalize_part_number(sku or "")
        if not sku_key:
            continue
        if any(sku_key == normalize_part_number(other) and other != part_number
               for other in part_numbers):
            return GTIN_EVIDENCE_SKU_BRIDGE

    if any(is_placeholder_part_number(p) for p in part_numbers):
        return GTIN_EVIDENCE_PLACEHOLDER

    populated = sorted((k for k in keys if k), key=len)
    if len(populated) >= 2:
        shortest, longest = populated[0], populated[-1]
        if shortest != longest:
            if shortest.lstrip("0") == longest.lstrip("0"):
                return GTIN_EVIDENCE_LEADING_ZERO
            if longest.endswith(shortest) or longest.startswith(shortest):
                return GTIN_EVIDENCE_PREFIX_SUFFIX
    return GTIN_EVIDENCE_UNRELATED


def classify_tier(part_numbers: typing.Iterable[str]) -> str:
    """
    Classify what actually differs between spellings that share a normalized key.

    Case and whitespace differences (T1/T2) cannot change which physical part is meant, so they
    are safe to merge on weaker evidence. Hyphen/dot differences (T3) are usually formatting but
    occasionally meaningful. Anything else (T4) includes ``+``, which encodes wheel offset.
    """
    uppercased = {strip_non_printable(pn).upper() for pn in part_numbers}
    if len(uppercased) == 1:
        return TIER_CASE_ONLY
    if len({"".join(u.split()) for u in uppercased}) == 1:
        return TIER_WHITESPACE_ONLY
    punctuation = set()
    for value in uppercased:
        punctuation |= {ch for ch in value if not ch.isalnum()}
    if punctuation <= _LOW_RISK_PUNCTUATION:
        return TIER_HYPHEN_DOT
    return TIER_OTHER_PUNCTUATION
