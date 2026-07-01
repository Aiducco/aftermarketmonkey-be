"""
Fuzzy matching of distributor/vendor brand names to `Brands` rows (word-prefix alignment).
Used by Keystone, Rough Country, Wheel Pros, Meyer, DLG, A-Tech, and other provider syncs.
"""
import re
import typing

from src import models as src_models

_NON_ALNUM_RE = re.compile(r"[^A-Z0-9]")


def normalize_upper_words(name: str) -> str:
    return " ".join((name or "").strip().upper().split())


def normalize_compact_key(name: str) -> str:
    """
    Strip everything but letters/digits (spaces, hyphens, periods, apostrophes, ampersands, ...).

    Word-prefix fuzzy matching compares whole tokens, so it never catches purely
    tokenization-shape differences like 'AUTOMETER' vs 'AUTO METER' or 'WD40' vs 'WD-40' -
    those need a punctuation/spacing-insensitive identity key instead.
    """
    return _NON_ALNUM_RE.sub("", (name or "").upper())


def brands_by_compact_key() -> typing.Dict[str, src_models.Brands]:
    """Index brands by compact key (first Brands row by id wins on collision)."""
    idx: typing.Dict[str, src_models.Brands] = {}
    for b in src_models.Brands.objects.only("id", "name", "aaia_code").order_by("id").iterator(
        chunk_size=2000
    ):
        key = normalize_compact_key(b.name or "")
        if key and key not in idx:
            idx[key] = b
    return idx


def word_prefix_align(shorter: typing.List[str], longer: typing.List[str]) -> bool:
    """
    Align the first len(shorter) words of `longer` with `shorter` (1:1).

    Each pair is a match if equal, or if the longer word starts with the shorter word and
    the shorter token has length >= 3 (abbrev/truncation).
    """
    if len(shorter) > len(longer):
        return False
    for i in range(len(shorter)):
        a, c = shorter[i], longer[i]
        if a == c:
            continue
        if len(a) < 3:
            return False
        if not c.startswith(a):
            return False
    return True


def fuzzy_brand_name_matches(source_name: str, brand_name: str) -> bool:
    """
    True when the source vendor name and the `brands` row name are the same chain with optional
    truncation on **either** side (extra trailing words on the other).

    - DB shorter: BAK IND vs BAK INDUSTRIES (each DB word equals or is a prefix of the source word).
    - DB longer: DIRTY LIFE vs DIRTY LIFE WHEELS (each source word equals or is a prefix of the DB word).
    Non-exact prefix pairs require the shorter token of the two to be at least 3 characters.
    """
    sn = normalize_upper_words(source_name)
    bn = normalize_upper_words(brand_name)
    if not sn or not bn:
        return False
    if sn == bn:
        return True
    s_parts = sn.split()
    b_parts = bn.split()
    if not s_parts or not b_parts:
        return False
    if len(b_parts) <= len(s_parts):
        return word_prefix_align(b_parts, s_parts)
    return word_prefix_align(s_parts, b_parts)


def best_fuzzy_brand_match(
    source_name: str,
    candidate_brands: typing.Iterable[src_models.Brands],
) -> typing.Optional[src_models.Brands]:
    """Among candidates, pick the longest `Brands.name` that fuzzy-matches source_name."""
    matches = [
        b for b in candidate_brands if fuzzy_brand_name_matches(source_name, b.name or "")
    ]
    if not matches:
        return None
    return max(matches, key=lambda b: len((b.name or "")))


def brands_by_first_token_upper() -> typing.Dict[str, typing.List[src_models.Brands]]:
    """Index brands by first uppercase word for fast fuzzy candidate lists."""
    idx: typing.Dict[str, typing.List[src_models.Brands]] = {}
    for b in src_models.Brands.objects.only("id", "name", "aaia_code").order_by("id").iterator(
        chunk_size=2000
    ):
        parts = normalize_upper_words(b.name or "").split()
        if not parts:
            continue
        idx.setdefault(parts[0], []).append(b)
    return idx
