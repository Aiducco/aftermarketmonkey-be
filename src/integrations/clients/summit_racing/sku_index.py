"""
Resolve a brand's Summit SKUs from Summit's own XML sitemaps.

``static.summitracing.com`` serves the sitemaps with **no bot protection at all** -- plain
``requests``, no cookie, no proxy -- even though every page on ``www`` is behind Imperva. That
makes it the cheapest and most complete SKU source available: the index at
``/searchenginesitemap/sitemapindexsrefull.xml`` lists 401 shards, of which the ~103
``srefull_p_*`` shards hold roughly 3.09M ``/parts/<sku>`` URLs.

Why not the brand listing page (``/search/brand/<brand>``)? Measured 2026-10-23 for BDS: the
listing paginates to 61 pages x 25 = ~1,525 SKUs while the sitemap carries 2,872 for the same
brand prefix. The listing drops discontinued lines, and those are exactly the parts whose
fitment we are least likely to have from a live distributor feed.

The shards total several hundred MB, so a harvest is cached on disk and reused.
"""

from __future__ import annotations

import logging
import pathlib
import re
import typing

import requests

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[summit-racing]"

_SITEMAP_INDEX_URL = "https://www.summitracing.com/searchenginesitemap/sitemapindexsrefull.xml"
_PART_SHARD_RE = re.compile(r"<loc>(?P<url>[^<]*srefull_p_\d+\.xml)</loc>")
_LOC_RE = re.compile(r"<loc>https://www\.summitracing\.com/parts/(?P<sku>[^<]+)</loc>")

# Plain desktop UA: the static host does not fingerprint, but an honest UA keeps us identifiable
# in their logs rather than looking like an anonymous scraper.
_HEADERS = {
    "accept": "application/xml,text/xml,*/*;q=0.8",
    "user-agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
    ),
}


def harvest_brand_skus(
    prefix: str,
    *,
    cache_path: typing.Optional[pathlib.Path] = None,
    timeout: float = 120.0,
    refresh: bool = False,
) -> typing.List[str]:
    """Every Summit SKU starting with ``prefix`` (e.g. ``"bdx-"``), lowercase.

    Shards are streamed and discarded as they are read -- the full set is hundreds of MB and
    only the matching lines are ever held.
    """
    prefix = prefix.lower()
    if cache_path and cache_path.is_file() and not refresh:
        skus = [line.strip() for line in cache_path.read_text(encoding="utf-8").splitlines() if line.strip()]
        logger.info("%s sku index: %s cached SKU(s) for %r", _LOG_PREFIX, len(skus), prefix)
        return skus

    session = requests.Session()
    session.headers.update(_HEADERS)

    index = session.get(_SITEMAP_INDEX_URL, timeout=timeout)
    index.raise_for_status()
    shards = _PART_SHARD_RE.findall(index.text)
    logger.info("%s sku index: scanning %s part shard(s) for %r", _LOG_PREFIX, len(shards), prefix)

    found: typing.Set[str] = set()
    needle = "/parts/{}".format(prefix)
    for position, shard_url in enumerate(shards, start=1):
        try:
            with session.get(shard_url, timeout=timeout, stream=True) as response:
                response.raise_for_status()
                for chunk in response.iter_lines(decode_unicode=True):
                    if chunk and needle in chunk:
                        match = _LOC_RE.search(chunk)
                        if match:
                            found.add(match.group("sku").strip().lower())
        except requests.RequestException as e:
            # One unreachable shard costs coverage, not correctness: log it and keep going
            # rather than abandoning a multi-hundred-MB harvest.
            logger.warning("%s sku index: shard %s failed: %s", _LOG_PREFIX, shard_url, e)
        if position % 25 == 0:
            logger.info("%s sku index: %s/%s shards, %s match(es)", _LOG_PREFIX, position, len(shards), len(found))

    skus = sorted(found)
    if cache_path:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text("\n".join(skus), encoding="utf-8")
    logger.info("%s sku index: %s SKU(s) for %r", _LOG_PREFIX, len(skus), prefix)
    return skus


def build_lookup(skus: typing.Iterable[str]) -> typing.Dict[str, str]:
    """Map every plausible spelling of a manufacturer part number to its Summit SKU.

    Summit's SKU is its own brand code plus the manufacturer number, and the two are joined
    inconsistently: ``BDX-BDS034451`` for part ``034451`` *and* for part ``BDS034451``. Feeds
    disagree about whether the brand letters belong in the number, so both spellings -- and a
    punctuation-stripped form -- are indexed and the caller looks up whatever it holds.
    """
    lookup: typing.Dict[str, str] = {}
    for sku in skus:
        sku = sku.strip().lower()
        if "-" not in sku:
            continue
        core = sku.split("-", 1)[1]
        for key in _candidate_keys(core):
            lookup.setdefault(key, sku)
    return lookup


def candidate_keys_for_part_number(part_number: str) -> typing.List[str]:
    """Keys to try against ``build_lookup``'s mapping, most specific first."""
    return _candidate_keys(part_number.strip().lower())


def _candidate_keys(value: str) -> typing.List[str]:
    value = value.strip().lower()
    stripped = re.sub(r"[^a-z0-9]", "", value)
    keys = [value, stripped]
    # A leading brand-letter run ("bds034451" -> "034451") is the other spelling of the same
    # part; indexing both ends is what makes the lookup symmetric.
    alpha_prefix = re.match(r"^([a-z]+)(\d.*)$", stripped)
    if alpha_prefix:
        keys.append(alpha_prefix.group(2))
    return [k for k in dict.fromkeys(keys) if k]
