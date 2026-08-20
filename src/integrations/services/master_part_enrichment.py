"""
Fill enrichment gaps on parts we already ingested -- ``MasterPartData`` plus the fill-if-blank
core fields on ``MasterPart`` -- from a distributor feed already sitting in our raw tables.

``MasterPartData`` is written today only by ASAP Network (paid, billed per call, run once per
brand): 13.3k rows against 3.19M master parts. Turn14 already ships the same kind of payload for
every part it carries -- 94% of Turn14-backed master parts have a ``turn14_brand_data`` row and
79% of those carry at least one image -- but the master parts sync only lifts ``description`` /
``thumbnail`` / ``barcode`` off ``Turn14Items`` and never touches the media feed at all.

**The output shape deliberately copies ASAP's**, because the FE renders both through the same
component. ASAP's ``product_description`` arrives from their API as a ready-made HTML blob
(``<p>prose</p><p><strong>Features and Benefits:</strong></p><ul>...``) and ``asap.py`` stores it
verbatim; Turn14 gives us the same content as typed rows, so here we compose the equivalent HTML
ourselves -- same section names, same order, no ``Overview:`` header (ASAP opens with bare
prose), and every value HTML-escaped, which the BigCommerce composers this borrows from don't do.

Writes are **fill-if-blank** per field, exactly like ``asap._patch_master_part_core_fields`` /
``_upsert_master_part_data``: a value another distributor already supplied is never clobbered
unless the caller passes ``overwrite=True``. That makes the whole thing safe to run repeatedly
and safe to interleave with the nightly ``ingest_all_providers`` catalog sync.

Adding a second source means implementing ``_Source`` (candidate SQL + payload builder) and
registering it in ``SOURCES``; everything downstream -- gap selection, merging, batched writes,
audit, Meilisearch handoff -- is source-agnostic.
"""
import dataclasses
import html as html_lib
import logging
import typing
from urllib.parse import urlparse

from django.db import connection
from django.utils import timezone

import pgbulk

from src import enums as src_enums
from src import models as src_models

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[MASTER-PART-ENRICHMENT]"

BATCH_SIZE = 2000

# ``MasterPartData`` columns this module knows how to fill.
DATA_FIELDS = (
    "images",
    "description",
    "installation_instructions",
    "youtube_video",
    "field_specs",
    # No source fills MasterPartData.material / .vehicle_type, so they are absent here on
    # purpose: ASAP remains their only writer.
    "color",
    "series",
    "warranty",
)
# ``MasterPart`` columns, prefixed so a --fields list can't confuse MasterPart.description
# (plain short text, a Meilisearch searchable attribute) with MasterPartData.description
# (the rich HTML blob).
CORE_FIELDS = (
    "core_image_url",
    "core_description",
    "core_gtin",
)
ALL_FIELDS = DATA_FIELDS + CORE_FIELDS

_CORE_FIELD_TO_COLUMN = {
    "core_image_url": "image_url",
    "core_description": "description",
    "core_gtin": "gtin",
}

# Which parts a run considers. Keyset pagination over provider_parts stays identical across all
# of them; only the EXISTS clause changes.
MODE_MISSING_ROW = "missing-row"      # no master_part_data row at all -- 790k of 792k Turn14 parts
MODE_BLANK_FIELDS = "blank-fields"    # row exists, at least one selected field still blank
MODE_STALE = "stale"                  # source refreshed its payload after we last wrote ours
MODE_ALL = "all"
MODES = (MODE_MISSING_ROW, MODE_BLANK_FIELDS, MODE_STALE, MODE_ALL)

# A Turn14 part can carry a few dozen media files and (via VCdb) hundreds of vehicle fitments.
# Both lists are for human consumption, so cap them rather than shipping a 500-row <ul>.
MAX_IMAGES = 25
MAX_APPLICATIONS = 100


@dataclasses.dataclass
class EnrichmentPayload:
    """Everything one source can offer for one master part. ``None`` means "source has nothing"."""
    images: typing.Optional[typing.List[str]] = None
    description: typing.Optional[str] = None
    installation_instructions: typing.Optional[typing.List[str]] = None
    youtube_video: typing.Optional[str] = None
    field_specs: typing.Optional[typing.List[typing.Dict[str, str]]] = None
    color: typing.Optional[str] = None
    series: typing.Optional[str] = None
    warranty: typing.Optional[str] = None
    core_image_url: typing.Optional[str] = None
    core_description: typing.Optional[str] = None
    core_gtin: typing.Optional[str] = None
    source_external_id: typing.Optional[str] = None


@dataclasses.dataclass
class EnrichmentStats:
    candidates: int = 0
    payloads: int = 0
    data_rows_written: int = 0
    core_rows_written: int = 0
    field_writes: typing.Dict[str, int] = dataclasses.field(default_factory=dict)
    last_provider_part_id: int = 0
    touched_master_part_ids: typing.List[int] = dataclasses.field(default_factory=list)

    def bump(self, field: str) -> None:
        self.field_writes[field] = self.field_writes.get(field, 0) + 1

    def as_message(self) -> str:
        writes = ", ".join(
            "{}={}".format(k, self.field_writes[k]) for k in sorted(self.field_writes)
        ) or "none"
        return (
            "candidates={} payloads={} master_part_data_rows={} master_part_rows={} "
            "field_writes[{}] last_provider_part_id={}".format(
                self.candidates,
                self.payloads,
                self.data_rows_written,
                self.core_rows_written,
                writes,
                self.last_provider_part_id,
            )
        )


def _is_blank(value: typing.Any) -> bool:
    """Same emptiness rule as ``asap._is_blank`` -- an empty list/dict counts as blank."""
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    if isinstance(value, (list, dict)):
        return len(value) == 0
    return False


# ---------------------------------------------------------------------------
# Turn14 source
# ---------------------------------------------------------------------------

# ``files[].type`` == 'Image' covers everything renderable, but 'Logo Image' is the brand mark,
# never the product, and it shows up on ~6k rows.
_EXCLUDED_IMAGE_MEDIA_CONTENT = frozenset({"Logo Image"})
# Order images the way a PDP wants them. Anything not listed keeps feed order behind these.
_IMAGE_MEDIA_CONTENT_PRIORITY = (
    "Photo - Primary",
    "Photo - out of package",
    "Photo - Unmounted",
    "Photo - Mounted",
    "Photo - in package",
    "Photo - Close Up",
    "Photo - lifestyle view",
)
_VALID_IMAGE_EXTENSIONS = frozenset({
    "png", "jpg", "jpeg", "gif", "webp", "bmp", "svg",
})
_INSTRUCTION_MEDIA_CONTENT = ("Installation Instructions", "Illustration Guide")
_YOUTUBE_HOSTS = ("youtube.com", "www.youtube.com", "youtu.be", "m.youtube.com")

# Prose that opens the description, best first.
_PROSE_DESCRIPTION_TYPES = (
    "Market Description",
    "Product Description - Extended",
    "Product Description - Long",
)
# Turn14 has no single "fitment notes" type; these two are what its feed actually uses for
# application caveats, and both land in ASAP's "Fitment Notes:" section.
_FITMENT_NOTE_DESCRIPTION_TYPES = ("Application Summary", "Associated Comments")
# Short text for MasterPart.description (plain, searchable), not the HTML blob.
_SHORT_DESCRIPTION_TYPES = ("Product Description - Short", "Product Description - Long")


def _as_list(value: typing.Any) -> typing.List[typing.Any]:
    return value if isinstance(value, list) else []


def _first_link_url(entry: typing.Dict) -> typing.Optional[str]:
    """Every Turn14 file entry observed in production carries exactly one link."""
    for link in _as_list(entry.get("links")):
        if not isinstance(link, dict):
            continue
        url = (link.get("url") or "").strip()
        if url:
            return url
    return None


def _is_http_url(url: str) -> bool:
    return url.lower().startswith(("http://", "https://"))


def _has_image_extension(url: str) -> bool:
    path = urlparse(url).path
    if "." not in path:
        return False
    return path.rsplit(".", 1)[-1].lower() in _VALID_IMAGE_EXTENSIONS


def _turn14_image_urls(files: typing.Any) -> typing.List[str]:
    """
    Product images in PDP order, deduped by URL.

    ``generic: true`` means the asset is shared across a family of SKUs rather than shot for this
    one -- in a sampled brand, 5,100 generic image rows resolved to 292 distinct URLs against
    5,068 distinct URLs for the same count of non-generic rows. It does **not** mean "not this
    product": Turn14's own ``Turn14Items.thumbnail`` is frequently the generic 'Photo - Primary'
    of the family, at the S size. So generic images stay in the gallery and only lose ties within
    the same ``media_content`` -- a generic 'Photo - Primary' still outranks a part-specific
    close-up, because it is the shot that belongs at the front.
    """
    ranked: typing.List[typing.Tuple[int, int, int, str]] = []
    seen: typing.Set[str] = set()
    for position, entry in enumerate(_as_list(files)):
        if not isinstance(entry, dict) or entry.get("type") != "Image":
            continue
        media_content = entry.get("media_content") or ""
        if media_content in _EXCLUDED_IMAGE_MEDIA_CONTENT:
            continue
        url = _first_link_url(entry)
        if not url or not _is_http_url(url) or not _has_image_extension(url):
            continue
        if url in seen:
            continue
        seen.add(url)
        generic_rank = 1 if entry.get("generic") else 0
        try:
            content_rank = _IMAGE_MEDIA_CONTENT_PRIORITY.index(media_content)
        except ValueError:
            content_rank = len(_IMAGE_MEDIA_CONTENT_PRIORITY)
        ranked.append((content_rank, generic_rank, position, url))

    ranked.sort()
    return [url for _c, _g, _p, url in ranked[:MAX_IMAGES]]


def _turn14_document_urls(files: typing.Any, media_contents: typing.Sequence[str]) -> typing.List[str]:
    urls: typing.List[str] = []
    seen: typing.Set[str] = set()
    for wanted in media_contents:
        for entry in _as_list(files):
            if not isinstance(entry, dict) or entry.get("media_content") != wanted:
                continue
            url = _first_link_url(entry)
            if not url or not _is_http_url(url) or url in seen:
                continue
            seen.add(url)
            urls.append(url)
    return urls


def _is_youtube_video_url(url: str) -> bool:
    """
    A *single video*, not a channel. ``MasterPartData.youtube_video`` is rendered as an embed, so
    a ``/channel/UC...`` or ``/user/...`` link -- 204 of the first 507 Turn14 Video links -- is
    the wrong kind of URL even though the host matches.
    """
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    if host not in _YOUTUBE_HOSTS:
        return False
    path = parsed.path or ""
    if host == "youtu.be":
        return len(path.strip("/")) > 0
    if path.startswith(("/embed/", "/shorts/", "/v/")):
        return len(path.rstrip("/").rsplit("/", 1)[-1]) > 0
    return path.rstrip("/") == "/watch" and "v=" in (parsed.query or "")


def _turn14_youtube_url(files: typing.Any) -> typing.Optional[str]:
    """
    ``MasterPartData.youtube_video`` holds a YouTube video URL (that is what ASAP puts there).
    Turn14's Video files are mostly raw CloudFront ``.MP4`` assets or brand channel links --
    neither is that.
    """
    for entry in _as_list(files):
        if not isinstance(entry, dict) or entry.get("type") != "Video":
            continue
        url = _first_link_url(entry)
        if url and _is_http_url(url) and _is_youtube_video_url(url):
            return url
    return None


def _turn14_descriptions_by_type(descriptions: typing.Any) -> typing.Dict[str, typing.List[str]]:
    by_type: typing.Dict[str, typing.List[str]] = {}
    for entry in _as_list(descriptions):
        if not isinstance(entry, dict):
            continue
        desc_type = entry.get("type")
        text = entry.get("description")
        if not desc_type or not isinstance(text, str):
            continue
        text = text.strip()
        if not text:
            continue
        by_type.setdefault(desc_type, []).append(text)
    return by_type


def _first_of_types(
    by_type: typing.Dict[str, typing.List[str]],
    types: typing.Sequence[str],
) -> typing.Optional[str]:
    for desc_type in types:
        for text in by_type.get(desc_type, []):
            if text:
                return text
    return None


def _split_semicolon_items(values: typing.Iterable[str]) -> typing.List[str]:
    """
    Turn14 packs several notes into one 'Associated Comments' row separated by ';'
    (same handling the BigCommerce composer applies).
    """
    items: typing.List[str] = []
    for value in values:
        parts = value.split(";") if ";" in value else [value]
        for part in parts:
            part = part.strip()
            if part and part not in items:
                items.append(part)
    return items


def _normalized_text(value: str) -> str:
    return " ".join(value.split()).casefold()


def _drop_prose_duplicates(
    items: typing.Sequence[str],
    prose: typing.Optional[str],
) -> typing.List[str]:
    """
    Turn14 routinely repeats the whole Market Description inside 'Associated Comments', which
    would render the marketing blurb twice -- once as the opening prose and again as a Fitment
    Notes bullet. Drop notes that the prose already says.
    """
    if not prose:
        return list(items)
    normalized_prose = _normalized_text(prose)
    kept = []
    for item in items:
        normalized_item = _normalized_text(item)
        if normalized_item and normalized_item not in normalized_prose:
            kept.append(item)
    return kept


def _turn14_field_specs(item: typing.Optional[typing.Dict]) -> typing.List[typing.Dict[str, str]]:
    """
    ``[{spec_name, spec_value}]`` -- ASAP's shape. Turn14's media feed has no spec array, so
    these come off the item row itself. 'Unknown' / 'N/A' placeholders are dropped rather than
    rendered as a spec that says nothing.
    """
    if not item:
        return []

    specs: typing.List[typing.Dict[str, str]] = []

    def add(name: str, value: typing.Any) -> None:
        if value is None:
            return
        text = str(value).strip()
        if text:
            specs.append({"spec_name": name, "spec_value": text})

    add("UPC", item.get("barcode"))

    prop_65 = (item.get("prop_65") or "").strip().upper()
    if prop_65 == "Y":
        add("Prop 65 Warning", "Yes")
    elif prop_65 == "N":
        add("Prop 65 Warning", "No")

    epa = (item.get("epa") or "").strip()
    if epa and epa.upper() != "N/A":
        add("EPA", epa)

    units_per_sku = item.get("units_per_sku")
    if units_per_sku:
        add("Units Per SKU", units_per_sku)

    if item.get("ltl_freight_required"):
        add("Shipping", "LTL Freight Required")

    boxes = [b for b in _as_list(item.get("dimensions")) if isinstance(b, dict)]
    if len(boxes) == 1:
        box = boxes[0]
        add("Length", box.get("length"))
        add("Width", box.get("width"))
        add("Height", box.get("height"))
        add("Weight", box.get("weight"))
    elif len(boxes) > 1:
        add("Boxes", len(boxes))
        total_weight = sum(b.get("weight") or 0 for b in boxes)
        if total_weight:
            add("Total Weight", total_weight)

    return specs


def _esc(value: typing.Any) -> str:
    return html_lib.escape(str(value), quote=False)


def _html_list_section(title: str, items: typing.Sequence[str]) -> str:
    if not items:
        return ""
    lis = "".join("<li>{}</li>".format(_esc(item)) for item in items)
    return "<p><strong>{}:</strong></p><ul>{}</ul>".format(_esc(title), lis)


def _format_application(fitment: typing.Dict) -> str:
    year_start = fitment.get("year_start")
    year_end = fitment.get("year_end")
    years = str(year_start) if year_start == year_end else "{}-{}".format(year_start, year_end)
    parts = [years, fitment.get("make") or "", fitment.get("model") or "", fitment.get("submodel") or ""]
    return " ".join(p for p in (str(x).strip() for x in parts) if p)


def compose_description_html(
    *,
    prose: typing.Optional[str],
    features: typing.Sequence[str] = (),
    fitment_notes: typing.Sequence[str] = (),
    field_specs: typing.Sequence[typing.Dict[str, str]] = (),
    applications: typing.Sequence[typing.Dict] = (),
    applications_total: typing.Optional[int] = None,
) -> str:
    """
    ASAP's blob, rebuilt from whatever typed pieces a source can supply: bare prose, then
    Features and Benefits, Fitment Notes, Details, Applications -- same section names, same
    order, no 'Overview:' header, every value escaped. Shared by every source so the FE renders
    all of them through the one component.

    Returns "" unless the part has something to actually say. Details is built from box
    dimensions and shipping flags, so a Details-only blob would be a "description" reading
    'Length: 27, Width: 11.5' -- worse on the PDP than leaving the field empty.
    """
    fitment_notes = _drop_prose_duplicates(_split_semicolon_items(fitment_notes), prose)
    features = _drop_prose_duplicates(list(features), prose)
    if not prose and not features and not fitment_notes:
        return ""

    html_parts: typing.List[str] = []

    if prose:
        for paragraph in (p.strip() for p in prose.split("\n")):
            if paragraph:
                html_parts.append("<p>{}</p>".format(_esc(paragraph)))

    html_parts.append(_html_list_section("Features and Benefits", features))
    html_parts.append(_html_list_section("Fitment Notes", fitment_notes))
    html_parts.append(
        _html_list_section(
            "Details",
            ["{}: {}".format(s["spec_name"], s["spec_value"]) for s in field_specs],
        )
    )

    # Rows arrive already deduped and capped by _applications_for; the second dedupe here is
    # belt-and-braces for callers that build the list themselves.
    seen_applications: typing.Set[str] = set()
    application_rows: typing.List[str] = []
    for fitment in applications:
        rendered = _format_application(fitment)
        if rendered and rendered not in seen_applications:
            seen_applications.add(rendered)
            application_rows.append(rendered)
    if applications_total is None and applications:
        applications_total = applications[0].get("total")
    dropped = (applications_total or len(application_rows)) - len(application_rows)
    if dropped > 0:
        application_rows.append("and {} more applications".format(dropped))
    html_parts.append(_html_list_section("Applications", application_rows))

    return "".join(part for part in html_parts if part)


def _turn14_description_html(
    by_type: typing.Dict[str, typing.List[str]],
    field_specs: typing.Sequence[typing.Dict[str, str]],
    applications: typing.Sequence[typing.Dict],
) -> str:
    fitment_notes: typing.List[str] = []
    for desc_type in _FITMENT_NOTE_DESCRIPTION_TYPES:
        fitment_notes.extend(by_type.get(desc_type, []))
    return compose_description_html(
        prose=_first_of_types(by_type, _PROSE_DESCRIPTION_TYPES),
        features=by_type.get("Features and Benefits", []),
        fitment_notes=fitment_notes,
        field_specs=field_specs,
        applications=applications,
    )


def _applications_for(
    rows: typing.List[typing.Dict],
) -> typing.Dict[int, typing.List[typing.Dict]]:
    """
    Vehicle applications per master part, each row carrying the part's distinct ``total`` so the
    caller can say "and N more" without a second query.

    Deduped and capped in SQL, not afterwards. ``MasterPartFitment`` is unique on a tuple that
    includes engine and drive_type, neither of which this list renders, so one vehicle arrives as
    several rows -- and a Rough Country part can carry hundreds. Pulling them all for a 2,000-part
    batch and trimming in Python stalled the run for minutes on a single batch; ``DISTINCT`` plus
    a ``row_number()`` cap keeps it to at most ``MAX_APPLICATIONS`` rows per part, while the
    window ``count`` still gives the honest total for the "and N more" line.
    """
    master_part_ids = [r["master_part_id"] for r in rows]
    if not master_part_ids:
        return {}

    sql = """
        WITH distinct_apps AS (
            SELECT DISTINCT master_part_id, year_start, year_end, make, model, submodel
            FROM master_part_fitments
            WHERE master_part_id = ANY(%s)
        ), ranked AS (
            SELECT *,
                   row_number() OVER (
                       PARTITION BY master_part_id ORDER BY year_start, make, model, submodel
                   ) AS rn,
                   count(*) OVER (PARTITION BY master_part_id) AS total
            FROM distinct_apps
        )
        SELECT master_part_id, year_start, year_end, make, model, submodel, total
        FROM ranked WHERE rn <= %s ORDER BY master_part_id, rn
    """
    out: typing.Dict[int, typing.List[typing.Dict]] = {}
    with connection.cursor() as cur:
        cur.execute(sql, [master_part_ids, MAX_APPLICATIONS])
        for master_part_id, year_start, year_end, make, model, submodel, total in cur.fetchall():
            out.setdefault(master_part_id, []).append({
                "year_start": year_start,
                "year_end": year_end,
                "make": make,
                "model": model,
                "submodel": submodel,
                "total": total,
            })
    return out


# Feed placeholders that mean "no value" -- Premier writes the literal string 'NA' into image_url
# for 536,353 of its 686,935 rows, and several feeds use 'N/A' / 'Unknown' / '-' the same way.
# Treating them as text would put "Image: NA" on a PDP.
_PLACEHOLDER_VALUES = frozenset({"na", "n/a", "none", "null", "unknown", "-", "--", "tbd"})


def _clean(value: typing.Any) -> typing.Optional[str]:
    """Trimmed string, or None when blank or a feed placeholder."""
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.casefold() in _PLACEHOLDER_VALUES:
        return None
    return text


def _clean_barcode(value: typing.Any) -> typing.Optional[str]:
    """
    A barcode, not a number. Several feeds round-trip the column through a float and hand us
    '843030158118.0' (Rough Country does this on every row); the trailing '.0' would be stored
    as a GTIN and never match anyone else's spelling of the same code.
    """
    text = _clean(value)
    if text is None:
        return None
    if text.endswith(".0") and text[:-2].isdigit():
        text = text[:-2]
    return text or None


#: Spec names where a zero is a real measurement rather than an unset field. A zero-offset wheel
#: exists; a part 0 inches long does not.
_ZERO_IS_MEANINGFUL = frozenset({"Offset"})


def _tidy_number(text: str) -> str:
    """'41.000' -> '41', '35.15000' -> '35.15'. Feeds pad decimals; a spec list should not."""
    if "." not in text:
        return text
    trimmed = text.rstrip("0").rstrip(".")
    return trimmed or "0"


def _spec_value(name: str, value: typing.Any) -> typing.Optional[str]:
    """
    One ``spec_value``, or None to drop the spec entirely.

    Booleans render as Yes and disappear when False -- Premier stores prop 65 and California
    legality as booleans, and a Details list saying 'Prop 65 Carcinogen: False' is noise. Numeric
    zeros drop too, because feeds use 0 as "no value": Vossen writes 0 into every wheel dimension
    for a centre cap, which would otherwise render 'Diameter: 0'.
    """
    if isinstance(value, bool):
        return "Yes" if value else None

    cleaned = _clean(value)
    if cleaned is None:
        return None

    try:
        numeric = float(cleaned)
    except ValueError:
        return cleaned
    if numeric == 0 and name not in _ZERO_IS_MEANINGFUL:
        return None
    return _tidy_number(cleaned)


def _specs(pairs: typing.Sequence[typing.Tuple[str, typing.Any]]) -> typing.List[typing.Dict[str, str]]:
    """ASAP's ``[{spec_name, spec_value}]`` shape, dropping blanks, placeholders and unset zeros."""
    out: typing.List[typing.Dict[str, str]] = []
    for name, value in pairs:
        cleaned = _spec_value(name, value)
        if cleaned is not None:
            out.append({"spec_name": name, "spec_value": cleaned})
    return out


def _image_urls(candidates: typing.Iterable[typing.Any]) -> typing.List[str]:
    """Absolute http(s) image URLs, deduped, order preserved, capped."""
    urls: typing.List[str] = []
    seen: typing.Set[str] = set()
    for candidate in candidates:
        url = _clean(candidate)
        if not url or not _is_http_url(url) or url in seen:
            continue
        seen.add(url)
        urls.append(url)
        if len(urls) >= MAX_IMAGES:
            break
    return urls


def _split_lines(value: typing.Any) -> typing.List[str]:
    """Feeds that pack a bullet list into one cell, one bullet per line (Rough Country)."""
    text = _clean(value)
    if not text:
        return []
    return [line.strip() for line in text.splitlines() if line.strip()]


def _raw_rows_by_external_id(
    *,
    table: str,
    brand_column: str,
    key_column: str,
    select_columns: typing.Sequence[str],
    external_ids: typing.Sequence[str],
) -> typing.Dict[str, typing.Dict]:
    """
    Fetch raw provider rows keyed by the ``{brand_id}_{key}`` ``provider_external_id`` every
    non-Turn14 provider uses (see the ``_*_provider_external_id`` encoders in master_parts.py).

    Joins through a VALUES list on ``(brand_id, key)`` so the composite unique index on each raw
    table does the work -- splitting the id in SQL instead would defeat every one of them.
    """
    pairs: typing.List[typing.Tuple[int, str]] = []
    for external_id in external_ids:
        if not external_id or "_" not in external_id:
            continue
        brand_part, key = external_id.split("_", 1)
        try:
            pairs.append((int(brand_part), key))
        except ValueError:
            continue
    if not pairs:
        return {}

    columns = ", ".join("t.{}".format(c) for c in select_columns)
    placeholders = ", ".join(["(%s::bigint, %s)"] * len(pairs))
    params: typing.List[typing.Any] = [x for pair in pairs for x in pair]
    sql = (
        "SELECT t.{brand} AS _brand_id, t.{key} AS _key, {columns} "
        "FROM {table} t JOIN (VALUES {placeholders}) AS v(brand_id, key_value) "
        "ON t.{brand} = v.brand_id AND t.{key} = v.key_value".format(
            brand=brand_column,
            key=key_column,
            columns=columns,
            table=table,
            placeholders=placeholders,
        )
    )
    out: typing.Dict[str, typing.Dict] = {}
    with connection.cursor() as cur:
        cur.execute(sql, params)
        names = [c[0] for c in cur.description]
        for record in cur.fetchall():
            row = dict(zip(names, record))
            out["{}_{}".format(row.pop("_brand_id"), row.pop("_key"))] = row
    return out


def _raw_rows_by_bare_key(
    *,
    table: str,
    key_column: str,
    select_columns: typing.Sequence[str],
    external_ids: typing.Sequence[str],
) -> typing.Dict[str, typing.Dict]:
    """
    Fetch raw rows for the providers whose ``provider_external_id`` is the feed key alone, with no
    ``{brand_id}_`` prefix -- Keystone stores the bare VCPN and Meyer the bare Meyer part number
    (see where their ingests set ``provider_external_id`` in master_parts.py). Both are unique on
    ``(key, brand_id)`` with the key leading, so a plain ``= ANY`` is an index scan.
    """
    keys = [e for e in external_ids if e]
    if not keys:
        return {}

    columns = ", ".join("t.{}".format(c) for c in select_columns)
    sql = "SELECT t.{key} AS _key, {columns} FROM {table} t WHERE t.{key} = ANY(%s)".format(
        key=key_column, columns=columns, table=table
    )
    out: typing.Dict[str, typing.Dict] = {}
    with connection.cursor() as cur:
        cur.execute(sql, [keys])
        names = [c[0] for c in cur.description]
        for record in cur.fetchall():
            row = dict(zip(names, record))
            out[row.pop("_key")] = row
    return out


class _Turn14Source:
    """
    Candidate selection + payload building against ``turn14_brand_data`` / ``turn14_items``,
    joined on ``ProviderPart.provider_external_id`` (Turn14's item id is the primary key of both
    raw tables, so both joins are unique-index lookups).
    """

    name = "turn14"
    provider_kind = src_enums.BrandProviderKind.TURN_14.value

    def stale_clause(self) -> str:
        return (
            "AND EXISTS (SELECT 1 FROM master_part_data d "
            "JOIN turn14_brand_data bd ON bd.external_id = pp.provider_external_id "
            "WHERE d.master_part_id = pp.master_part_id AND bd.updated_at > d.updated_at)"
        )

    def payloads_for(
        self,
        rows: typing.List[typing.Dict],
        *,
        want_description: bool,
    ) -> typing.Dict[int, EnrichmentPayload]:
        external_ids = [r["provider_external_id"] for r in rows if r["provider_external_id"]]
        if not external_ids:
            return {}

        brand_data = {
            bd.external_id: bd
            for bd in src_models.Turn14BrandData.objects.filter(
                external_id__in=external_ids
            ).only("external_id", "files", "descriptions")
        }
        items = {
            row["external_id"]: row
            for row in src_models.Turn14Items.objects.filter(external_id__in=external_ids).values(
                "external_id",
                "barcode",
                "thumbnail",
                "prop_65",
                "epa",
                "units_per_sku",
                "ltl_freight_required",
                "dimensions",
            )
        }

        applications = _applications_for(rows) if want_description else {}

        payloads: typing.Dict[int, EnrichmentPayload] = {}
        for row in rows:
            external_id = row["provider_external_id"]
            data = brand_data.get(external_id)
            item = items.get(external_id)
            if data is None and item is None:
                continue

            files = data.files if data is not None else None
            by_type = _turn14_descriptions_by_type(data.descriptions if data is not None else None)
            field_specs = _turn14_field_specs(item)
            images = _turn14_image_urls(files)

            payload = EnrichmentPayload(
                images=images or None,
                installation_instructions=(
                    _turn14_document_urls(files, _INSTRUCTION_MEDIA_CONTENT) or None
                ),
                youtube_video=_turn14_youtube_url(files),
                field_specs=field_specs or None,
                # The gallery's lead image (an L-size asset) beats Turn14Items.thumbnail, which is
                # the S-size render of that same shot; the thumbnail is only the fallback for a
                # part whose media feed has no usable image at all.
                core_image_url=(
                    (images[0] if images else None)
                    or ((item or {}).get("thumbnail") or "").strip()
                    or None
                ),
                core_description=(
                    _first_of_types(by_type, _SHORT_DESCRIPTION_TYPES)
                    or _first_of_types(by_type, _PROSE_DESCRIPTION_TYPES)
                ),
                core_gtin=((item or {}).get("barcode") or "").strip() or None,
                source_external_id=external_id,
            )
            if want_description:
                payload.description = (
                    _turn14_description_html(
                        by_type, field_specs, applications.get(row["master_part_id"], [])
                    )
                    or None
                )
            payloads[row["master_part_id"]] = payload

        return payloads


# ---------------------------------------------------------------------------
# Flat-feed sources
#
# Every non-Turn14 provider keys its ProviderPart as ``{brand_row_id}_{feed_key}`` (see the
# ``_*_provider_external_id`` encoders in master_parts.py), so they all share one fetch path and
# differ only in which columns they read and how those columns map onto the payload.
#
# Run order is the precedence: the merge is fill-if-blank, so whichever source runs first wins a
# field and later ones only fill what is still empty. Ordered richest-first per part:
#   rough_country -> wps -> wheelpros -> premier -> vossen -> tirerack -> helmet_house -> atech
# ---------------------------------------------------------------------------

class _FlatFeedSource:
    """Shared plumbing: fetch raw rows for a batch, then let the subclass map one row."""

    name: str = ""
    provider_kind: int = 0
    table: str = ""
    brand_column: str = "brand_id"
    key_column: str = ""
    select_columns: typing.Sequence[str] = ()
    #: Set when the payload's description needs the part's fitments (Applications section).
    uses_applications: bool = True
    #: True for providers whose provider_external_id is the bare feed key with no {brand_id}_
    #: prefix (Keystone's VCPN, Meyer's part number).
    bare_key: bool = False

    def fetch_raw(self, external_ids: typing.Sequence[str]) -> typing.Dict[str, typing.Dict]:
        if self.bare_key:
            return _raw_rows_by_bare_key(
                table=self.table,
                key_column=self.key_column,
                select_columns=self.select_columns,
                external_ids=external_ids,
            )
        return _raw_rows_by_external_id(
            table=self.table,
            brand_column=self.brand_column,
            key_column=self.key_column,
            select_columns=self.select_columns,
            external_ids=external_ids,
        )

    def stale_clause(self) -> str:
        # Turn14 can do this cheaply because its raw tables are keyed by the same external_id the
        # ProviderPart carries. Here the join needs the id split apart, which no index covers, so
        # a stale scan would be a full sequential pass over provider_parts. Use blank-fields.
        raise ValueError(
            "--mode stale is not supported for source '{}' (no indexed join from "
            "provider_external_id to the raw feed); use --mode blank-fields.".format(self.name)
        )

    def payload_from_row(
        self,
        row: typing.Dict,
        *,
        applications: typing.Sequence[typing.Dict],
        want_description: bool,
    ) -> typing.Optional[EnrichmentPayload]:
        raise NotImplementedError

    def payloads_for(
        self,
        rows: typing.List[typing.Dict],
        *,
        want_description: bool,
    ) -> typing.Dict[int, EnrichmentPayload]:
        external_ids = [r["provider_external_id"] for r in rows if r["provider_external_id"]]
        if not external_ids:
            return {}

        raw_by_external_id = self.fetch_raw(external_ids)
        if not raw_by_external_id:
            return {}

        applications = (
            _applications_for(rows) if (want_description and self.uses_applications) else {}
        )

        payloads: typing.Dict[int, EnrichmentPayload] = {}
        for row in rows:
            raw = raw_by_external_id.get(row["provider_external_id"])
            if raw is None:
                continue
            payload = self.payload_from_row(
                raw,
                applications=applications.get(row["master_part_id"], []),
                want_description=want_description,
            )
            if payload is None:
                continue
            payload.source_external_id = row["provider_external_id"]
            payloads[row["master_part_id"]] = payload
        return payloads


class _RoughCountrySource(_FlatFeedSource):
    """
    Richest per part of any non-Turn14 feed: six image columns, a real feature list, install
    notes and a YouTube link, on all 15,689 of 15,695 rows.
    """

    name = "rough_country"
    provider_kind = src_enums.BrandProviderKind.ROUGH_COUNTRY.value
    table = "rough_country_parts"
    key_column = "sku"
    select_columns = (
        "sku", "title", "description", "features", "notes", "image_1", "image_2", "image_3",
        "image_4", "image_5", "image_6", "video", "upc", "weight", "length", "width", "height",
        "category",
    )

    def payload_from_row(self, row, *, applications, want_description):
        images = _image_urls(
            [row.get("image_{}".format(n)) for n in range(1, 7)]
        )
        field_specs = _specs([
            ("UPC", _clean_barcode(row.get("upc"))),
            ("Category", row.get("category")),
            ("Length", row.get("length")),
            ("Width", row.get("width")),
            ("Height", row.get("height")),
            ("Weight", row.get("weight")),
        ])
        video = _clean(row.get("video"))
        payload = EnrichmentPayload(
            images=images or None,
            youtube_video=video if (video and _is_youtube_video_url(video)) else None,
            field_specs=field_specs or None,
            core_image_url=images[0] if images else None,
            core_description=_clean(row.get("title")),
            core_gtin=_clean_barcode(row.get("upc")),
        )
        if want_description:
            payload.description = compose_description_html(
                prose=_clean(row.get("description")),
                features=_split_lines(row.get("features")),
                fitment_notes=_split_lines(row.get("notes")),
                field_specs=field_specs,
                applications=applications,
            ) or None
        return payload


class _WpsSource(_FlatFeedSource):
    """
    WPS carries images on 114,909 of its 126,712 items, already as a JSON list of absolute URLs
    -- the exact shape ``MasterPartData.images`` wants. Prose is the weak side: only 19,369
    ``wps_products`` rows exist and just 3,130 have a description, so most parts get the item
    name and specs only.
    """

    name = "wps"
    provider_kind = src_enums.BrandProviderKind.WESTERN_POWER_SPORTS.value
    table = "wps_items"
    key_column = "sku"
    select_columns = (
        "sku", "name", "image_url", "images", "upc", "weight", "length", "width", "height",
        "product_type", "product_id", "data",
    )

    def payloads_for(self, rows, *, want_description):
        payloads = super().payloads_for(rows, want_description=want_description)
        if not payloads or not want_description:
            for payload in payloads.values():
                payload.__dict__.pop("_wps_product_id", None)
                payload.__dict__.pop("_wps_applications", None)
            return payloads

        # Second hop for the prose, over WpsItem's FK: wps_items.product_id -> wps_products.id.
        product_ids = {
            p._wps_product_id for p in payloads.values() if getattr(p, "_wps_product_id", None)
        }
        if product_ids:
            descriptions = {
                row["id"]: row["description"]
                for row in src_models.WpsProduct.objects.filter(
                    id__in=list(product_ids)
                ).values("id", "description")
            }
            for payload in payloads.values():
                product_id = getattr(payload, "_wps_product_id", None)
                prose = _clean(descriptions.get(product_id)) if product_id else None
                if prose:
                    # Real product prose beats the item name the row-level pass fell back to.
                    payload.description = compose_description_html(
                        prose=prose,
                        field_specs=payload.field_specs or (),
                        applications=payload._wps_applications,
                    ) or payload.description
        for payload in payloads.values():
            payload.__dict__.pop("_wps_product_id", None)
            payload.__dict__.pop("_wps_applications", None)
        return payloads

    def payload_from_row(self, row, *, applications, want_description):
        images = _image_urls(_as_list(row.get("images")) or [row.get("image_url")])
        data = row.get("data") if isinstance(row.get("data"), dict) else {}
        field_specs = _specs([
            ("UPC", _clean_barcode(row.get("upc"))),
            ("Product Type", row.get("product_type")),
            ("Prop 65 Warning", data.get("prop_65_detail") or data.get("prop_65_code")),
            ("Length", row.get("length")),
            ("Width", row.get("width")),
            ("Height", row.get("height")),
            ("Weight", row.get("weight")),
        ])
        payload = EnrichmentPayload(
            images=images or None,
            field_specs=field_specs or None,
            core_image_url=images[0] if images else None,
            core_description=_clean(row.get("name")),
            core_gtin=_clean_barcode(row.get("upc")),
        )
        if want_description:
            payload.description = compose_description_html(
                prose=_clean(row.get("name")),
                field_specs=field_specs,
                applications=applications,
            ) or None
        # Stashed for the product-description hop in payloads_for; stripped before returning.
        #
        # This is WpsItem's Django FK -- wps_products.**id**, the local row PK -- not the API's
        # product id, which lives in wps_products.external_id (wps.py:321 maps one to the other at
        # ingest). Joining it against external_id instead silently resolves to whichever unrelated
        # product happens to share that number: it matched 6,511 rows and described a carb nozzle
        # as a "900 RZR XP High Flow Intake Kit".
        payload._wps_product_id = row.get("product_id")
        payload._wps_applications = list(applications)
        return payload


class _WheelProsSource(_FlatFeedSource):
    """
    Wheels: one image on 61,048 of 69,058 rows, plus the spec set that actually matters for a
    wheel (bolt pattern, offset, center bore, load rating) and the centre-cap details that only
    live in ``raw_data``. ``finish`` doubles as the colour and ``style`` as the product line.
    """

    name = "wheelpros"
    provider_kind = src_enums.BrandProviderKind.WHEELPROS.value
    table = "wheelpros_parts"
    key_column = "part_number"
    select_columns = (
        "part_number", "part_description", "image_url", "finish", "size", "bolt_pattern",
        "offset", "center_bore", "load_rating", "style", "display_style_no", "shipping_weight",
        "raw_data",
    )

    def payload_from_row(self, row, *, applications, want_description):
        images = _image_urls([row.get("image_url")])
        raw = row.get("raw_data") if isinstance(row.get("raw_data"), dict) else {}
        field_specs = _specs([
            ("Size", row.get("size")),
            ("Bolt Pattern", row.get("bolt_pattern")),
            ("Offset", row.get("offset")),
            ("Center Bore", row.get("center_bore")),
            ("Load Rating", row.get("load_rating")),
            ("Finish", row.get("finish")),
            ("Style", row.get("style")),
            ("Style Number", row.get("display_style_no")),
            ("Cap Style", raw.get("CapStyleDescription")),
            ("Cap Hardware", raw.get("CapHardwareDescription")),
            ("Cap Wrench", raw.get("CapWrench")),
            ("Shipping Weight", row.get("shipping_weight")),
        ])
        payload = EnrichmentPayload(
            images=images or None,
            field_specs=field_specs or None,
            color=_clean(row.get("finish")),
            series=_clean(row.get("style")),
            core_image_url=images[0] if images else None,
            core_description=_clean(row.get("part_description")),
        )
        if want_description:
            payload.description = compose_description_html(
                prose=_clean(row.get("part_description")),
                field_specs=field_specs,
                applications=applications,
            ) or None
        return payload


class _PremierSource(_FlatFeedSource):
    """
    150,318 of 686,935 rows carry a real image URL; the other 536,353 hold the literal string
    'NA', which ``_clean`` drops. Both description columns are title-length rather than prose.
    """

    name = "premier"
    provider_kind = src_enums.BrandProviderKind.PREMIER_PERFORMANCE.value
    table = "premier_parts"
    key_column = "premier_part_number"
    select_columns = (
        "premier_part_number", "long_description", "external_long_description", "image_url",
        "upc_code", "length", "width", "height", "weight", "part_category", "part_subcategory",
        "part_terminology", "prop65_carcinogen", "prop65_reproductive_harm", "california_legal",
    )

    def payload_from_row(self, row, *, applications, want_description):
        images = _image_urls([row.get("image_url")])
        prose = _clean(row.get("external_long_description")) or _clean(row.get("long_description"))
        field_specs = _specs([
            ("UPC", _clean_barcode(row.get("upc_code"))),
            ("Category", row.get("part_category")),
            ("Subcategory", row.get("part_subcategory")),
            ("Part Type", row.get("part_terminology")),
            ("Prop 65 Carcinogen", row.get("prop65_carcinogen")),
            ("Prop 65 Reproductive Harm", row.get("prop65_reproductive_harm")),
            ("California Legal", row.get("california_legal")),
            ("Length", row.get("length")),
            ("Width", row.get("width")),
            ("Height", row.get("height")),
            ("Weight", row.get("weight")),
        ])
        payload = EnrichmentPayload(
            images=images or None,
            field_specs=field_specs or None,
            core_image_url=images[0] if images else None,
            core_description=_clean(row.get("long_description")) or prose,
            core_gtin=_clean_barcode(row.get("upc_code")),
        )
        if want_description:
            payload.description = compose_description_html(
                prose=prose,
                field_specs=field_specs,
                applications=applications,
            ) or None
        return payload


class _VossenSource(_FlatFeedSource):
    """Wheels, no images in the feed -- contributes the wheel spec set and a title."""

    name = "vossen"
    provider_kind = src_enums.BrandProviderKind.VOSSEN.value
    table = "vossen_parts"
    key_column = "sku"
    select_columns = ("sku", "description", "diameter", "width", "offset", "bolt_pattern", "center_bore")

    def payload_from_row(self, row, *, applications, want_description):
        # Vossen's feed carries accessories (centre caps, hardware) alongside wheels and zero-fills
        # every wheel dimension on them. The dimensions only mean anything as a set, so a row with
        # no diameter gets none of them -- otherwise a centre cap renders a lone 'Offset: 0', which
        # survives the generic zero-drop because a zero-offset wheel is real.
        is_wheel = _spec_value("Diameter", row.get("diameter")) is not None
        field_specs = _specs([
            ("Diameter", row.get("diameter")),
            ("Width", row.get("width")),
            ("Offset", row.get("offset")),
            ("Bolt Pattern", row.get("bolt_pattern")),
            ("Center Bore", row.get("center_bore")),
        ]) if is_wheel else []
        payload = EnrichmentPayload(
            field_specs=field_specs or None,
            core_description=_clean(row.get("description")),
        )
        if want_description:
            payload.description = compose_description_html(
                prose=_clean(row.get("description")),
                field_specs=field_specs,
                applications=applications,
            ) or None
        return payload


class _TireRackSource(_FlatFeedSource):
    """
    No images, but the only feed in the catalogue with a genuine warranty *term*
    ("5 Years / 70000 Miles") rather than a PDF link -- the first real fill of
    ``MasterPartData.warranty`` since ASAP.
    """

    name = "tirerack"
    provider_kind = src_enums.BrandProviderKind.TIRERACK.value
    table = "tirerack_parts"
    key_column = "part_number"
    select_columns = (
        "part_number", "description", "country_of_origin", "road_hazard_warranty",
        "treadlife_warranty_1", "treadlife_warranty_2", "treadlife_warranty_3", "fet",
    )

    def payload_from_row(self, row, *, applications, want_description):
        treadlife = [
            _clean(row.get("treadlife_warranty_{}".format(n))) for n in (1, 2, 3)
        ]
        treadlife = [t for t in treadlife if t]
        field_specs = _specs([
            ("Country of Origin", row.get("country_of_origin")),
            ("Road Hazard Warranty", row.get("road_hazard_warranty")),
            ("Treadlife Warranty", treadlife[0] if treadlife else None),
            ("Federal Excise Tax", row.get("fet")),
        ])
        payload = EnrichmentPayload(
            field_specs=field_specs or None,
            warranty=treadlife[0] if treadlife else _clean(row.get("road_hazard_warranty")),
            core_description=_clean(row.get("description")),
        )
        if want_description:
            payload.description = compose_description_html(
                prose=_clean(row.get("description")),
                field_specs=field_specs,
                applications=applications,
            ) or None
        return payload


class _HelmetHouseSource(_FlatFeedSource):
    """
    No images on purpose: ``photo_filename`` is a bare filename and their image host answers 404
    for every one of them, so building a URL would put a dead link on 33,844 parts -- see the
    ``HelmetHousePart`` docstring. Contributes the long description, colour, and apparel specs.
    """

    name = "helmet_house"
    provider_kind = src_enums.BrandProviderKind.HELMHOUSE.value
    table = "helmet_house_parts"
    key_column = "sku"
    select_columns = (
        "sku", "description", "long_description", "color", "size", "model", "country_of_origin",
        "upc", "category", "product_class", "weight", "length", "width", "depth",
    )

    def payload_from_row(self, row, *, applications, want_description):
        field_specs = _specs([
            ("UPC", _clean_barcode(row.get("upc"))),
            ("Color", row.get("color")),
            ("Size", row.get("size")),
            ("Model", row.get("model")),
            ("Category", row.get("category")),
            ("Class", row.get("product_class")),
            ("Country of Origin", row.get("country_of_origin")),
            ("Length", row.get("length")),
            ("Width", row.get("width")),
            ("Depth", row.get("depth")),
            ("Weight", row.get("weight")),
        ])
        payload = EnrichmentPayload(
            field_specs=field_specs or None,
            color=_clean(row.get("color")),
            core_description=_clean(row.get("description")),
            core_gtin=_clean_barcode(row.get("upc")),
        )
        if want_description:
            payload.description = compose_description_html(
                prose=_clean(row.get("long_description")),
                field_specs=field_specs,
                applications=applications,
            ) or None
        return payload


class _AtechSource(_FlatFeedSource):
    """
    Thin on purpose. A-Tech's feed has no media column of its own -- ``image_url`` is populated
    only by the one-off Summit Racing scrape (``backfill_atech_summit_image_urls``), which has
    reached **7 rows out of 1,434,995** -- and its ``description`` is the same title text the
    main sync already writes to ``MasterPart.description``. Runs last, so it only ever fills
    what the other seven left blank.
    """

    name = "atech"
    provider_kind = src_enums.BrandProviderKind.ATECH.value
    table = "atech_parts"
    key_column = "part_number"
    select_columns = ("part_number", "description", "image_url", "gtin")

    def payload_from_row(self, row, *, applications, want_description):
        images = _image_urls([row.get("image_url")])
        field_specs = _specs([("UPC", _clean_barcode(row.get("gtin")))])
        payload = EnrichmentPayload(
            images=images or None,
            field_specs=field_specs or None,
            core_image_url=images[0] if images else None,
            core_description=_clean(row.get("description")),
            core_gtin=_clean_barcode(row.get("gtin")),
        )
        if want_description:
            payload.description = compose_description_html(
                prose=_clean(row.get("description")),
                field_specs=field_specs,
                applications=applications,
            ) or None
        return payload


class _TheWheelGroupSource(_FlatFeedSource):
    """
    The richest flat feed in the catalogue, and the smallest: four image columns, three prose
    columns plus bullet points, both warranty terms, and the full wheel spec set down to TPMS
    compatibility and lugnut type -- across only 2,079 parts.
    """

    name = "the_wheel_group"
    provider_kind = src_enums.BrandProviderKind.THE_WHEEL_GROUP.value
    table = "thewheelgroup_parts"
    key_column = "sku"
    select_columns = (
        "sku", "name", "description", "short_description", "sales_description", "bullet_points",
        "image_1", "image_2", "image_3", "image_4", "color", "finish", "diameter", "wheel_width",
        "hub_bore", "bolt_pattern_1", "bolt_pattern_2", "offset", "backspace", "load_rating",
        "upc", "country_of_origin", "structure_warranty", "finish_warranty", "tpms_compatible",
        "lugseat_type", "wheel_cap", "product_weight", "note", "beadlock_instructions_url",
    )

    def payload_from_row(self, row, *, applications, want_description):
        images = _image_urls([row.get("image_{}".format(n)) for n in range(1, 5)])
        field_specs = _specs([
            ("UPC", _clean_barcode(row.get("upc"))),
            ("Diameter", row.get("diameter")),
            ("Width", row.get("wheel_width")),
            ("Bolt Pattern", row.get("bolt_pattern_1")),
            ("Second Bolt Pattern", row.get("bolt_pattern_2")),
            ("Offset", row.get("offset")),
            ("Backspace", row.get("backspace")),
            ("Hub Bore", row.get("hub_bore")),
            ("Load Rating", row.get("load_rating")),
            ("Finish", row.get("finish")),
            ("Color", row.get("color")),
            ("TPMS Compatible", row.get("tpms_compatible")),
            ("Lugseat Type", row.get("lugseat_type")),
            ("Wheel Cap", row.get("wheel_cap")),
            ("Country of Origin", row.get("country_of_origin")),
            ("Weight", row.get("product_weight")),
        ])
        # Two separate warranty terms; join them rather than dropping one.
        warranties = [
            "{} structure".format(_clean(row.get("structure_warranty")))
            if _clean(row.get("structure_warranty")) else None,
            "{} finish".format(_clean(row.get("finish_warranty")))
            if _clean(row.get("finish_warranty")) else None,
        ]
        warranties = [w for w in warranties if w]

        payload = EnrichmentPayload(
            images=images or None,
            field_specs=field_specs or None,
            color=_clean(row.get("color")) or _clean(row.get("finish")),
            warranty=", ".join(warranties) or None,
            installation_instructions=(
                _image_urls([row.get("beadlock_instructions_url")]) or None
            ) if _is_http_url(_clean(row.get("beadlock_instructions_url")) or "") else None,
            core_image_url=images[0] if images else None,
            core_description=_clean(row.get("name")) or _clean(row.get("short_description")),
            core_gtin=_clean_barcode(row.get("upc")),
        )
        if want_description:
            payload.description = compose_description_html(
                prose=(
                    _clean(row.get("sales_description"))
                    or _clean(row.get("description"))
                    or _clean(row.get("short_description"))
                ),
                features=_split_lines(row.get("bullet_points")),
                fitment_notes=_split_lines(row.get("note")),
                field_specs=field_specs,
                applications=applications,
            ) or None
        return payload


class _EliteWheelSource(_FlatFeedSource):
    """
    Two raw tables, wheels and tires, joined by the same key: an ``EliteWheelBrand`` row belongs to
    exactly one half (``WHEEL:AZARA`` vs ``TIRE:LEXANI``), so a brand id can only match one table
    and merging the two lookups is unambiguous -- see ``_elite_wheel_provider_external_id``.
    No images or prose in either feed; specs only.
    """

    name = "elite_wheel"
    provider_kind = src_enums.BrandProviderKind.ELITE_WHEEL.value
    table = "elitewheels_part_wheels"
    key_column = "part_number"
    select_columns = (
        "part_number", "group_label", "size", "bolt_pattern_1", "bolt_pattern_2", "offset",
        "center_bore", "finish",
    )
    _TIRE_COLUMNS = ("part_number", "model", "raw_size", "tire_diameter")

    def fetch_raw(self, external_ids):
        rows = super().fetch_raw(external_ids)
        tires = _raw_rows_by_external_id(
            table="elitewheels_part_tires",
            brand_column="brand_id",
            key_column="part_number",
            select_columns=self._TIRE_COLUMNS,
            external_ids=external_ids,
        )
        rows.update(tires)
        return rows

    def payload_from_row(self, row, *, applications, want_description):
        is_tire = "raw_size" in row
        if is_tire:
            field_specs = _specs([
                ("Model", row.get("model")),
                ("Size", row.get("raw_size")),
                ("Diameter", row.get("tire_diameter")),
            ])
            title = " ".join(
                p for p in (_clean(row.get("model")), _clean(row.get("raw_size"))) if p
            )
        else:
            field_specs = _specs([
                ("Size", row.get("size")),
                ("Bolt Pattern", row.get("bolt_pattern_1")),
                ("Second Bolt Pattern", row.get("bolt_pattern_2")),
                ("Offset", row.get("offset")),
                ("Center Bore", row.get("center_bore")),
                ("Finish", row.get("finish")),
                ("Group", row.get("group_label")),
            ])
            title = " ".join(
                p for p in (_clean(row.get("group_label")), _clean(row.get("size")),
                            _clean(row.get("finish"))) if p
            )

        payload = EnrichmentPayload(
            field_specs=field_specs or None,
            color=None if is_tire else _clean(row.get("finish")),
            core_description=title or None,
        )
        if want_description:
            payload.description = compose_description_html(
                prose=title or None,
                field_specs=field_specs,
                applications=applications,
            ) or None
        return payload


class _KeystoneSource(_FlatFeedSource):
    """
    ``provider_external_id`` is the bare VCPN, not the ``{brand_id}_{key}`` compound the other flat
    feeds use (see where the Keystone ingest sets it). No images in the feed; a real
    ``long_description`` plus dimensions and prop 65 toxicity.
    """

    name = "keystone"
    provider_kind = src_enums.BrandProviderKind.KEYSTONE.value
    table = "keystone_parts"
    key_column = "vcpn"
    bare_key = True
    select_columns = (
        "vcpn", "long_description", "upc_code", "weight", "height", "length", "width",
        "prop65_toxicity", "is_hazmat", "is_chemical", "case_qty", "aaia_code",
    )

    def payload_from_row(self, row, *, applications, want_description):
        # Keystone codes prop 65 as a bare Y/N. 'Prop 65 Toxicity: N' reads as a code rather than
        # an answer, so surface only the positive case -- same call the Turn14 mapping makes.
        prop65 = (_clean(row.get("prop65_toxicity")) or "").upper()
        field_specs = _specs([
            ("UPC", _clean_barcode(row.get("upc_code"))),
            ("Prop 65 Warning", "Yes" if prop65 == "Y" else None),
            ("Hazmat", row.get("is_hazmat")),
            ("Chemical", row.get("is_chemical")),
            ("Case Quantity", row.get("case_qty")),
            ("Length", row.get("length")),
            ("Width", row.get("width")),
            ("Height", row.get("height")),
            ("Weight", row.get("weight")),
        ])
        payload = EnrichmentPayload(
            field_specs=field_specs or None,
            core_description=_clean(row.get("long_description")),
            core_gtin=_clean_barcode(row.get("upc_code")),
        )
        if want_description:
            payload.description = compose_description_html(
                prose=_clean(row.get("long_description")),
                field_specs=field_specs,
                applications=applications,
            ) or None
        return payload


class _MeyerSource(_FlatFeedSource):
    """Bare ``meyer_part`` key, like Keystone. Description, category and dimensions; no images."""

    name = "meyer"
    provider_kind = src_enums.BrandProviderKind.MEYER.value
    table = "meyer_parts"
    key_column = "meyer_part"
    bare_key = True
    select_columns = (
        "meyer_part", "description", "upc", "length", "width", "height", "weight",
        "category", "sub_category", "is_ltl", "is_oversize",
    )

    def payload_from_row(self, row, *, applications, want_description):
        field_specs = _specs([
            ("UPC", _clean_barcode(row.get("upc"))),
            ("Category", row.get("category")),
            ("Subcategory", row.get("sub_category")),
            ("Ships LTL", row.get("is_ltl")),
            ("Oversize", row.get("is_oversize")),
            ("Length", row.get("length")),
            ("Width", row.get("width")),
            ("Height", row.get("height")),
            ("Weight", row.get("weight")),
        ])
        payload = EnrichmentPayload(
            field_specs=field_specs or None,
            core_description=_clean(row.get("description")),
            core_gtin=_clean_barcode(row.get("upc")),
        )
        if want_description:
            payload.description = compose_description_html(
                prose=_clean(row.get("description")),
                field_specs=field_specs,
                applications=applications,
            ) or None
        return payload


class _MotorStateSource(_FlatFeedSource):
    """
    Thin: a short description and Motor State's own notes. Their richer per-product detail is
    behind a throttled per-item API that the nightly ingest deliberately does not call.
    """

    name = "motorstate"
    provider_kind = src_enums.BrandProviderKind.MOTOR_STATE_DISTRIBUTING.value
    table = "motorstate_products"
    key_column = "part_number"
    select_columns = ("part_number", "short_description", "vendor_part_number", "data")

    def payload_from_row(self, row, *, applications, want_description):
        data = row.get("data") if isinstance(row.get("data"), dict) else {}
        field_specs = _specs([
            ("Manufacturer Part Number", row.get("vendor_part_number")),
            ("Notes", data.get("MotorStateNotes")),
        ])
        payload = EnrichmentPayload(
            field_specs=field_specs or None,
            core_description=_clean(row.get("short_description")),
        )
        if want_description:
            payload.description = compose_description_html(
                prose=_clean(row.get("short_description")),
                field_specs=field_specs,
                applications=applications,
            ) or None
        return payload


class _QuadratecSource(_FlatFeedSource):
    """Title plus description; the feed carries no media column."""

    name = "quadratec"
    provider_kind = src_enums.BrandProviderKind.QUADRATEC.value
    table = "quadratec_parts"
    key_column = "sku"
    select_columns = ("sku", "title", "description", "upc", "mpn")

    def payload_from_row(self, row, *, applications, want_description):
        field_specs = _specs([
            ("UPC", _clean_barcode(row.get("upc"))),
            ("Manufacturer Part Number", row.get("mpn")),
        ])
        payload = EnrichmentPayload(
            field_specs=field_specs or None,
            core_description=_clean(row.get("title")),
            core_gtin=_clean_barcode(row.get("upc")),
        )
        if want_description:
            payload.description = compose_description_html(
                prose=_clean(row.get("description")) or _clean(row.get("title")),
                field_specs=field_specs,
                applications=applications,
            ) or None
        return payload


#: Registry. Dict order is the recommended run order = per-field precedence (see module docstring).
SOURCES: typing.Dict[str, typing.Any] = {
    source.name: source
    for source in (
        _Turn14Source(),
        _RoughCountrySource(),
        _TheWheelGroupSource(),
        _WpsSource(),
        _WheelProsSource(),
        _PremierSource(),
        _VossenSource(),
        _EliteWheelSource(),
        _TireRackSource(),
        _HelmetHouseSource(),
        _KeystoneSource(),
        _MeyerSource(),
        _QuadratecSource(),
        _MotorStateSource(),
        _AtechSource(),
    )
}

DEFAULT_SOURCE = _Turn14Source.name

#: Run these in order and precedence takes care of itself: fill-if-blank means the first source
#: to supply a field keeps it. Ordered by how much each one actually carries per part -- measured,
#: not guessed (image fill rates: Rough Country 15,689/15,695, WPS 114,909/126,712, WheelPros
#: 61,048/69,058, Premier 150,318/686,935, A-Tech 7/1,434,995; Vossen, TireRack and Helmet House
#: have no usable image column at all).
RECOMMENDED_ORDER = (
    "rough_country",
    "the_wheel_group",
    "wps",
    "wheelpros",
    "premier",
    "vossen",
    "elite_wheel",
    "tirerack",
    "helmet_house",
    "keystone",
    "meyer",
    "quadratec",
    "motorstate",
    "atech",
)


# ---------------------------------------------------------------------------
# Generic run loop
# ---------------------------------------------------------------------------

def _blank_field_clause(fields: typing.Sequence[str]) -> str:
    """
    SQL for "this part already has a row, but at least one field this run would fill is still
    empty" -- the top-up case: a part enriched by a richer provider that left gaps a later source
    can close, without revisiting the millions of parts that are already complete.

    Only IS NULL is tested, no empty-string/empty-array checks: the writer stores None rather than
    '' or [] for anything blank (see ``or None`` on every payload field), so a non-null column
    always means a real value.
    """
    data_fields = [f for f in fields if f in DATA_FIELDS]
    core_fields = [f for f in fields if f in CORE_FIELDS]

    tests = ["d.{} IS NULL".format(f) for f in data_fields]
    tests += [
        "(mp.{col} IS NULL OR mp.{col} = '')".format(col=_CORE_FIELD_TO_COLUMN[f])
        for f in core_fields
    ]
    if not tests:
        return ""

    joined = " OR ".join(tests)
    if core_fields:
        return (
            "AND EXISTS (SELECT 1 FROM master_part_data d "
            "JOIN master_parts mp ON mp.id = d.master_part_id "
            "WHERE d.master_part_id = pp.master_part_id AND ({}))".format(joined)
        )
    return (
        "AND EXISTS (SELECT 1 FROM master_part_data d "
        "WHERE d.master_part_id = pp.master_part_id AND ({}))".format(joined)
    )


def _candidate_batch(
    source: typing.Any,
    provider_id: int,
    mode: str,
    after_provider_part_id: int,
    batch_size: int,
    brand_ids: typing.Optional[typing.Sequence[int]],
    fields: typing.Sequence[str] = (),
) -> typing.List[typing.Dict]:
    """
    One keyset page of provider parts, ordered by ``provider_parts.id`` -- a range scan on
    ``provider_parts_provider_id_id_idx`` (migration 0157) that resumes exactly where the previous
    page ended, regardless of how large the table gets.
    """
    clauses = []
    params: typing.List[typing.Any] = [provider_id, after_provider_part_id]

    if mode == MODE_MISSING_ROW:
        clauses.append(
            "AND NOT EXISTS (SELECT 1 FROM master_part_data d WHERE d.master_part_id = pp.master_part_id)"
        )
    elif mode == MODE_BLANK_FIELDS:
        clauses.append(_blank_field_clause(fields))
    elif mode == MODE_STALE:
        clauses.append(source.stale_clause())

    if brand_ids:
        clauses.append(
            "AND EXISTS (SELECT 1 FROM master_parts mp WHERE mp.id = pp.master_part_id "
            "AND mp.brand_id = ANY(%s))"
        )
        params.append(list(brand_ids))

    params.append(batch_size)
    sql = (
        "SELECT pp.id, pp.master_part_id, pp.provider_external_id "
        "FROM provider_parts pp "
        "WHERE pp.provider_id = %s AND pp.id > %s {} "
        "ORDER BY pp.id LIMIT %s".format(" ".join(clauses))
    )
    with connection.cursor() as cur:
        cur.execute(sql, params)
        return [
            {"id": r[0], "master_part_id": r[1], "provider_external_id": r[2]}
            for r in cur.fetchall()
        ]


def _write_core_fields(
    updates: typing.List[typing.Tuple[int, typing.Optional[str], typing.Optional[str], typing.Optional[str]]],
) -> None:
    """
    Bulk ``UPDATE master_parts`` from a VALUES list -- same idiom the Keystone ingest uses for
    its existing-row patch, rather than 2k individual saves.
    """
    if not updates:
        return
    placeholders = ", ".join(["(%s::bigint, %s, %s, %s)"] * len(updates))
    params: typing.List[typing.Any] = [x for row in updates for x in row]
    with connection.cursor() as cur:
        cur.execute(
            """
            UPDATE master_parts mp SET
                image_url = v.image_url,
                description = v.description,
                gtin = v.gtin,
                updated_at = NOW()
            FROM (VALUES {}) AS v(id, image_url, description, gtin)
            WHERE mp.id = v.id::bigint
            """.format(placeholders),
            params,
        )


def _apply_batch(
    rows: typing.List[typing.Dict],
    payloads: typing.Dict[int, EnrichmentPayload],
    *,
    fields: typing.Sequence[str],
    provider: src_models.Providers,
    overwrite: bool,
    dry_run: bool,
    stats: EnrichmentStats,
    collect_touched_ids: bool,
) -> None:
    data_fields = [f for f in fields if f in DATA_FIELDS]
    core_fields = [f for f in fields if f in CORE_FIELDS]
    master_part_ids = list(payloads)
    if not master_part_ids:
        return

    existing_data = {
        d.master_part_id: d
        for d in src_models.MasterPartData.objects.filter(master_part_id__in=master_part_ids)
    }
    existing_core = {
        row["id"]: row
        for row in src_models.MasterPart.objects.filter(id__in=master_part_ids).values(
            "id", "image_url", "description", "gtin"
        )
    }

    now = timezone.now()
    data_rows: typing.List[src_models.MasterPartData] = []
    core_updates: typing.List[typing.Tuple] = []

    for master_part_id, payload in payloads.items():
        touched = False

        # -- MasterPartData ------------------------------------------------
        if data_fields:
            current = existing_data.get(master_part_id)
            merged = {
                field: getattr(current, field) if current is not None else None
                for field in DATA_FIELDS
            }
            data_changed = False
            for field in data_fields:
                new_value = getattr(payload, field)
                if _is_blank(new_value):
                    continue
                if not overwrite and not _is_blank(merged[field]):
                    continue
                if merged[field] == new_value:
                    continue
                merged[field] = new_value
                data_changed = True
                stats.bump(field)

            if data_changed:
                # source_provider/source_external_id record who filled the row first; carry the
                # existing values through the upsert so a row ASAP seeded keeps its provenance.
                source_provider_id = (
                    current.source_provider_id if current is not None and current.source_provider_id else provider.id
                )
                source_external_id = (
                    current.source_external_id
                    if current is not None and current.source_external_id
                    else payload.source_external_id
                )
                data_rows.append(
                    src_models.MasterPartData(
                        master_part_id=master_part_id,
                        source_provider_id=source_provider_id,
                        source_external_id=source_external_id,
                        updated_at=now,
                        **merged,
                    )
                )
                touched = True

        # -- MasterPart core fields ----------------------------------------
        if core_fields:
            current_core = existing_core.get(master_part_id)
            if current_core is not None:
                merged_core = {
                    column: current_core[column] for column in _CORE_FIELD_TO_COLUMN.values()
                }
                core_changed = False
                for field in core_fields:
                    column = _CORE_FIELD_TO_COLUMN[field]
                    new_value = getattr(payload, field)
                    if _is_blank(new_value):
                        continue
                    if not overwrite and not _is_blank(merged_core[column]):
                        continue
                    if merged_core[column] == new_value:
                        continue
                    merged_core[column] = new_value
                    core_changed = True
                    stats.bump(field)

                if core_changed:
                    core_updates.append(
                        (
                            master_part_id,
                            merged_core["image_url"],
                            merged_core["description"],
                            merged_core["gtin"],
                        )
                    )
                    touched = True

        if touched and collect_touched_ids:
            stats.touched_master_part_ids.append(master_part_id)

    if dry_run:
        stats.data_rows_written += len(data_rows)
        stats.core_rows_written += len(core_updates)
        return

    if data_rows:
        pgbulk.upsert(
            src_models.MasterPartData,
            data_rows,
            unique_fields=["master_part"],
            update_fields=list(DATA_FIELDS) + ["source_provider", "source_external_id", "updated_at"],
        )
        stats.data_rows_written += len(data_rows)

    _write_core_fields(core_updates)
    stats.core_rows_written += len(core_updates)


def preview_payloads(
    *,
    source_name: str,
    mode: str = MODE_MISSING_ROW,
    count: int = 3,
    brand_ids: typing.Optional[typing.Sequence[int]] = None,
    start_after_provider_part_id: int = 0,
) -> typing.List[typing.Tuple[int, EnrichmentPayload]]:
    """
    Build (but never write) the payloads for the first ``count`` candidates -- so a run can be
    eyeballed before it touches 790k rows.
    """
    source = SOURCES.get(source_name)
    if source is None:
        raise ValueError("Unknown enrichment source: {}".format(source_name))

    provider = src_models.Providers.objects.filter(kind=source.provider_kind).first()
    if provider is None:
        raise ValueError("No Providers row for kind={}".format(source.provider_kind))

    out: typing.List[typing.Tuple[int, EnrichmentPayload]] = []
    last_id = start_after_provider_part_id
    while len(out) < count:
        rows = _candidate_batch(source, provider.id, mode, last_id, max(count, 50), brand_ids)
        if not rows:
            break
        last_id = rows[-1]["id"]
        payloads = source.payloads_for(rows, want_description=True)
        for row in rows:
            payload = payloads.get(row["master_part_id"])
            if payload is not None:
                out.append((row["master_part_id"], payload))
            if len(out) >= count:
                break
    return out


def enrich_master_parts(
    *,
    source_name: str,
    fields: typing.Sequence[str],
    mode: str = MODE_MISSING_ROW,
    brand_ids: typing.Optional[typing.Sequence[int]] = None,
    limit: typing.Optional[int] = None,
    batch_size: int = BATCH_SIZE,
    start_after_provider_part_id: int = 0,
    overwrite: bool = False,
    dry_run: bool = False,
    collect_touched_ids: bool = False,
) -> EnrichmentStats:
    """
    Walk every ``ProviderPart`` of ``source_name``'s provider that matches ``mode`` and fill the
    requested ``fields`` on its master part, fill-if-blank unless ``overwrite``.

    ``limit`` bounds candidates *examined* (not written), so a nightly cron can chew through the
    backlog in fixed-size bites; the returned ``last_provider_part_id`` feeds the next run's
    ``start_after_provider_part_id`` if you'd rather resume than re-scan.
    """
    source = SOURCES.get(source_name)
    if source is None:
        raise ValueError("Unknown enrichment source: {}".format(source_name))
    if mode not in MODES:
        raise ValueError("Unknown mode: {}".format(mode))

    unknown_fields = [f for f in fields if f not in ALL_FIELDS]
    if unknown_fields:
        raise ValueError("Unknown field(s): {}".format(", ".join(unknown_fields)))
    if not fields:
        raise ValueError("No fields selected.")

    provider = src_models.Providers.objects.filter(kind=source.provider_kind).first()
    if provider is None:
        raise ValueError("No Providers row for kind={}".format(source.provider_kind))

    stats = EnrichmentStats(last_provider_part_id=start_after_provider_part_id)
    want_description = "description" in fields
    last_id = start_after_provider_part_id
    batch_num = 0

    logger.info(
        "{} Starting: source={} mode={} fields={} brand_ids={} limit={} batch_size={} "
        "overwrite={} dry_run={}".format(
            _LOG_PREFIX, source_name, mode, ",".join(fields), brand_ids or "all",
            limit or "none", batch_size, overwrite, dry_run,
        )
    )

    while True:
        page_size = batch_size
        if limit is not None:
            remaining = limit - stats.candidates
            if remaining <= 0:
                break
            page_size = min(batch_size, remaining)

        rows = _candidate_batch(
            source, provider.id, mode, last_id, page_size, brand_ids, fields
        )
        if not rows:
            break

        batch_num += 1
        last_id = rows[-1]["id"]
        stats.candidates += len(rows)
        stats.last_provider_part_id = last_id

        payloads = source.payloads_for(rows, want_description=want_description)
        stats.payloads += len(payloads)

        _apply_batch(
            rows,
            payloads,
            fields=fields,
            provider=provider,
            overwrite=overwrite,
            dry_run=dry_run,
            stats=stats,
            collect_touched_ids=collect_touched_ids,
        )

        logger.info(
            "{} Batch {}: {} candidates -> {} payloads, {} data rows, {} core rows "
            "(last_provider_part_id={})".format(
                _LOG_PREFIX, batch_num, len(rows), len(payloads),
                stats.data_rows_written, stats.core_rows_written, last_id,
            )
        )
        connection.close()

    logger.info("{} Completed: {}".format(_LOG_PREFIX, stats.as_message()))
    return stats
