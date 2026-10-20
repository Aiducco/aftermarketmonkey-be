"""
Shape of S&B's two shared dealer workbooks, and of the live-inventory file they email on request.

Everything here is **what the real files actually contain**, checked against both workbooks on
2026-10-16 -- tab names, the header row the catalog actually starts on, the exact column spellings
(including S&B's own typo, ``tilte_description``), and the sentinel they use for "not applicable".
Kept apart from ``client.py`` so the parser stays small and this file is the one place to edit when
S&B change a column name.

Tabs are matched on a normalized substring rather than by exact name because two of the four move:
the pricing tab is named for the date of the last price change ("SKUs Affected by Price Change as
of Oct 7, 2026") and the Shopify export's tabs are "Shopify Export Fitment w Images" and the
leftover "Sheet3". Matching loosely means a renamed tab still ingests instead of silently
disappearing.
"""
import typing

# --------------------------------------------------------------------------------------
# Pricing master workbook ("S&B and JLT Product Pricing Master")
# --------------------------------------------------------------------------------------

#: ``All Product Info`` -- the authoritative catalog. Two rows of S&B's own notes sit above the
#: header ("See notes tab for info on packages, free freight" and the Level 2 qualification), so
#: the header is on row 3. The client finds it by looking for the ``sku`` column rather than
#: trusting that count, so an added note row doesn't break ingest.
CATALOG_TAB_KEYS = ("all product info",)
CATALOG_HEADER_MARKER_COLUMNS = ("sku",)
CATALOG_MAX_HEADER_SCAN_ROWS = 10

#: ``Fitment`` -- sku / Year / Make / Model / Engine / title / vendor.
FITMENT_TAB_KEYS = ("fitment",)

#: ``miva export`` -- PRODUCT_CODE / PRODUCT_SKU / PRODUCT_NAME / YEAR_MAKE_MODEL_ENGINE. A legacy
#: storefront dump: 21.5k SKUs against the catalog's 828, so most rows have no catalog part to
#: attach to, but the ones that do carry fitment for older SKUs the Fitment tab has dropped.
MIVA_TAB_KEYS = ("miva",)

#: ``Notes`` -- dealer terms (discount packages, free freight). Prose, not data; read so the
#: ingest log can report that it was seen, never parsed.
NOTES_TAB_KEYS = ("notes",)

CATALOG_COLUMNS = {
    "sku": ("sku",),
    "upc": ("upc",),
    "brand": ("brand",),
    "category": ("category",),
    "filter_type": ("filter_type",),
    "vehicle_type": ("vehicle_type",),
    # S&B's own spelling. Both are accepted so a corrected header keeps working.
    "title": ("tilte_description", "title_description", "title"),
    "us_retail_price": ("us retail price",),
    "canadian_retail_price": ("canadian retail price",),
    "level_1_price": ("level 1",),
    "level_2_price": ("level 2",),
    "weight": ("weight",),
    "shipping_width": ("shipping_width",),
    "shipping_height": ("shipping_height",),
    "shipping_length": ("shipping_length",),
    "prop_65_warning": ("prop_65_warning",),
    "warning_label_letter": ("warning_label_letter",),
    "tune_required": ("tune_required",),
    "emissions_code": ("emissions_code",),
    "fifty_state_street_legal": ("50_state_street_legal",),
    # S&B's own spelling again ("exampt").
    "carb_exempt_id": ("carb_exampt_id", "carb_exempt_id"),
    "iso5011_test_results": ("iso5011 test results",),
    "install_video_url": ("install_video_url",),
    "installation_instructions_url": ("installation_instructions_url",),
    "quantity_of_oil": ("quantity_of_oil",),
    "quantity_of_oil_power_stack": (
        "quantity_of_oil_power_stack_(if_applicable)",
        "quantity_of_oil_power_stack",
    ),
}

FITMENT_COLUMNS = {
    "sku": ("sku",),
    "year": ("year",),
    "make": ("make",),
    "model": ("model",),
    "engine": ("engine",),
    "title": ("title",),
    "vendor": ("vendor",),
}

MIVA_COLUMNS = {
    "product_code": ("product_code",),
    "sku": ("product_sku",),
    "title": ("product_name",),
    "fitment": ("year_make_model_engine",),
}

# --------------------------------------------------------------------------------------
# Shopify export workbook ("S&B Shopify Export with Fitment and Images")
# --------------------------------------------------------------------------------------

#: Both tabs are the same export taken at different times -- the named one is current, ``Sheet3``
#: an older pass that still covers SKUs the newer one dropped. Both are read, current first, and
#: the service only keeps the first value it sees per SKU.
SHOPIFY_TAB_KEYS = ("shopify export", "sheet3")
#: Order matters: earlier keys win when the same SKU appears in both tabs.
SHOPIFY_TAB_PRIORITY = ("shopify export", "sheet3")

SHOPIFY_COLUMNS = {
    "product_id": ("id",),
    "sku": ("variant sku",),
    "title": ("title",),
    "body_html": ("body (html)", "body html"),
    "status": ("status",),
    "product_type": ("type",),
    "vendor": ("vendor",),
    "image_src": ("image src",),
    "image_type": ("image type",),
    "fitment": (
        "metafield: convermax.fitment [list.single_line_text_field]",
        "metafield: convermax.fitment",
    ),
}

#: Shopify's export repeats a product across consecutive rows: the first row of a product carries
#: the product-level fields and the rest carry only additional images. ``Image Type`` is ``Image``
#: for those and ``VIDEO`` on a handful of rows in the older tab -- a video URL is not an image and
#: is kept separately.
SHOPIFY_IMAGE_TYPE_IMAGE = "image"
SHOPIFY_IMAGE_TYPE_VIDEO = "video"

# --------------------------------------------------------------------------------------
# Live inventory file (emailed on request, forwarded to support, dropped on the relay)
# --------------------------------------------------------------------------------------

#: **Unconfirmed format.** S&B have not yet delivered one of these, so rather than pin a layout we
#: have never seen, the parser looks for a SKU-ish column and a quantity-ish column by header name
#: and logs loudly when it cannot find them. Add the real spellings to the front of these tuples
#: once a file arrives; nothing else needs to change.
INVENTORY_SKU_COLUMNS = (
    "sku",
    "part number",
    "part_number",
    "part #",
    "part no",
    "item",
    "item number",
    "product_code",
    "product code",
)
INVENTORY_QTY_COLUMNS = (
    "available",
    "available qty",
    "available quantity",
    "qty",
    "quantity",
    "quantity available",
    "on hand",
    "available on hand",
    "stock",
    "in stock",
    "inventory",
)
INVENTORY_MAX_HEADER_SCAN_ROWS = 10


def normalize_header(value: typing.Any) -> str:
    """Header cell -> lookup key: stripped, collapsed whitespace, lowercased."""
    return " ".join(str(value or "").split()).lower()


def normalize_tab_name(value: typing.Any) -> str:
    return normalize_header(value)


def find_tab(
    sheet_names: typing.Sequence[str], tab_keys: typing.Sequence[str]
) -> typing.Optional[str]:
    """
    First sheet whose normalized name contains one of ``tab_keys``, trying the keys in order so
    callers control precedence (see SHOPIFY_TAB_PRIORITY).
    """
    normalized = [(name, normalize_tab_name(name)) for name in sheet_names]
    for key in tab_keys:
        for name, norm in normalized:
            if key in norm:
                return name
    return None


def find_tabs(
    sheet_names: typing.Sequence[str], tab_keys: typing.Sequence[str]
) -> typing.List[str]:
    """Every sheet matching any of ``tab_keys``, in ``tab_keys`` order, without duplicates."""
    normalized = [(name, normalize_tab_name(name)) for name in sheet_names]
    out: typing.List[str] = []
    for key in tab_keys:
        for name, norm in normalized:
            if key in norm and name not in out:
                out.append(name)
    return out


def map_columns(
    header: typing.Sequence[typing.Any],
    spec: typing.Mapping[str, typing.Sequence[str]],
) -> typing.Dict[str, int]:
    """
    Resolve ``{field: column index}`` from a header row. A field whose column is absent is simply
    missing from the result -- callers treat that as "the sheet doesn't carry it" rather than an
    error, since S&B add and drop optional columns between revisions.
    """
    by_name = {}
    for index, cell in enumerate(header):
        key = normalize_header(cell)
        if key and key not in by_name:
            by_name[key] = index

    out: typing.Dict[str, int] = {}
    for field, candidates in spec.items():
        for candidate in candidates:
            index = by_name.get(normalize_header(candidate))
            if index is not None:
                out[field] = index
                break
    return out


def find_column(
    header: typing.Sequence[typing.Any], candidates: typing.Sequence[str]
) -> typing.Optional[int]:
    """
    Index of the first header cell matching one of ``candidates``, exactly first and then as a
    substring. The substring pass is what lets the unconfirmed inventory file match a header like
    "Available On Hand Qty" against "available on hand".
    """
    normalized = [(index, normalize_header(cell)) for index, cell in enumerate(header)]
    for candidate in candidates:
        key = normalize_header(candidate)
        for index, cell in normalized:
            if cell == key:
                return index
    for candidate in candidates:
        key = normalize_header(candidate)
        for index, cell in normalized:
            if key and key in cell:
                return index
    return None
