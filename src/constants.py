import typing

# Provider kind_name -> display name (used by parts API)
PROVIDER_DISPLAY_NAMES = {
    "ALLPRO_DISTRIBUTING": "AllPro Distributing",
    "TURN_14": "Turn 14",
    "KEYSTONE": "Keystone",
    "MEYER": "Meyer",
    "ATECH": "A-Tech",
    "DLG": "DLG",
    "ROUGH_COUNTRY": "Rough Country",
    "SDC": "SDC",
    "WHEELPROS": "Wheel Pros",
    "AUTOMATIC_DISTRIBUTORS": "Automatic Distributors",
    "CTP_DISTRIBUTORS": "CTP Distributors",
    "CROWN_AUTOMOTIVE": "Crown Automotive",
    "DIX_PERF_NORTH": "DIX Perf North",
    "EARL_OWEN": "Earl Owen",
    "ELITE_WHEEL": "Elite Wheel",
    "FASTCO": "FastCo",
    "GRANDWEST_ENTERPRISES": "GrandWest Enterprises",
    "HELMHOUSE": "HelmHouse",
    "THIBAULT": "Thibault",
    "MARCOR": "Marcor",
    "OVERLAND_VEHICLE_SYSTEMS": "Overland Vehicle Systems",
    "PARTS_AUTHORITY": "Parts Authority",
    "PARTS_CANADA": "Parts Canada",
    "PARTS_UNLIMITED": "Parts Unlimited",
    "PREMIER_PERFORMANCE": "APG (Premier)",
    "SSF_IMPORTED_AUTO_PARTS": "SSF Imported Auto Parts",
    "THE_WHEEL_GROUP": "The Wheel Group",
    "THIBERT": "Thibert",
    "WESTERN_POWER_SPORTS": "Western Power Sports",
    "XDP": "XDP",
    "ATD": "ATD",
    "VOSSEN": "Vossen",
    "TIRERACK": "TireRack",
    "MOTOR_STATE_DISTRIBUTING": "Motor State Distributing",
    "QUADRATEC": "Quadratec",
    "TRI_STATE": "Tri-State",
}

# Provider kind_name -> image URL (used by parts API)
PROVIDER_IMAGE_URLS = {
    "ALLPRO_DISTRIBUTING": "https://api.aftermarketscout.com/uploads/allpro_logo.png",
    "TURN_14": "https://api.aftermarketscout.com/uploads/t14_logo.png",
    "KEYSTONE": "https://api.aftermarketscout.com/uploads/keystone.png",
    "MEYER": "https://api.aftermarketscout.com/uploads/meyer_logo.png",
    "ATECH": "https://api.aftermarketscout.com/uploads/atech_logo.png",
    "DLG": "https://api.aftermarketscout.com/uploads/dlg_logo.png",
    "ROUGH_COUNTRY": "https://api.aftermarketscout.com/uploads/rough_country.png",
    "SDC": "",
    "WHEELPROS": "https://api.aftermarketscout.com/uploads/wheel_pros_logo.png",
    "AUTOMATIC_DISTRIBUTORS": "https://api.aftermarketscout.com/uploads/automatic_distributors_logo.png",
    "CTP_DISTRIBUTORS": "https://api.aftermarketscout.com/uploads/ctp_distributors_logo.png",
    "CROWN_AUTOMOTIVE": "https://api.aftermarketscout.com/uploads/crown_automotive_logo.png",
    "DIX_PERF_NORTH": "https://api.aftermarketscout.com/uploads/dix_perf_north_logo.png",
    "EARL_OWEN": "https://api.aftermarketscout.com/uploads/earl_owen_logo.png",
    "ELITE_WHEEL": "https://api.aftermarketscout.com/uploads/elite_wheel_logo.png",
    "FASTCO": "https://api.aftermarketscout.com/uploads/fastco_logo.png",
    "GRANDWEST_ENTERPRISES": "https://api.aftermarketscout.com/uploads/grandwest_logo.png",
    "HELMHOUSE": "https://api.aftermarketscout.com/uploads/helmet_house_logo.png",
    "THIBAULT": "https://api.aftermarketscout.com/uploads/thibault_logo.png",
    "MARCOR": "https://api.aftermarketscout.com/uploads/marcor_logo.png",
    "OVERLAND_VEHICLE_SYSTEMS": "https://api.aftermarketscout.com/uploads/overland_vehicle_systems_logo.png",
    "PARTS_AUTHORITY": "https://api.aftermarketscout.com/uploads/parts_authority_logo.png",
    "PARTS_CANADA": "https://api.aftermarketscout.com/uploads/parts_canada_logo.png",
    "PARTS_UNLIMITED": "https://api.aftermarketscout.com/uploads/parts_unlimited_logo.png",
    "PREMIER_PERFORMANCE": "https://api.aftermarketscout.com/uploads/apg_wholesale_logo.png",
    "SSF_IMPORTED_AUTO_PARTS": "https://api.aftermarketscout.com/uploads/ssf_logo.png",
    "THE_WHEEL_GROUP": "https://api.aftermarketscout.com/uploads/the_wheel_group_logo.png",
    "THIBERT": "https://api.aftermarketscout.com/uploads/thibert_logo.png",
    "WESTERN_POWER_SPORTS": "https://api.aftermarketscout.com/uploads/wps_logo.png",
    "XDP": "https://api.aftermarketscout.com/uploads/xdp_logo.png",
    "ATD": "https://api.aftermarketscout.com/uploads/atd_logo.png",
    "VOSSEN": "https://api.aftermarketscout.com/uploads/vossen_logo.png",
    "TIRERACK": "https://api.aftermarketscout.com/uploads/tirerack_logo.png",
    "MOTOR_STATE_DISTRIBUTING": "https://api.aftermarketscout.com/uploads/motor_state_logo.png",
    "QUADRATEC": "https://api.aftermarketscout.com/uploads/quadratec_logo.png",
    "TRI_STATE": "https://api.aftermarketscout.com/uploads/tristate.png",
}

# Public "open in distributor" links (parts API ``provider_go_to_link``); ``urllib.parse.quote`` at call sites.
# DLG: site search — brand + part in ``keywords`` so short part numbers alone are not ambiguous.
DLG_B2B_INVENTORY_SEARCH_URL_TEMPLATE = "https://www.dlgb2b.com/search?keywords={keywords}"
ATECH_INVENTORY_PART_URL_TEMPLATE = "https://www.atechmotorsports.com/parts/{part_slug}"
# ``AtechParts`` qty_* columns -> labels stored in ``ProviderPartInventory.warehouse_availability`` (parts API).
ATECH_DC_QTY_FIELD_TO_LOCATION_LABEL = {
    "qty_tallmadge": "Tallmadge, OH",
    "qty_sparks": "Sparks, NV",
    "qty_mcdonough": "McDonough, GA",
    "qty_arlington": "Arlington, TX",
}
# ``EliteWheelPartWheel`` / ``EliteWheelPartTire`` qty_* columns -> the workbook's own location
# labels, stored in ``ProviderPartInventory.warehouse_availability`` (parts API). Elite names its
# columns "<Location> Available"; any location outside this set is still ingested (kept in
# ``location_availability`` and counted in the total) but has no dedicated column yet.
ELITE_WHEEL_QTY_FIELD_TO_LOCATION_LABEL = {
    "qty_tampa": "Tampa, FL",
    "qty_atlanta": "Atlanta, GA",
    "qty_miami": "Miami, FL",
    "qty_decatur": "Decatur, AL",
}
# Workbook location label (the part before " Available") -> the qty_* column it lands in.
ELITE_WHEEL_LOCATION_TO_QTY_FIELD = {
    "Tampa": "qty_tampa",
    "Atlanta": "qty_atlanta",
    "Miami": "qty_miami",
    "Decatur": "qty_decatur",
}
# Premier Performance (APG Wholesale) warehouse qty fields -> user-facing location labels (FE display).
PREMIER_WAREHOUSE_QTY_FIELD_TO_LOCATION_LABEL = {
    "nv_qty": "Nevada",
    "ky_qty": "Kentucky",
    "wa_qty": "Washington",
}
ROUGH_COUNTRY_INVENTORY_SEARCH_URL_TEMPLATE = "https://www.roughcountry.com/search/{sku}"
# WheelPros' own part-number is the page slug directly (e.g. Nitto "N205-770" ->
# https://dl.wheelpros.com/us_en/n205-770.html) -- confirmed against a real WheelProsPart row.
WHEELPROS_INVENTORY_PART_URL_TEMPLATE = "https://dl.wheelpros.com/us_en/{part_slug}.html"

# DLG: AfterMarketScout SFTP relay (single fixed endpoint; DlgSFTPClient always uses these).
DLG_RELAY_SFTP_HOST = "5.161.121.143"
DLG_RELAY_SFTP_PORT = 22
DLG_RELAY_SFTP_DIRECTORY = "uploads"
# Same as ``src.integrations.clients.dlg.feed_spec`` expected remote basename.
DLG_INVENTORY_CSV_FILENAME = "dlg_inventory.csv"
# Inbox for dealers to forward DLG’s inventory file; reading/parsing TBD. Used in provider catalog copy only.
DLG_INVENTORY_FORWARD_TO_EMAIL = "support@aftermarketscout.com"
# CompanyProviders.credentials key: the dealer’s email that receives mail from DLG (identifies the tenant in forwards).
DLG_CREDENTIALS_EMAIL_FROM = "email_from"


def dlg_b2b_search_keywords(dlg_brand_name: typing.Optional[str], part_number: str) -> str:
    """DLG B2B search: combine brand label and part number so queries are specific (not e.g. a short code alone)."""
    bn = (dlg_brand_name or "").strip()
    pn = (part_number or "").strip()
    if bn and pn:
        return "{} {}".format(bn, pn)
    return pn or bn


# CompanyProviders.credentials JSON key for Rough Country: jobber Excel URL (required per connection).
ROUGH_COUNTRY_CREDENTIALS_FEED_URL = "feed_url"

# CompanyProviders.credentials JSON keys for Quadratec: two per-connection feed URLs. The catalog
# feed (pricingSheet_quad.xlsx: Quadratec PN, MPN, Description, UPC, Brand, Retail/Wholesale price,
# Shipping Surcharge) and the pricing/inventory feed (quadratec_wholesale.csv: Quadratec Part No,
# Part No, Description, Brand, per-warehouse + total inventory, Cost, Surcharge, UPC, MAP). Both key
# on the Quadratec part number. While these are collected per connection, the ingest currently reads
# two bundled static files (see src.integrations.clients.quadratec.client DEFAULT_*_LOCAL_FILE) so a
# connection can be provisioned before the dealer's live URLs are wired up.
QUADRATEC_CREDENTIALS_CATALOG_FEED_URL = "catalog_feed_url"
QUADRATEC_CREDENTIALS_PRICING_FEED_URL = "pricing_feed_url"

# CompanyProviders.credentials JSON keys for Elite Wheel & Tire. Elite drops one
# TriWeeklyUpdate<MM-DD-YYYY>.xlsx per update (three per week) holding the whole warehouse
# snapshot -- a worksheet per wheel manufacturer plus a Tires sheet. Two transports read the same
# workbook (see src.integrations.clients.elite_wheel.client):
#   * the dealer's own SFTP account (sftp_host/sftp_port/sftp_user/sftp_password, optional
#     sftp_directory) -- what Elite hands out from their inventory request form, and the only
#     source that carries dealer prices;
#   * Elite's public inventory share (public_share_id, optionally public_share_url for a different
#     host) -- inventory only, no prices, used until a dealer account is provisioned.
# A connection with SFTP credentials uses SFTP; otherwise it falls back to the public share. Set
# ELITE_WHEEL_FORCE_PUBLIC_SHARE=True in settings to pin every connection to the public share.
ELITE_WHEEL_CREDENTIALS_SFTP_HOST = "sftp_host"
ELITE_WHEEL_CREDENTIALS_SFTP_PORT = "sftp_port"
ELITE_WHEEL_CREDENTIALS_SFTP_USER = "sftp_user"
ELITE_WHEEL_CREDENTIALS_SFTP_PASSWORD = "sftp_password"
ELITE_WHEEL_CREDENTIALS_SFTP_DIRECTORY = "sftp_directory"
ELITE_WHEEL_CREDENTIALS_PUBLIC_SHARE_ID = "public_share_id"
ELITE_WHEEL_CREDENTIALS_PUBLIC_SHARE_URL = "public_share_url"

# CompanyProviders.credentials JSON key for Vossen: AfterMarket.aspx CSV feed URL (required per connection).
VOSSEN_CREDENTIALS_FEED_URL = "feed_url"
VOSSEN_FEED_URL_HOST_PREFIX = "http://inventory.vossenwheels.com/"
# Percent *off* the feed's Price column (0-100) this company's dealer cost is computed from --
# cost = Price * (1 - discount_percent / 100). Same shape as WheelPros' wheel_markup/etc (see
# src.integrations.services.wheelpros.dealer_cost_from_msrp) but a single field since Vossen has
# only one feed/price column, not a per-category one.
VOSSEN_CREDENTIALS_DISCOUNT_PERCENT = "discount_percent"

# CompanyProviders.credentials JSON keys for TireRack's SFTP feed (password-only auth --
# confirmed live that public-key auth fails for this account while password auth succeeds, see
# src.integrations.clients.tirerack.client.TireRackSFTPClient).
TIRERACK_CREDENTIALS_SFTP_HOST = "sftp_host"
TIRERACK_CREDENTIALS_SFTP_PORT = "sftp_port"
TIRERACK_CREDENTIALS_SFTP_USER = "sftp_user"
TIRERACK_CREDENTIALS_SFTP_PASSWORD = "sftp_password"

# CompanyProviders.credentials JSON keys for Helmet House. Plain FTP on port 21 (no TLS -- their
# server offers none), host fixed in settings, so a connection only carries the login. Unlike every
# other distributor here this is NOT a per-dealer account: Helmet House publishes one shared login
# for their whole feed, so every connected company reads the same file and therefore sees the same
# dealer cost. Prices are still stored per company (HelmetHouseCompanyPricing) so the master pricing
# layer keys the same way as every other provider, and so the day Helmet House does issue per-dealer
# logins nothing downstream has to change.
#
# Never hardcode the shared username/password here -- it would commit a live credential to the repo
# and surface it in the public provider catalog response. Dealers get it from Helmet House and enter
# it themselves (see the HELMHOUSE entry in PROVIDER_CATALOG).
HELMET_HOUSE_CREDENTIALS_FTP_HOST = "ftp_host"
HELMET_HOUSE_CREDENTIALS_FTP_PORT = "ftp_port"
HELMET_HOUSE_CREDENTIALS_FTP_USER = "ftp_user"
HELMET_HOUSE_CREDENTIALS_FTP_PASSWORD = "ftp_password"
# Optional per-connection override of the catalog filename (see the client's CATALOG_FILENAMES for
# the default preference order).
HELMET_HOUSE_CREDENTIALS_CATALOG_FILENAME = "catalog_filename"

# Motor State Distributing: catalog, stock and pricing all come from the dealer's own FTP feed
# file, so a connection needs the FTP login rather than the API key it used to ask for. The
# file is named after the account number carried in the login itself
# (853809@motorstateftp.com -> 853809.csv), and host/port are fixed, so those two fields are
# the whole form.
MOTOR_STATE_CREDENTIALS_FTP_USER = "ftp_user"
MOTOR_STATE_CREDENTIALS_FTP_PASSWORD = "ftp_password"
# Optional second account used for content only (image URL, category levels, long description).
# Motor State enables those columns per account, and the account that has them is not
# necessarily the one with the freshest stock -- when they differ, the main login above drives
# catalog/stock/price and this one supplies content.
MOTOR_STATE_CREDENTIALS_CONTENT_FTP_USER = "content_ftp_user"
MOTOR_STATE_CREDENTIALS_CONTENT_FTP_PASSWORD = "content_ftp_password"
# The API key is no longer used for ingest. It stays because order placement will need it.
MOTOR_STATE_CREDENTIALS_API_KEY = "api_key"

# Helmet House brand column -> the name we resolve against Brands. Their Brand column carries a few
# in-house abbreviations that would otherwise never match ("T/M" is Tourmaster, whose own parts also
# appear spelled out in the descriptions), plus two buckets that are not brands at all ("MISC",
# "BAGS" -- shields, decals, luggage). Those two are folded into a Helmet House house brand rather
# than creating literal MISC/BAGS brands in the catalog.
#
# The last two entries pin a name the shared fuzzy matcher resolves to the wrong Brands row, both
# verified against live data:
#   * "HJC" fuzzy-matches both HJC HELMETS and HJC MOTORSPORTS - INACTIVE, and
#     best_fuzzy_brand_match breaks the tie on the longest name -- so all 10,363 HJC parts would
#     land on the inactive stub (1 master part, 0 of the feed's numbers) instead of HJC HELMETS
#     (1,652 master parts, 210 of the feed's numbers already present).
#   * "SENA" already resolves correctly, but is pinned so the mapping cannot drift if another
#     SENA* brand is ever created.
#
# "ALPINESTARS" is pinned for a different reason: the catalog carries three Alpinestars rows
# (RACE, MX, USA) and none is a natural parent, so the choice is a taxonomy decision rather than
# something the data settles -- the feed's 3,973 parts UPC-match only 26 rows in RACE and 13 in
# MX. RACE is where Western Power Sports, the closest peer powersports distributor, keeps its
# 4,187 Alpinestars parts, so sending Helmet House there is what lets the same physical part from
# the two of them resolve to one master part.
HELMET_HOUSE_HOUSE_BRAND_NAME = "HELMET HOUSE"
HELMET_HOUSE_BRAND_ALIASES = {
    "T/M": "TOURMASTER",
    "100 %": "100%",
    "MISC": HELMET_HOUSE_HOUSE_BRAND_NAME,
    "BAGS": HELMET_HOUSE_HOUSE_BRAND_NAME,
    "HJC": "HJC HELMETS",
    "SENA": "SENA TECHNOLOGIES",
    "ALPINESTARS": "ALPINESTARS RACE",
}

# Brands that must never be resolved by the fuzzy word-prefix phase -- exact and compact-key
# matching only, otherwise create the brand.
#
# fuzzy_brand_name_matches treats a single token that is a >=3-character prefix of the other as a
# match, so "FASTHOUSE" matches the unrelated existing brand "FAST" (Fuel Air Spark Technology, an
# EFI brand) and would drag all 9,386 Fast House apparel parts onto it. Confirmed against live
# data: 0 of the feed's 9,385 Fast House part numbers exist under FAST or FAST SHAFTS, while the
# brands the matcher gets right overlap by 2-23%. Fast House has no existing Brands row, so the
# correct outcome is to create one.
#
# This is a guard for this feed only; the shared matcher is used by a dozen other providers whose
# existing mappings would shift if its tie-breaking were changed.
HELMET_HOUSE_EXACT_MATCH_ONLY_BRANDS = frozenset({"FASTHOUSE"})

# WheelPros SFTP feed paths (relative; leading / added by client when downloading)
WHEELPROS_FEED_PATHS = {
    "wheel": "CommonFeed/USD/WHEEL/wheelInvPriceData.csv",
    "tire": "CommonFeed/USD/TIRE/tireInvPriceData.csv",
    "accessories": "CommonFeed/USD/ACCESSORIES/accessoriesInvPriceData.csv",
}

# Egress IP dealers must add under My Profile -> My IPs in Keystone's SDK portal. Keystone
# IP-allowlists per SDK key for both the FTP feed and the order API, and neither Keystone client
# goes through a proxy (unlike Meyer, see MEYER_ORDER_PROXY_URL) — so this is the app server's own
# outbound address, the same box as the relay SFTP host above. Shown verbatim in Keystone's
# installation instructions; if the server ever moves, every connected dealer has to re-enter it,
# so change this and the copy together.
KEYSTONE_ALLOWLIST_IP = "5.161.121.143"

# Wrapper for the blocks in installation_instructions_html a dealer is meant to copy out — relay
# endpoint details, draft emails to a distributor rep. Styles are inline because the panel renders
# this HTML with no stylesheet of its own: a bare <blockquote> came out flush with the body copy,
# so the draft email read as more instructions instead of something to send. Greys are rgba around
# mid-grey so the box reads the same on a light or dark panel background. If the frontend ever
# strips style attributes this degrades to an unstyled div — no worse than the blockquote was.
_INSTRUCTIONS_CALLOUT_STYLE = (
    "border:1px solid rgba(127,127,127,0.3);border-radius:8px;"
    "padding:4px 16px;margin:16px 0;background:rgba(127,127,127,0.08);"
)
_CALLOUT_OPEN = '<div style="' + _INSTRUCTIONS_CALLOUT_STYLE + '">'

"""
Provider catalog: list of all available providers for the integrations catalog.
Used by seed_providers command and catalog endpoint.
Each provider maps to BrandProviderKind; connection status comes from company_providers.
"""
from src import enums

PROVIDER_CATALOG = [
    {
        "kind": enums.BrandProviderKind.TURN_14,
        "name": "Turn 14",
        "description": "Access real-time inventory, pricing, and product data from Turn 14 Distribution.",
        "icon_url": "https://api.aftermarketscout.com/uploads/t14_logo.png",
        "category": "Distributors",
        "connection_required_fields": ["client_id", "client_secret"],
        # Order-placement credentials (Turn 14 Electronic Order API) — same OAuth2 client
        # id/secret shape as the feed above, but declared and validated as a distinct namespace
        # like every other distributor: catalog-API and order-API access are separate permission
        # grants on Turn 14's side even when the credential *values* are the same pair, so
        # entering (and validating) them again here confirms order placement actually works
        # instead of assuming it does because the feed connected fine. See
        # _validate_turn14_order_connection in src/api/services/integrations.py.
        "order_connection_required_fields": ["client_id", "client_secret"],
        # Email-channel ordering — always available regardless of whether the API adapter above
        # is connected; see src.enums.OrderMethod and src/integrations/orders/email_order.py.
        # A company picks the channel per order account (Settings > Integrations > Ordering).
        "email_order_connection_required_fields": ["rep_email"],
        "email_order_connection_optional_fields": ["account_number", "cc_email", "reply_to_email"],
        "integration_time": "Data available within 1-2 hours",
        # Screenshots referenced below live in ``resources/uploads`` and are served from
        # ``{_UPLOADS}`` (same host/dir as every provider ``icon_url``) — add the PNG there and it
        # is reachable at that URL. Written literally instead of interpolated because _UPLOADS is
        # defined after PROVIDER_CATALOG.
        "installation_instructions_html": (
            "<p>Turn 14 connects with a <strong>Client ID</strong> and <strong>Client Secret</strong> from your "
            "Turn 14 API settings. API access needs Turn 14's approval, and new accounts start in "
            "<strong>test mode</strong>, so follow these steps in order.</p>"

            "<p><strong>1. Open your API settings</strong></p>"
            "<p>Sign in to Turn 14 and go to "
            "<a href=\"https://www.turn14.com/api_settings.php\" target=\"_blank\" rel=\"noopener noreferrer\">"
            "turn14.com/api_settings.php</a> (or <strong>SETTINGS &amp; DATA</strong> &rarr; "
            "<strong>API</strong>).</p>"
            "<p><img src=\"https://api.aftermarketscout.com/uploads/turn14_step1_api_settings_nav.png\" "
            "alt=\"Turn 14 top navigation with SETTINGS &amp; DATA and API highlighted\" "
            "style=\"max-width:100%;height:auto;\" /></p>"

            "<p><strong>2. Get production API access</strong></p>"
            "<p>Which case applies depends on what the page shows:</p>"
            "<ul>"
            "<li>You've never used the API: accept Turn 14's API terms and conditions and request "
            "access.</li>"
            "<li>The line under <strong>API Permissions</strong> mentions only "
            "<code>apitest.turn14.com</code>: you're in test mode. Click <strong>Request Production "
            "Access</strong>.</li>"
            "<li>It mentions both <code>apitest.turn14.com</code> and <code>api.turn14.com</code>: you already "
            "have production access. Go to step 3.</li>"
            "</ul>"
            "<p>In the <strong>Developers</strong> section, enter these details and click <strong>Save</strong> "
            "before requesting either kind of access:</p>"
            "<ul>"
            "<li>Company: <code>AfterMarketScout</code></li>"
            "<li>Name: <code>Gojko Hajdukovic</code></li>"
            "<li>Email: <code>support@aftermarketscout.com</code></li>"
            "</ul>"
            "<p>Turn 14 reviews each request on their side, so wait for their approval before continuing. "
            "Until production access is approved, your credentials will save but return no data.</p>"
            "<p><img src=\"https://api.aftermarketscout.com/uploads/turn14_step3_request_production_access.png\" "
            "alt=\"Turn 14 Developers form filled in, with the Request Production Access button below the "
            "testing-server message\" style=\"max-width:100%;height:auto;\" /></p>"

            "<p><strong>3. Confirm your permissions</strong></p>"
            "<p>Under <strong>API Permissions</strong>, check that these are all ticked: <strong>Brands</strong>, "
            "<strong>Items</strong>, <strong>Inventory</strong>, <strong>Pricing</strong>, "
            "<strong>Quote</strong>, <strong>Order</strong>. If any are missing, email "
            "<a href=\"mailto:apisupport@turn14.com\">apisupport@turn14.com</a> with your account number and ask "
            "them to enable those.</p>"
            "<p><img src=\"https://api.aftermarketscout.com/uploads/turn14_step4_api_permissions.png\" "
            "alt=\"Turn 14 API Permissions checklist with all endpoints ticked\" "
            "style=\"max-width:100%;height:auto;\" /></p>"

            "<p><strong>4. Connect the product feed</strong></p>"
            "<p>Under <strong>API Credentials</strong>, copy the <strong>Client ID</strong> and "
            "<strong>Client Secret</strong>. Paste them into the <strong>Product feed</strong> fields below and "
            "click <strong>Save feed credentials</strong>.</p>"
            "<p><img src=\"https://api.aftermarketscout.com/uploads/turn14_step5_api_credentials.png\" "
            "alt=\"Turn 14 API Credentials section showing the Client ID and Client Secret fields\" "
            "style=\"max-width:100%;height:auto;\" /></p>"

            "<p><strong>5. Add an ordering account</strong></p>"
            "<p>Click <strong>+ Add account</strong> under <strong>Ordering</strong>, give it a label "
            "(e.g. &ldquo;Primary&rdquo;), paste the same Client ID and Client Secret, and click "
            "<strong>Add Account</strong>.</p>"

            "<p><strong>Troubleshooting</strong></p>"
            "<ul>"
            "<li><strong>Credentials saved but no data?</strong> Production access hasn't been approved yet. "
            "Check step 2.</li>"
            "<li><strong>Feed works but orders fail?</strong> Check that <strong>Quote</strong> and "
            "<strong>Order</strong> are ticked (step 3). Turn 14 grants them separately from catalog "
            "access.</li>"
            "<li><strong>More than one Turn 14 account?</strong> Brick-and-mortar, drop-ship and per-location "
            "accounts each have their own credentials. Add each one as a separate ordering account, under its "
            "own label.</li>"
            "</ul>"
        ),
    },
    {
        "kind": enums.BrandProviderKind.KEYSTONE,
        "name": "Keystone",
        "description": "Access inventory and pricing from Keystone Automotive via FTP.",
        "icon_url": "https://api.aftermarketscout.com/uploads/keystone.png",
        "category": "Distributors",
        "connection_required_fields": ["ftp_user", "ftp_password"],
        # Order-placement credentials (Electronic Order Web Service, SOAP) — entirely separate
        # from the FTP catalog feed above. Optional: a company can connect the FTP feed without
        # ever filling these in; order placement simply stays unavailable until they do.
        "order_connection_required_fields": ["account_number", "security_key"],
        # Email-channel ordering — see the matching note on Turn 14 above.
        "email_order_connection_required_fields": ["rep_email"],
        "email_order_connection_optional_fields": ["account_number", "cc_email", "reply_to_email"],
        "integration_time": "Data available within 1-2 hours",
        # Screenshots live in ``resources/uploads`` and are served from ``{_UPLOADS}`` — see the
        # matching note on Turn 14 above. The allowlist IP is interpolated from
        # KEYSTONE_ALLOWLIST_IP rather than typed out, so the copy can't drift from the constant.
        "installation_instructions_html": (
            "<p><strong>Keystone</strong> uses two sets of credentials, both from Keystone's SDK portal:</p>"
            "<ul>"
            "<li><strong>FTP username and password</strong> for your inventory and dealer pricing</li>"
            "<li><strong>Account number</strong> and <strong>Production Key</strong> for orders and tracking</li>"
            "</ul>"
            "<p>Your Keystone Sales Rep has to grant access, and your SDK profile must be complete before "
            "anything connects, so follow these steps in order.</p>"

            "<p><strong>1. Email your Keystone Sales Rep</strong></p>"
            "<p>Skip this if you already have SDK portal login details and a Production Key. Otherwise, "
            "send:</p>"
            + _CALLOUT_OPEN +
            "<p>Hi,</p>"
            "<p>We're setting up automated inventory and ordering on our side. Could you please:</p>"
            "<ol>"
            "<li>Grant us SDK access for FTP inventory data and API ordering</li>"
            "<li>Issue a Production Key for our account</li>"
            "</ol>"
            "<p>Thanks!</p>"
            "</div>"
            "<p>Your rep will reply with your SDK portal login.</p>"

            "<p><strong>2. Complete your SDK profile</strong></p>"
            "<p>Sign in at <a href=\"https://sdkportal.ekeystone.com/\" target=\"_blank\" "
            "rel=\"noopener noreferrer\">sdkportal.ekeystone.com</a> and scroll to <strong>My Profile</strong>. "
            "Keystone won't activate access until all three items are saved:</p>"
            "<ul>"
            "<li><strong>Registered Contacts:</strong> your contact details</li>"
            "<li><strong>My IPs:</strong> add <code>"
            + KEYSTONE_ALLOWLIST_IP +
            "</code>. Keystone blocks connections from any other address.</li>"
            "<li><strong>Signed NDA:</strong> sign and upload as directed</li>"
            "</ul>"
            "<p><img src=\"https://api.aftermarketscout.com/uploads/keystone_step2_sdk_profile.png\" "
            "alt=\"Keystone SDK portal, My Profile section\" style=\"max-width:100%;height:auto;\" /></p>"
            "<p><img src=\"https://api.aftermarketscout.com/uploads/keystone_step2_allowlist_ip.png\" "
            "alt=\"AfterMarketScout Keystone panel with the IP address to allowlist highlighted\" "
            "style=\"max-width:100%;height:auto;\" /></p>"

            "<p><strong>3. Connect the product feed</strong></p>"
            "<p>In the SDK portal, copy your <strong>FTP Username</strong> and <strong>FTP Password</strong>. "
            "Paste them into the <strong>Product feed</strong> fields below and click <strong>Save feed "
            "credentials</strong>. The card will show <strong>Feed Connected</strong>.</p>"
            "<p><img src=\"https://api.aftermarketscout.com/uploads/keystone_step3_ftp_credentials.png\" "
            "alt=\"Keystone SDK portal, FTP credentials\" style=\"max-width:100%;height:auto;\" /></p>"

            "<p><strong>4. Add an ordering account</strong></p>"
            "<p>Click <strong>+ Add account</strong> under <strong>Ordering</strong> and fill in:</p>"
            "<ul>"
            "<li><strong>Label:</strong> e.g. &ldquo;Primary&rdquo; or &ldquo;Dropship&rdquo;</li>"
            "<li><strong>Account Number:</strong> usually the numeric part of your SDK username "
            "(<code>123456admin</code> &rarr; <code>123456</code>)</li>"
            "<li><strong>Security Key:</strong> your Production Key. Test or development keys won't work.</li>"
            "</ul>"
            "<p>Click <strong>Add Account</strong>. The card will show <strong>Ordering Connected</strong>.</p>"
            "<p><img src=\"https://api.aftermarketscout.com/uploads/keystone_step4_production_key.png\" "
            "alt=\"Keystone SDK portal, Production Key\" style=\"max-width:100%;height:auto;\" /></p>"

            "<p><strong>Troubleshooting</strong></p>"
            "<ul>"
            "<li><strong>Saved but no data?</strong> Usually the IP address is missing. Check that "
            "<strong>My Profile &rarr; My IPs</strong> lists <code>"
            + KEYSTONE_ALLOWLIST_IP +
            "</code> exactly.</li>"
            "<li><strong>Feed works but orders fail?</strong> Check that you used the Production Key, not a "
            "test key, and that the account number matches your SDK username.</li>"
            "<li><strong>More than one Keystone account?</strong> Brick-and-mortar, drop-ship and per-location "
            "accounts each have their own account number and Production Key. Add each one, then use the "
            "<strong>&#8942;</strong> menu to set your default.</li>"
            "<li><strong>Don't know your Sales Rep?</strong> Contact Keystone customer service with your "
            "account number. Only Keystone can grant SDK access.</li>"
            "</ul>"
        ),
    },
    {
        "kind": enums.BrandProviderKind.ROUGH_COUNTRY,
        "name": "Rough Country",
        "description": "Access catalog, pricing, and fitment from Rough Country via jobber feed.",
        "icon_url": "https://api.aftermarketscout.com/uploads/rough_country.png",
        "category": "Distributors",
        "connection_required_fields": [ROUGH_COUNTRY_CREDENTIALS_FEED_URL],
        # Email-channel ordering — Rough Country has no order API of its own, so this is the
        # only way to place orders through AfterMarketScout for it. See the matching note on
        # Turn 14 above.
        "email_order_connection_required_fields": ["rep_email"],
        "email_order_connection_optional_fields": ["account_number", "cc_email", "reply_to_email"],
        "integration_time": "Data available within 1-2 hours",
        # Screenshots live in ``resources/uploads`` and are served from ``{_UPLOADS}`` — see the
        # matching note on Turn 14 above.
        "installation_instructions_html": (
            "<p><strong>Rough Country</strong> connects through your <strong>jobber feed URL</strong> &mdash; a "
            "private link to your dealer catalog and pricing. The URL works as your password, so don't share "
            "it.</p>"

            "<p><strong>1. Open your downloads page</strong></p>"
            "<p>Sign in at <a href=\"https://roughcountry.com/account/downloads\" target=\"_blank\" "
            "rel=\"noopener noreferrer\">roughcountry.com/account/downloads</a>. The top-left banner should read "
            "<strong>VIEWING ROUGHCOUNTRY.COM DEALER SITE</strong>. If it doesn't, you're on the retail site.</p>"

            "<p><strong>2. Copy the JOBBER link</strong></p>"
            "<p>Under <strong>PRODUCT FEEDS</strong>, right-click <strong>DOWNLOAD</strong> on the "
            "<strong>JOBBER</strong> card and choose <strong>Copy Link Address</strong> "
            "(<strong>Copy Link</strong> in Firefox/Safari). Don't click <strong>DOWNLOAD</strong> itself, and "
            "don't use the Inventory, ACES or PIES cards. The link looks like "
            "<code>https://feeds.roughcountry.com/jobber_xxxxx.xlsx</code>.</p>"
            "<p><img src=\"https://api.aftermarketscout.com/uploads/rough_country_step2_copy_jobber_link.png\" "
            "alt=\"Rough Country downloads page with the right-click menu open on the JOBBER card and Copy Link "
            "Address highlighted\" style=\"max-width:100%;height:auto;\" /></p>"

            "<p><strong>3. Paste it below</strong></p>"
            "<p>Paste the link into <strong>Feed URL</strong> and click <strong>Save feed credentials</strong>. "
            "The card will show <strong>Feed Connected</strong>.</p>"

            "<p><strong>Troubleshooting</strong></p>"
            "<ul>"
            "<li><strong>No JOBBER card?</strong> Jobber pricing may not be enabled on your account. Ask your "
            "Rough Country rep.</li>"
            "<li><strong>More than one account?</strong> Each account has its own URL. Repeat these steps for "
            "each one.</li>"
            "</ul>"
        ),
    },
    {
        "kind": enums.BrandProviderKind.QUADRATEC,
        "name": "Quadratec",
        "description": "Access catalog, per-company pricing, and inventory from Quadratec via two downloadable feeds.",
        "icon_url": "https://api.aftermarketscout.com/uploads/quadratec_logo.png",
        "category": "Distributors",
        "connection_required_fields": [
            QUADRATEC_CREDENTIALS_CATALOG_FEED_URL,
            QUADRATEC_CREDENTIALS_PRICING_FEED_URL,
        ],
        # Email-channel ordering — Quadratec has no order API of its own (same pattern as Rough
        # Country / Vossen), so this is the only way to place orders through AfterMarketScout for it.
        "email_order_connection_required_fields": ["rep_email"],
        "email_order_connection_optional_fields": ["account_number", "cc_email", "reply_to_email"],
        "integration_time": "Data available within 1-2 hours",
        "installation_instructions_html": (
            "<p><strong>Quadratec</strong> provides two downloadable dealer feeds — a catalog/pricing "
            "sheet and a wholesale cost + inventory file. Both are per dealer account.</p>"
            "<ol>"
            "<li>In your Quadratec dealer portal, locate your <strong>catalog/pricing feed</strong> "
            "(Quadratec PN, MPN, description, UPC, brand, retail &amp; wholesale price). Copy its full "
            "HTTPS URL and paste it into <strong>catalog_feed_url</strong> below.</li>"
            "<li>Locate your <strong>wholesale cost &amp; inventory feed</strong> (Quadratec Part No, "
            "part no, brand, per-warehouse and total inventory, cost, UPC, MAP). Copy its full HTTPS URL "
            "and paste it into <strong>pricing_feed_url</strong> below.</li>"
            "<li>Save the connection.</li>"
            "</ol>"
            "<p><strong>Optional: place orders through AfterMarketScout</strong></p>"
            "<p>Quadratec has no ordering API — orders are emailed to your Quadratec rep as a PDF "
            "purchase order instead. Enter your rep's <strong>email</strong> below (and an optional "
            "internal <strong>CC</strong> address) and save. You can skip it if you don't plan to place "
            "orders through AfterMarketScout.</p>"
        ),
    },
    {
        "kind": enums.BrandProviderKind.WHEELPROS,
        "name": "Wheel Pros",
        "description": "Access wheels, tires, and accessories inventory and pricing from Wheel Pros via SFTP.",
        "icon_url": "https://api.aftermarketscout.com/uploads/wheel_pros_logo.png",
        "category": "Distributors",
        "connection_required_fields": [
            "sftp_user",
            "sftp_password",
            "wheel_markup",
            "tire_markup",
            "accessories_markup",
        ],
        # Remote CSV path per feed (wheel/tire/accessories); defaults in settings if omitted
        "connection_optional_fields": ["sftp_path"],
        # Order-placement credentials (Wheel Pros Orders API, https://api.wheelpros.com) — a
        # completely separate REST API/login from the SFTP feed above. "username"/"password" are
        # the Product Data Portal account credentials, exchanged for a short-lived (1hr) Bearer
        # token via POST /auth/v1/authorize; the adapter re-authenticates as needed rather than
        # storing the token itself. No separate customer/dealer number is needed here — the
        # Orders API auto-populates it from the authenticated account (the "customer" query param
        # only exists to override that for Wheel Pros' own internal service accounts, which
        # doesn't apply to a dealer connection). Optional: a company can connect the SFTP feed
        # without ever filling these in; order placement simply stays unavailable until they do.
        "order_connection_required_fields": ["username", "password"],
        # Email-channel ordering — see the matching note on Turn 14 above.
        "email_order_connection_required_fields": ["rep_email"],
        "email_order_connection_optional_fields": ["account_number", "cc_email", "reply_to_email"],
        "integration_time": "Data available within 1-2 hours",
        # Screenshots live in ``resources/uploads`` and are served from ``{_UPLOADS}`` — see the
        # matching note on Turn 14 above.
        "installation_instructions_html": (
            "<p><strong>Wheel Pros</strong> connects to AfterMarketScout in two parts, using different "
            "credentials for each:</p>"
            "<ul>"
            "<li><strong>Product feed</strong> &mdash; inventory and pricing, delivered over SFTP "
            "(SFTP username + password).</li>"
            "<li><strong>Ordering</strong> &mdash; purchase orders through the Wheel Pros API.</li>"
            "</ul>"
            "<p>Both are managed through Wheel Pros' Product Data Portal, but each is granted separately and by "
            "a different contact. Ordering is optional.</p>"
            "<p><strong>Multiple accounts?</strong> If you have more than one Wheel Pros dealer account "
            "(brick-and-mortar, drop-ship, or separate locations), complete these steps for each one.</p>"

            "<p><strong>1. Register for the Product Data Portal</strong></p>"
            "<p>Skip this if you already sign in at <code>data.wheelpros.com</code>.</p>"
            "<ol>"
            "<li>Register at <a href=\"https://data.wheelpros.com/auth/register\" target=\"_blank\" "
            "rel=\"noopener noreferrer\">https://data.wheelpros.com/auth/register</a> using your Wheel Pros "
            "dealer account.</li>"
            "<li>Email your Wheel Pros Sales Rep:</li>"
            "</ol>"
            + _CALLOUT_OPEN +
            "<p>We've registered for an account on data.wheelpros.com. Could you please grant us access to "
            "allow FTP inventory data and API ordering?</p>"
            "</div>"

            "<p><strong>2. Set your SFTP username</strong></p>"
            "<p>The SFTP delivery option stays locked until a username exists on your account, so do this "
            "before step 3.</p>"
            "<ol>"
            "<li>Sign in to the <a href=\"https://data.wheelpros.com/\" target=\"_blank\" "
            "rel=\"noopener noreferrer\">Product Data Portal</a>.</li>"
            "<li>Click the profile icon in the upper right corner, then click <strong>Profile</strong>.</li>"
            "<li>Set your <strong>SFTP username</strong> and save.</li>"
            "</ol>"
            "<p>Note your SFTP username and password &mdash; you'll paste both into AfterMarketScout in "
            "step 4.</p>"

            "<p><strong>3. Turn on SFTP delivery</strong></p>"
            "<ol>"
            "<li>On the main menu, click <strong>DOWNLOADS</strong>.</li>"
            "<li>On the side menu, under <strong>TECH GUIDE</strong>, click <strong>DELIVERY OPTIONS</strong>.</li>"
            "<li>Switch the <strong>SFTP WHEELPROS</strong> toggle on.</li>"
            "<li>Set <strong>Notification Frequency</strong> to <strong>DAILY</strong> and <strong>File "
            "Format</strong> to <strong>CSV</strong>.</li>"
            "<li>Click <strong>SAVE CHANGES</strong>.</li>"
            "</ol>"
            "<p><img src=\"https://api.aftermarketscout.com/uploads/wheelpros_step3_delivery_options.png\" "
            "alt=\"Wheel Pros Product Data Portal, Delivery Options with the SFTP WHEELPROS toggle on\" "
            "style=\"max-width:100%;height:auto;\" /></p>"
            "<p>If the SFTP toggle won't turn on, the message beneath it tells you why: no SFTP username has "
            "been set yet. Go back to step 2.</p>"
            "<p>The <strong>EMAIL</strong> toggle above is a separate delivery method &mdash; it sends the file "
            "to an inbox and isn't used by AfterMarketScout. Leave it as-is.</p>"

            "<p><strong>4. Connect the feed in AfterMarketScout</strong></p>"
            "<ol>"
            "<li>Paste your <strong>SFTP Username</strong> and <strong>Password</strong> into the "
            "<strong>Product feed</strong> fields below.</li>"
            "<li>Enter <strong>wheel_markup</strong>, <strong>tire_markup</strong>, and "
            "<strong>accessories_markup</strong> &mdash; the percent <em>off</em> list price (0&ndash;100) your "
            "agreement gives you on each feed. We derive dealer <strong>cost</strong> from MSRP: "
            "cost = MSRP &times; (1 &minus; percent/100). All three are required; enter <code>0</code> for a "
            "feed you get no discount on.</li>"
            "<li>If your agreement uses non-default remote paths, set the optional <strong>sftp_path</strong> "
            "field.</li>"
            "<li>Click <strong>Save feed credentials</strong>.</li>"
            "</ol>"
            "<p>Once validated, the Wheel Pros card shows <strong>Feed Connected</strong>. The first file "
            "arrives on the next scheduled delivery, so data may not appear immediately.</p>"

            "<p><strong>5. Optional &mdash; place orders through AfterMarketScout</strong></p>"
            "<p>Skip this if you don't plan to send purchase orders from AfterMarketScout. API access is "
            "granted separately from feed access, and by a different team:</p>"
            "<ol>"
            "<li>Email <a href=\"mailto:data@wheelpros.com\">data@wheelpros.com</a> to request API access, "
            "referencing your dealer account.</li>"
            "<li>Once approved, you'll receive a welcome email confirming your account can make API calls.</li>"
            "<li>In the <strong>Ordering</strong> section below, click <strong>+ Add account</strong> and enter "
            "the <strong>username</strong> and <strong>password</strong> from that welcome email.</li>"
            "</ol>"

            "<p><strong>Notes</strong></p>"
            "<ul>"
            "<li><strong>Feed works but orders fail.</strong> Feed access and API access are two separate "
            "grants. Confirm you received the API welcome email from "
            "<a href=\"mailto:data@wheelpros.com\">data@wheelpros.com</a> &mdash; portal access alone doesn't "
            "enable ordering.</li>"
            "<li><strong>No data after connecting.</strong> Check that the SFTP toggle on Delivery Options is "
            "still on and that <strong>SAVE CHANGES</strong> was clicked. The toggle reverts if the page is "
            "left without saving.</li>"
            "<li><strong>Can't register for the portal?</strong> Registration requires an active Wheel Pros "
            "dealer account. Contact your Wheel Pros sales rep to open one.</li>"
            "</ul>"
        ),
    },
    {
        "kind": enums.BrandProviderKind.MEYER,
        "name": "Meyer",
        "description": "Access Meyer catalog, inventory, and pricing from AfterMarketScout's SFTP relay.",
        "icon_url": "https://api.aftermarketscout.com/uploads/meyer_logo.png",
        "category": "Distributors",
        "connection_required_fields": [],
        "connection_optional_fields": [],
        "relay_provisioned": True,
        "relay_credential_fields": ("sftp_user", "sftp_password"),
        # Order-placement credentials (Meyer REST API, https://meyerapi.meyerdistributing.com) —
        # entirely separate from the SFTP relay feed above. "api_key" is a static key issued
        # directly by a Meyer rep — per Meyer's own docs, a company issued a static key never
        # needs to call the username/password Authentication exchange at all, so we don't
        # either; it's sent as-is in every call's Authorization header. "customer_number" is
        # required on every order call (CreateOrder, CancelOrder, and most read calls) as a
        # separate account identifier from the key itself. Optional: a company can connect the
        # feed without ever filling these in; order placement simply stays unavailable until
        # they do.
        "order_connection_required_fields": ["api_key", "customer_number"],
        # Email-channel ordering — see the matching note on Turn 14 above.
        "email_order_connection_required_fields": ["rep_email"],
        "email_order_connection_optional_fields": ["account_number", "cc_email", "reply_to_email"],
        # Not "within 1-2 hours" like the API-credential providers: nothing arrives until the
        # dealer's rep sets up delivery to the relay folder, which is out of our hands and takes
        # days in practice. Processing after the first drop is quick, but that is not what a
        # dealer reading this is asking. Kept in step with the "can take a few days" line in the
        # instructions below — change the two together.
        "integration_time": "Data available after Meyer's first delivery, typically a few days",
        # {{SFTP_USER}}/{{SFTP_PASSWORD}} are substituted per company by
        # _render_relay_instructions_html (src/api/services/integrations.py) — they must stay
        # verbatim here, including inside the sample email below.
        "installation_instructions_html": (
            "<p><strong>Meyer</strong> works differently: there are no feed credentials to enter. We've already "
            "created a private SFTP folder for your company, and Meyer delivers your catalog, pricing and "
            "inventory files to it. You also need a Meyer <strong>API key</strong> and your <strong>customer "
            "number</strong>. One email to your Meyer rep covers both.</p>"

            "<p><strong>1. Email your Meyer rep</strong></p>"
            "<p>Copy the message below and send it to your Meyer account rep, with "
            "<a href=\"mailto:support@aftermarketscout.com\">support@aftermarketscout.com</a> on CC. The "
            "connection details are unique to your company and already filled in. Keep them private, because "
            "anyone who has them can write to your feed folder.</p>"
            + _CALLOUT_OPEN +
            "<p>Hi,</p>"
            "<p>We use AfterMarketScout as our product information management (PIM) and ordering platform, and "
            "need two things set up for our Meyer account:</p>"
            "<p><strong>1. SFTP feed delivery.</strong> Please deliver our pricing and inventory files to "
            "AfterMarketScout's server:</p>"
            "<p>"
            "Host: <code>5.161.121.143</code><br />"
            "Port: <code>22</code><br />"
            "Username: <code>{{SFTP_USER}}</code><br />"
            "Password: <code>{{SFTP_PASSWORD}}</code><br />"
            "Folder: <code>uploads</code>"
            "</p>"
            "<p><strong>2. API access.</strong> Please issue an API key for our account and confirm our Meyer "
            "customer number.</p>"
            "<p>I've copied AfterMarketScout support "
            "(<a href=\"mailto:support@aftermarketscout.com\">support@aftermarketscout.com</a>) in case you "
            "have any technical questions.</p>"
            "<p>Thanks!</p>"
            "</div>"

            "<p><strong>2. Click Connect</strong></p>"
            "<p>AfterMarketScout starts watching your folder. Data appears after Meyer's first delivery, which "
            "can take a few days depending on your rep.</p>"

            "<p><strong>3. Add an ordering account</strong></p>"
            "<p>When your rep sends your API key and customer number, click <strong>+ Add account</strong> under "
            "<strong>Ordering</strong> below. Enter both values and click <strong>Save</strong>.</p>"

            "<p><strong>Troubleshooting</strong></p>"
            "<ul>"
            "<li><strong>No files after a few days?</strong> Ask your rep to confirm the host, that the files "
            "are going to the <code>uploads</code> folder, and that they're named <code>Meyer Pricing.csv</code> "
            "and <code>Meyer Inventory.csv</code> exactly &mdash; we look for those names and won't see the "
            "feed under any other.</li>"
            "<li><strong>Feed works but the API key doesn't?</strong> The SFTP feed and API access are set up "
            "separately at Meyer. Check that your rep actually issued the key.</li>"
            "<li><strong>More than one Meyer account?</strong> Each account needs its own folder. Email "
            "<a href=\"mailto:support@aftermarketscout.com\">support@aftermarketscout.com</a> and we'll create "
            "another one.</li>"
            "</ul>"
        ),
    },
    {
        "kind": enums.BrandProviderKind.TRI_STATE,
        "name": "Tri-State",
        "description": "Access live pricing and inventory from Tri-State Enterprises via their API.",
        "icon_url": "https://api.aftermarketscout.com/uploads/tristate.png",
        "category": "Distributors",
        # Temporarily withheld from the catalog and onboarding picker — see the note on CTP
        # Distributors above. Delete this line to bring it back.
        "hidden": True,
        "connection_required_fields": ["app_id", "api_key"],
        "integration_time": "Data available within 1-2 hours",
        "installation_instructions_html": (
            "<p><strong>Tri-State</strong> connects through the PACE API, using an <strong>App-Id</strong> "
            "and <strong>Api-Key</strong> issued by Tri-State. The same credentials cover pricing and "
            "inventory.</p>"

            "<p><strong>1. Email Tri-State</strong></p>"
            "<p>Send the message below to Mitch Sharp at "
            "<a href=\"mailto:mitch.sharp@etristate.com\">mitch.sharp@etristate.com</a>, with "
            "<a href=\"mailto:support@aftermarketscout.com\">support@aftermarketscout.com</a> on CC so we can "
            "follow up if anything stalls.</p>"
            + _CALLOUT_OPEN +
            "<p>Hi Mitch,</p>"
            "<p>We're integrating our Tri-State account [your account number] with AfterMarketScout. Could you "
            "please:</p>"
            "<ol>"
            "<li>Grant us production access to the PACE API for pricing, inventory and ordering</li>"
            "<li>Allow connections from AfterMarketScout (IP <code>"
            + KEYSTONE_ALLOWLIST_IP +
            "</code>) for our account</li>"
            "</ol>"
            "<p>Thanks!</p>"
            "</div>"

            "<p><strong>2. Connect the feed</strong></p>"
            "<p>When Mitch sends your App-Id and Api-Key, paste them into the <strong>Product feed</strong> "
            "fields below and click <strong>Save feed credentials</strong>. The card will show "
            "<strong>Feed Connected</strong>.</p>"

            "<p><strong>Note</strong></p>"
            "<p>Tri-State's API returns pricing and inventory per item rather than a full catalog dump, so "
            "coverage starts with the brands we've already mapped and widens over time.</p>"

            "<p><strong>Troubleshooting</strong></p>"
            "<ul>"
            "<li><strong>Saved but no data?</strong> Check that you got production credentials, not test ones, "
            "and that the IP <code>"
            + KEYSTONE_ALLOWLIST_IP +
            "</code> was allowed.</li>"
            "<li><strong>More than one Tri-State account?</strong> Each account needs its own App-Id and "
            "Api-Key. Ask Mitch for one per account.</li>"
            "</ul>"
        ),
    },
    {
        "kind": enums.BrandProviderKind.ATECH,
        "name": "A-Tech",
        "description": "Access A-Tech catalog, inventory, and pricing from AfterMarketScout's SFTP relay.",
        "icon_url": "https://api.aftermarketscout.com/uploads/atech_logo.png",
        "category": "Distributors",
        "connection_required_fields": [],
        "connection_optional_fields": [],
        "relay_provisioned": True,
        "relay_credential_fields": ("sftp_user", "sftp_password"),
        # Nothing arrives until the rep sets up delivery to the relay folder — out of our hands,
        # days in practice. Same reasoning as Meyer's; kept in step with the "a few days" line
        # in the instructions below.
        "integration_time": "Data available after A-Tech's first delivery, typically a few days",
        # {{SFTP_USER}}/{{SFTP_PASSWORD}} are substituted per company by
        # _render_relay_instructions_html — see the matching note on Meyer above.
        "installation_instructions_html": (
            "<p><strong>A-Tech</strong> works differently: there are no feed credentials to enter. We've "
            "already created a private SFTP folder for your company, and A-Tech delivers one combined file to "
            "it that updates your catalog, inventory and pricing together. A-Tech has no ordering API, so "
            "orders go to your rep as a PDF purchase order by email. One email to your rep sets up both.</p>"

            "<p><strong>1. Email your A-Tech rep</strong></p>"
            "<p>Copy the message below and send it to your A-Tech rep, with "
            "<a href=\"mailto:support@aftermarketscout.com\">support@aftermarketscout.com</a> on CC. The "
            "connection details are unique to your company and already filled in. Keep them private, because "
            "anyone who has them can write to your feed folder.</p>"
            + _CALLOUT_OPEN +
            "<p>Hi,</p>"
            "<p>We use AfterMarketScout as our product information management (PIM) and ordering platform, and "
            "need two things set up for our A-Tech account:</p>"
            "<p><strong>1. SFTP feed delivery.</strong> Please deliver your standard combined catalog and "
            "pricing extract to AfterMarketScout's server:</p>"
            "<p>"
            "Host: <code>5.161.121.143</code><br />"
            "Port: <code>22</code><br />"
            "Username: <code>{{SFTP_USER}}</code><br />"
            "Password: <code>{{SFTP_PASSWORD}}</code><br />"
            "Folder: <code>uploads</code>"
            "</p>"
            "<p><strong>2. Purchase orders.</strong> Please confirm the email address we should send purchase "
            "orders to.</p>"
            "<p>I've copied AfterMarketScout support "
            "(<a href=\"mailto:support@aftermarketscout.com\">support@aftermarketscout.com</a>) in case you "
            "have any technical questions.</p>"
            "<p>Thanks!</p>"
            "</div>"

            "<p><strong>2. Click Connect</strong></p>"
            "<p>AfterMarketScout starts watching your folder. Data appears after A-Tech's first delivery, "
            "which can take a few days depending on your rep.</p>"

            "<p><strong>3. Set up ordering</strong></p>"
            "<p>Enter the purchase-order email your rep confirmed below. You can also add an internal "
            "<strong>CC</strong> so your team gets a copy of every order. Click <strong>Save</strong>.</p>"
            "<p>Orders go out as PDF emails, so confirmations and tracking come back from your rep directly "
            "rather than into AfterMarketScout.</p>"

            "<p><strong>Troubleshooting</strong></p>"
            "<ul>"
            "<li><strong>No file after a few days?</strong> Ask your rep to confirm the host, that the file is "
            "going to the <code>uploads</code> folder, and that it's named <code>atechfile.txt</code> &mdash; "
            "we look for that name and won't see the feed under any other.</li>"
            "<li><strong>Prices look out of date?</strong> Pricing only updates when A-Tech sends a new file. "
            "Ask your rep for a fresh extract.</li>"
            "<li><strong>More than one A-Tech account?</strong> Each account needs its own folder. Email "
            "<a href=\"mailto:support@aftermarketscout.com\">support@aftermarketscout.com</a> and we'll create "
            "another one.</li>"
            "</ul>"
        ),
        # Email-channel ordering — A-Tech has no order API of its own; see the matching note on
        # Turn 14 above.
        "email_order_connection_required_fields": ["rep_email"],
        "email_order_connection_optional_fields": ["account_number", "cc_email", "reply_to_email"],
    },
    {
        "kind": enums.BrandProviderKind.DLG,
        "name": "DLG",
        "description": (
            "DLG emails the inventory file to your business address. Forward that email to AftermarketScout support so we can process it."
        ),
        "icon_url": "https://api.aftermarketscout.com/uploads/dlg_logo.png",
        "category": "Distributors",
        "connection_required_fields": [DLG_CREDENTIALS_EMAIL_FROM],
        "connection_optional_fields": [],
        "integration_time": "Data available within 1-2 days",
        "installation_instructions_html": (
            "<p><strong>DLG</strong> works differently from the other distributors: there's no direct "
            "connection. DLG emails your inventory file (CSV) to your business address &mdash; the one they "
            "already have on file &mdash; and you forward that message to AfterMarketScout. We match the "
            "forward to your account and load the inventory.</p>"
            "<p>Automated email ingestion is coming later. For now, the forward is how inventory updates reach "
            "your account.</p>"
            "<p>DLG has no ordering API. If you want to place orders through AfterMarketScout, they're sent to "
            "your rep as a PDF purchase order by email.</p>"

            "<p><strong>1. Tell us which address receives DLG's email</strong></p>"
            "<p>In the <strong>{email_from_key}</strong> field below, enter the exact address that receives "
            "DLG's inventory email, then save.</p>"
            "<p>This is how we tell which company a forwarded message belongs to, so it has to match what DLG "
            "actually sends to &mdash; not a personal alias, unless that alias is the address DLG targets. If "
            "you're unsure, check the <strong>To</strong> line on the last inventory email DLG sent you.</p>"

            "<p><strong>2. Forward the inventory email to us</strong></p>"
            "<p>When DLG's inventory email arrives, forward the message (or just the CSV attachment) to "
            "<a href=\"mailto:{dlg_fwd}\">{dlg_fwd}</a>. Do this each time DLG sends an update &mdash; inventory "
            "reflects the most recent file we've received.</p>"
            "<p><strong>Worth setting up once:</strong> most mail providers let you create a rule that forwards "
            "these automatically, so nobody has to remember. In Gmail it's <strong>Settings &rarr; Filters and "
            "Blocked Addresses</strong>; in Outlook it's <strong>Rules</strong>. Filter on DLG's sending address "
            "and forward to <a href=\"mailto:{dlg_fwd}\">{dlg_fwd}</a>. Your IT team can set this up in a couple "
            "of minutes, and it's the difference between inventory that stays current and inventory that goes "
            "stale when someone's on holiday.</p>"

            "<p><strong>3. Optional &mdash; place orders through AfterMarketScout</strong></p>"
            "<p>Skip this if you don't plan to send purchase orders from AfterMarketScout. DLG has no ordering "
            "API, so orders are emailed to your rep as a PDF purchase order instead.</p>"
            "<ol>"
            "<li>Enter your DLG rep's <strong>email</strong> address below.</li>"
            "<li>Optionally add an internal <strong>CC</strong> address so your own team receives a copy of "
            "every order.</li>"
            "<li>Click <strong>Save</strong>.</li>"
            "</ol>"
            "<p>Because orders arrive as email rather than through an API, confirmation and tracking come back "
            "from your rep directly, not automatically into AfterMarketScout.</p>"

            "<p><strong>Notes</strong></p>"
            "<ul>"
            "<li><strong>Inventory looks out of date.</strong> Check whether a DLG email has arrived since the "
            "last forward. If DLG has stopped sending, contact your DLG rep &mdash; the file originates on "
            "their side.</li>"
            "<li><strong>Not receiving DLG's emails at all.</strong> Ask your DLG rep to confirm which address "
            "they have on file, then make sure the <strong>{email_from_key}</strong> value above matches it.</li>"
            "<li><strong>Forwarded but nothing appeared.</strong> The forward must come from, or reference, the "
            "address saved in <strong>{email_from_key}</strong>. If it was sent from a different mailbox, we "
            "can't match it to your account.</li>"
            "<li><strong>Need help?</strong> Contact <a href=\"mailto:{dlg_fwd}\">{dlg_fwd}</a>.</li>"
            "</ul>"
        ).format(
            dlg_fwd=DLG_INVENTORY_FORWARD_TO_EMAIL,
            email_from_key=DLG_CREDENTIALS_EMAIL_FROM,
        ),
        # Email-channel ordering — DLG has no order API of its own; see the matching note on
        # Turn 14 above.
        "email_order_connection_required_fields": ["rep_email"],
        "email_order_connection_optional_fields": ["account_number", "cc_email", "reply_to_email"],
    },
    {
        "kind": enums.BrandProviderKind.AUTOMATIC_DISTRIBUTORS,
        "name": "Automatic Distributors",
        "description": "Access inventory and pricing from Automatic Distributors via FTP.",
        "icon_url": "https://api.aftermarketscout.com/uploads/automatic_distributors_logo.png",
        "category": "Distributors",
        "connection_required_fields": ["ftp_host", "ftp_port", "ftp_user", "ftp_password"],
        # Email-channel ordering — see the matching note on Turn 14 above.
        "email_order_connection_required_fields": ["rep_email"],
        "email_order_connection_optional_fields": ["account_number", "cc_email", "reply_to_email"],
        "integration_time": "Data available within 1-2 days",
        "installation_instructions_html": (
            "<p><strong>Automatic Distributors</strong> provides an FTP account for your product data, but it "
            "isn't created automatically &mdash; a request has to be raised with your account manager first. "
            "These are the same credentials as their catalog connection, so if you already have them, skip to "
            "step 2.</p>"

            "<p><strong>1. Request FTP access</strong></p>"
            "<p>Contact your Automatic Distributors account manager and request FTP access for data feeds:</p>"
            + _CALLOUT_OPEN +
            "<p>Could you please create an FTP account for our data feeds? We're connecting our product data "
            "to our inventory system.</p>"
            "</div>"
            "<p>They'll send you an <strong>FTP Host</strong>, <strong>FTP Port</strong>, "
            "<strong>FTP Login</strong>, and <strong>FTP Password</strong>.</p>"

            "<p><strong>2. Enter your credentials</strong></p>"
            "<ol>"
            "<li>Enter the <strong>FTP Host</strong>, <strong>FTP Port</strong>, <strong>FTP Login</strong>, "
            "and <strong>FTP Password</strong> below.</li>"
            "<li>Click <strong>Save</strong>.</li>"
            "</ol>"
            "<p>Paste each value rather than retyping it &mdash; a trailing space is the most common reason a "
            "correct-looking connection fails.</p>"
            "<p>Once validated, this integration shows <strong>Feed Connected</strong>.</p>"

            "<p><strong>Notes</strong></p>"
            "<ul>"
            "<li><strong>Connection fails.</strong> Confirm the port with your account manager, then re-paste "
            "the login and password to rule out a stray space.</li>"
            "<li><strong>Already using these credentials elsewhere?</strong> They're the same as your Automatic "
            "Distributors catalog connection, so you can reuse them here &mdash; no need to request a second "
            "account.</li>"
            "<li><strong>Placing orders.</strong> Automatic Distributors has no ordering API, so orders are "
            "emailed to your rep as a PDF purchase order instead. Enter your rep's <strong>email</strong> "
            "below (and an optional internal <strong>CC</strong> address) and save &mdash; skip it if you "
            "don't plan to place orders through AfterMarketScout.</li>"
            "<li><strong>Need help?</strong> Distributor: "
            "<a href=\"mailto:Custserv@autodist.com\">Custserv@autodist.com</a> &middot; AfterMarketScout: "
            "<a href=\"mailto:support@aftermarketscout.com\">support@aftermarketscout.com</a></li>"
            "</ul>"
        ),
    },
    {
        "kind": enums.BrandProviderKind.CTP_DISTRIBUTORS,
        "name": "CTP Distributors",
        "description": "Access inventory and pricing from CTP Distributors via FTP relay.",
        "icon_url": "https://api.aftermarketscout.com/uploads/ctp_distributors_logo.png",
        "category": "Distributors",
        # Temporarily withheld from the catalog and the onboarding picker — delete this line to
        # bring it back. The entry itself stays in PROVIDER_CATALOG so credential validation,
        # syncing, and provider detail lookups keep working for anyone already connected; only the
        # two places that list connectable providers skip it (see visible_provider_catalog).
        "hidden": True,
        "connection_required_fields": [],
        "relay_provisioned": True,
        "relay_credential_fields": ("ftp_user", "ftp_password"),
        # Email-channel ordering — see the matching note on Turn 14 above.
        "email_order_connection_required_fields": ["rep_email"],
        "email_order_connection_optional_fields": ["account_number", "cc_email", "reply_to_email"],
        "integration_time": "Data available within 1-2 days",
        "installation_instructions_html": (
            "<p>AfterMarketScout has already created a dedicated FTP account for your company. "
            "Share the details below with your CTP account representative and ask them to set up "
            "their feed to connect to <strong>our</strong> endpoint.</p>"
            "<p><strong>Endpoint for your CTP rep</strong></p>"
            "<ul>"
            "<li><strong>FTP:</strong> <code>5.161.121.143</code></li>"
            "<li><strong>Port:</strong> <code>22</code></li>"
            "<li><strong>Folder:</strong> <code>uploads</code></li>"
            "<li><strong>Username:</strong> <code>{{SFTP_USER}}</code></li>"
            "<li><strong>Password:</strong> <code>{{SFTP_PASSWORD}}</code></li>"
            "</ul>"
            "<p>No credentials to enter here &mdash; just click <strong>Connect</strong>. "
            "For assistance please contact: "
            "<a href=\"https://www.ctpdistributors.com/contact\" target=\"_blank\" rel=\"noopener noreferrer\">"
            "https://www.ctpdistributors.com/contact</a>.</p>"
        ),
    },
    {
        "kind": enums.BrandProviderKind.CROWN_AUTOMOTIVE,
        "name": "Crown Automotive",
        "description": "Access Crown Automotive inventory and pricing via FTP relay.",
        "icon_url": "https://api.aftermarketscout.com/uploads/crown_automotive_logo.png",
        "category": "Distributors",
        "connection_required_fields": [],
        "relay_provisioned": True,
        "relay_credential_fields": ("ftp_user", "ftp_password"),
        # Email-channel ordering — see the matching note on Turn 14 above.
        "email_order_connection_required_fields": ["rep_email"],
        "email_order_connection_optional_fields": ["account_number", "cc_email", "reply_to_email"],
        "integration_time": "Data available within 1-2 days",
        # {{SFTP_USER}}/{{SFTP_PASSWORD}} are substituted per company by
        # _render_relay_instructions_html — see the matching note on Meyer above.
        "installation_instructions_html": (
            "<p><strong>Crown</strong> works differently: there are no feed credentials to enter. We've already "
            "created a private SFTP folder for your company, and Crown delivers your stock and pricing files to "
            "it. Orders go to your Crown rep as a PDF purchase order by email. One email to your rep sets up "
            "both.</p>"

            "<p><strong>1. Email your Crown rep</strong></p>"
            "<p>Copy the message below and send it to your Crown rep, with "
            "<a href=\"mailto:support@aftermarketscout.com\">support@aftermarketscout.com</a> on CC. The "
            "connection details are unique to your company and already filled in. Keep them private, because "
            "anyone who has them can write to your feed folder.</p>"
            + _CALLOUT_OPEN +
            "<p>Hi,</p>"
            "<p>We use AfterMarketScout as our product information management (PIM) and ordering platform, and "
            "need two things set up for our Crown account:</p>"
            "<p><strong>1. SFTP feed delivery.</strong> Please deliver our stock and pricing files to "
            "AfterMarketScout's server:</p>"
            "<p>"
            "Host: <code>5.161.121.143</code><br />"
            "Port: <code>22</code><br />"
            "Username: <code>{{SFTP_USER}}</code><br />"
            "Password: <code>{{SFTP_PASSWORD}}</code><br />"
            "Folder: <code>uploads</code>"
            "</p>"
            "<p><strong>2. Purchase orders.</strong> Please confirm the email address we should send purchase "
            "orders to.</p>"
            "<p>I've copied AfterMarketScout support "
            "(<a href=\"mailto:support@aftermarketscout.com\">support@aftermarketscout.com</a>) in case you "
            "have any technical questions.</p>"
            "<p>Thanks!</p>"
            "</div>"

            "<p><strong>2. Click Connect</strong></p>"
            "<p>AfterMarketScout starts watching your folder. Data appears after Crown's first delivery, which "
            "can take a few days depending on your rep.</p>"

            "<p><strong>3. Set up ordering</strong></p>"
            "<p>Enter the purchase-order email your rep confirmed below. You can also add an internal "
            "<strong>CC</strong> so your team gets a copy of every order. Click <strong>Save</strong>.</p>"
            "<p>Orders go out as PDF emails, so confirmations and tracking come back from your rep directly "
            "rather than into AfterMarketScout.</p>"

            "<p><strong>Troubleshooting</strong></p>"
            "<ul>"
            "<li><strong>No files after a few days?</strong> Ask your rep to confirm the host and that the "
            "files are going to the <code>uploads</code> folder.</li>"
            "<li><strong>More than one Crown account?</strong> Each account needs its own folder. Email "
            "<a href=\"mailto:support@aftermarketscout.com\">support@aftermarketscout.com</a> and we'll create "
            "another one.</li>"
            "</ul>"
        ),
    },
    {
        "kind": enums.BrandProviderKind.DIX_PERF_NORTH,
        "name": "DIX Perf North",
        "description": "Access DIX Performance North inventory and pricing via FTP relay.",
        "icon_url": "https://api.aftermarketscout.com/uploads/dix_perf_north_logo.png",
        "category": "Distributors",
        # Temporarily withheld from the catalog and onboarding picker — see the note on CTP
        # Distributors above. Delete this line to bring it back.
        "hidden": True,
        "connection_required_fields": [],
        "relay_provisioned": True,
        "relay_credential_fields": ("ftp_user", "ftp_password"),
        # Email-channel ordering — see the matching note on Turn 14 above.
        "email_order_connection_required_fields": ["rep_email"],
        "email_order_connection_optional_fields": ["account_number", "cc_email", "reply_to_email"],
        "integration_time": "Data available within 1-2 days",
        "installation_instructions_html": (
            "<p>AfterMarketScout has already created a dedicated FTP account for your company. "
            "Share the details below with your DIX account representative so they can push your "
            "stock and pricing data to <strong>our</strong> endpoint.</p>"
            "<p><strong>Endpoint for your DIX rep</strong></p>"
            "<ul>"
            "<li><strong>FTP:</strong> <code>5.161.121.143</code></li>"
            "<li><strong>Port:</strong> <code>22</code></li>"
            "<li><strong>Folder:</strong> <code>uploads</code></li>"
            "<li><strong>Username:</strong> <code>{{SFTP_USER}}</code></li>"
            "<li><strong>Password:</strong> <code>{{SFTP_PASSWORD}}</code></li>"
            "</ul>"
            "<p>No credentials to enter here &mdash; just click <strong>Connect</strong>. "
            "For assistance contact: "
            "<a href=\"mailto:sp@dixperformancenorth.com\">sp@dixperformancenorth.com</a>.</p>"
        ),
    },
    {
        "kind": enums.BrandProviderKind.ELITE_WHEEL,
        "name": "Elite Wheel",
        "description": "Access Elite Wheel & Tire inventory and pricing via SFTP, updated hourly.",
        "icon_url": "https://api.aftermarketscout.com/uploads/elite_wheel_logo.png",
        "category": "Distributors",
        "connection_required_fields": [
            ELITE_WHEEL_CREDENTIALS_SFTP_HOST,
            ELITE_WHEEL_CREDENTIALS_SFTP_PORT,
            ELITE_WHEEL_CREDENTIALS_SFTP_USER,
            ELITE_WHEEL_CREDENTIALS_SFTP_PASSWORD,
        ],
        # sftp_directory: only needed if Elite drops the workbook somewhere other than the account
        # home. public_share_id / public_share_url override the inventory-only public share the
        # shared catalog reads from (see ELITE_WHEEL_CREDENTIALS_* above).
        "connection_optional_fields": [
            ELITE_WHEEL_CREDENTIALS_SFTP_DIRECTORY,
            ELITE_WHEEL_CREDENTIALS_PUBLIC_SHARE_ID,
            ELITE_WHEEL_CREDENTIALS_PUBLIC_SHARE_URL,
        ],
        # Email-channel ordering — see the matching note on Turn 14 above.
        "email_order_connection_required_fields": ["rep_email"],
        "email_order_connection_optional_fields": ["account_number", "cc_email", "reply_to_email"],
        "integration_time": "Data available within 1-2 hours",
        "installation_instructions_html": (
            "<p><strong>Elite Wheel &amp; Tire</strong>'s warehouse inventory is already in AfterMarketScout. "
            "Connecting adds your own dealer pricing, which Elite delivers over SFTP every hour.</p>"

            "<p><strong>1. Request SFTP access</strong></p>"
            "<p>Sign in at <a href=\"https://shop.ewwfl.com/\" target=\"_blank\" rel=\"noopener noreferrer\">"
            "shop.ewwfl.com</a> and open the <a href=\"https://shop.ewwfl.com/inventory-request-form/\" "
            "target=\"_blank\" rel=\"noopener noreferrer\">inventory request form</a>. No dealer account yet? "
            "<a href=\"https://shop.ewwfl.com/dealer-application/\" target=\"_blank\" "
            "rel=\"noopener noreferrer\">Apply here</a> first.</p>"
            "<p>Fill in your business details, and for these three fields choose:</p>"
            "<ul>"
            "<li><strong>Inventory Request Type:</strong> SFTP (updated hourly)</li>"
            "<li><strong>What products are being requested:</strong> Both, unless you only stock wheels or "
            "only tires</li>"
            "<li><strong>Location of products:</strong> All</li>"
            "</ul>"
            "<p>No assigned sales rep? Answer <strong>No</strong> and leave the name blank. Click "
            "<strong>Send</strong>.</p>"
            "<p><img src=\"https://api.aftermarketscout.com/uploads/elite_wheel_step2_inventory_request_form.png\" "
            "alt=\"Elite inventory request form with SFTP selected\" style=\"max-width:100%;height:auto;\" /></p>"
            "<p>Then email <a href=\"mailto:it@ewwfl.com\">it@ewwfl.com</a>, with "
            "<a href=\"mailto:support@aftermarketscout.com\">support@aftermarketscout.com</a> on CC:</p>"
            + _CALLOUT_OPEN +
            "<p>Hi,</p>"
            "<p>We've just submitted the inventory request form for SFTP access. We're integrating our Elite "
            "account [your account number] with AfterMarketScout. Could you please also allow connections from "
            "AfterMarketScout (IP <code>"
            + KEYSTONE_ALLOWLIST_IP +
            "</code>) for our account?</p>"
            "<p>Thanks!</p>"
            "</div>"

            "<p><strong>2. Connect the product feed</strong></p>"
            "<p>When Elite emails your SFTP <strong>host</strong>, <strong>port</strong>, "
            "<strong>username</strong> and <strong>password</strong>, paste each into the "
            "<strong>Product feed</strong> fields below and click <strong>Save feed credentials</strong>. Paste "
            "rather than retype, because a stray space is the most common reason a connection fails.</p>"
            "<p>The card shows <strong>Feed Connected</strong>, and your pricing appears after the next hourly "
            "refresh.</p>"

            "<p><strong>3. Optional: ordering</strong></p>"
            "<p>Elite has no ordering API, so we email your rep a PDF purchase order instead. Add your rep's "
            "email (and an optional internal <strong>CC</strong>) to enable this.</p>"

            "<p><strong>Troubleshooting</strong></p>"
            "<ul>"
            "<li><strong>Picked Excel Sheet or API on the form by mistake?</strong> Email "
            "<a href=\"mailto:it@ewwfl.com\">it@ewwfl.com</a> and ask to switch the feed to SFTP &mdash; we "
            "can't read an emailed spreadsheet.</li>"
            "<li><strong>No reply?</strong> Follow up with <a href=\"mailto:it@ewwfl.com\">it@ewwfl.com</a>, "
            "using the business name you entered on the form.</li>"
            "<li><strong>Connection fails?</strong> Re-paste all four values. If it still fails, ask "
            "<a href=\"mailto:it@ewwfl.com\">it@ewwfl.com</a> to confirm the credentials are active and that "
            "the IP <code>"
            + KEYSTONE_ALLOWLIST_IP +
            "</code> is allowed.</li>"
            "</ul>"
        ),
    },
    {
        "kind": enums.BrandProviderKind.FASTCO,
        "name": "FastCo",
        "description": "Access FastCo inventory and pricing via FTP.",
        "icon_url": "https://api.aftermarketscout.com/uploads/fastco_logo.png",
        "category": "Distributors",
        # Temporarily withheld from the catalog and onboarding picker — see the note on CTP
        # Distributors above. Delete this line to bring it back.
        "hidden": True,
        "connection_required_fields": ["ftp_host", "ftp_port", "ftp_user", "ftp_password"],
        # Email-channel ordering — see the matching note on Turn 14 above.
        "email_order_connection_required_fields": ["rep_email"],
        "email_order_connection_optional_fields": ["account_number", "cc_email", "reply_to_email"],
        "integration_time": "Data available within 1-2 days",
        "installation_instructions_html": (
            "<p><strong>FastCo</strong> provides an FTP account for your product data. "
            "However, a request must be raised with your account manager before they will create it.</p>"
            "<ol>"
            "<li>Contact your FastCo account manager and request FTP access.</li>"
            "<li>Once you have the connection details, enter your <strong>FTP Host</strong>, "
            "<strong>FTP Port</strong>, <strong>FTP Login</strong>, and "
            "<strong>FTP Password</strong> below and save.</li>"
            "</ol>"
            "<p>For assistance contact: <a href=\"mailto:PYoshida@fastco.ca\">PYoshida@fastco.ca</a>.</p>"
        ),
    },
    {
        "kind": enums.BrandProviderKind.GRANDWEST_ENTERPRISES,
        "name": "GrandWest Enterprises",
        "description": "Access GrandWest Enterprises inventory and pricing via account feed.",
        "icon_url": "https://api.aftermarketscout.com/uploads/grandwest_logo.png",
        "category": "Distributors",
        # Temporarily withheld from the catalog and onboarding picker — see the note on CTP
        # Distributors above. Delete this line to bring it back.
        "hidden": True,
        "connection_required_fields": ["account_number"],
        "connection_optional_fields": ["access_token", "token_secret"],
        # Email-channel ordering — see the matching note on Turn 14 above.
        "email_order_connection_required_fields": ["rep_email"],
        "email_order_connection_optional_fields": ["account_number", "cc_email", "reply_to_email"],
        "integration_time": "Data available within 1-2 days",
        "installation_instructions_html": (
            "<p><strong>GrandWest Enterprises</strong> provides a data feed to keep your stock and pricing up to date.</p>"
            "<ol>"
            "<li>Contact your GrandWest sales representative and ask them for access.</li>"
            "<li>You should receive an <strong>Account Number</strong> — enter it below.</li>"
            "<li>If GrandWest also provides an <strong>Access Token</strong> and <strong>Token Secret</strong> "
            "for order tracking, enter those in the optional fields.</li>"
            "</ol>"
            "<p>For assistance contact: "
            "<a href=\"mailto:itsupport@grandwestauto.com\">itsupport@grandwestauto.com</a>.</p>"
        ),
    },
    {
        "kind": enums.BrandProviderKind.HELMHOUSE,
        "name": "HelmHouse",
        "description": "Access Helmet House price and stock data via their shared FTP feed.",
        "icon_url": "https://api.aftermarketscout.com/uploads/helmet_house_logo.png",
        "category": "Distributors",
        "connection_required_fields": ["ftp_user", "ftp_password"],
        # Email-channel ordering — see the matching note on Turn 14 above.
        "email_order_connection_required_fields": ["rep_email"],
        "email_order_connection_optional_fields": ["account_number", "cc_email", "reply_to_email"],
        "integration_time": "Data available within 1-2 hours",
        # The login is a single shared credential (not issued per dealer), so it is NOT stored
        # here -- dealers get it from Helmet House and enter it themselves. Never hardcode the
        # username/password into this copy: it would commit a live credential to the repo and
        # surface it in the public catalog response.
        "installation_instructions_html": (
            "<p><strong>Helmet House</strong> publishes price and stock data with one shared FTP login for all "
            "dealers, so there's nothing to apply for.</p>"

            "<p><strong>1. Enter the FTP login</strong></p>"
            "<p>Paste the Helmet House FTP <strong>username</strong> and <strong>password</strong> into the "
            "fields below and click <strong>Save feed credentials</strong>. The card will show "
            "<strong>Feed Connected</strong>.</p>"
            "<p>Don't have the login? Email "
            "<a href=\"mailto:info@helmethouse.com\">info@helmethouse.com</a> and ask for the current FTP "
            "username and password.</p>"

            "<p><strong>Troubleshooting</strong></p>"
            "<ul>"
            "<li><strong>Connection fails?</strong> Re-paste both values. If it still fails, the shared login "
            "may have changed without notice, so ask "
            "<a href=\"mailto:info@helmethouse.com\">info@helmethouse.com</a> for the current one.</li>"
            "</ul>"
        ),
    },
    {
        "kind": enums.BrandProviderKind.THIBAULT,
        "name": "Thibault",
        "description": "Access Thibault (Importations Thibault) inventory via their open FTP feed.",
        "icon_url": "https://api.aftermarketscout.com/uploads/thibault_logo.png",
        "category": "Distributors",
        # Temporarily withheld from the catalog and onboarding picker — see the note on CTP
        # Distributors above. Delete this line to bring it back.
        "hidden": True,
        "connection_required_fields": [],
        # Email-channel ordering — see the matching note on Turn 14 above.
        "email_order_connection_required_fields": ["rep_email"],
        "email_order_connection_optional_fields": ["account_number", "cc_email", "reply_to_email"],
        "integration_time": "Data available within 1-2 days",
        "installation_instructions_html": (
            "<p><strong>Thibault</strong> (Importations Thibault) FTP feed is already enabled for all dealers — "
            "no store-specific login credentials are required. Simply save the connection to activate it.</p>"
            "<p>For assistance contact: "
            "<a href=\"mailto:info@importationsthibault.com\">info@importationsthibault.com</a>.</p>"
        ),
    },
    {
        "kind": enums.BrandProviderKind.MARCOR,
        "name": "Marcor",
        "description": "Access Marcor Automotive public inventory and pricing — no account required.",
        "icon_url": "https://api.aftermarketscout.com/uploads/marcor_logo.png",
        # Temporarily withheld from the catalog and onboarding picker — see the note on CTP
        # Distributors above. Delete this line to bring it back.
        "hidden": True,
        "category": "Distributors",
        "connection_required_fields": [],
        # Email-channel ordering — see the matching note on Turn 14 above.
        "email_order_connection_required_fields": ["rep_email"],
        "email_order_connection_optional_fields": ["account_number", "cc_email", "reply_to_email"],
        "integration_time": "Data available within 1-2 days",
        "installation_instructions_html": (
            "<p><strong>Marcor Automotive</strong> provides a public price and stock data feed — "
            "an account is not required. Simply save the connection to activate it.</p>"
            "<p>For assistance contact: "
            "<a href=\"mailto:sales@marcor.ca\">sales@marcor.ca</a>.</p>"
        ),
    },
    {
        "kind": enums.BrandProviderKind.OVERLAND_VEHICLE_SYSTEMS,
        "name": "Overland Vehicle Systems",
        "description": "Access Overland Vehicle Systems inventory and pricing via FTP.",
        "icon_url": "https://api.aftermarketscout.com/uploads/overland_vehicle_systems_logo.png",
        "category": "Distributors",
        "connection_required_fields": ["ftp_host", "ftp_port", "ftp_user", "ftp_password"],
        # Email-channel ordering — see the matching note on Turn 14 above.
        "email_order_connection_required_fields": ["rep_email"],
        "email_order_connection_optional_fields": ["account_number", "cc_email", "reply_to_email"],
        "integration_time": "Data available within 1-2 days",
        "installation_instructions_html": (
            "<p><strong>Overland Vehicle Systems</strong> provides the pricing file via FTP. "
            "A request must first be made with Overland Vehicle Systems to enable access.</p>"
            "<ol>"
            "<li>Contact Overland Vehicle Systems to request FTP access for your account.</li>"
            "<li>Once access is enabled, enter your <strong>FTP Host</strong>, <strong>FTP Port</strong>, "
            "<strong>FTP Login</strong>, and <strong>FTP Password</strong> below and save the connection.</li>"
            "</ol>"
            "<p>For assistance contact: "
            "<a href=\"mailto:support@aftermarketscout.com\">support@aftermarketscout.com</a>.</p>"
        ),
    },
    {
        "kind": enums.BrandProviderKind.PARTS_AUTHORITY,
        "name": "Parts Authority",
        "description": "Access Parts Authority inventory and pricing via FTP.",
        "icon_url": "https://api.aftermarketscout.com/uploads/parts_authority_logo.png",
        "category": "Distributors",
        "connection_required_fields": ["ftp_host", "ftp_port", "ftp_user", "ftp_password"],
        # Email-channel ordering — see the matching note on Turn 14 above.
        "email_order_connection_required_fields": ["rep_email"],
        "email_order_connection_optional_fields": ["account_number", "cc_email", "reply_to_email"],
        "integration_time": "Data available within 1-2 days",
        "installation_instructions_html": (
            "<p><strong>Parts Authority</strong> delivers inventory and pricing via their FTP server. You "
            "request credentials from Parts Authority, then enter them here.</p>"
            "<p>The host and port are usually the same for every account, so in most cases you only need a "
            "login and password from them.</p>"

            "<p><strong>1. Request your FTP credentials</strong></p>"
            "<p>Contact Parts Authority &mdash; your account manager, or "
            "<a href=\"mailto:contactus@partsauthority.com\">contactus@partsauthority.com</a>:</p>"
            + _CALLOUT_OPEN +
            "<p>Could you please provide the FTP credentials for our inventory and pricing feed? We're "
            "connecting it to our inventory system.</p>"
            "</div>"

            "<p><strong>2. Enter the connection details</strong></p>"
            "<ol>"
            "<li>Enter the <strong>FTP Host</strong>, <strong>FTP Port</strong>, <strong>FTP Login</strong>, "
            "and <strong>FTP Password</strong> below.</li>"
            "<li>Click <strong>Save</strong>.</li>"
            "</ol>"
            "<p>Unless Parts Authority tells you otherwise, use these defaults:</p>"
            + _CALLOUT_OPEN +
            "<p>"
            "FTP Host: <code>ftp.panetny.com</code><br />"
            "FTP Port: <code>21</code>"
            "</p>"
            "</div>"
            "<p>Paste the login and password rather than retyping them &mdash; a trailing space is the most "
            "common reason a correct-looking connection fails.</p>"
            "<p>Once validated, this integration shows <strong>Feed Connected</strong>.</p>"

            "<p><strong>Notes</strong></p>"
            "<ul>"
            "<li><strong>Connection fails.</strong> Confirm the host and port against anything Parts Authority "
            "sent you &mdash; the defaults above cover most accounts, but some are pointed at a different "
            "server. Then re-paste the login and password to rule out a stray space.</li>"
            "<li><strong>Only received a username and password?</strong> That's normal. Use the default host "
            "and port above.</li>"
            "<li><strong>Need help?</strong> Distributor: "
            "<a href=\"mailto:contactus@partsauthority.com\">contactus@partsauthority.com</a> &middot; "
            "AfterMarketScout: "
            "<a href=\"mailto:support@aftermarketscout.com\">support@aftermarketscout.com</a></li>"
            "</ul>"
        ),
    },
    {
        "kind": enums.BrandProviderKind.PARTS_CANADA,
        "name": "Parts Canada",
        "description": "Access Parts Canada inventory and pricing via API access token.",
        "icon_url": "https://api.aftermarketscout.com/uploads/parts_canada_logo.png",
        "category": "Distributors",
        # Temporarily withheld from the catalog and onboarding picker — see the note on CTP
        # Distributors above. Delete this line to bring it back.
        "hidden": True,
        "connection_required_fields": ["access_token"],
        # Email-channel ordering — see the matching note on Turn 14 above.
        "email_order_connection_required_fields": ["rep_email"],
        "email_order_connection_optional_fields": ["account_number", "cc_email", "reply_to_email"],
        "integration_time": "Data available within 1-2 days",
        "installation_instructions_html": (
            "<p><strong>Parts Canada</strong> provides API access using an access token. "
            "Accounts start in sandbox mode for development; once you are ready to go live, "
            "Parts Canada will activate your production access.</p>"
            "<ol>"
            "<li>Contact your Parts Canada representative to request your <strong>production access token</strong>, "
            "which is required to retrieve live stock and pricing.</li>"
            "<li>Enter the token in the <strong>Access Token</strong> field below and save.</li>"
            "</ol>"
            "<p>For assistance contact: "
            "<a href=\"mailto:support@aftermarketscout.com\">support@aftermarketscout.com</a>.</p>"
        ),
    },
    {
        "kind": enums.BrandProviderKind.PARTS_UNLIMITED,
        "name": "Parts Unlimited",
        "description": "Access Parts Unlimited inventory and pricing via their API.",
        "icon_url": "https://api.aftermarketscout.com/uploads/parts_unlimited_logo.png",
        "category": "Distributors",
        "connection_required_fields": ["api_key"],
        # Email-channel ordering — see the matching note on Turn 14 above.
        "email_order_connection_required_fields": ["rep_email"],
        "email_order_connection_optional_fields": ["account_number", "cc_email", "reply_to_email"],
        "integration_time": "Data available within 1-2 hours",
        "installation_instructions_html": (
            "<p><strong>Parts Unlimited</strong> provides inventory and pricing through their API at "
            "<code>api.parts-unlimited.com</code>. AfterMarketScout handles the connection &mdash; you only "
            "need to enter an API key.</p>"

            "<p><strong>1. Request an API key</strong></p>"
            "<p>Contact Parts Unlimited at "
            "<a href=\"mailto:AGelsinger@parts-unltd.com\">AGelsinger@parts-unltd.com</a>:</p>"
            + _CALLOUT_OPEN +
            "<p>Could you please issue an API key for our account so we can access inventory and pricing data? "
            "We're connecting it to our inventory system.</p>"
            "</div>"
            "<p>Include your Parts Unlimited account number so they can match the request to your account.</p>"

            "<p><strong>2. Enter your API key</strong></p>"
            "<ol>"
            "<li>Paste the <strong>API key</strong> into the field below.</li>"
            "<li>Click <strong>Save</strong>.</li>"
            "</ol>"
            "<p>Paste the key rather than retyping it &mdash; these are long strings, and a single wrong "
            "character or trailing space will fail validation.</p>"
            "<p>Once validated, this integration shows <strong>Feed Connected</strong>.</p>"

            "<p><strong>Notes</strong></p>"
            "<ul>"
            "<li><strong>Connection fails.</strong> Re-paste the key to rule out a stray space or a truncated "
            "copy. If it still fails, ask <a href=\"mailto:AGelsinger@parts-unltd.com\">AGelsinger@parts-unltd.com</a> "
            "to confirm the key is active for your account.</li>"
            "<li><strong>Key stopped working.</strong> API keys can be rotated or revoked. Request a new one and "
            "paste it here &mdash; nothing else needs changing.</li>"
            "<li><strong>Need help?</strong> Distributor: "
            "<a href=\"mailto:AGelsinger@parts-unltd.com\">AGelsinger@parts-unltd.com</a> &middot; "
            "AfterMarketScout: "
            "<a href=\"mailto:support@aftermarketscout.com\">support@aftermarketscout.com</a></li>"
            "</ul>"
        ),
    },
    {
        "kind": enums.BrandProviderKind.PREMIER_PERFORMANCE,
        "name": "APG Wholesale (Premier)",
        "description": "Access APG Wholesale (Premier) inventory and pricing via FTP using your account credentials.",
        "icon_url": "https://api.aftermarketscout.com/uploads/apg_wholesale_logo.png",
        "category": "Distributors",
        "connection_required_fields": ["ftp_user", "ftp_password"],
        # Order-placement credentials (Premier REST API, https://api.premierwd.com) — entirely
        # separate from the FTP catalog feed above. A single "api_key" (issued by a Premier
        # regional rep) is exchanged for a session token via GET /authenticate?apiKey=...; the
        # adapter re-authenticates as needed rather than storing the session token itself. No
        # separate customer/account number is needed — the API key alone identifies the account,
        # and backorder/dropship preferences are configured server-side against it by Premier.
        # Optional: a company can connect the FTP feed without ever filling this in; order
        # placement simply stays unavailable until they do.
        "order_connection_required_fields": ["api_key"],
        # Email-channel ordering — see the matching note on Turn 14 above.
        "email_order_connection_required_fields": ["rep_email"],
        "email_order_connection_optional_fields": ["account_number", "cc_email", "reply_to_email"],
        "integration_time": "Data available within 1-2 hours",
        "installation_instructions_html": (
            "<p><strong>APG Wholesale (Premier)</strong> uses two sets of credentials:</p>"
            "<ul>"
            "<li><strong>FTP login and password</strong> for your daily inventory and pricing feed</li>"
            "<li><strong>API key</strong> for live stock checks, orders and shipment tracking</li>"
            "</ul>"
            "<p>They come from different APG teams, so request both in one email in step 1.</p>"

            "<p><strong>1. Email APG</strong></p>"
            "<p>Send the message below to your regional APG (Premier) rep, with "
            "<a href=\"mailto:datateam@premierwd.com\">datateam@premierwd.com</a> and "
            "<a href=\"mailto:support@aftermarketscout.com\">support@aftermarketscout.com</a> on CC:</p>"
            + _CALLOUT_OPEN +
            "<p>Hi,</p>"
            "<p>We use AfterMarketScout as our product information management (PIM) and ordering platform, and "
            "need two things set up for our APG account [your account number]:</p>"
            "<ol>"
            "<li>Data team: please set up FTP data feed access for our account and send us the FTP login and "
            "password.</li>"
            "<li>Sales: please issue an API key for the Premier Sales Orders API, so we can check stock on "
            "demand, place orders and track shipments.</li>"
            "</ol>"
            "<p>Thanks!</p>"
            "</div>"

            "<p><strong>2. Connect the product feed</strong></p>"
            "<p>When the data team sends your <strong>FTP Login</strong> and <strong>FTP Password</strong>, "
            "paste them into the <strong>Product feed</strong> fields below and click <strong>Save feed "
            "credentials</strong>. Paste rather than retype, because a stray space is the most common reason a "
            "connection fails.</p>"
            "<p>There's no host or port to enter &mdash; we connect to "
            "<code>datafeed.pppwd.com</code> on port <code>21</code> automatically.</p>"
            "<p>The card shows <strong>Feed Connected</strong>, and data arrives with the next daily "
            "refresh.</p>"

            "<p><strong>3. Add your API key</strong></p>"
            "<p>When your rep sends your API key, click <strong>+ Add account</strong> under "
            "<strong>Ordering</strong>. Enter a label (e.g. &ldquo;Primary&rdquo;) and the API key, then click "
            "<strong>Add Account</strong>. The card will show <strong>Ordering Connected</strong>.</p>"

            "<p><strong>Troubleshooting</strong></p>"
            "<ul>"
            "<li><strong>Connection fails?</strong> Re-paste the login and password. If it still fails, ask "
            "<a href=\"mailto:datateam@premierwd.com\">datateam@premierwd.com</a> to confirm the credentials "
            "are active.</li>"
            "<li><strong>Stock checks or orders don't work?</strong> The FTP login and the API key are separate "
            "grants. Check that your rep actually issued the API key.</li>"
            "<li><strong>Pricing hasn't changed today?</strong> The feed refreshes once a day, so changes made "
            "during the day appear in the next update.</li>"
            "</ul>"
        ),
    },
    {
        "kind": enums.BrandProviderKind.SSF_IMPORTED_AUTO_PARTS,
        "name": "SSF Imported Auto Parts",
        "description": "Access SSF Imported Auto Parts inventory and pricing via SFTP.",
        "icon_url": "https://api.aftermarketscout.com/uploads/ssf_logo.png",
        "category": "Distributors",
        # Temporarily withheld from the catalog and onboarding picker — see the note on CTP
        # Distributors above. Delete this line to bring it back.
        "hidden": True,
        "connection_required_fields": ["account_number", "sftp_host", "sftp_port", "sftp_user", "sftp_password"],
        # Email-channel ordering — see the matching note on Turn 14 above.
        "email_order_connection_required_fields": ["rep_email"],
        "email_order_connection_optional_fields": ["account_number", "cc_email", "reply_to_email"],
        "integration_time": "Data available within 1-2 days",
        "installation_instructions_html": (
            "<p><strong>SSF Imported Auto Parts</strong> provides FTP accounts for stock and pricing data. "
            "SSF data is confidential and proprietary and may be used only to search for and purchase products.</p>"
            "<ol>"
            "<li>Contact your SSF representative to obtain your <strong>Account Number</strong>, "
            "<strong>SFTP Host</strong>, <strong>SFTP Port</strong>, "
            "<strong>SFTP Login</strong>, and <strong>SFTP Password</strong>.</li>"
            "<li>Enter all five fields below and save the connection.</li>"
            "</ol>"
            "<p>For assistance contact: "
            "<a href=\"mailto:info@ssfautoparts.com\">info@ssfautoparts.com</a>.</p>"
        ),
    },
    {
        "kind": enums.BrandProviderKind.THE_WHEEL_GROUP,
        "name": "The Wheel Group",
        "description": "Access The Wheel Group's wheel catalog and pricing via AfterMarketScout's SFTP relay.",
        "icon_url": "https://api.aftermarketscout.com/uploads/the_wheel_group_logo.png",
        "category": "Distributors",
        "connection_required_fields": [],
        "relay_provisioned": True,
        "relay_credential_fields": ("ftp_user", "ftp_password"),
        # Email-channel ordering — see the matching note on Turn 14 above.
        "email_order_connection_required_fields": ["rep_email"],
        "email_order_connection_optional_fields": ["account_number", "cc_email", "reply_to_email"],
        # Catalog is live the moment they connect (we read Wheel Group's published mastersheet),
        # but their own cost and stock wait on the rep setting up delivery to the relay folder —
        # out of our hands, days in practice. Kept in step with the "a few days" line in the
        # instructions below; change the two together.
        "integration_time": "Catalog available immediately; your pricing after their first delivery",
        # {{SFTP_USER}}/{{SFTP_PASSWORD}} are substituted per company by
        # _render_relay_instructions_html — see the matching note on Meyer above.
        "installation_instructions_html": (
            "<p><strong>The Wheel Group</strong> works differently: there are no feed credentials to enter. "
            "Their full wheel catalog, with specs, images, MSRP and MAP, is already in AfterMarketScout. "
            "Connecting adds your own dealer pricing and stock, which The Wheel Group delivers to a private "
            "SFTP folder we've created for your company. Orders go to your rep as a PDF purchase order by "
            "email. One email to your rep sets up both.</p>"

            "<p><strong>1. Email your Wheel Group rep</strong></p>"
            "<p>Copy the message below and send it to your account rep, or to "
            "<a href=\"mailto:prestonw@wheel-1.com\">prestonw@wheel-1.com</a>, with "
            "<a href=\"mailto:support@aftermarketscout.com\">support@aftermarketscout.com</a> on CC. The "
            "connection details are unique to your company and already filled in. Keep them private, because "
            "anyone who has them can write to your feed folder.</p>"
            + _CALLOUT_OPEN +
            "<p>Hi,</p>"
            "<p>We use AfterMarketScout as our product information management (PIM) and ordering platform, and "
            "need two things set up for our Wheel Group account:</p>"
            "<p><strong>1. SFTP feed delivery.</strong> Please deliver our pricing and inventory files to "
            "AfterMarketScout's server:</p>"
            "<p>"
            "Host: <code>5.161.121.143</code><br />"
            "Port: <code>22</code><br />"
            "Username: <code>{{SFTP_USER}}</code><br />"
            "Password: <code>{{SFTP_PASSWORD}}</code><br />"
            "Folder: <code>uploads</code>"
            "</p>"
            "<p><strong>2. Purchase orders.</strong> Please confirm the email address we should send purchase "
            "orders to.</p>"
            "<p>I've copied AfterMarketScout support "
            "(<a href=\"mailto:support@aftermarketscout.com\">support@aftermarketscout.com</a>) in case you "
            "have any technical questions.</p>"
            "<p>Thanks!</p>"
            "</div>"

            "<p><strong>2. Click Connect</strong></p>"
            "<p>AfterMarketScout starts watching your folder. The catalog with MSRP and MAP is available right "
            "away. Your cost and stock appear after The Wheel Group's first delivery, which can take a few days "
            "depending on your rep.</p>"

            "<p><strong>3. Set up ordering</strong></p>"
            "<p>Enter the purchase-order email your rep confirmed below. You can also add an internal "
            "<strong>CC</strong> so your team gets a copy of every order. Click <strong>Save</strong>.</p>"
            "<p>Orders go out as PDF emails, so confirmations and tracking come back from your rep directly "
            "rather than into AfterMarketScout.</p>"

            "<p><strong>Troubleshooting</strong></p>"
            "<ul>"
            "<li><strong>No files after a few days?</strong> Ask your rep to confirm the host and that the "
            "files are going to the <code>uploads</code> folder. Your catalog and list pricing keep working in "
            "the meantime.</li>"
            "<li><strong>More than one Wheel Group account?</strong> Each account needs its own folder. Email "
            "<a href=\"mailto:support@aftermarketscout.com\">support@aftermarketscout.com</a> and we'll create "
            "another one.</li>"
            "</ul>"
        ),
    },
    {
        "kind": enums.BrandProviderKind.THIBERT,
        "name": "Thibert",
        "description": "Access Thibert inventory and pricing via FTP.",
        "icon_url": "https://api.aftermarketscout.com/uploads/thibert_logo.png",
        "category": "Distributors",
        # Temporarily withheld from the catalog and onboarding picker — see the note on CTP
        # Distributors above. Delete this line to bring it back.
        "hidden": True,
        "connection_required_fields": ["ftp_host", "ftp_port", "ftp_user", "ftp_password"],
        # Email-channel ordering — see the matching note on Turn 14 above.
        "email_order_connection_required_fields": ["rep_email"],
        "email_order_connection_optional_fields": ["account_number", "cc_email", "reply_to_email"],
        "integration_time": "Data available within 1-2 days",
        "installation_instructions_html": (
            "<p><strong>Thibert</strong> provides an FTP account for your product data. "
            "However, a request must be raised with your account manager before they will create it.</p>"
            "<ol>"
            "<li>Contact your Thibert account manager to request FTP access.</li>"
            "<li>Once you have your credentials, enter your <strong>FTP Host</strong>, <strong>FTP Port</strong>, "
            "<strong>FTP Login</strong>, and <strong>FTP Password</strong> below and save.</li>"
            "</ol>"
            "<p>For assistance contact: "
            "<a href=\"mailto:infoecommerce@rthibert.com\">infoecommerce@rthibert.com</a>.</p>"
        ),
    },
    {
        "kind": enums.BrandProviderKind.WESTERN_POWER_SPORTS,
        "name": "Western Power Sports",
        "description": "Access Western Power Sports inventory and pricing via their Data Depot API.",
        "icon_url": "https://api.aftermarketscout.com/uploads/wps_logo.png",
        "category": "Distributors",
        "connection_required_fields": ["api_key"],
        # Email-channel ordering — see the matching note on Turn 14 above.
        "email_order_connection_required_fields": ["rep_email"],
        "email_order_connection_optional_fields": ["account_number", "cc_email", "reply_to_email"],
        "integration_time": "Data available within 1-2 hours",
        "installation_instructions_html": (
            "<p><strong>Western Power Sports (WPS)</strong> connects with an API token you generate yourself "
            "in WPS's Data Depot. One token covers your catalog, inventory and pricing.</p>"

            "<p><strong>1. Generate your API token</strong></p>"
            "<p>Sign in to the <a href=\"https://www.wps-inc.com/data-depot/v4/front\" target=\"_blank\" "
            "rel=\"noopener noreferrer\">WPS Data Depot</a> with your wpsorders.com login:</p>"
            "<ul>"
            "<li><strong>Dealer ID:</strong> with the letter in front, e.g. <code>D12345</code> or "
            "<code>S12345</code>. It is never <code>admin</code> or a person's name.</li>"
            "<li><strong>Password:</strong> your wpsorders password</li>"
            "</ul>"
            "<p>Then go to <strong>API Documentation</strong> &rarr; "
            "<a href=\"https://www.wps-inc.com/data-depot/v4/api/api-token\" target=\"_blank\" "
            "rel=\"noopener noreferrer\">API Token</a>, sign up for a token and copy it.</p>"
            "<p><img src=\"https://api.aftermarketscout.com/uploads/wps_step1_api_token.png\" "
            "alt=\"WPS Data Depot API Token page with the issued token shown\" "
            "style=\"max-width:100%;height:auto;\" /></p>"
            "<p>No wpsorders login? Your WPS account manager can set one up.</p>"

            "<p><strong>2. Connect the product feed</strong></p>"
            "<p>Paste the token into the <strong>Product feed</strong> field below and click <strong>Save feed "
            "credentials</strong>. The card will show <strong>Feed Connected</strong>.</p>"

            "<p><strong>3. Set up ordering</strong></p>"
            "<p>WPS orders go to your rep as a PDF purchase order by email. Enter your rep's email below (and "
            "an optional internal <strong>CC</strong> so your team gets a copy of every order) and click "
            "<strong>Save</strong>.</p>"

            "<p><strong>Troubleshooting</strong></p>"
            "<ul>"
            "<li><strong>Can't sign in?</strong> Use your Dealer ID with its letter prefix, not a "
            "username.</li>"
            "<li><strong>Connection fails?</strong> Re-paste the token, since the long string is easy to cut "
            "off when copying. If it still fails, ask "
            "<a href=\"mailto:webservices@wps-inc.com\">webservices@wps-inc.com</a> to confirm it's "
            "active.</li>"
            "<li><strong>Token stopped working?</strong> Tokens can be rotated or revoked. Generate a new one "
            "in the Data Depot and paste it in below.</li>"
            "</ul>"
        ),
    },
    # XDP removed from the connectable catalog 2026-10-13: it was listed as "no account required"
    # with no backend ever built -- no client, no fetch command, no data source confirmed. Two
    # companies (Trident Motorsports, Recon Overland & Off Road) had already connected to it with
    # nothing behind it. PROVIDER_NAMES/PROVIDER_IMAGE_URLS above still carry an "XDP" entry so
    # those two existing (non-functional) CompanyProviders rows keep a name/icon to display rather
    # than rendering blank -- only the catalog entry that offers it for a *new* connection is gone.
    {
        "kind": enums.BrandProviderKind.ATD,
        "name": "ATD",
        "description": "Connect to American Tire Distributors (ATD) Ship to Home API for inventory and pricing.",
        "icon_url": "https://api.aftermarketscout.com/uploads/atd_logo.png",
        "category": "Distributors",
        "connection_required_fields": ["username", "password", "client_id", "location"],
        # Email-channel ordering — see the matching note on Turn 14 above.
        "email_order_connection_required_fields": ["rep_email"],
        "email_order_connection_optional_fields": ["account_number", "cc_email", "reply_to_email"],
        "integration_time": "Data available within 1-2 hours",
        "installation_instructions_html": (
            "<p><strong>ATD (American Tire Distributors)</strong> delivers inventory and pricing through their "
            "Ship to Home API. Connecting takes four values from ATD &mdash; they're issued together, so one "
            "request covers everything.</p>"

            "<p><strong>1. Request your API credentials</strong></p>"
            "<p>Contact your ATD representative, or "
            "<a href=\"mailto:bmoyer@atd-us.com\">bmoyer@atd-us.com</a>:</p>"
            + _CALLOUT_OPEN +
            "<p>Could you please provide our Ship to Home API credentials &mdash; username, password, client "
            "ID, and location code? We're connecting our inventory and pricing to our inventory system.</p>"
            "</div>"
            "<p>Include your ATD account number so they can match the request to your account.</p>"

            "<p><strong>2. Enter your credentials</strong></p>"
            "<p>Enter all four values below, then click <strong>Save</strong>.</p>"
            "<ul>"
            "<li><strong>Username</strong> &mdash; your ATD API username, issued for the API, not your ATD "
            "website login.</li>"
            "<li><strong>Password</strong> &mdash; your ATD API password.</li>"
            "<li><strong>Client ID</strong> &mdash; your ATD client identifier.</li>"
            "<li><strong>Location</strong> &mdash; your ATD location code, which determines the warehouse your "
            "availability and pricing come from.</li>"
            "</ul>"
            "<p>Paste each value rather than retyping it &mdash; a trailing space is the most common reason a "
            "correct-looking connection fails.</p>"
            "<p>Once validated, this integration shows <strong>Feed Connected</strong>.</p>"

            "<p><strong>Notes</strong></p>"
            "<ul>"
            "<li><strong>Connection fails.</strong> Re-paste each value to rule out a stray space, then check "
            "the client ID in particular &mdash; it's easy to confuse with the username. If it still fails, ask "
            "your ATD rep to confirm the credentials are active.</li>"
            "<li><strong>Connected, but availability looks wrong.</strong> Check the <strong>Location</strong> "
            "code. If it points at a different warehouse than you buy from, stock and pricing will both look off "
            "even though the connection is working.</li>"
            "<li><strong>More than one ATD location?</strong> Each location code returns that warehouse's "
            "availability. Ask your ATD rep which code matches the location you order from.</li>"
            "<li><strong>Need help?</strong> Distributor: "
            "<a href=\"mailto:bmoyer@atd-us.com\">bmoyer@atd-us.com</a> &middot; AfterMarketScout: "
            "<a href=\"mailto:support@aftermarketscout.com\">support@aftermarketscout.com</a></li>"
            "</ul>"
        ),
    },
    {
        "kind": enums.BrandProviderKind.VOSSEN,
        "name": "Vossen",
        "description": "Access Vossen wheel inventory and pricing via their AfterMarket CSV feed.",
        "icon_url": "https://api.aftermarketscout.com/uploads/vossen_logo.png",
        "category": "Distributors",
        "connection_required_fields": [VOSSEN_CREDENTIALS_FEED_URL, VOSSEN_CREDENTIALS_DISCOUNT_PERCENT],
        "integration_time": "Data available within 1-2 hours",
        "installation_instructions_html": (
            "<p><strong>Vossen</strong> provides a downloadable CSV inventory feed "
            "(<code>AfterMarket.aspx</code>), unique to each dealer account. There's no API key or password "
            "&mdash; the URL itself is your credential, so treat it as private.</p>"
            "<p>The feed carries Vossen's <strong>list price</strong> rather than your dealer cost, so you'll "
            "also enter your discount percentage and we'll calculate cost from it.</p>"
            "<p>Vossen has no ordering API. If you want to place orders through AfterMarketScout, they're sent "
            "to your rep as a PDF purchase order by email.</p>"

            "<p><strong>1. Get your dealer feed URL</strong></p>"
            "<p>Contact your Vossen rep and ask for your dealer inventory feed URL. It starts with "
            "<code>{host_prefix}</code> and ends in <code>AfterMarket.aspx</code>.</p>"
            "<p>The URL is specific to your dealer account &mdash; another dealer's link won't return your "
            "pricing.</p>"

            "<p><strong>2. Paste the URL and set your discount</strong></p>"
            "<ol>"
            "<li>Paste the full URL into <strong>{feed_url_key}</strong> below.</li>"
            "<li>Enter your <strong>{discount_key}</strong> &mdash; the percent <em>off</em> the feed's list "
            "price (0&ndash;100) that represents your dealer cost.</li>"
            "<li>Click <strong>Save</strong>.</li>"
            "</ol>"
            "<p>We calculate cost as:</p>"
            + _CALLOUT_OPEN +
            "<p><code>cost = price &times; (1 &minus; {discount_key} / 100)</code></p>"
            "</div>"
            "<p>So a 40% dealer discount on a $1,000 wheel gives a cost of $600. If you're unsure of your "
            "percentage, your Vossen rep can confirm it &mdash; entering the wrong number won't break the "
            "connection, but every cost and margin figure will be off.</p>"

            "<p><strong>3. Optional &mdash; place orders through AfterMarketScout</strong></p>"
            "<p>Skip this if you don't plan to send purchase orders from AfterMarketScout. Vossen has no "
            "ordering API, so orders are emailed to your rep as a PDF purchase order instead.</p>"
            "<ol>"
            "<li>Enter your Vossen rep's <strong>email</strong> address below.</li>"
            "<li>Optionally add an internal <strong>CC</strong> address so your own team receives a copy of "
            "every order.</li>"
            "<li>Click <strong>Save</strong>.</li>"
            "</ol>"
            "<p>Because orders arrive as email rather than through an API, confirmation and tracking come back "
            "from your rep directly, not automatically into AfterMarketScout.</p>"

            "<p><strong>Notes</strong></p>"
            "<ul>"
            "<li><strong>Costs look wrong across the board.</strong> This is almost always the discount "
            "percentage. Check <strong>{discount_key}</strong> against what your rep quoted &mdash; it's a "
            "percent <em>off</em> list, not the percentage you pay.</li>"
            "<li><strong>Your discount changed.</strong> Update <strong>{discount_key}</strong> here and save. "
            "Costs recalculate from the feed's list prices; there's no need to touch the URL.</li>"
            "<li><strong>Feed stopped updating.</strong> Ask your Vossen rep to confirm your feed URL is still "
            "active &mdash; dealer links can be reissued.</li>"
            "<li><strong>Need help?</strong> Contact "
            "<a href=\"mailto:support@aftermarketscout.com\">support@aftermarketscout.com</a>.</li>"
            "</ul>"
        ).format(
            host_prefix=VOSSEN_FEED_URL_HOST_PREFIX,
            feed_url_key=VOSSEN_CREDENTIALS_FEED_URL,
            discount_key=VOSSEN_CREDENTIALS_DISCOUNT_PERCENT,
        ),
        # Email-channel ordering — Vossen has no order API of its own; see the matching note on
        # Turn 14 above.
        "email_order_connection_required_fields": ["rep_email"],
        "email_order_connection_optional_fields": ["account_number", "cc_email", "reply_to_email"],
    },
    {
        "kind": enums.BrandProviderKind.TIRERACK,
        "name": "TireRack",
        "description": "Access TireRack tire pricing and inventory via their daily SFTP CSV feed.",
        "icon_url": "https://api.aftermarketscout.com/uploads/tirerack_logo.png",
        "category": "Distributors",
        "connection_required_fields": [
            TIRERACK_CREDENTIALS_SFTP_HOST,
            TIRERACK_CREDENTIALS_SFTP_PORT,
            TIRERACK_CREDENTIALS_SFTP_USER,
            TIRERACK_CREDENTIALS_SFTP_PASSWORD,
        ],
        "integration_time": "Data available within 1-2 hours",
        "installation_instructions_html": (
            "<p><strong>Tire Rack</strong> posts a new pricing and inventory file to your dealer SFTP account "
            "every morning, and AfterMarketScout collects it from there. Tire Rack has no ordering API, so "
            "orders go to your rep as a PDF purchase order by email. One email to your rep sets up both.</p>"

            "<p><strong>1. Email your Tire Rack rep</strong></p>"
            "<p>Send the message below to your Tire Rack rep, with "
            "<a href=\"mailto:support@aftermarketscout.com\">support@aftermarketscout.com</a> on CC:</p>"
            + _CALLOUT_OPEN +
            "<p>Hi,</p>"
            "<p>We use AfterMarketScout as our product information management (PIM) and ordering platform, and "
            "need two things for our Tire Rack account [your account number]:</p>"
            "<ol>"
            "<li>SFTP feed. Please send us the SFTP details for our dealer pricing and inventory feed: host, "
            "port, username and password.</li>"
            "<li>Purchase orders. Please confirm the email address we should send purchase orders to.</li>"
            "</ol>"
            "<p>Thanks!</p>"
            "</div>"

            "<p><strong>2. Connect the product feed</strong></p>"
            "<p>Paste the <strong>host</strong>, <strong>port</strong>, <strong>username</strong> and "
            "<strong>password</strong> into the <strong>Product feed</strong> fields below and click "
            "<strong>Save feed credentials</strong>. Paste rather than retype, because a stray space is the "
            "most common reason a connection fails.</p>"
            "<p>The card shows <strong>Feed Connected</strong>, and data arrives with the next morning's "
            "file.</p>"

            "<p><strong>3. Set up ordering</strong></p>"
            "<p>Enter the purchase-order email your rep confirmed below. You can also add an internal "
            "<strong>CC</strong> so your team gets a copy of every order. Click <strong>Save</strong>.</p>"
            "<p>Orders go out as PDF emails, so confirmations and tracking come back from your rep directly "
            "rather than into AfterMarketScout.</p>"

            "<p><strong>Troubleshooting</strong></p>"
            "<ul>"
            "<li><strong>Connection fails?</strong> Check the port with your rep. It's usually <code>22</code>, "
            "but some accounts use a different one. Then re-paste the username and password.</li>"
            "<li><strong>Pricing hasn't changed today?</strong> The file arrives once each morning. If nothing "
            "new has come in for more than a day, ask your rep to confirm the file is still being "
            "generated.</li>"
            "<li><strong>More than one Tire Rack account?</strong> Each account has its own SFTP login and "
            "pricing. Connect each one separately.</li>"
            "</ul>"
        ),
        # Email-channel ordering — TireRack has no order API of its own; see the matching note
        # on Turn 14 above.
        "email_order_connection_required_fields": ["rep_email"],
        "email_order_connection_optional_fields": ["account_number", "cc_email", "reply_to_email"],
    },
    {
        "kind": enums.BrandProviderKind.MOTOR_STATE_DISTRIBUTING,
        "name": "Motor State Distributing",
        "description": "Access Motor State Distributing inventory and pricing via their FTP feed.",
        "icon_url": "https://api.aftermarketscout.com/uploads/motor_state_logo.png",
        "category": "Distributors",
        "connection_required_fields": [
            MOTOR_STATE_CREDENTIALS_FTP_USER,
            MOTOR_STATE_CREDENTIALS_FTP_PASSWORD,
        ],
        "connection_optional_fields": [
            MOTOR_STATE_CREDENTIALS_CONTENT_FTP_USER,
            MOTOR_STATE_CREDENTIALS_CONTENT_FTP_PASSWORD,
        ],
        # Email-channel ordering — see the matching note on Turn 14 above.
        "email_order_connection_required_fields": ["rep_email"],
        "email_order_connection_optional_fields": ["account_number", "cc_email", "reply_to_email"],
        "integration_time": "Data available within 1-2 hours",
        "installation_instructions_html": (
            "<p><strong>Motor State Distributing</strong> connects with an FTP login for your daily inventory "
            "and pricing file. Motor State has no ordering API, so orders go to your account manager as a PDF "
            "purchase order by email. One email to them sets up both.</p>"

            "<p><strong>1. Email your Motor State account manager</strong></p>"
            "<p>Send the message below to your Motor State account manager, with "
            "<a href=\"mailto:support@aftermarketscout.com\">support@aftermarketscout.com</a> on CC:</p>"
            + _CALLOUT_OPEN +
            "<p>Hi,</p>"
            "<p>We use AfterMarketScout as our product information management (PIM) and ordering platform, and "
            "need a few things set up for our Motor State account [your account number]:</p>"
            "<ol>"
            "<li>FTP feed access for our inventory and pricing</li>"
            "<li>Product data (images, categories and long descriptions), if available for our account</li>"
            "<li>Please confirm the email address we should send purchase orders to</li>"
            "</ol>"
            "<p>Thanks!</p>"
            "</div>"

            "<p><strong>2. Connect the product feed</strong></p>"
            "<p>Paste your FTP username (it looks like <code>123456@motorstateftp.com</code>) and password into "
            "the <strong>Product feed</strong> fields below and click <strong>Save feed credentials</strong>. "
            "The card will show <strong>Feed Connected</strong>.</p>"
            "<p>If Motor State gave you a separate login for product data, enter it in the <strong>content "
            "FTP</strong> fields too. Images, categories and descriptions then come from that login, while your "
            "main login keeps driving stock and pricing.</p>"

            "<p><strong>3. Set up ordering</strong></p>"
            "<p>Enter the purchase-order email your account manager confirmed below. You can also add an "
            "internal <strong>CC</strong> so your team gets a copy of every order. Click "
            "<strong>Save</strong>.</p>"
            "<p>Orders go out as PDF emails, so confirmations and tracking come back from your account manager "
            "directly rather than into AfterMarketScout.</p>"

            "<p><strong>Troubleshooting</strong></p>"
            "<ul>"
            "<li><strong>Connection fails?</strong> Check that the username includes "
            "<code>@motorstateftp.com</code>, then re-paste the password. If it still fails, ask your account "
            "manager to confirm FTP access is active.</li>"
            "<li><strong>Connected but no data?</strong> Your login works, but Motor State hasn't set up a feed "
            "file for your account yet. Ask them to enable it.</li>"
            "<li><strong>Prices look out of date?</strong> The file is generated once a day, so changes made "
            "during the day appear in the next one.</li>"
            "</ul>"
        ),
    },
    {
        "kind": enums.BrandProviderKind.ALLPRO_DISTRIBUTING,
        "name": "AllPro Distributing",
        "description": "Access AllPro Distributing inventory and pricing via their FTP feed.",
        "icon_url": "https://api.aftermarketscout.com/uploads/allpro_logo.png",
        "category": "Distributors",
        "connection_required_fields": ["username", "password"],
        # Email-channel ordering — see the matching note on Turn 14 above.
        "email_order_connection_required_fields": ["rep_email"],
        "email_order_connection_optional_fields": ["account_number", "cc_email", "reply_to_email"],
        "integration_time": "Data available within 1-2 days",
        "installation_instructions_html": (
            "<p><strong>AllPro Distributing</strong> publishes your inventory and pricing on their FTP "
            "server. AfterMarketScout handles the connection &mdash; you only need your FTP username and "
            "password.</p>"

            "<p><strong>1. Request FTP credentials</strong></p>"
            "<p>AllPro issues a username and password per customer rather than one shared login. Contact "
            "your AllPro representative to request them:</p>"
            + _CALLOUT_OPEN +
            "<p>Could you please send us FTP username and password to pull our inventory and pricing? "
            "We're connecting it to our inventory system.</p>"
            "</div>"
            "<p>Email <a href=\"mailto:Andrew.Avello@allprob2b.com\">Andrew.Avello@allprob2b.com</a> and "
            "include your <strong>AllPro customer number</strong> so the request can be matched to your "
            "account.</p>"

            "<p><strong>2. Enter your FTP credentials</strong></p>"
            "<ol>"
            "<li>Paste the <strong>username</strong> and <strong>password</strong> AllPro sent you into "
            "the fields below.</li>"
            "<li>Click <strong>Save</strong>.</li>"
            "</ol>"

            "<p><strong>Notes</strong></p>"
            "<ul>"
            "<li><strong>Connection fails.</strong> Re-paste the password to rule out a stray space or a "
            "truncated copy. If it still fails, ask Andrew to confirm the username/password and that FTP "
            "access is active for your account.</li>"
            "<li><strong>Need help?</strong> Contact "
            "<a href=\"mailto:support@aftermarketscout.com\">support@aftermarketscout.com</a>.</li>"
            "</ul>"
        ),
    },
]


def visible_provider_catalog() -> typing.List[typing.Dict[str, typing.Any]]:
    """
    PROVIDER_CATALOG minus entries flagged ``hidden`` — for the two places that list providers a
    company can *start* connecting (integrations catalog, onboarding picker). Everything else
    (validation, sync, provider detail) keeps reading PROVIDER_CATALOG directly, so hiding a
    provider never orphans a company already connected to it.
    """
    return [entry for entry in PROVIDER_CATALOG if not entry.get("hidden")]


_UPLOADS = "https://api.aftermarketscout.com/uploads"

COMING_SOON_PROVIDERS = [
    {"kind": enums.BrandProviderKind.HOLLEY_PERFORMANCE,      "name": "Holley Performance",         "category": "Distributors", "icon_url": f"{_UPLOADS}/holley_logo.png"},
]

"""
Shop-management provider catalog: list of connectable shop-management systems for the
shop-management integrations catalog. Used by seed_shop_management_providers and the shop
management catalog endpoint. Separate from PROVIDER_CATALOG (distributors) on purpose — see
ShopManagementProviders/CompanyShopManagementProviders in src/models.py.
"""
SHOP_MANAGEMENT_PROVIDER_CATALOG = [
    {
        "kind": enums.ShopManagementProviderKind.SHOPMONKEY,
        "name": "ShopMonkey",
        "description": "Connect your ShopMonkey shop management account.",
        "icon_url": f"{_UPLOADS}/shopmonkey_logo.png",
        "category": "Shop Management",
        "connection_required_fields": ["api_key"],
        "installation_instructions_html": (
            "<p><strong>ShopMonkey</strong> connections use an API key issued from your ShopMonkey account.</p>"
            "<ol>"
            "<li>Sign in to ShopMonkey and open your account's API settings.</li>"
            "<li>Generate (or copy your existing) <strong>API key</strong>.</li>"
            "<li>Paste it into the field below and save. AfterMarketScout uses it only to call "
            "ShopMonkey on your behalf.</li>"
            "</ol>"
        ),
    },
]

# Field priority configuration for merging CATALOG and DISTRIBUTOR parts
# Each field maps to its primary source (CATALOG or DISTRIBUTOR)
# If field is null/empty in primary source, fallback to the other source
# Fields not listed default to CATALOG priority

BIGCOMMERCE_PART_FIELD_PRIORITY = {
    'brand_id': 'CATALOG',
    'product_title': 'CATALOG',
    'sku': 'DISTRIBUTOR',
    'mpn': 'CATALOG',
    'description': 'CATALOG',
    'images': 'DISTRIBUTOR',
    'custom_fields': 'CATALOG',
    'active': 'CATALOG',
    'default_price': 'DISTRIBUTOR',
    'cost': 'DISTRIBUTOR',
    'msrp': 'DISTRIBUTOR',
    'weight': 'DISTRIBUTOR',
    'width': 'DISTRIBUTOR',
    'height': 'DISTRIBUTOR',
    'depth': 'DISTRIBUTOR',
    'inventory': 'DISTRIBUTOR',
    'category': 'CATALOG',
    'subcategory': 'CATALOG',
}

# Mapping from Turn14 (category, subcategory) to PCDB (category, subcategory)
# Maps Turn14 category/subcategory pairs to their PCDB equivalents
TURN14_TO_PCDB_CATEGORY_MAP = {
    ("Deflectors", "Window Vents"): ("Exterior Accessories", "Window Deflectors"),
    ("Body Armor & Protection", "Skid Plates"): ("Exterior Protection", "Skid Plates"),
    ("Engine Components", "Gasket Kits"): ("Engine", "Gasket Sets"),
    ("Drivetrain", "Clutch Rebuild Kits"): ("Drivetrain", "Clutch Kits"),
    ("Suspension", "Suspension Controllers"): ("Suspension", "Suspension Electronics"),
    ("Engine Components", "Piston Sets - Powersports"): ("Engine Internal", "Pistons"),
    ("Suspension", "Fork Springs"): ("Suspension", "Springs"),
    ("Suspension", "Control Arms"): ("Suspension", "Control Arms"),
    ("Marketing", "POP Displays"): ("Merchandising", "Point of Purchase Displays"),
    ("Windshields", "Window Shades"): ("Exterior Accessories", "Window Shades"),
    ("Engine Components", "Valves"): ("Engine Internal", "Valves"),
    ("Drivetrain", "Clutch Covers"): ("Drivetrain", "Clutch Components"),
    ("Engine Components", "Bearings"): ("Engine Internal", "Bearings"),
    ("Engine Components", "Engines"): ("Engine", "Complete Engines"),
    ("Suspension", "Steering Stabilizer"): ("Steering", "Steering Stabilizers"),
    ("Engine Components", "Piston Sets - Forged - 5cyl"): ("Engine Internal", "Pistons"),
    ("Apparel", "Shirts"): ("Apparel", "Shirts"),
    ("Engine Components", "Head Gaskets"): ("Engine", "Head Gaskets"),
    ("Lights", "Work Lights"): ("Lighting", "Work Lights"),
    ("Roofs & Roof Accessories", "Chase Racks"): ("Exterior Accessories", "Chase Racks"),
    ("Engine Components", "Hardware - Singles"): ("Engine", "Hardware"),
    ("Suspension", "Alignment Kits"): ("Suspension", "Alignment Components"),
    ("Suspension", "Coilovers"): ("Suspension", "Coilover Kits"),
    ("Suspension", "Bump Stops"): ("Suspension", "Bump Stops"),
    ("Deflectors", "Hood Deflectors"): ("Exterior Accessories", "Hood Deflectors"),
    ("Lights", "Fog Lights"): ("Lighting", "Fog Lights"),
    ("Engine Components", "Piston Coating"): ("Engine Internal", "Piston Accessories"),
    ("Winches & Hitches", "Winch Accessories"): ("Winches", "Winch Accessories"),
    ("Suspension", "Tie Rods"): ("Steering", "Tie Rods"),
    ("Drivetrain", "Clutch Kits - Single"): ("Drivetrain", "Clutch Kits"),
    ("Suspension", "Coilover Components"): ("Suspension", "Coilover Components"),
    ("Engine Components", "Piston Rings"): ("Engine Internal", "Piston Rings"),
    ("Engine Components", "Piston Sets - Custom"): ("Engine Internal", "Pistons"),
    ("Nerf Bars & Running Boards", "Running Boards"): ("Exterior Accessories", "Running Boards"),
    ("Drivetrain", "Pressure Plates"): ("Drivetrain", "Clutch Pressure Plates"),
    ("Bumpers, Grilles & Guards", "Bumper Beams"): ("Body", "Bumper Reinforcements"),
    ("Brakes, Rotors & Pads", "Brake Line Kits"): ("Brakes", "Brake Lines"),
    ("Wheel and Tire Accessories", "Spare Tire Carriers"): ("Exterior Accessories", "Spare Tire Carriers"),
    ("Suspension", "Subframes"): ("Suspension", "Subframes"),
    ("Suspension", "Chassis Bracing"): ("Suspension", "Chassis Bracing"),
    ("Suspension", "Air Compressors"): ("Suspension", "Air Compressors"),
    ("Body Armor & Protection", "Body Armor & Rock Rails"): ("Exterior Protection", "Rock Sliders"),
    ("Drivetrain", "Axles"): ("Drivetrain", "Axle Shafts"),
    ("Lights", "Headlights"): ("Lighting", "Headlights"),
    ("Suspension", "Lift Springs"): ("Suspension", "Lift Springs"),
    ("Lights", "Sidemarkers & Indicators"): ("Lighting", "Side Marker Lights"),
    ("Drivetrain", "Diff Covers"): ("Drivetrain", "Differential Covers"),
    ("Suspension", "Lift Kits"): ("Suspension", "Lift Kits"),
    ("Fuel Delivery", "Fuel Tanks"): ("Fuel System", "Fuel Tanks"),
    ("Engine Components", "Pistons - Custom - Single"): ("Engine Internal", "Pistons"),
    ("Winches & Hitches", "Hitch Receivers"): ("Towing", "Hitch Receivers"),
    ("Lights", "Tail Lights"): ("Lighting", "Tail Lights"),
    ("Suspension", "Coilover Springs"): ("Suspension", "Springs"),
    ("Suspension", "Steering Knuckles & Spindles"): ("Steering", "Steering Knuckles"),
    ("Suspension", "Steering Dampers"): ("Steering", "Steering Dampers"),
    ("Floor Mats", "Floor Mats - Rubber"): ("Interior Accessories", "Floor Mats"),
    ("Truck Bed Accessories", "Cargo Organization"): ("Truck Bed", "Cargo Management"),
    ("Fuel Delivery", "Fuel Systems"): ("Fuel System", "Fuel System Kits"),
    ("Drivetrain", "Clutch Baskets"): ("Drivetrain", "Clutch Components"),
    ("Nerf Bars & Running Boards", "Side Steps"): ("Exterior Accessories", "Side Steps"),
    ("Suspension", "OE Replacement Springs"): ("Suspension", "Replacement Springs"),
    ("Suspension", "Sway Bar Endlinks"): ("Suspension", "Sway BCar End Links"),
    ("Exterior Styling", "Stickers/Decals/Banners"): ("Exterior Accessories", "Decals & Graphics"),
    ("Drivetrain", "Spindles"): ("Drivetrain", "Spindles"),
    ("Roofs & Roof Accessories", "Roofs"): ("Exterior Accessories", "Roof Panels"),
    ("Exterior Styling", "Exterior Trim"): ("Body", "Exterior Trim"),
    ("Engine Components", "Wiring Harnesses"): ("Electrical", "Wiring Harnesses"),
    ("Suspension", "Air Tank Components"): ("Suspension", "Air Suspension Components"),
    ("Drivetrain", "Transmission Mounts"): ("Drivetrain", "Transmission Mounts"),
    ("Suspension", "Shock Mounts & Camber Plates"): ("Suspension", "Shock Mounts"),
    ("Winches & Hitches", "Recovery Boards"): ("Recovery", "Traction Boards"),
    ("Engine Components", "Piston Sets - Forged - 6cyl"): ("Engine Internal", "Pistons"),
    ("Suspension", "Air Springs"): ("Suspension", "Air Springs"),
    ("Suspension", "Shock & Spring Kits"): ("Suspension", "Shock and Spring Kits"),
    ("Engine Components", "Piston Pins"): ("Engine Internal", "Piston Pins"),
    ("Exterior Styling", "Fenders"): ("Body", "Fenders"),
    ("Suspension", "Sway Bars"): ("Suspension", "Sway Bars"),
    ("Bumpers, Grilles & Guards", "Grilles"): ("Body", "Grilles"),
    ("Exterior Styling", "Antennas"): ("Exterior Accessories", "Antennas"),
    ("Exterior Styling", "Doors"): ("Body", "Doors"),
    ("Engine Components", "Valve Covers"): ("Engine", "Valve Covers"),
    ("Wheel and Tire Accessories", "Wheel Spacers & Adapters"): ("Wheels", "Wheel Spacers"),
    ("Roofs & Roof Accessories", "Roof Rack"): ("Exterior Accessories", "Roof Racks"),
    ("Drivetrain", "Clutch Discs"): ("Drivetrain", "Clutch Discs"),
    ("Winches & Hitches", "Winch Kit"): ("Winches", "Winches"),
    ("Interior Accessories", "Shift Knobs"): ("Interior Accessories", "Shift Knobs"),
    ("Suspension", "Traction Bars"): ("Suspension", "Traction Bars"),
    ("Nerf Bars & Running Boards", "Bed Steps"): ("Truck Bed", "Bed Steps"),
    ("Apparel", "Headwear"): ("Apparel", "Headwear"),
    ("Lights", "Light Covers and Guards"): ("Lighting", "Light Covers"),
    ("Suspension", "Lowering Kits"): ("Suspension", "Lowering Kits"),
    ("Truck Bed Accessories", "Bed Racks"): ("Truck Bed", "Bed Racks"),
    ("Wheel and Tire Accessories", "Lug Nuts"): ("Wheels", "Lug Nuts"),
    ("Safety", "Fire Safety"): ("Safety", "Fire Safety"),
    ("Suspension", "Lowering Springs"): ("Suspension", "Lowering Springs"),
    ("Suspension", "Air Tanks"): ("Suspension", "Air Suspension Tanks"),
    ("Roofs & Roof Accessories", "Cargo Boxes & Bags"): ("Exterior Accessories", "Cargo Boxes"),
    ("Exterior Styling", "Hoods"): ("Body", "Hoods"),
    ("Suspension", "Shackle Kits"): ("Suspension", "Leaf Spring Shackles"),
    ("Engine Components", "Piston Pin Locks"): ("Engine Internal", "Piston Pin Locks"),
    ("Apparel", "Keychains"): ("Merchandise", "Keychains"),
    ("Exterior Styling", "License Plate Relocation"): ("Exterior Accessories", "License Plate Brackets"),
    ("Engine Components", "Timing Chains"): ("Engine", "Timing Chains"),
    ("Engine Components", "Hardware Kits - Other"): ("Engine", "Hardware Kits"),
    ("Suspension", "Shocks and Struts"): ("Suspension", "Shocks and Struts"),
    ("Suspension", "Leveling Kits"): ("Suspension", "Leveling Kits"),
    ("Interior Accessories", "Dash & Interior Trim"): ("Interior Accessories", "Interior Trim"),
    ("Suspension", "Leaf Springs & Accessories"): ("Suspension", "Leaf Springs"),
    ("Data Acquisition", "Data Acquisition"): ("Electronics", "Data Acquisition"),
    ("Suspension", "Sway Bar Brackets"): ("Suspension", "Sway Bar Components"),
    ("Truck Bed Accessories", "Bed Bars"): ("Truck Bed", "Bed Bars"),
    ("Interior Accessories", "Relays"): ("Electrical", "Relays"),
    ("Suspension", "Spring Insulators"): ("Suspension", "Spring Insulators"),
    ("Lights", "Bulbs"): ("Lighting", "Light Bulbs"),
    ("Brakes, Rotors & Pads", "Brake Adapters"): ("Brakes", "Brake Adapters"),
    ("Forced Induction", "Intercoolers"): ("Forced Induction", "Intercoolers"),
    ("Fabrication", "Fuel Lines"): ("Fuel System", "Fuel Lines"),
    ("Interior Accessories", "Pedal Covers"): ("Interior Accessories", "Pedal Covers"),
    ("Programmers & Chips", "Switch Panels"): ("Electronics", "Switch Panels"),
    ("Drivetrain", "Driveshafts"): ("Drivetrain", "Driveshafts"),
    ("Suspension", "Ball Joints"): ("Suspension", "Ball Joints"),
    ("Gauges & Pods", "Gauges"): ("Interior Accessories", "Gauges"),
    ("Suspension", "Suspension Arms & Components"): ("Suspension", "Suspension Arms"),
    ("Interior Accessories", "Dash Mounts"): ("Interior Accessories", "Dash Mounts"),
    ("Lights", "Light Accessories and Wiring"): ("Lighting", "Lighting Accessories"),
    ("Engine Components", "Crankshafts"): ("Engine Internal", "Crankshafts"),
    ("Body Armor & Protection", "Mud Flaps"): ("Exterior Accessories", "Mud Flaps"),
    ("Bumpers, Grilles & Guards", "Bumpers - Steel"): ("Body", "Bumpers"),
    ("Fabrication", "Filler Necks"): ("Fuel System", "Filler Necks"),
    ("Exhaust, Mufflers & Tips", "Exhaust Valve Controllers"): ("Exhaust", "Exhaust Electronics"),
    ("Suspension", "Panhard Bars"): ("Suspension", "Panhard Bars"),
    ("Suspension", "Suspension Packages"): ("Suspension", "Suspension Kits"),
    ("Fabrication", "Brackets"): ("Fabrication", "Mounting Brackets"),
    ("Suspension", "Boots"): ("Suspension", "Protective Boots"),
    ("Lights", "Light Bars & Cubes"): ("Lighting", "Light Bars"),
    ("Engine Components", "Piston Sets - Forged - 8cyl"): ("Engine Internal", "Pistons"),
    ("Suspension", "Camber Kits"): ("Suspension", "Camber Kits"),
    ("Truck Bed Accessories", "Truck Bed Rail Protectors"): ("Truck Bed", "Bed Rail Protectors"),
    ("Fabrication", "Clamps"): ("Fabrication", "Clamps"),
    ("Exhaust, Mufflers & Tips", "Exhaust Hardware"): ("Exhaust", "Exhaust Hardware"),
    ("Suspension", "Steering Racks"): ("Steering", "Steering Racks"),
    ("Engine Components", "Wiring Connectors"): ("Electrical", "Electrical Connectors"),
    ("Drivetrain", "Diff Braces"): ("Drivetrain", "Differential Braces"),
    ("Suspension", "Air Suspension Kits"): ("Suspension", "Air Suspension Kits"),
    ("Engine Components", "Piston Sets - Forged - 4cyl"): ("Engine Internal", "Pistons"),
    ("Exterior Styling", "Hood Pins"): ("Exterior Accessories", "Hood Pins"),
    ("Suspension", "Bushing Kits"): ("Suspension", "Bushing Kits"),
    ("Bumpers, Grilles & Guards", "Bumper Accessories"): ("Body", "Bumper Accessories"),
    ("Suspension", "Air Compressor Systems"): ("Suspension", "Air Compressor Systems"),
    ("Engine Components", "Engine Hardware"): ("Engine", "Engine Hardware"),
    ("Body Armor & Protection", "Wheel Well Liners"): ("Exterior Protection", "Wheel Well Liners"),
    ("Truck Bed Accessories", "Bed Liners"): ("Truck Bed", "Bed Liners"),
    ("Oils & Oil Filters", "Hydraulic Oils"): ("Fluids", "Hydraulic Fluid"),
    ("Uncategorized", "Uncategorized"): ("Miscellaneous", "Uncategorized"),
    ("Lights", "Light Mounts"): ("Lighting", "Light Mounts"),
    ("Suspension", "Suspension Arm Bushings"): ("Suspension", "Control Arm Bushings"),
    ("Engine Components", "Pistons - Forged - Single"): ("Engine Internal", "Pistons"),
    ("Fuel Delivery", "Fuel Caps"): ("Fuel System", "Fuel Caps"),
    ("Fender Flares & Trim", "Fender Flares"): ("Exterior Accessories", "Fender Flares"),
    ("Truck Bed Accessories", "Tailgate Accessories"): ("Truck Bed", "Tailgate Accessories"),
    ("Tools", "Tools"): ("Tools", "Automotive Tools"),
    ("Exterior Styling", "Tool Storage"): ("Exterior Accessories", "Tool Storage"),
    ("Truck Bed Accessories", "Truck Bed Cover Replacement Parts"): ("Truck Bed", "Tonneau Covers"),
    ("Truck Bed Accessories", "Cargo Tie-Downs"): ("Truck Bed", "Cargo Tie-Downs"),
    ("Exhaust, Mufflers & Tips", "Catback"): ("Exhaust", "Cat-Back Systems"),
    ("Lights", "Interior Lighting"): ("Lighting", "Interior Lighting"),
    ("Tonneau Covers", "Bed Covers - Folding"): ("Truck Bed", "Tonneau Covers"),
    ("Programmers & Chips", "Programmer Accessories"): ("Electronics", "Programmer Accessories"),
    ("Fabrication", "Hoses"): ("Fabrication", "Hoses"),
    ("Tonneau Covers", "Bed Caps"): ("Truck Bed", "Tonneau Covers"),
    ("Suspension", "Fork Cartridge Kits"): ("Suspension", "Fork Components"),
    ("Exhaust, Mufflers & Tips", "Tips"): ("Exhaust", "Exhaust Tips"),
    ("Forced Induction", "Turbochargers"): ("Forced Induction", "Turbochargers"),
    ("Exhaust, Mufflers & Tips", "Connecting Pipes"): ("Exhaust", "Exhaust Pipes"),
    ("Bumpers, Grilles & Guards", "Grille Guards"): ("Body", "Grille Guards"),
    ("Tonneau Covers", "Tonneau Covers - Retractable"): ("Truck Bed", "Tonneau Covers"),
    ("Roofs & Roof Accessories", "Storage Racks"): ("Exterior Accessories", "Roof Racks"),
    ("Fabrication", "Fittings"): ("Fabrication", "Fittings"),
    ("Wheels", "Wheels - Cast"): ("Wheels", "Wheels"),
    ("Truck Bed Accessories", "Truck Boxes & Storage"): ("Truck Bed", "Storage Boxes"),
    ("Fabrication", "Heat Shields"): ("Fabrication", "Heat Shields"),
    ("Exterior Styling", "Spray Bottles"): ("Exterior Accessories", "Cleaning Supplies"),
    ("Exhaust, Mufflers & Tips", "Resonators"): ("Exhaust", "Resonators"),
    ("Air Intake Systems", "Air Intake Components"): ("Air Intake", "Air Intake Components"),
    ("Suspension", "Caster Kits"): ("Suspension", "Caster Kits"),
    ("Tonneau Covers", "Bed Covers - Hinged"): ("Truck Bed", "Tonneau Covers"),
    ("Fuel Delivery", "Fuel Pumps"): ("Fuel System", "Fuel Pumps"),
    ("Exhaust, Mufflers & Tips", "Muffler"): ("Exhaust", "Mufflers"),
    ("Engine Components", "Mass Air Flow Sensors"): ("Engine", "Mass Air Flow Sensors"),
    ("Exhaust, Mufflers & Tips", "X Pipes"): ("Exhaust", "X-Pipes"),
    ("Exhaust, Mufflers & Tips", "Headers & Manifolds"): ("Exhaust", "Headers"),
    ("Programmers & Chips", "Programmers & Tuners"): ("Electronics", "Programmers & Tuners"),
    ("Programmers & Chips", "In-Line Modules"): ("Electronics", "Performance Modules"),
    ("Tonneau Covers", "Bed Covers - Roll Up"): ("Truck Bed", "Tonneau Covers"),
    ("Marketing", "Marketing"): ("Merchandising", "Marketing Materials"),
    ("Lights", "Light Strip LED"): ("Lighting", "LED Light Strips"),
    ("Wheel and Tire Accessories", "Wheel Center Caps"): ("Wheels", "Center Caps"),
    ("Fabrication", "Steel Tubing"): ("Fabrication", "Steel Tubing"),
    ("Truck Bed Accessories", "Truck Bed Rack"): ("Truck Bed", "Bed Racks"),
    ("Air Intake Systems", "Scoops & Snorkels"): ("Air Intake", "Air Intake Scoops"),
    ("Tonneau Covers", "Tonneau Covers - Roll Up"): ("Truck Bed", "Tonneau Covers"),
    ("Tonneau Covers", "Tonneau Covers - Hard Fold"): ("Truck Bed", "Tonneau Covers"),
    ("Tonneau Covers", "Retractable Bed Covers"): ("Truck Bed", "Tonneau Covers"),
    ("Programmers & Chips", "Throttle Controllers"): ("Electronics", "Throttle Controllers"),
    ("Roofs & Roof Accessories", "Hard Top Accessories"): ("Exterior Accessories", "Hard Top Accessories"),
    ("Lights", "Lights Bed Rail"): ("Lighting", "Bed Rail Lights"),
    ("Suspension", "Strut Bars"): ("Suspension", "Strut Bars"),
    ("Forced Induction", "Reservoirs"): ("Forced Induction", "Reservoirs"),
    ("Exhaust, Mufflers & Tips", "Axle Back"): ("Exhaust", "Axle-Back Systems"),
}

