import typing

# Provider kind_name -> display name (used by parts API)
PROVIDER_DISPLAY_NAMES = {
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
}

# Provider kind_name -> image URL (used by parts API)
PROVIDER_IMAGE_URLS = {
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
    "HELMHOUSE": "https://api.aftermarketscout.com/uploads/helmhouse_logo.png",
    "THIBAULT": "https://api.aftermarketscout.com/uploads/thibault_logo.png",
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
ROUGH_COUNTRY_INVENTORY_SEARCH_URL_TEMPLATE = "https://www.roughcountry.com/search/{sku}"

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

# WheelPros SFTP feed paths (relative; leading / added by client when downloading)
WHEELPROS_FEED_PATHS = {
    "wheel": "CommonFeed/USD/WHEEL/wheelInvPriceData.csv",
    "tire": "CommonFeed/USD/TIRE/tireInvPriceData.csv",
    "accessories": "CommonFeed/USD/ACCESSORIES/accessoriesInvPriceData.csv",
}

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
        "installation_instructions_html": (
            "<p>Turn 14 connections use OAuth2. Your <strong>client ID</strong> and <strong>client secret</strong> "
            "are shown on Turn 14's API settings page.</p>"
            "<ul>"
            "<li>Sign in to your Turn 14 account and open "
            "<a href=\"https://www.turn14.com/api_settings.php\" target=\"_blank\" rel=\"noopener noreferrer\">"
            "https://www.turn14.com/api_settings.php</a>.</li>"
            "<li>Copy the <strong>client ID</strong> and <strong>client secret</strong> from that page.</li>"
            "<li>Paste them into the fields below and save. AfterMarketScout uses them only to call Turn 14 on your behalf.</li>"
            "</ul>"
            "<p>If you cannot access that page or the credentials are missing, contact Turn 14 support or your account manager.</p>"
        ),
    },
    {
        "kind": enums.BrandProviderKind.KEYSTONE,
        "name": "Keystone",
        "description": "Access inventory and pricing from Keystone Automotive via FTP.",
        "icon_url": "https://api.aftermarketscout.com/uploads/keystone.png",
        "category": "Distributors",
        "connection_required_fields": ["ftp_user", "ftp_password"],
        "installation_instructions_html": (
            "<p><strong>Keystone</strong> data is loaded from their FTP inventory feed using your account credentials.</p>"
            "<ol>"
            "<li>Obtain your Keystone FTP <strong>username</strong> and <strong>password</strong> "
            "(from Keystone onboarding or your rep).</li>"
            "<li>Enter them exactly as provided—no <code>ftp://</code> prefix in the username field.</li>"
            "<li>Save the connection. We will pull inventory and pricing from the standard Keystone CSV layouts.</li>"
            "</ol>"
        ),
    },
    {
        "kind": enums.BrandProviderKind.ROUGH_COUNTRY,
        "name": "Rough Country",
        "description": "Access catalog, pricing, and fitment from Rough Country via jobber feed.",
        "icon_url": "https://api.aftermarketscout.com/uploads/rough_country.png",
        "category": "Distributors",
        "connection_required_fields": [ROUGH_COUNTRY_CREDENTIALS_FEED_URL],
        "installation_instructions_html": (
            "<p><strong>Rough Country</strong> uses a downloadable Excel jobber feed URL per dealer account.</p>"
            "<ol>"
            "<li>In your Rough Country jobber portal, <strong>locate the feed</strong> (or download link) for "
            "the full catalog (General, Fitment, Discontinued). Copy the <strong>full HTTPS URL</strong>—it "
            "should start with <code>https://feeds.roughcountry.com/jobber_</code>.</li>"
            "<li>Paste that URL into <strong>feed_url</strong> below and save the connection.</li>"
            "</ol>"
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
        "installation_instructions_html": (
            "<p><strong>Wheel Pros</strong> inventory and pricing CSVs (wheels, tires, accessories) are on Wheel "
            "Pros&rsquo; SFTP server (<code>sftp.wheelpros.com</code>, port 22). AfterMarketScout connects there "
            "automatically; you only enter the account credentials Wheel Pros gives you.</p>"
            "<ol>"
            "<li>Request your SFTP <strong>username</strong> and <strong>password</strong> from Wheel Pros.</li>"
            "<li>Enter <strong>wheel_markup</strong>, <strong>tire_markup</strong>, and <strong>accessories_markup</strong> "
            "as the percent <em>off</em> list price (0–100) for wheels, tires, and accessories feeds. "
            "We derive dealer <strong>cost</strong> from MSRP: cost = MSRP &times; (1 &minus; percent/100). "
            "If left blank, <strong>20%</strong> off MSRP is used per feed.</li>"
            "<li>If your agreement uses non-default remote paths, set optional <strong>sftp_path</strong>.</li>"
            "<li>Save the connection. Company-specific pricing is read from SFTP after catalog sync.</li>"
            "</ol>"
        ),
    },
    {
        "kind": enums.BrandProviderKind.MEYER,
        "name": "Meyer",
        "description": "Access Meyer catalog, inventory, and pricing from AfterMarketScout's SFTP relay.",
        "icon_url": "https://api.aftermarketscout.com/uploads/meyer_logo.png",
        "category": "Distributors",
        "connection_required_fields": ["sftp_user", "sftp_password"],
        "connection_optional_fields": [],
        "installation_instructions_html": (
            "<p>Email <a href=\"mailto:info@aftermarketscout.com\">info@aftermarketscout.com</a> so we can "
            "create a unique SFTP account for you and send a <strong>username</strong> and <strong>password</strong>. "
            "Meyer Distributing delivers the data feed to AfterMarketScout&rsquo;s SFTP relay; ask your Meyer account "
            "representative to set up the feed to connect to <strong>our</strong> SFTP endpoint using the details below.</p>"
            "<p><strong>Endpoint for your Meyer rep</strong></p>"
            "<ul>"
            "<li><strong>SFTP:</strong> <code>5.161.121.143</code></li>"
            "<li><strong>Port:</strong> <code>22</code></li>"
            "<li><strong>Folder:</strong> <code>uploads</code></li>"
            "<li><strong>Files:</strong> <code>Meyer Pricing.csv</code>, <code>Meyer Inventory.csv</code></li>"
            "<li><strong>User / password:</strong> we provide these after you email us</li>"
            "</ul>"
            "<p>When you have your login, enter <strong>sftp_user</strong> and <strong>sftp_password</strong> below "
            "and save the connection. For help, contact "
            "<a href=\"mailto:info@aftermarketscout.com\">info@aftermarketscout.com</a>.</p>"
        ),
    },
    {
        "kind": enums.BrandProviderKind.ATECH,
        "name": "A-Tech",
        "description": "Access A-Tech catalog, inventory, and pricing from AfterMarketScout's SFTP relay.",
        "icon_url": "https://api.aftermarketscout.com/uploads/atech_logo.png",
        "category": "Distributors",
        "connection_required_fields": ["sftp_user", "sftp_password"],
        "connection_optional_fields": [],
        "installation_instructions_html": (
            "<p>Email <a href=\"mailto:info@aftermarketscout.com\">info@aftermarketscout.com</a> so we can "
            "provision a dedicated SFTP login (<strong>username</strong> and <strong>password</strong>) on "
            "AfterMarketScout&rsquo;s relay. Ask your A-Tech representative to deliver their standard "
            "combined catalog and pricing extract to <strong>our</strong> endpoint so a single drop updates your data."
            "</p>"
            "<p><strong>Relay endpoint (for your A-Tech rep)</strong></p>"
            "<ul>"
            "<li><strong>Host:</strong> <code>5.161.121.143</code> (SFTP)</li>"
            "<li><strong>Port:</strong> <code>22</code></li>"
            "<li><strong>Directory:</strong> <code>uploads</code></li>"
            "<li><strong>Credentials:</strong> we send you the SFTP user and password after onboarding</li>"
            "</ul>"
            "<p><strong>What we sync from their combined feed</strong></p>"
            "<ul>"
            "<li><strong>Catalog:</strong> distributor part numbers, manufacturer references, descriptions, and brand "
            "mapping hooks (prefix-based) so parts line up with your aftermarket catalog.</li>"
            "<li><strong>Multi-location inventory:</strong> separate on-hand quantities for each A-Tech DC&mdash;"
            "Tallmadge (OH), Sparks (NV), McDonough (GA), and Arlington (TX)&mdash;surfaced as regional availability "
            "in your integration layer.</li>"
            "<li><strong>Pricing:</strong> cost, retail, and jobber columns from the relay feed; per-company pulls "
            "reuse the same field layout so your negotiated prices stay aligned when reps refresh your file.</li>"
            "<li><strong>Extras:</strong> core and freight-related fee columns, hazmat / handling flags where present, "
            "and GTIN when the supplier includes it.</li>"
            "</ul>"
            "<p>After we send your SFTP details, enter <strong>sftp_user</strong> and <strong>sftp_password</strong> "
            "below and save. Saving also schedules a pricing refresh for your company where supported. "
            "Questions: <a href=\"mailto:info@aftermarketscout.com\">info@aftermarketscout.com</a>.</p>"
        ),
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
        "installation_instructions_html": (
            "<p><strong>How the feed works</strong></p>"
            "<p>DLG does not load inventory into AftermarketScout directly. They email the "
            "inventory file (CSV) to <strong>your</strong> business address—the same one DLG has on file. "
            "You (or your IT) must <strong>forward</strong> that message (or the attachment) to "
            "<a href=\"mailto:{dlg_fwd}\">{dlg_fwd}</a>. Full automated email ingestion is coming later; for now, "
            "this forward is how we match inventory updates to your account."
            "</p>"
            "<p><strong>What to enter here</strong></p>"
            "<ul>"
            "<li><strong>{email_from_key}:</strong> the exact address that <em>receives</em> DLG&rsquo;s inventory email, "
            "so we can tell which company a message belongs to. Use the mailbox DLG actually uses, not a personal alias "
            "unless that is the address DLG targets.</li>"
            "</ul>"
        ).format(
            dlg_fwd=DLG_INVENTORY_FORWARD_TO_EMAIL,
            email_from_key=DLG_CREDENTIALS_EMAIL_FROM,
        ),
    },
    {
        "kind": enums.BrandProviderKind.AUTOMATIC_DISTRIBUTORS,
        "name": "Automatic Distributors",
        "description": "Access inventory and pricing from Automatic Distributors via FTP.",
        "icon_url": "https://api.aftermarketscout.com/uploads/automatic_distributors_logo.png",
        "category": "Distributors",
        "connection_required_fields": ["ftp_user", "ftp_password"],
        "installation_instructions_html": (
            "<p><strong>Automatic Distributors</strong> provides an FTP account for your product data. "
            "However, a request must be raised with your account manager before they will create it. "
            "These are the same credentials as their catalog connection.</p>"
            "<ol>"
            "<li>Contact your Automatic Distributors account manager and request FTP access for data feeds.</li>"
            "<li>Once you receive your <strong>FTP Login</strong> and <strong>FTP Password</strong>, "
            "enter them below and save the connection.</li>"
            "</ol>"
            "<p>For assistance please contact: "
            "<a href=\"mailto:Custserv@autodist.com\">Custserv@autodist.com</a>.</p>"
        ),
    },
    {
        "kind": enums.BrandProviderKind.CTP_DISTRIBUTORS,
        "name": "CTP Distributors",
        "description": "Access inventory and pricing from CTP Distributors via FTP relay.",
        "icon_url": "https://api.aftermarketscout.com/uploads/ctp_distributors_logo.png",
        "category": "Distributors",
        "connection_required_fields": ["ftp_user", "ftp_password"],
        "installation_instructions_html": (
            "<p><strong>CTP Distributors</strong> sends their data feed directly to an FTP site. "
            "Please ask your CTP account representative to set up this FTP connection.</p>"
            "<ol>"
            "<li>Email <a href=\"mailto:info@aftermarketscout.com\">info@aftermarketscout.com</a> "
            "and they will create a unique account for you to receive CTP&rsquo;s data.</li>"
            "<li>Once you have your credentials, enter your <strong>FTP Login</strong> and "
            "<strong>FTP Password</strong> below.</li>"
            "</ol>"
            "<p><strong>Example connection details</strong></p>"
            "<ul>"
            "<li><strong>FTP:</strong> <code>5.161.121.143</code></li>"
            "<li><strong>Port:</strong> <code>22</code></li>"
            "<li><strong>Folder:</strong> <code>/ctp</code></li>"
            "</ul>"
            "<p>For assistance please contact: "
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
        "connection_required_fields": ["ftp_user", "ftp_password"],
        "installation_instructions_html": (
            "<p><strong>Crown Automotive</strong> provides an FTP-based data feed. "
            "If you do not have your own FTP site, email "
            "<a href=\"mailto:info@aftermarketscout.com\">info@aftermarketscout.com</a> "
            "and they will create a unique account for you to receive Crown&rsquo;s data. "
            "This login can be shared with your Crown account representative so they can provide "
            "your stock and pricing data.</p>"
            "<ol>"
            "<li>Obtain your FTP credentials from <a href=\"mailto:info@aftermarketscout.com\">"
            "info@aftermarketscout.com</a>.</li>"
            "<li>Share the credentials with your Crown account rep and ask them to push the feed.</li>"
            "<li>Enter your <strong>FTP Login</strong> and <strong>FTP Password</strong> below and save.</li>"
            "</ol>"
            "<p><strong>Example connection details</strong></p>"
            "<ul>"
            "<li><strong>FTP:</strong> <code>5.161.121.143</code></li>"
            "<li><strong>Folder:</strong> <code>/crown</code></li>"
            "</ul>"
            "<p>For assistance contact: "
            "<a href=\"mailto:info@aftermarketscout.com\">info@aftermarketscout.com</a>.</p>"
        ),
    },
    {
        "kind": enums.BrandProviderKind.DIX_PERF_NORTH,
        "name": "DIX Perf North",
        "description": "Access DIX Performance North inventory and pricing via FTP relay.",
        "icon_url": "https://api.aftermarketscout.com/uploads/dix_perf_north_logo.png",
        "category": "Distributors",
        "connection_required_fields": ["ftp_user", "ftp_password"],
        "installation_instructions_html": (
            "<p><strong>DIX Performance North</strong> provides an FTP-based data feed. "
            "If you do not have your own FTP site, email "
            "<a href=\"mailto:info@aftermarketscout.com\">info@aftermarketscout.com</a> "
            "and they will create a unique account for you to receive DIX&rsquo;s data. "
            "This login can be shared with your DIX account representative so they can provide "
            "your stock and pricing data.</p>"
            "<ol>"
            "<li>Obtain your FTP credentials from "
            "<a href=\"mailto:info@aftermarketscout.com\">info@aftermarketscout.com</a>.</li>"
            "<li>Share the credentials with your DIX rep and ask them to push the feed.</li>"
            "<li>Enter your <strong>FTP Login</strong> and <strong>FTP Password</strong> below and save.</li>"
            "</ol>"
            "<p><strong>Example connection details</strong></p>"
            "<ul>"
            "<li><strong>FTP:</strong> <code>5.161.121.143</code></li>"
            "<li><strong>Port:</strong> <code>22</code></li>"
            "<li><strong>Folder:</strong> <code>/dix</code></li>"
            "</ul>"
            "<p>For assistance contact: "
            "<a href=\"mailto:sp@dixperformancenorth.com\">sp@dixperformancenorth.com</a>.</p>"
        ),
    },
    {
        "kind": enums.BrandProviderKind.EARL_OWEN,
        "name": "Earl Owen",
        "description": "Access Earl Owen Company inventory and pricing via FTP relay.",
        "icon_url": "https://api.aftermarketscout.com/uploads/earl_owen_logo.png",
        "category": "Distributors",
        "connection_required_fields": ["ftp_user", "ftp_password"],
        "installation_instructions_html": (
            "<p><strong>Earl Owen Company</strong> provides an FTP-based data feed. "
            "If you do not have your own FTP site, email "
            "<a href=\"mailto:info@aftermarketscout.com\">info@aftermarketscout.com</a> "
            "and they will create a unique account for you to receive Earl Owen&rsquo;s data. "
            "This login can be shared with your Earl Owen account representative so they can provide "
            "your stock and pricing data.</p>"
            "<ol>"
            "<li>Obtain your FTP credentials from "
            "<a href=\"mailto:info@aftermarketscout.com\">info@aftermarketscout.com</a>.</li>"
            "<li>Share the credentials with your Earl Owen rep and ask them to push the feed.</li>"
            "<li>Enter your <strong>FTP Login</strong> and <strong>FTP Password</strong> below and save.</li>"
            "</ol>"
            "<p><strong>Example connection details</strong></p>"
            "<ul>"
            "<li><strong>FTP:</strong> <code>5.161.121.143</code></li>"
            "<li><strong>Port:</strong> <code>22</code></li>"
            "<li><strong>Folder:</strong> <code>/earlowen</code></li>"
            "</ul>"
            "<p>For assistance contact: "
            "<a href=\"mailto:JSmith@earlowen.com\">JSmith@earlowen.com</a>.</p>"
        ),
    },
    {
        "kind": enums.BrandProviderKind.ELITE_WHEEL,
        "name": "Elite Wheel",
        "description": "Access Elite Wheel & Tire inventory and pricing via API.",
        "icon_url": "https://api.aftermarketscout.com/uploads/elite_wheel_logo.png",
        "category": "Distributors",
        "connection_required_fields": ["username", "password"],
        "installation_instructions_html": (
            "<p><strong>Elite Wheel &amp; Tire</strong> provides access via their API using a "
            "<strong>Username</strong> and <strong>Password</strong>.</p>"
            "<ol>"
            "<li>Fill out the "
            "<a href=\"https://www.ewwfl.com\" target=\"_blank\" rel=\"noopener noreferrer\">Inventory Request Form</a> "
            "on the Elite Wheel website — this is the most efficient way to get API access.</li>"
            "<li>Alternatively, email <a href=\"mailto:it@ewwfl.com\">it@ewwfl.com</a> or contact your Elite sales rep.</li>"
            "<li>Once you receive your credentials, enter your <strong>Username</strong> and <strong>Password</strong> "
            "below and save the connection.</li>"
            "</ol>"
            "<p>For assistance contact: <a href=\"mailto:it@ewwfl.com\">it@ewwfl.com</a>.</p>"
        ),
    },
    {
        "kind": enums.BrandProviderKind.FASTCO,
        "name": "FastCo",
        "description": "Access FastCo inventory and pricing via FTP.",
        "icon_url": "https://api.aftermarketscout.com/uploads/fastco_logo.png",
        "category": "Distributors",
        "connection_required_fields": ["ftp_user", "ftp_password"],
        "installation_instructions_html": (
            "<p><strong>FastCo</strong> provides an FTP account for your product data. "
            "However, a request must be raised with your account manager before they will create it.</p>"
            "<ol>"
            "<li>Contact your FastCo account manager and request FTP access.</li>"
            "<li>Once you have the connection details, enter your <strong>FTP Login</strong> and "
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
        "connection_required_fields": ["account_number"],
        "connection_optional_fields": ["access_token", "token_secret"],
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
        "description": "Access HelmHouse public inventory and pricing — no account required.",
        "icon_url": "https://api.aftermarketscout.com/uploads/helmhouse_logo.png",
        "category": "Distributors",
        "connection_required_fields": [],
        "installation_instructions_html": (
            "<p><strong>Helmet House</strong> provides public price and stock data — "
            "an account is not required. Simply save the connection to activate it.</p>"
            "<p>For assistance contact: "
            "<a href=\"mailto:info@helmethouse.com\">info@helmethouse.com</a>.</p>"
        ),
    },
    {
        "kind": enums.BrandProviderKind.THIBAULT,
        "name": "Thibault",
        "description": "Access Thibault (Importations Thibault) inventory via their open FTP feed.",
        "icon_url": "https://api.aftermarketscout.com/uploads/thibault_logo.png",
        "category": "Distributors",
        "connection_required_fields": [],
        "installation_instructions_html": (
            "<p><strong>Thibault</strong> (Importations Thibault) FTP feed is already enabled for all dealers — "
            "no store-specific login credentials are required. Simply save the connection to activate it.</p>"
            "<p>For assistance contact: "
            "<a href=\"mailto:info@importationsthibault.com\">info@importationsthibault.com</a>.</p>"
        ),
    },
]

_UPLOADS = "https://api.aftermarketscout.com/uploads"

COMING_SOON_PROVIDERS = [
    {"kind": enums.BrandProviderKind.ATD,                     "name": "ATD",                       "category": "Distributors", "icon_url": f"{_UPLOADS}/atd_logo.png"},
    {"kind": enums.BrandProviderKind.ALLPRO_DISTRIBUTING,     "name": "AllPro Distributing",        "category": "Distributors", "icon_url": f"{_UPLOADS}/allpro_logo.png"},
    {"kind": enums.BrandProviderKind.HOLLEY_PERFORMANCE,      "name": "Holley Performance",         "category": "Distributors", "icon_url": f"{_UPLOADS}/holley_logo.png"},
    {"kind": enums.BrandProviderKind.MARCOR,                  "name": "Marcor",                     "category": "Distributors", "icon_url": f"{_UPLOADS}/marcor_logo.png"},
    {"kind": enums.BrandProviderKind.MOTOR_STATE_DISTRIBUTING,"name": "Motor State Distributing",   "category": "Distributors", "icon_url": f"{_UPLOADS}/motor_state_logo.png"},
    {"kind": enums.BrandProviderKind.OVERLAND_VEHICLE_SYSTEMS,"name": "Overland Vehicle Systems",   "category": "Distributors", "icon_url": f"{_UPLOADS}/overland_vehicle_systems_logo.png"},
    {"kind": enums.BrandProviderKind.PARTS_AUTHORITY,         "name": "Parts Authority",            "category": "Distributors", "icon_url": f"{_UPLOADS}/parts_authority_logo.png"},
    {"kind": enums.BrandProviderKind.PARTS_CANADA,            "name": "Parts Canada",               "category": "Distributors", "icon_url": f"{_UPLOADS}/parts_canada_logo.png"},
    # {"kind": enums.BrandProviderKind.PARTS_UNLIMITED,         "name": "Parts Unlimited",            "category": "Distributors", "icon_url": f"{_UPLOADS}/parts_unlimited_logo.png"},
    {"kind": enums.BrandProviderKind.PREMIER_PERFORMANCE,     "name": "Premier Performance",        "category": "Distributors", "icon_url": f"{_UPLOADS}/premier_performance_logo.png"},
    {"kind": enums.BrandProviderKind.SSF_IMPORTED_AUTO_PARTS, "name": "SSF Imported Auto Parts",    "category": "Distributors", "icon_url": f"{_UPLOADS}/ssf_logo.png"},
    {"kind": enums.BrandProviderKind.THE_WHEEL_GROUP,         "name": "The Wheel Group",            "category": "Distributors", "icon_url": f"{_UPLOADS}/the_wheel_group_logo.png"},
    # {"kind": enums.BrandProviderKind.THIBERT,                 "name": "Thibert",                    "category": "Distributors", "icon_url": f"{_UPLOADS}/thibert_logo.png"},
    # {"kind": enums.BrandProviderKind.WESTERN_POWER_SPORTS,    "name": "Western Power Sports",       "category": "Distributors", "icon_url": f"{_UPLOADS}/western_power_sports_logo.png"},
    # {"kind": enums.BrandProviderKind.XDP,                     "name": "XDP",                        "category": "Distributors", "icon_url": f"{_UPLOADS}/xdp_logo.png"},
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

