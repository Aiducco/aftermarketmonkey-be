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
}

# Provider kind_name -> image URL (used by parts API)
PROVIDER_IMAGE_URLS = {
    "TURN_14": "https://api.aftermarketmonkey.com/uploads/t14_logo.png",
    "KEYSTONE": "https://api.aftermarketmonkey.com/uploads/keystone.png",
    "MEYER": "https://api.aftermarketmonkey.com/uploads/meyer_logo.png",
    "ATECH": "https://api.aftermarketmonkey.com/uploads/atech_logo.png",
    "DLG": "https://api.aftermarketmonkey.com/uploads/dlg_logo.png",
    "ROUGH_COUNTRY": "https://api.aftermarketmonkey.com/uploads/rough_country.png",
    "SDC": "",
    "WHEELPROS": "https://api.aftermarketmonkey.com/uploads/wheel_pros_logo.png",
}

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
        "icon_url": "https://api.aftermarketmonkey.com/uploads/t14_logo.png",
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
            "<li>Paste them into the fields below and save. aftermarketmonkey uses them only to call Turn 14 on your behalf.</li>"
            "</ul>"
            "<p>If you cannot access that page or the credentials are missing, contact Turn 14 support or your account manager.</p>"
        ),
    },
    {
        "kind": enums.BrandProviderKind.KEYSTONE,
        "name": "Keystone",
        "description": "Sync inventory and pricing from Keystone Automotive via FTP.",
        "icon_url": "https://api.aftermarketmonkey.com/uploads/keystone.png",
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
        "description": (
            "Sync parts catalog, pricing, and vehicle fitment from Rough Country via jobber Excel feed."
        ),
        "icon_url": "https://api.aftermarketmonkey.com/uploads/rough_country.png",
        "category": "Distributors",
        "connection_required_fields": [ROUGH_COUNTRY_CREDENTIALS_FEED_URL],
        "installation_instructions_html": (
            "<p><strong>Rough Country</strong> uses a downloadable Excel jobber feed URL per dealer account.</p>"
            "<ol>"
            "<li>In your Rough Country jobber portal, <strong>locate the feed</strong> (or download link) for "
            "the full catalog (General, Fitment, Discontinued). Copy the <strong>full HTTPS URL</strong>—the "
            "complete link starting with <code>https://</code>.</li>"
            "<li>Paste that URL into <strong>feed_url</strong> below and save the connection.</li>"
            "</ol>"
        ),
    },
    {
        "kind": enums.BrandProviderKind.WHEELPROS,
        "name": "Wheel Pros",
        "description": (
            "Sync wheels, tires, and accessories inventory and pricing from WheelPros via SFTP."
        ),
        "icon_url": "https://api.aftermarketmonkey.com/uploads/wheel_pros_logo.png",
        "category": "Distributors",
        "connection_required_fields": ["sftp_user", "sftp_password"],
        # Remote CSV path per feed (wheel/tire/accessories); defaults in settings if omitted
        "connection_optional_fields": ["sftp_path"],
        "installation_instructions_html": (
            "<p><strong>Wheel Pros</strong> inventory and pricing CSVs (wheels, tires, accessories) are on Wheel "
            "Pros&rsquo; SFTP server (<code>sftp.wheelpros.com</code>, port 22). AftermarketMonkey connects there "
            "automatically; you only enter the account credentials Wheel Pros gives you.</p>"
            "<ol>"
            "<li>Request your SFTP <strong>username</strong> and <strong>password</strong> from Wheel Pros.</li>"
            "<li>Enter <strong>sftp_user</strong> and <strong>sftp_password</strong> below. If your agreement uses "
            "non-default remote paths, set optional <strong>sftp_path</strong>; otherwise defaults apply per feed.</li>"
            "<li>Save the connection. Company-specific pricing is read from SFTP after catalog sync.</li>"
            "</ol>"
        ),
    },
    {
        "kind": enums.BrandProviderKind.MEYER,
        "name": "Meyer",
        "description": (
            "Sync Meyer pricing and inventory from AftermarketMonkey's SFTP relay (Meyer Pricing + Meyer Inventory CSVs)."
        ),
        "icon_url": "https://api.aftermarketmonkey.com/uploads/meyer_logo.png",
        "category": "Distributors",
        "connection_required_fields": ["sftp_user", "sftp_password"],
        "connection_optional_fields": [],
        "installation_instructions_html": (
            "<p>Email <a href=\"mailto:info@aftermarketmonkey.com\">info@aftermarketmonkey.com</a> so we can "
            "create a unique SFTP account for you and send a <strong>username</strong> and <strong>password</strong>. "
            "Meyer Distributing delivers the data feed to AftermarketMonkey&rsquo;s SFTP relay; ask your Meyer account "
            "representative to set up the feed to connect to <strong>our</strong> SFTP endpoint using the details below.</p>"
            "<p><strong>Endpoint for your Meyer rep</strong></p>"
            "<ul>"
            "<li><strong>SFTP:</strong> <code>54.145.82.238</code></li>"
            "<li><strong>Port:</strong> <code>22</code></li>"
            "<li><strong>Folder:</strong> <code>uploads</code></li>"
            "<li><strong>Files:</strong> <code>Meyer Pricing.csv</code>, <code>Meyer Inventory.csv</code></li>"
            "<li><strong>User / password:</strong> we provide these after you email us</li>"
            "</ul>"
            "<p>When you have your login, enter <strong>sftp_user</strong> and <strong>sftp_password</strong> below "
            "and save the connection. For help, contact "
            "<a href=\"mailto:info@aftermarketmonkey.com\">info@aftermarketmonkey.com</a>.</p>"
        ),
    },
    {
        "kind": enums.BrandProviderKind.ATECH,
        "name": "A-Tech",
        "description": (
            "Sync A-Tech catalog, multi-location inventory, and pricing from AftermarketMonkey's SFTP relay "
            "(single feed file <code>atechfile.txt</code>)."
        ),
        "icon_url": "https://api.aftermarketmonkey.com/uploads/atech_logo.png",
        "category": "Distributors",
        "connection_required_fields": ["sftp_user", "sftp_password"],
        "connection_optional_fields": [],
        "installation_instructions_html": (
            "<p>Email <a href=\"mailto:info@aftermarketmonkey.com\">info@aftermarketmonkey.com</a> so we can "
            "create a unique SFTP account for you and send a <strong>username</strong> and <strong>password</strong>. "
            "A-Tech delivers one combined feed to AftermarketMonkey&rsquo;s SFTP relay; ask your A-Tech account "
            "representative to set up the feed to connect to <strong>our</strong> SFTP endpoint using the details below.</p>"
            "<p><strong>Endpoint for your A-Tech rep</strong></p>"
            "<ul>"
            "<li><strong>SFTP:</strong> <code>54.145.82.238</code></li>"
            "<li><strong>Port:</strong> <code>22</code></li>"
            "<li><strong>Folder:</strong> <code>uploads</code></li>"
            "<li><strong>File:</strong> <code>atechfile.txt</code> (pricing, warehouse quantities, fees, GTIN)</li>"
            "<li><strong>User / password:</strong> we provide these after you email us</li>"
            "</ul>"
            "<p><strong>Warehouse quantity columns</strong> (by DC): Tallmadge OH, Sparks NV, McDonough GA, Arlington TX.</p>"
            "<p>When you have your login, enter <strong>sftp_user</strong> and <strong>sftp_password</strong> below "
            "and save the connection. For help, contact "
            "<a href=\"mailto:info@aftermarketmonkey.com\">info@aftermarketmonkey.com</a>.</p>"
        ),
    },
    {
        "kind": enums.BrandProviderKind.DLG,
        "name": "DLG",
        "description": (
            "Sync DLG inventory from AftermarketMonkey's SFTP relay (dlg_inventory.csv: brand, SKU, description, qty, price)."
        ),
        "icon_url": "https://api.aftermarketmonkey.com/uploads/dlg_logo.png",
        "category": "Distributors",
        "connection_required_fields": ["sftp_user", "sftp_password"],
        "connection_optional_fields": [],
        "installation_instructions_html": (
            "<p>Email <a href=\"mailto:info@aftermarketmonkey.com\">info@aftermarketmonkey.com</a> for SFTP credentials. "
            "DLG inventory is delivered to AftermarketMonkey&rsquo;s relay as <code>dlg_inventory.csv</code> "
            "(Brand, Name, Display Name, Available On Hand, Units, Base Price).</p>"
            "<p><strong>Endpoint</strong></p>"
            "<ul>"
            "<li><strong>SFTP:</strong> <code>54.145.82.238</code></li>"
            "<li><strong>Port:</strong> <code>22</code></li>"
            "<li><strong>Folder:</strong> <code>uploads</code></li>"
            "<li><strong>File:</strong> <code>dlg_inventory.csv</code></li>"
            "</ul>"
            "<p>Enter <strong>sftp_user</strong> and <strong>sftp_password</strong> below and save.</p>"
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

