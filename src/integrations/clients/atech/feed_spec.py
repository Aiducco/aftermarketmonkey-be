"""
A-Tech relay feed: one delimited text file (comma-separated in samples; header row).

Use these names when parsing (normalize headers with strip()).
"""

# Default remote filename on the relay SFTP.
DEFAULT_REMOTE_FEED_FILENAME = "atechfile.txt"

# Expected column order / names from the header row.
FEED_COLUMNS = (
    "part_number",
    "description",
    "price_atech_current",
    "price_current_month",
    "cost_current_sheet",
    "tallmadge_qty",
    "sparks_qty",
    "mcdonough_qty",
    "arlington_qty",
    "cost_core",
    "fee_hazmat",
    "fee_truck_us",
    "fee_handling_ground",
    "fee_handling_air",
    "gtin",
)

# Feed column -> meaning (aligned to A-Tech website labels).
COLUMN_MEANING = {
    "part_number": "Full distributor line in file (e.g. ACC-35370); DB also stores suffix after prefix",
    "description": "Description",
    "price_atech_current": "Cost",
    "price_current_month": "Retail",
    "cost_current_sheet": "Jobber",
    "tallmadge_qty": "Qty — Tallmadge, OH",
    "sparks_qty": "Qty — Sparks, NV",
    "mcdonough_qty": "Qty — McDonough, GA",
    "arlington_qty": "Qty — Arlington, TX",
    "cost_core": "Core charge",
    "fee_hazmat": "Shipping fee (HAZMAT)",
    "fee_truck_us": "Shipping fee (freight truck)",
    "fee_handling_ground": "Shipping fee (oversize — ground)",
    "fee_handling_air": "Shipping fee (oversize — air)",
    "gtin": "GTIN",
}

WAREHOUSE_QTY_COLUMNS = ("tallmadge_qty", "sparks_qty", "mcdonough_qty", "arlington_qty")
