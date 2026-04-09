"""DLG SFTP inventory CSV (comma-separated; header row)."""

DEFAULT_REMOTE_INVENTORY_FILENAME = "dlg_inventory.csv"

# Expected headers (strip keys when parsing).
INVENTORY_COLUMNS = (
    "Brand",
    "Name",
    "Display Name",
    "Available On Hand",
    "Units",
    "Base Price",
)
