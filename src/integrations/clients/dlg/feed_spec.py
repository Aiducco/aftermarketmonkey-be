"""DLG SFTP inventory CSV (comma-separated; header row)."""

from src import constants as src_constants

DEFAULT_REMOTE_INVENTORY_FILENAME = src_constants.DLG_INVENTORY_CSV_FILENAME

# Expected headers (strip keys when parsing).
INVENTORY_COLUMNS = (
    "Brand",
    "Name",
    "Display Name",
    "Available On Hand",
    "Units",
    "Base Price",
)
