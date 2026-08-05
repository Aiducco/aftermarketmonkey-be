"""
Client for the Quadratec dealer feeds. Quadratec exposes its catalog/pricing/inventory as two
downloadable files, both keyed on the Quadratec part number:

  * catalog feed  -- ``pricingSheet_quad.xlsx``: Quadratec PN, MPN, Description, UPC Code, Brand,
    Retail Price, Wholesale Price, Shipping Surcharge. No header row is trusted -- columns are
    positional and the first row is skipped (it is a header in the dealer export).
  * pricing/inventory feed -- ``quadratec_wholesale.csv``: Quadratec Part No, Part No, Description,
    Brand, Inventory PA1/PA2/NV1/Total, Cost, Surcharge, UPC, MAP. Same positional treatment.

Per-company feed URLs live in ``CompanyProviders.credentials['feed']`` as
``catalog_feed_url`` / ``pricing_feed_url`` (see ``src.constants.QUADRATEC_CREDENTIALS_*``). Until
live per-dealer URLs are wired up, the client reads two bundled static files under
``<BASE_DIR>/resources/quadratec/`` (override with ``QUADRATEC_CATALOG_LOCAL_FILE`` /
``QUADRATEC_PRICING_LOCAL_FILE`` in settings). Flip ``QUADRATEC_READ_LOCAL_STATIC`` to False once
the download path is implemented; the URL fields are already accepted and stored.
"""
import logging
import os
import typing

import pandas as pd
from django.conf import settings

from src.integrations.clients.quadratec import exceptions

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[QUADRATEC-CLIENT]"

DEFAULT_CATALOG_LOCAL_FILE_NAME = "pricingSheet_quad.xlsx"
DEFAULT_PRICING_LOCAL_FILE_NAME = "quadratec_wholesale.csv"

# Positional column names (both feeds carry a header row we override + skip -- see module docstring).
CATALOG_COLUMNS = [
    "quadratec_pn",
    "mpn",
    "description",
    "upc",
    "brand",
    "retail_price",
    "wholesale_price",
    "shipping_surcharge",
]
PRICING_COLUMNS = [
    "quadratec_pn",
    "mpn",
    "description",
    "brand",
    "inv_pa1",
    "inv_pa2",
    "inv_nv1",
    "inv_total",
    "cost",
    "surcharge",
    "upc",
    "map",
]


def _default_static_dir() -> str:
    base = getattr(settings, "BASE_DIR", None)
    if base is not None:
        return os.path.join(str(base), "resources", "quadratec")
    # Fallback: repo root relative to this file (src/integrations/clients/quadratec/client.py).
    here = os.path.dirname(os.path.abspath(__file__))
    repo_root = os.path.abspath(os.path.join(here, "..", "..", "..", ".."))
    return os.path.join(repo_root, "resources", "quadratec")


class QuadratecFeedClient:
    """
    Reads the two Quadratec feeds and returns raw row dicts (numeric parsing/normalization is the
    service layer's job, matching the Rough Country client). ``catalog_feed_url`` /
    ``pricing_feed_url`` are accepted and stored so connections can be provisioned now, but while
    ``read_local_static`` is True the two bundled static files are read instead.
    """

    def __init__(
        self,
        catalog_feed_url: typing.Optional[str] = None,
        pricing_feed_url: typing.Optional[str] = None,
        catalog_local_path: typing.Optional[str] = None,
        pricing_local_path: typing.Optional[str] = None,
        read_local_static: typing.Optional[bool] = None,
    ):
        self.catalog_feed_url = (catalog_feed_url or "").strip() or None
        self.pricing_feed_url = (pricing_feed_url or "").strip() or None
        static_dir = _default_static_dir()
        self.catalog_local_path = (
            catalog_local_path
            or getattr(settings, "QUADRATEC_CATALOG_LOCAL_FILE", None)
            or os.path.join(static_dir, DEFAULT_CATALOG_LOCAL_FILE_NAME)
        )
        self.pricing_local_path = (
            pricing_local_path
            or getattr(settings, "QUADRATEC_PRICING_LOCAL_FILE", None)
            or os.path.join(static_dir, DEFAULT_PRICING_LOCAL_FILE_NAME)
        )
        if read_local_static is None:
            read_local_static = bool(getattr(settings, "QUADRATEC_READ_LOCAL_STATIC", True))
        self.read_local_static = read_local_static

    def _require_local_files(self) -> None:
        for path in (self.catalog_local_path, self.pricing_local_path):
            if not path or not os.path.exists(path):
                raise exceptions.QuadratecDownloadError(
                    "Quadratec static feed file missing: {}.".format(path)
                )

    def test_connection(self) -> None:
        """
        Connect-time check. While reading local static files, confirm both are present and
        readable; once live URLs are wired up this will validate ``catalog_feed_url`` /
        ``pricing_feed_url`` reachability instead.
        """
        if self.read_local_static:
            self._require_local_files()
            for path in (self.catalog_local_path, self.pricing_local_path):
                if os.path.getsize(path) <= 0:
                    raise exceptions.QuadratecDownloadError(
                        "Quadratec static feed file is empty: {}.".format(path)
                    )
            return
        # Live-URL validation is added when the download path is implemented.
        if not self.catalog_feed_url or not self.pricing_feed_url:
            raise ValueError("Both catalog_feed_url and pricing_feed_url are required.")

    def _read_catalog(self) -> typing.List[typing.Dict]:
        path = self.catalog_local_path
        try:
            df = pd.read_excel(
                path,
                header=None,
                names=CATALOG_COLUMNS,
                skiprows=1,
                engine="openpyxl",
                dtype=str,
            )
        except Exception as e:
            raise exceptions.QuadratecParseError(
                "Failed to parse Quadratec catalog file {}: {}.".format(path, str(e))
            )
        return df.where(pd.notna(df), None).to_dict("records")

    def _read_pricing_inventory(self) -> typing.List[typing.Dict]:
        path = self.pricing_local_path
        try:
            df = pd.read_csv(
                path,
                header=None,
                names=PRICING_COLUMNS,
                skiprows=1,
                dtype=str,
                keep_default_na=False,
                na_values=[""],
            )
        except Exception as e:
            raise exceptions.QuadratecParseError(
                "Failed to parse Quadratec pricing/inventory file {}: {}.".format(path, str(e))
            )
        return df.where(pd.notna(df), None).to_dict("records")

    def get_feed_data(self) -> typing.Dict[str, typing.List[typing.Dict]]:
        """
        Return ``{"catalog": [...], "pricing_inventory": [...]}``. Each list holds row dicts keyed
        by the positional column names above; both are keyed on ``quadratec_pn`` for joining.
        """
        if not self.read_local_static:
            raise exceptions.QuadratecDownloadError(
                "Live Quadratec feed download is not implemented yet; set QUADRATEC_READ_LOCAL_STATIC=True."
            )
        self._require_local_files()
        catalog = self._read_catalog()
        pricing_inventory = self._read_pricing_inventory()
        logger.info(
            "{} Loaded catalog={} pricing_inventory={} from static files.".format(
                _LOG_PREFIX, len(catalog), len(pricing_inventory)
            )
        )
        return {"catalog": catalog, "pricing_inventory": pricing_inventory}
