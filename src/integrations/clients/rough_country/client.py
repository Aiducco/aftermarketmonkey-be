"""
Client for Rough Country jobber feed (Excel).
Downloads jobber_pc2A.xlsx and parses General, Vehicle Fitment, and Discontinued sheets.
Uses a browser-like User-Agent to avoid 403. Configure ROUGH_COUNTRY_FEED_URL in settings if needed.
"""
import logging
import typing
import urllib.error
import urllib.request

import pandas as pd
from django.conf import settings

from src.integrations.clients.rough_country import exceptions

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[ROUGH-COUNTRY-CLIENT]"

DEFAULT_FILE_URL = "https://feeds.roughcountry.com/jobber_pc2A.xlsx"
DEFAULT_LOCAL_FILE_NAME = "jobber_pc2A.xlsx"


def _df_to_list_of_dicts(df: pd.DataFrame) -> typing.List[typing.Dict]:
    """Convert DataFrame to list of dicts, with NaN -> None and strip column names."""
    if df is None or df.empty:
        return []
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    return df.replace({pd.NA: None}).to_dict("records")


class RoughCountryFeedClient:
    """
    Fetches and parses the Rough Country jobber Excel feed.
    Sheets: General (products), Vehicle Fitment or Fitment, Discontinued.
    """

    def __init__(
        self,
        file_url: typing.Optional[str] = None,
        local_file_name: typing.Optional[str] = None,
        local_file_path: typing.Optional[str] = None,
    ):
        self.file_url = (
            file_url
            or getattr(settings, "ROUGH_COUNTRY_FEED_URL", None)
            or DEFAULT_FILE_URL
        )
        self.local_file_name = local_file_name or DEFAULT_LOCAL_FILE_NAME
        self.local_file_path = local_file_path

    def _get_xlsx_path(self) -> str:
        """Return path to local xlsx (download to temp if not using local_file_path)."""
        if self.local_file_path:
            return self.local_file_path
        import tempfile
        import os
        return os.path.join(tempfile.gettempdir(), self.local_file_name)

    def download(self) -> str:
        """Download the Excel file from file_url to local path. Returns path."""
        path = self._get_xlsx_path()
        try:
            logger.info("{} Downloading feed from {}.".format(_LOG_PREFIX, self.file_url))
            req = urllib.request.Request(
                self.file_url,
                headers={
                    "User-Agent": "Mozilla/5.0 (compatible; AftermarketMonkey/1.0; +https://aftermarketmonkey.com)",
                    "Accept": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,*/*",
                },
            )
            with urllib.request.urlopen(req, timeout=120) as resp:
                with open(path, "wb") as f:
                    f.write(resp.read())
            logger.info("{} Saved to {}.".format(_LOG_PREFIX, path))
            return path
        except urllib.error.HTTPError as e:
            msg = "HTTP {} when downloading feed: {}.".format(e.code, e.reason)
            logger.error("{} {}".format(_LOG_PREFIX, msg))
            raise exceptions.RoughCountryDownloadError(msg)
        except Exception as e:
            msg = "Failed to download feed: {}.".format(str(e))
            logger.error("{} {}".format(_LOG_PREFIX, msg))
            raise exceptions.RoughCountryDownloadError(msg)

    def get_feed_data(
        self,
        download_if_missing: bool = True,
    ) -> typing.Dict[str, typing.List[typing.Dict]]:
        """
        Load Excel and return dict with keys: general, fitment, discontinued.
        Each value is a list of row dicts (column names as keys).
        """
        import os
        path = self._get_xlsx_path()
        if download_if_missing and not os.path.exists(path):
            self.download()
        path = self._get_xlsx_path()

        try:
            sheets = pd.read_excel(path, sheet_name=None, engine="openpyxl")
        except Exception as e:
            msg = "Failed to parse Excel: {}.".format(str(e))
            logger.error("{} {}".format(_LOG_PREFIX, msg))
            raise exceptions.RoughCountryParseError(msg)

        # Normalize sheet names (strip, match General / Fitment / Discontinued)
        result = {
            "general": [],
            "fitment": [],
            "discontinued": [],
        }
        for name, df in sheets.items():
            name_clean = str(name).strip().lower()
            if "general" in name_clean:
                result["general"] = _df_to_list_of_dicts(df)
            elif "fitment" in name_clean or "vehicle" in name_clean:
                # Prefer "Vehicle Fitment" content; if we have both, merge or take one
                result["fitment"] = _df_to_list_of_dicts(df)
            elif "discontinued" in name_clean:
                result["discontinued"] = _df_to_list_of_dicts(df)

        logger.info(
            "{} Loaded general={} fitment={} discontinued={}.".format(
                _LOG_PREFIX,
                len(result["general"]),
                len(result["fitment"]),
                len(result["discontinued"]),
            )
        )
        return result
