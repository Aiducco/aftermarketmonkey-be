"""
Client for Vossen's AfterMarket.aspx CSV inventory feed.
Per-company feed URL is stored in CompanyProviders.credentials as feed_url
(see src.constants.VOSSEN_CREDENTIALS_FEED_URL).
"""
import csv
import logging
import os
import time
import typing
import urllib.error
import urllib.request

from django.conf import settings

from src.integrations.clients.vossen import exceptions

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[VOSSEN-CLIENT]"

DEFAULT_FILE_URL = "http://inventory.vossenwheels.com/AfterMarket.aspx"
DEFAULT_LOCAL_FILE_NAME = "vossen_inventory.csv"
REQUIRED_FEED_URL_PREFIX = "http://inventory.vossenwheels.com/"
DEFAULT_FILE_MAX_AGE_SECONDS = 6 * 60 * 60  # 6 hours

_REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; AfterMarketScout/1.0; +https://aftermarketscout.com)",
    "Accept": "text/csv,text/plain,text/html,*/*",
}


class VossenFeedClient:
    """
    Fetches and parses Vossen's AfterMarket.aspx CSV feed: SKU, Description, Available, Price,
    Diameter, Width, Offset, BoltPattern, Centerbore. The server declares
    ``Content-Type: text/html`` even though the body is plain CSV — parsing never relies on
    that header, only on the response body.
    """

    def __init__(
        self,
        file_url: typing.Optional[str] = None,
        local_file_name: typing.Optional[str] = None,
        local_file_path: typing.Optional[str] = None,
    ):
        self.file_url = (
            file_url
            or getattr(settings, "VOSSEN_FEED_URL", None)
            or DEFAULT_FILE_URL
        )
        if self.file_url and not self.file_url.startswith(REQUIRED_FEED_URL_PREFIX):
            raise ValueError(
                "Invalid Vossen feed URL — must start with '{}'. Got: '{}'.".format(
                    REQUIRED_FEED_URL_PREFIX, self.file_url
                )
            )
        self.local_file_name = local_file_name or DEFAULT_LOCAL_FILE_NAME
        self.local_file_path = local_file_path

    def _get_csv_path(self) -> str:
        """Return path to local CSV (download to temp if not using local_file_path)."""
        if self.local_file_path:
            return self.local_file_path
        import tempfile
        return os.path.join(tempfile.gettempdir(), self.local_file_name)

    def is_file_outdated(
        self, path: typing.Optional[str] = None, max_age: int = DEFAULT_FILE_MAX_AGE_SECONDS
    ) -> bool:
        """Return True if local file is missing or older than max_age seconds."""
        p = path or self._get_csv_path()
        if not os.path.exists(p):
            return True
        return (time.time() - os.path.getmtime(p)) > max_age

    def download(self) -> str:
        """Download the CSV feed from file_url to local path. Returns path."""
        path = self._get_csv_path()
        try:
            logger.info("{} Downloading feed from {}.".format(_LOG_PREFIX, self.file_url))
            req = urllib.request.Request(self.file_url, headers=_REQUEST_HEADERS)
            with urllib.request.urlopen(req, timeout=120) as resp:
                with open(path, "wb") as f:
                    f.write(resp.read())
            logger.info("{} Saved to {}.".format(_LOG_PREFIX, path))
            return path
        except urllib.error.HTTPError as e:
            msg = "HTTP {} when downloading feed: {}.".format(e.code, e.reason)
            logger.error("{} {}".format(_LOG_PREFIX, msg))
            raise exceptions.VossenDownloadError(msg)
        except Exception as e:
            msg = "Failed to download feed: {}.".format(str(e))
            logger.error("{} {}".format(_LOG_PREFIX, msg))
            raise exceptions.VossenDownloadError(msg)

    def test_connection(self, timeout: int = 20) -> None:
        """
        Confirm feed_url is reachable without downloading the full CSV — requests only the
        first KB via a Range header (servers that ignore Range just send more than we read; we
        stop after the first chunk either way) and checks the SKU header is present.
        """
        if not self.file_url:
            raise ValueError("feed_url is required.")
        req = urllib.request.Request(
            self.file_url,
            headers=dict(_REQUEST_HEADERS, Range="bytes=0-1023"),
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                status = resp.status if hasattr(resp, "status") else resp.getcode()
                chunk = resp.read(1024)
        except urllib.error.HTTPError as e:
            msg = "HTTP {} when checking feed_url: {}.".format(e.code, e.reason)
            logger.error("{} {}".format(_LOG_PREFIX, msg))
            raise exceptions.VossenDownloadError(msg)
        except urllib.error.URLError as e:
            msg = "Could not reach feed_url: {}.".format(e.reason)
            logger.error("{} {}".format(_LOG_PREFIX, msg))
            raise exceptions.VossenDownloadError(msg)

        if status not in (200, 206) or not chunk:
            msg = "Unexpected response (status={}) when checking feed_url.".format(status)
            logger.error("{} {}".format(_LOG_PREFIX, msg))
            raise exceptions.VossenDownloadError(msg)
        if b"sku" not in chunk.lower():
            msg = "Response does not look like the Vossen CSV feed (missing SKU header)."
            logger.error("{} {}".format(_LOG_PREFIX, msg))
            raise exceptions.VossenDownloadError(msg)

    def get_feed_data(self, download_if_missing: bool = True) -> typing.List[typing.Dict]:
        """
        Load the CSV and return a list of row dicts keyed by the feed's own column names
        (SKU, Description, Available, Price, Diameter, Width, Offset, BoltPattern, Centerbore).
        Re-downloads if the file is missing or older than 6 hours.
        """
        path = self._get_csv_path()
        if download_if_missing and self.is_file_outdated(path):
            self.download()
        path = self._get_csv_path()

        try:
            with open(path, "r", encoding="utf-8-sig", newline="") as f:
                reader = csv.DictReader(f)
                rows = [
                    {(k or "").strip(): v.strip() if isinstance(v, str) else v for k, v in row.items()}
                    for row in reader
                ]
        except Exception as e:
            msg = "Failed to parse CSV: {}.".format(str(e))
            logger.error("{} {}".format(_LOG_PREFIX, msg))
            raise exceptions.VossenParseError(msg)

        logger.info("{} Loaded {} rows.".format(_LOG_PREFIX, len(rows)))
        return rows
