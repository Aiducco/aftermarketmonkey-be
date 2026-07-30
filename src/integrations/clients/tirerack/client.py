import io
import logging
import re
import typing

import paramiko

from src.integrations.clients.tirerack import exceptions

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[TIRERACK-SFTP-CLIENT]"

_CATALOG_FILENAME_PATTERN = re.compile(r"^CustomTruck_(\d{1,2})-(\d{1,2})-(\d{2})_.*\.csv$", re.IGNORECASE)


class TireRackSFTPClient(object):
    """
    Direct-connect SFTP client for TireRack's daily pricing/inventory feed (one new
    ``CustomTruck_<M>-<D>-<YY>_<H>-<M>-<S>.csv`` file dropped in the account's home directory
    each morning, ~30 days retained). Password-only auth -- confirmed live against the real
    server that public-key auth fails for this account while password auth succeeds, so this
    client does not attempt key-based auth.
    """

    def __init__(self, credentials: typing.Dict):
        self.sftp_host = credentials.get("sftp_host", "")
        self.sftp_port = int(credentials.get("sftp_port") or 22)
        self.sftp_user = credentials.get("sftp_user", "")
        self.sftp_password = credentials.get("sftp_password", "")

        if not all([self.sftp_host, self.sftp_user, self.sftp_password]):
            raise ValueError("Invalid credentials parameter. Missing required SFTP credentials.")

        self._transport = None
        self._sftp = None

    def _connect(self) -> None:
        try:
            self._transport = paramiko.Transport((self.sftp_host, self.sftp_port))
            self._transport.connect(username=self.sftp_user, password=self.sftp_password)
            self._sftp = paramiko.SFTPClient.from_transport(self._transport)
            logger.debug("{} Connected to {}:{}.".format(_LOG_PREFIX, self.sftp_host, self.sftp_port))
        except Exception as e:
            msg = "Failed to connect to SFTP server. Error: {}".format(str(e))
            logger.error("{} {}".format(_LOG_PREFIX, msg))
            raise exceptions.TireRackSFTPConnectionError(msg)

    def _disconnect(self) -> None:
        try:
            if self._sftp:
                self._sftp.close()
                self._sftp = None
            if self._transport:
                self._transport.close()
                self._transport = None
        except Exception as e:
            logger.warning("{} Error during disconnect: {}".format(_LOG_PREFIX, str(e)))
            self._sftp = None
            self._transport = None

    def test_connection(self) -> None:
        """Connect and list the home directory -- confirms credentials work, no download."""
        try:
            self._connect()
            self._sftp.listdir(".")
        finally:
            self._disconnect()

    def _latest_catalog_filename(self) -> str:
        """
        Pick the newest catalog file by its embedded date rather than assuming directory order
        or sorting the raw filename string, which sorts wrong across single/double-digit
        month-day (e.g. "7-3" vs "7-30").
        """
        entries = self._sftp.listdir_attr(".")
        candidates = []
        for entry in entries:
            match = _CATALOG_FILENAME_PATTERN.match(entry.filename)
            if not match:
                continue
            month, day, year = (int(g) for g in match.groups())
            candidates.append((year, month, day, entry.filename))
        if not candidates:
            raise exceptions.TireRackFileNotFoundError(
                "No CustomTruck_*.csv catalog file found in the TireRack SFTP account."
            )
        candidates.sort()
        return candidates[-1][3]

    def download_latest_catalog(self) -> typing.Tuple[str, bytes]:
        """Download the most recent day's catalog CSV. Returns (filename, file contents)."""
        try:
            self._connect()
            filename = self._latest_catalog_filename()
            file_obj = io.BytesIO()
            self._sftp.getfo(filename, file_obj)
            file_obj.seek(0)
            content = file_obj.read()
            logger.info("{} Downloaded {} ({} bytes).".format(_LOG_PREFIX, filename, len(content)))
            return filename, content
        except exceptions.TireRackException:
            raise
        except Exception as e:
            msg = "Failed to download catalog file. Error: {}".format(str(e))
            logger.error("{} {}".format(_LOG_PREFIX, msg))
            raise exceptions.TireRackException(msg)
        finally:
            self._disconnect()
