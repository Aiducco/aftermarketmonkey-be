import csv
import logging
import os
import socket
import time
import typing

import paramiko
from django.conf import settings

from src.integrations.clients.wheelpros import exceptions

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[WHEELPROS-SFTP-CLIENT]"

DEFAULT_SFTP_HOST = "sftp.wheelpros.com"
DEFAULT_SFTP_PORT = 22
DEFAULT_SFTP_PATH = "CommonFeed/USD/WHEEL/wheelInvPriceData.csv"
DEFAULT_LOCAL_FILE_NAME = "wheelpros_wheel_inventory.csv"
DEFAULT_FILE_MAX_AGE_SECONDS = 6 * 60 * 60

# paramiko.Transport((host, port)) opens the socket with no timeout, so a bad host/firewall
# could hang the calling thread indefinitely — matters now that test_connection() runs
# synchronously inside an HTTP request.
DEFAULT_CONNECT_TIMEOUT_SECONDS = 15


class WheelProsSFTPClient:
    """
    SFTP client for WheelPros CSV feed.

    Host and port always come from Django settings (``WHEELPROS_SFTP_HOST`` /
    ``WHEELPROS_SFTP_PORT``); per-company credentials only supply username and password
    (``sftp_user`` / ``sftp_password`` or ``username`` / ``password``).
    """

    def __init__(
        self,
        credentials: typing.Optional[typing.Dict] = None,
        local_file_path: typing.Optional[str] = None,
        file_max_age: int = DEFAULT_FILE_MAX_AGE_SECONDS,
        require_credentials: bool = True,
    ):
        creds = credentials or {}
        self.sftp_server = str(
            getattr(settings, "WHEELPROS_SFTP_HOST", DEFAULT_SFTP_HOST) or DEFAULT_SFTP_HOST
        ).strip()
        self.sftp_port = int(getattr(settings, "WHEELPROS_SFTP_PORT", DEFAULT_SFTP_PORT))
        self.sftp_user = str(creds.get("sftp_user") or creds.get("username") or getattr(
            settings, "WHEELPROS_SFTP_USER", ""
        ) or "").strip()
        self.sftp_password = str(creds.get("sftp_password") or creds.get("password") or getattr(
            settings, "WHEELPROS_SFTP_PASSWORD", ""
        ) or "").strip()
        self.sftp_path = str(creds.get("sftp_path") or getattr(settings, "WHEELPROS_SFTP_PATH", DEFAULT_SFTP_PATH) or "").strip()
        self.local_file_path = local_file_path or getattr(
            settings, "WHEELPROS_INVENTORY_LOCAL_PATH", os.path.join("/tmp", DEFAULT_LOCAL_FILE_NAME)
        )
        self.file_max_age = file_max_age

        if require_credentials and not all([self.sftp_user, self.sftp_password]):
            raise ValueError("Invalid credentials/configuration. Missing required WheelPros SFTP user/password.")

        self._transport = None
        self._sftp = None

    def _connect(self) -> None:
        """Establish SFTP connection (same pattern as WheelProsAccessoriesProvider)."""
        try:
            sock = socket.create_connection(
                (self.sftp_server, self.sftp_port), timeout=DEFAULT_CONNECT_TIMEOUT_SECONDS
            )
            self._transport = paramiko.Transport(sock)
            self._transport.connect(username=self.sftp_user, password=self.sftp_password)
            self._sftp = paramiko.SFTPClient.from_transport(self._transport)
        except paramiko.AuthenticationException as e:
            # Distinct from an unreachable host/timeout, so callers can tell bad credentials
            # apart from a connectivity problem.
            msg = "Login rejected by SFTP server. Error: {}".format(str(e))
            logger.error("{} {}".format(_LOG_PREFIX, msg))
            raise exceptions.WheelProsAuthError(msg)
        except Exception as e:
            msg = "Failed to connect to SFTP server. Error: {}".format(str(e))
            logger.error("{} {}".format(_LOG_PREFIX, msg))
            raise exceptions.WheelProsSFTPConnectionError(msg)

    def _disconnect(self) -> None:
        """Close SFTP and Transport connection."""
        try:
            if self._sftp:
                self._sftp.close()
                self._sftp = None
            if self._transport:
                self._transport.close()
                self._transport = None
        except Exception as e:
            logger.warning("{} Error during disconnect: {}.".format(_LOG_PREFIX, str(e)))
            self._sftp = None
            self._transport = None

    def test_connection(self, remote_paths: typing.Optional[typing.Iterable[str]] = None) -> None:
        """
        Log in, then confirm each of ``remote_paths`` (defaults to ``self.sftp_path``) is
        actually readable — some accounts authenticate over SFTP successfully but lack
        permission on a specific feed's directory, which would otherwise only surface later
        as a failed pricing sync. Uses ``stat()`` so nothing is downloaded.
        """
        self._connect()
        try:
            paths = [p for p in (remote_paths or [self.sftp_path]) if p]
            inaccessible: typing.List[str] = []
            for path in paths:
                remote_path = path if path.startswith("/") else "/" + path
                try:
                    self._sftp.stat(remote_path)
                except (FileNotFoundError, IOError, OSError) as e:
                    inaccessible.append("{} ({})".format(remote_path, str(e)))
            if inaccessible:
                raise exceptions.WheelProsPermissionError(
                    "Connected, but could not access: {}. Contact Wheel Pros support to confirm "
                    "your account has access to these feeds.".format("; ".join(inaccessible))
                )
        finally:
            self._disconnect()

    def is_file_outdated(self, local_path: typing.Optional[str] = None) -> bool:
        path = local_path or self.local_file_path
        if not os.path.exists(path):
            return True
        return (time.time() - os.path.getmtime(path)) > self.file_max_age

    def download_feed_file(
        self,
        force_download: bool = False,
        sftp_path: typing.Optional[str] = None,
        local_file_path: typing.Optional[str] = None,
    ) -> str:
        """
        Download the WheelPros CSV from SFTP to the local file path.
        Returns the local file path.
        """
        remote_path = sftp_path or self.sftp_path
        local_path = local_file_path or self.local_file_path
        if not remote_path:
            raise ValueError("SFTP path is required for download.")

        if not force_download and not self.is_file_outdated(local_path):
            logger.info("{} Using cached file {}.".format(_LOG_PREFIX, local_path))
            return local_path

        tmp_path = local_path + ".tmp"
        try:
            self._connect()
            if not remote_path.startswith("/"):
                remote_path = "/" + remote_path
            self._sftp.get(remote_path, tmp_path)
            os.replace(tmp_path, local_path)
            logger.info("{} Downloaded {} -> {}.".format(_LOG_PREFIX, remote_path, local_path))
            return local_path
        except FileNotFoundError:
            msg = "File not found on SFTP server: {}".format(remote_path)
            logger.error("{} {}".format(_LOG_PREFIX, msg))
            raise exceptions.WheelProsFileNotFoundError(msg)
        except Exception as e:
            msg = "Failed to download WheelPros file: {}".format(str(e))
            logger.error("{} {}".format(_LOG_PREFIX, msg))
            raise exceptions.WheelProsDownloadError(msg)
        finally:
            self._disconnect()
            if os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass

    def get_feed_records(
        self,
        force_download: bool = False,
        local_only: bool = False,
        sftp_path: typing.Optional[str] = None,
        local_file_path: typing.Optional[str] = None,
    ) -> typing.List[typing.Dict]:
        """
        Download the CSV if needed and return rows as list of dicts.
        When local_only=True, use the local file only (no SFTP connection); raises if file missing.
        """
        local_path = local_file_path or self.local_file_path
        if local_only:
            if not os.path.exists(local_path):
                raise exceptions.WheelProsFileNotFoundError(
                    "Local file not found: {}. Use without --no-download to fetch from SFTP.".format(
                        local_path
                    )
                )
            path = local_path
            logger.info("{} Using local file only: {}.".format(_LOG_PREFIX, path))
        else:
            path = self.download_feed_file(
                force_download=force_download,
                sftp_path=sftp_path or self.sftp_path,
                local_file_path=local_path,
            )
        try:
            enc = None
            last_error: typing.Optional[Exception] = None
            try:
                with open(path, "rb") as bf:
                    sample = bf.read(8 * 1024 * 1024)
            except OSError as e:
                raise exceptions.WheelProsParseError("Could not open WheelPros CSV: {}".format(e))
            for encoding in ("utf-8", "utf-8-sig", "cp1252", "latin-1"):
                try:
                    sample.decode(encoding)
                    enc = encoding
                    break
                except UnicodeDecodeError as e:
                    last_error = e
                    continue
            if enc is None:
                raise exceptions.WheelProsParseError(
                    "Unable to decode WheelPros CSV with supported encodings: {}".format(last_error)
                )
            records: typing.List[typing.Dict] = []
            with open(path, "r", newline="", encoding=enc, errors="replace") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # Strip column name whitespace; replace empty strings with None
                    norm = {
                        (k.strip() if isinstance(k, str) else k): (None if v == "" else v)
                        for k, v in row.items()
                    }
                    records.append(norm)
            return records
        except exceptions.WheelProsParseError:
            raise
        except Exception as e:
            msg = "Failed to parse WheelPros CSV: {}".format(str(e))
            logger.error("{} {}".format(_LOG_PREFIX, msg))
            raise exceptions.WheelProsParseError(msg)
