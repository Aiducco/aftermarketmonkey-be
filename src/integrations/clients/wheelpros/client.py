import logging
import os
import time
import typing

import pandas as pd
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


class WheelProsSFTPClient:
    """
    SFTP client for WheelPros CSV feed.
    Credentials from dict (e.g. CompanyProviders.credentials) or from settings.
    """

    def __init__(
        self,
        credentials: typing.Optional[typing.Dict] = None,
        local_file_path: typing.Optional[str] = None,
        file_max_age: int = DEFAULT_FILE_MAX_AGE_SECONDS,
        require_credentials: bool = True,
    ):
        creds = credentials or {}
        self.sftp_server = str(creds.get("sftp_server") or getattr(settings, "WHEELPROS_SFTP_HOST", DEFAULT_SFTP_HOST)).strip()
        self.sftp_port = int(creds.get("sftp_port") or getattr(settings, "WHEELPROS_SFTP_PORT", DEFAULT_SFTP_PORT))
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

        if require_credentials and not all([self.sftp_server, self.sftp_user, self.sftp_password]):
            raise ValueError("Invalid credentials/configuration. Missing required WheelPros SFTP settings.")

        self._transport = None
        self._sftp = None

    def _connect(self) -> None:
        """Establish SFTP connection (same pattern as WheelProsAccessoriesProvider)."""
        try:
            self._transport = paramiko.Transport((self.sftp_server, self.sftp_port))
            self._transport.connect(username=self.sftp_user, password=self.sftp_password)
            self._sftp = paramiko.SFTPClient.from_transport(self._transport)
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
            df = None
            for encoding in ("utf-8", "utf-8-sig", "cp1252", "latin-1"):
                try:
                    df = pd.read_csv(
                        path,
                        encoding=encoding,
                        dtype=str,
                        keep_default_na=False,
                        on_bad_lines="warn",
                    )
                    break
                except UnicodeDecodeError:
                    continue
            if df is None:
                raise ValueError("Unable to decode CSV with supported encodings.")
            if df.empty:
                return []
            df.columns = [str(c).strip() for c in df.columns]
            return df.replace({pd.NA: None, "": None}).to_dict("records")
        except Exception as e:
            msg = "Failed to parse WheelPros CSV: {}".format(str(e))
            logger.error("{} {}".format(_LOG_PREFIX, msg))
            raise exceptions.WheelProsParseError(msg)
