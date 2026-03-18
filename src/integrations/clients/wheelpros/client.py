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
        self.sftp_server = creds.get("sftp_server") or getattr(settings, "WHEELPROS_SFTP_HOST", DEFAULT_SFTP_HOST)
        self.sftp_port = int(creds.get("sftp_port") or getattr(settings, "WHEELPROS_SFTP_PORT", DEFAULT_SFTP_PORT))
        self.sftp_user = creds.get("sftp_user") or creds.get("username") or getattr(
            settings, "WHEELPROS_SFTP_USER", ""
        )
        self.sftp_password = creds.get("sftp_password") or creds.get("password") or getattr(
            settings, "WHEELPROS_SFTP_PASSWORD", ""
        )
        self.sftp_path = creds.get("sftp_path") or getattr(settings, "WHEELPROS_SFTP_PATH", DEFAULT_SFTP_PATH)
        self.local_file_path = local_file_path or getattr(
            settings, "WHEELPROS_INVENTORY_LOCAL_PATH", os.path.join("/tmp", DEFAULT_LOCAL_FILE_NAME)
        )
        self.file_max_age = file_max_age
        self.auto_add_host_key = getattr(settings, "WHEELPROS_SFTP_AUTO_ADD_HOST_KEY", True)

        if require_credentials and not all([self.sftp_server, self.sftp_user, self.sftp_password, self.sftp_path]):
            raise ValueError("Invalid credentials/configuration. Missing required WheelPros SFTP settings.")

        self._ssh_client = None
        self._sftp = None

    def _connect(self) -> None:
        """Establish SFTP connection via SSHClient (supports host key policy)."""
        try:
            self._ssh_client = paramiko.SSHClient()
            if self.auto_add_host_key:
                self._ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            else:
                self._ssh_client.load_system_host_keys()
            self._ssh_client.connect(
                hostname=self.sftp_server,
                port=self.sftp_port,
                username=self.sftp_user,
                password=self.sftp_password,
                look_for_keys=False,
                allow_agent=False,
            )
            self._sftp = self._ssh_client.open_sftp()
            logger.debug(
                "{} Connected to SFTP server {}:{}.".format(_LOG_PREFIX, self.sftp_server, self.sftp_port)
            )
        except Exception as e:
            msg = "Failed to connect to SFTP server. Error: {}".format(str(e))
            logger.error("{} {}".format(_LOG_PREFIX, msg))
            raise exceptions.WheelProsSFTPConnectionError(msg)

    def _disconnect(self) -> None:
        """Close SFTP and SSH connection."""
        try:
            if self._sftp:
                self._sftp.close()
                self._sftp = None
            if self._ssh_client:
                self._ssh_client.close()
                self._ssh_client = None
        except Exception as e:
            logger.warning("{} Error during disconnect: {}.".format(_LOG_PREFIX, str(e)))
            self._sftp = None
            self._ssh_client = None

    def is_file_outdated(self) -> bool:
        if not os.path.exists(self.local_file_path):
            return True
        return (time.time() - os.path.getmtime(self.local_file_path)) > self.file_max_age

    def download_feed_file(self, force_download: bool = False) -> str:
        """
        Download the WheelPros CSV from SFTP to the local file path.
        Returns the local file path.
        """
        if not force_download and not self.is_file_outdated():
            logger.info("{} Using cached WheelPros file {}.".format(_LOG_PREFIX, self.local_file_path))
            return self.local_file_path

        tmp_path = self.local_file_path + ".tmp"
        sftp = None
        try:
            self._connect()
            sftp = self._sftp
            remote_path = self.sftp_path
            with open(tmp_path, "wb") as out:
                sftp.getfo(remote_path, out)
            os.replace(tmp_path, self.local_file_path)
            logger.info("{} Downloaded WheelPros CSV to {}.".format(_LOG_PREFIX, self.local_file_path))
            return self.local_file_path
        except FileNotFoundError:
            msg = "File not found on SFTP server: {}".format(self.sftp_path)
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
    ) -> typing.List[typing.Dict]:
        """
        Download the CSV if needed and return rows as list of dicts.
        When local_only=True, use the local file only (no SFTP connection); raises if file missing.
        """
        if local_only:
            if not os.path.exists(self.local_file_path):
                raise exceptions.WheelProsFileNotFoundError(
                    "Local file not found: {}. Use without --no-download to fetch from SFTP.".format(
                        self.local_file_path
                    )
                )
            path = self.local_file_path
            logger.info("{} Using local file only: {}.".format(_LOG_PREFIX, path))
        else:
            path = self.download_feed_file(force_download=force_download)
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
