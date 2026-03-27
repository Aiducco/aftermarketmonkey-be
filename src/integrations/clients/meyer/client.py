import logging
import os
import time
import typing

import paramiko
from django.conf import settings

from src.integrations.clients.meyer import exceptions

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[MEYER-SFTP-CLIENT]"

DEFAULT_SFTP_HOST = "54.145.82.238"
DEFAULT_SFTP_PORT = 22
DEFAULT_SFTP_DIRECTORY = "uploads"
DEFAULT_PRICING_FILENAME = "Meyer Pricing.csv"
DEFAULT_INVENTORY_FILENAME = "Meyer Inventory.csv"
DEFAULT_LOCAL_PRICING = "/tmp/meyer_pricing.csv"
DEFAULT_LOCAL_INVENTORY = "/tmp/meyer_inventory.csv"
DEFAULT_FILE_MAX_AGE_SECONDS = 6 * 60 * 60


def _remote_join(directory: str, filename: str) -> str:
    directory = (directory or "").strip().strip("/")
    filename = (filename or "").strip().lstrip("/")
    path = "/{}/{}".format(directory, filename) if directory else "/{}".format(filename)
    return path


class MeyerSFTPClient:
    """
    SFTP client for Meyer Distributing pricing + inventory CSV feeds.
    Credentials: sftp_server, sftp_user, sftp_password, optional sftp_directory,
    pricing_remote_file, inventory_remote_file (see MEYER_* settings fallbacks).
    """

    def __init__(
        self,
        credentials: typing.Optional[typing.Dict] = None,
        local_pricing_path: typing.Optional[str] = None,
        local_inventory_path: typing.Optional[str] = None,
        file_max_age: int = DEFAULT_FILE_MAX_AGE_SECONDS,
        require_credentials: bool = True,
    ):
        creds = credentials or {}
        self.sftp_server = str(
            creds.get("sftp_server")
            or creds.get("sftp_host")
            or getattr(settings, "MEYER_SFTP_HOST", DEFAULT_SFTP_HOST)
        ).strip()
        self.sftp_port = int(
            creds.get("sftp_port") or getattr(settings, "MEYER_SFTP_PORT", DEFAULT_SFTP_PORT)
        )
        self.sftp_user = str(
            creds.get("sftp_user")
            or creds.get("username")
            or getattr(settings, "MEYER_SFTP_USER", "")
            or ""
        ).strip()
        self.sftp_password = str(
            creds.get("sftp_password")
            or creds.get("password")
            or getattr(settings, "MEYER_SFTP_PASSWORD", "")
            or ""
        ).strip()
        self.sftp_directory = str(
            creds.get("sftp_directory")
            or getattr(settings, "MEYER_SFTP_DIRECTORY", DEFAULT_SFTP_DIRECTORY)
            or DEFAULT_SFTP_DIRECTORY
        ).strip()
        self.pricing_remote_file = str(
            creds.get("pricing_remote_file")
            or getattr(settings, "MEYER_PRICING_REMOTE_FILE", DEFAULT_PRICING_FILENAME)
        ).strip()
        self.inventory_remote_file = str(
            creds.get("inventory_remote_file")
            or getattr(settings, "MEYER_INVENTORY_REMOTE_FILE", DEFAULT_INVENTORY_FILENAME)
        ).strip()
        self.local_pricing_path = local_pricing_path or getattr(
            settings, "MEYER_PRICING_LOCAL_PATH", DEFAULT_LOCAL_PRICING
        )
        self.local_inventory_path = local_inventory_path or getattr(
            settings, "MEYER_INVENTORY_LOCAL_PATH", DEFAULT_LOCAL_INVENTORY
        )
        self.file_max_age = file_max_age

        if require_credentials and not all([self.sftp_server, self.sftp_user, self.sftp_password]):
            raise ValueError("Invalid Meyer SFTP configuration: host, user, and password are required.")

        self._transport = None
        self._sftp = None

    def _connect(self) -> None:
        try:
            self._transport = paramiko.Transport((self.sftp_server, self.sftp_port))
            self._transport.connect(username=self.sftp_user, password=self.sftp_password)
            self._sftp = paramiko.SFTPClient.from_transport(self._transport)
        except Exception as e:
            msg = "Failed to connect to Meyer SFTP: {}".format(str(e))
            logger.error("{} {}".format(_LOG_PREFIX, msg))
            raise exceptions.MeyerSFTPConnectionError(msg)

    def _disconnect(self) -> None:
        try:
            if self._sftp:
                self._sftp.close()
                self._sftp = None
            if self._transport:
                self._transport.close()
                self._transport = None
        except Exception as e:
            logger.warning("{} Disconnect error: {}.".format(_LOG_PREFIX, str(e)))
            self._sftp = None
            self._transport = None

    def is_file_outdated(self, local_path: str) -> bool:
        if not os.path.exists(local_path):
            return True
        return (time.time() - os.path.getmtime(local_path)) > self.file_max_age

    def _download(
        self,
        remote_filename: str,
        local_path: str,
        force_download: bool,
    ) -> str:
        remote_path = _remote_join(self.sftp_directory, remote_filename)
        if not remote_filename:
            raise ValueError("Remote file name is required.")

        if not force_download and not self.is_file_outdated(local_path):
            logger.info("{} Using cached file {}.".format(_LOG_PREFIX, local_path))
            return local_path

        tmp_path = local_path + ".tmp"
        try:
            self._connect()
            self._sftp.get(remote_path, tmp_path)
            os.replace(tmp_path, local_path)
            logger.info("{} Downloaded {} -> {}.".format(_LOG_PREFIX, remote_path, local_path))
            return local_path
        except FileNotFoundError:
            msg = "Meyer file not found on SFTP: {}".format(remote_path)
            logger.error("{} {}".format(_LOG_PREFIX, msg))
            raise exceptions.MeyerFileNotFoundError(msg)
        except exceptions.MeyerSFTPConnectionError:
            raise
        except Exception as e:
            if os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass
            msg = "Meyer SFTP download failed: {}".format(str(e))
            logger.error("{} {}".format(_LOG_PREFIX, msg))
            raise exceptions.MeyerSFTPConnectionError(msg)
        finally:
            self._disconnect()

    def download_pricing_file(self, force_download: bool = False) -> str:
        return self._download(self.pricing_remote_file, self.local_pricing_path, force_download)

    def download_inventory_file(self, force_download: bool = False) -> str:
        return self._download(
            self.inventory_remote_file,
            self.local_inventory_path,
            force_download,
        )
