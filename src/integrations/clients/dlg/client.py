import logging
import os
import time
import typing
from urllib.parse import urlparse

import paramiko
from django.conf import settings

from src import constants as src_constants
from src.integrations.clients.dlg import exceptions

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[DLG-SFTP-CLIENT]"

DEFAULT_LOCAL_INVENTORY = "/tmp/dlg_inventory.csv"
DEFAULT_FILE_MAX_AGE_SECONDS = 6 * 60 * 60

_DLG_IGNORED_CREDENTIAL_KEYS = (
    "sftp_user",
    "sftp_password",
    "sftp_server",
    "sftp_host",
    "server_url",
    "sftp_port",
    "sftp_directory",
    "dlg_inventory_remote_file",
    "feed_remote_file",
)


def _normalize_sftp_server(value: typing.Any) -> str:
    s = str(value or "").strip()
    if not s:
        return ""
    if "://" in s:
        parsed = urlparse(s)
        return (parsed.hostname or "").strip()
    return s


def _remote_join(directory: str, filename: str) -> str:
    directory = (directory or "").strip().strip("/")
    filename = (filename or "").strip().lstrip("/")
    path = "/{}/{}".format(directory, filename) if directory else "/{}".format(filename)
    return path


def _warn_ignored_dlg_overrides(creds: typing.Dict) -> None:
    for k in _DLG_IGNORED_CREDENTIAL_KEYS:
        v = creds.get(k)
        if v is not None and str(v).strip():
            logger.warning(
                "{} Ignoring credentials {!r}={!r} — relay host, port, folder, and remote filename are fixed in code.".format(
                    _LOG_PREFIX, k, v,
                )
            )
    stray_inv = str(creds.get("inventory_remote_file") or "").strip()
    if stray_inv:
        logger.warning(
            "{} Ignoring credentials inventory_remote_file={!r} for DLG (Meyer key collision); using {!r}.".format(
                _LOG_PREFIX, stray_inv, src_constants.DLG_INVENTORY_CSV_FILENAME,
            )
        )


class DlgSFTPClient:
    """
    SFTP client for DLG ``dlg_inventory.csv`` on the AftermarketMonkey relay.

    Host, path, and remote filename are in ``src.constants``. SFTP login is from
    ``settings.DLG_RELAY_SFTP_USER`` and ``settings.DLG_RELAY_SFTP_PASSWORD`` (app-level, not per company).
    Company ``credentials`` may only set ``local_feed_path``; ``email_from`` and legacy SFTP keys are ignored
    (with a warning for SFTP keys).

    Local path: ``DLG_INVENTORY_LOCAL_PATH`` setting, else ``/tmp/dlg_inventory.csv``.
    """

    def __init__(
        self,
        credentials: typing.Optional[typing.Dict] = None,
        local_feed_path: typing.Optional[str] = None,
        file_max_age: int = DEFAULT_FILE_MAX_AGE_SECONDS,
        require_credentials: bool = True,
    ):
        creds = dict(credentials or {})
        _warn_ignored_dlg_overrides(creds)

        self.sftp_server = _normalize_sftp_server(src_constants.DLG_RELAY_SFTP_HOST)
        self.sftp_port = int(src_constants.DLG_RELAY_SFTP_PORT)
        self.sftp_directory = (src_constants.DLG_RELAY_SFTP_DIRECTORY or "").strip()
        self.inventory_remote_file = (src_constants.DLG_INVENTORY_CSV_FILENAME or "").strip()

        self.sftp_user = str(getattr(settings, "DLG_RELAY_SFTP_USER", None) or "").strip()
        self.sftp_password = str(getattr(settings, "DLG_RELAY_SFTP_PASSWORD", None) or "").strip()

        missing: typing.List[str] = []
        if not self.sftp_server:
            missing.append("DLG_RELAY_SFTP_HOST in src.constants — relay host is not configured")
        if not self.sftp_directory:
            missing.append("DLG_RELAY_SFTP_DIRECTORY in src.constants")
        if not self.inventory_remote_file:
            missing.append("DLG_INVENTORY_CSV_FILENAME in src.constants")
        if not self.sftp_user:
            missing.append("DLG_RELAY_SFTP_USER in Django settings (set via environment)")
        if not self.sftp_password:
            missing.append("DLG_RELAY_SFTP_PASSWORD in Django settings (set via environment)")

        self.local_inventory_path = (
            str(creds.get("local_feed_path") or "").strip()
            or local_feed_path
            or getattr(settings, "DLG_INVENTORY_LOCAL_PATH", DEFAULT_LOCAL_INVENTORY)
        )
        self.file_max_age = file_max_age

        if require_credentials and missing:
            raise ValueError(
                "Invalid DLG SFTP configuration — missing: {}.".format(", ".join(missing))
            )

        self._transport = None
        self._sftp = None

    def _connect(self) -> None:
        try:
            self._transport = paramiko.Transport((self.sftp_server, self.sftp_port))
            self._transport.connect(username=self.sftp_user, password=self.sftp_password)
            self._sftp = paramiko.SFTPClient.from_transport(self._transport)
        except Exception as e:
            msg = "Failed to connect to DLG SFTP: {}".format(str(e))
            logger.error("{} {}".format(_LOG_PREFIX, msg))
            raise exceptions.DlgSFTPConnectionError(msg)

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
            msg = "DLG file not found on SFTP: {}".format(remote_path)
            logger.error("{} {}".format(_LOG_PREFIX, msg))
            raise exceptions.DlgFileNotFoundError(msg)
        except exceptions.DlgSFTPConnectionError:
            raise
        except Exception as e:
            if os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass
            msg = "DLG SFTP download failed: {}".format(str(e))
            logger.error("{} {}".format(_LOG_PREFIX, msg))
            raise exceptions.DlgSFTPConnectionError(msg)
        finally:
            self._disconnect()

    def download_inventory_file(self, force_download: bool = False) -> str:
        return self._download(self.inventory_remote_file, self.local_inventory_path, force_download)
