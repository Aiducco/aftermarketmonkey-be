import logging
import os
import time
import typing
from urllib.parse import urlparse

import paramiko
from django.conf import settings

from src.integrations.clients.meyer import exceptions

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[MEYER-SFTP-CLIENT]"

DEFAULT_LOCAL_PRICING = "/tmp/meyer_pricing.csv"
DEFAULT_LOCAL_INVENTORY = "/tmp/meyer_inventory.csv"
DEFAULT_FILE_MAX_AGE_SECONDS = 6 * 60 * 60


def _normalize_sftp_server(value: typing.Any) -> str:
    """Hostname, or URL (sftp/https) — host part only."""
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


def _require_non_empty_str(creds: typing.Dict, *keys: str) -> typing.Tuple[typing.List[str], typing.Dict[str, str]]:
    """Return (missing_keys, normalized_key -> value) for required string fields."""
    out: typing.Dict[str, str] = {}
    missing: typing.List[str] = []
    for k in keys:
        v = creds.get(k)
        s = str(v).strip() if v is not None else ""
        if not s:
            missing.append(k)
        else:
            out[k] = s
    return missing, out


class MeyerSFTPClient:
    """
    SFTP client for Meyer Distributing pricing + inventory CSV feeds.

    All connection settings must be supplied in ``credentials`` (no Django settings fallbacks
    for host, port, auth, remote directory, or filenames).

    Required keys:
      - ``sftp_server`` (hostname, or ``sftp://`` / ``https://`` URL — only host is used),
        aliases: ``sftp_host``, ``server_url``
      - ``sftp_port`` (int or numeric string)
      - ``sftp_user``, ``sftp_password``
      - ``sftp_directory`` — remote folder (e.g. ``uploads``; no leading slash required)
      - ``pricing_remote_file`` — CSV filename under that directory
      - ``inventory_remote_file`` — CSV filename under that directory

    Optional:
      - ``local_pricing_path``, ``local_inventory_path`` — local cache paths (else ``MEYER_*_LOCAL_PATH`` settings or /tmp defaults)
    """

    def __init__(
        self,
        credentials: typing.Optional[typing.Dict] = None,
        local_pricing_path: typing.Optional[str] = None,
        local_inventory_path: typing.Optional[str] = None,
        file_max_age: int = DEFAULT_FILE_MAX_AGE_SECONDS,
        require_credentials: bool = True,
    ):
        creds = dict(credentials or {})

        raw_server = (
            creds.get("sftp_server")
            or creds.get("sftp_host")
            or creds.get("server_url")
            or ""
        )
        self.sftp_server = _normalize_sftp_server(raw_server)

        try:
            self.sftp_port = int(creds.get("sftp_port"))
        except (TypeError, ValueError):
            self.sftp_port = 0

        missing: typing.List[str] = []
        if not self.sftp_server:
            missing.append("sftp_server (or sftp_host / server_url)")
        if not self.sftp_port or self.sftp_port < 1 or self.sftp_port > 65535:
            missing.append("sftp_port (1–65535)")

        m_str, str_fields = _require_non_empty_str(
            creds,
            "sftp_user",
            "sftp_password",
            "sftp_directory",
            "pricing_remote_file",
            "inventory_remote_file",
        )
        missing.extend(m_str)

        self.sftp_user = str_fields.get("sftp_user", "")
        self.sftp_password = str_fields.get("sftp_password", "")
        self.sftp_directory = str_fields.get("sftp_directory", "")
        self.pricing_remote_file = str_fields.get("pricing_remote_file", "")
        self.inventory_remote_file = str_fields.get("inventory_remote_file", "")

        self.local_pricing_path = (
            str(creds.get("local_pricing_path") or "").strip()
            or local_pricing_path
            or getattr(settings, "MEYER_PRICING_LOCAL_PATH", DEFAULT_LOCAL_PRICING)
        )
        self.local_inventory_path = (
            str(creds.get("local_inventory_path") or "").strip()
            or local_inventory_path
            or getattr(settings, "MEYER_INVENTORY_LOCAL_PATH", DEFAULT_LOCAL_INVENTORY)
        )
        self.file_max_age = file_max_age

        if require_credentials and missing:
            raise ValueError(
                "Invalid Meyer SFTP configuration — required in credentials: {}.".format(
                    ", ".join(missing)
                )
            )

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
