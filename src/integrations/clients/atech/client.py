import logging
import os
import time
import typing
from urllib.parse import urlparse

import paramiko
from django.conf import settings

from src.integrations.clients.atech import exceptions
from src.integrations.clients.atech.feed_spec import DEFAULT_REMOTE_FEED_FILENAME

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[ATECH-SFTP-CLIENT]"

DEFAULT_LOCAL_FEED = "/tmp/atechfile.txt"
DEFAULT_FILE_MAX_AGE_SECONDS = 6 * 60 * 60

_DEFAULT_ATECH_RELAY_HOST = "54.145.82.238"
_DEFAULT_ATECH_RELAY_PORT = 22
_DEFAULT_ATECH_RELAY_DIRECTORY = "uploads"


def _setting_str(name: str, fallback: str) -> str:
    raw = getattr(settings, name, None)
    s = str(raw).strip() if raw is not None else ""
    return s if s else fallback


def _setting_int_port(name: str, fallback: int) -> int:
    raw = getattr(settings, name, None)
    if raw is None:
        return fallback
    try:
        p = int(raw)
    except (TypeError, ValueError):
        return fallback
    if 1 <= p <= 65535:
        return p
    return fallback


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


def _coalesce_nonempty_str(creds: typing.Dict, key: str, fallback: str) -> str:
    v = creds.get(key)
    if v is not None and str(v).strip():
        return str(v).strip()
    return str(fallback or "").strip()


class AtechSFTPClient:
    """
    SFTP client for the A-Tech combined pricing + inventory feed (single file).

    Defaults host, port, remote directory, and filename from Django settings
    (``ATECH_SFTP_*``, ``ATECH_FEED_REMOTE_FILE``) on the AftermarketMonkey relay.
    Per-company ``credentials`` must include ``sftp_user`` and ``sftp_password``.
    Optional overrides: ``sftp_server`` (or ``sftp_host`` / ``server_url``),
    ``sftp_port``, ``sftp_directory``, ``feed_remote_file`` (or ``remote_file``).

    Optional:
      - ``local_feed_path`` — local cache path (else ``ATECH_FEED_LOCAL_PATH`` setting)
    """

    def __init__(
        self,
        credentials: typing.Optional[typing.Dict] = None,
        local_feed_path: typing.Optional[str] = None,
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
        if str(raw_server or "").strip():
            self.sftp_server = _normalize_sftp_server(raw_server)
        else:
            self.sftp_server = _normalize_sftp_server(
                _setting_str("ATECH_SFTP_HOST", _DEFAULT_ATECH_RELAY_HOST)
            )

        port_raw = creds.get("sftp_port")
        self.sftp_port = 0
        if port_raw is not None and str(port_raw).strip() != "":
            try:
                self.sftp_port = int(port_raw)
            except (TypeError, ValueError):
                self.sftp_port = 0
        if not self.sftp_port or self.sftp_port < 1 or self.sftp_port > 65535:
            self.sftp_port = _setting_int_port("ATECH_SFTP_PORT", _DEFAULT_ATECH_RELAY_PORT)

        m_auth, str_fields = _require_non_empty_str(creds, "sftp_user", "sftp_password")
        missing: typing.List[str] = list(m_auth)

        self.sftp_user = str_fields.get("sftp_user", "")
        self.sftp_password = str_fields.get("sftp_password", "")
        self.sftp_directory = _coalesce_nonempty_str(
            creds,
            "sftp_directory",
            _setting_str("ATECH_SFTP_DIRECTORY", _DEFAULT_ATECH_RELAY_DIRECTORY),
        )
        self.feed_remote_file = _coalesce_nonempty_str(
            creds,
            "feed_remote_file",
            _coalesce_nonempty_str(
                creds,
                "remote_file",
                _setting_str("ATECH_FEED_REMOTE_FILE", DEFAULT_REMOTE_FEED_FILENAME),
            ),
        )

        if not self.sftp_server:
            missing.append(
                "ATECH_SFTP_HOST (env) or sftp_server in credentials — relay host is not configured"
            )
        if not self.sftp_directory:
            missing.append("ATECH_SFTP_DIRECTORY (env) or sftp_directory in credentials")
        if not self.feed_remote_file:
            missing.append("ATECH_FEED_REMOTE_FILE (env) or feed_remote_file in credentials")

        self.local_feed_path = (
            str(creds.get("local_feed_path") or "").strip()
            or local_feed_path
            or getattr(settings, "ATECH_FEED_LOCAL_PATH", DEFAULT_LOCAL_FEED)
        )
        self.file_max_age = file_max_age

        if require_credentials and missing:
            raise ValueError(
                "Invalid A-Tech SFTP configuration — missing: {}. "
                "Company credentials must include sftp_user and sftp_password. "
                "Relay host, port, folder, and feed filename come from Django settings "
                "(ATECH_SFTP_* / ATECH_FEED_REMOTE_FILE env vars) unless overridden in credentials.".format(
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
            msg = "Failed to connect to A-Tech SFTP: {}".format(str(e))
            logger.error("{} {}".format(_LOG_PREFIX, msg))
            raise exceptions.AtechSFTPConnectionError(msg)

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
            msg = "A-Tech file not found on SFTP: {}".format(remote_path)
            logger.error("{} {}".format(_LOG_PREFIX, msg))
            raise exceptions.AtechFileNotFoundError(msg)
        except exceptions.AtechSFTPConnectionError:
            raise
        except Exception as e:
            if os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass
            msg = "A-Tech SFTP download failed: {}".format(str(e))
            logger.error("{} {}".format(_LOG_PREFIX, msg))
            raise exceptions.AtechSFTPConnectionError(msg)
        finally:
            self._disconnect()

    def download_feed_file(self, force_download: bool = False) -> str:
        """Download the combined A-Tech feed to the configured local path."""
        return self._download(self.feed_remote_file, self.local_feed_path, force_download)
