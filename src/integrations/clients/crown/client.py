"""
Relay SFTP client for Crown Automotive.

Crown is relay-provisioned exactly like Meyer and A-Tech (see their clients' own docstrings): the
dealer enters nothing, and their connection carries the relay account we generated for them
(``ftp_user``/``ftp_password`` — see ``constants.PROVIDERS_CATALOG`` -> ``relay_credential_fields``
for the ``CROWN_AUTOMOTIVE`` entry).

Unlike Meyer (two fixed filenames) and A-Tech (one), Crown has never delivered a file, so there is
no real sample to pin an exact filename to yet — the installation instructions only tell their rep
to "place the files in the uploads folder," not what to name them. ``feed_present()`` therefore
checks for *any* file in the directory rather than a specific name, which is enough to answer the
one question ``_relay_feed_connection_status`` actually needs right now: has Crown delivered
anything at all, so the connection can move off WAITING. Parsing/ingestion (a real client/
provider-part sync, the way Meyer and A-Tech have) is a separate piece of work for once a real
file -- and its format -- actually exists to build against.
"""
import logging
import socket
import typing
from urllib.parse import urlparse

import paramiko
from django.conf import settings

from src.integrations.clients.crown import exceptions

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[CROWN-SFTP-CLIENT]"

# Same shared relay every other relay-provisioned provider (Meyer, A-Tech, The Wheel Group, DLG)
# uses — see those clients' own modules.
_DEFAULT_CROWN_RELAY_HOST = "5.161.121.143"
_DEFAULT_CROWN_RELAY_PORT = 22
_DEFAULT_CROWN_RELAY_DIRECTORY = "uploads"

DEFAULT_CONNECT_TIMEOUT_SECONDS = 15


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
    return p if 1 <= p <= 65535 else fallback


def _normalize_sftp_server(value: typing.Any) -> str:
    s = str(value or "").strip()
    if not s:
        return ""
    if "://" in s:
        parsed = urlparse(s)
        return (parsed.hostname or "").strip()
    return s


def _require_non_empty_str(creds: typing.Dict, *keys: str) -> typing.Tuple[typing.List[str], typing.Dict[str, str]]:
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


class CrownSFTPClient:
    """
    Relay SFTP client for Crown Automotive — presence-check only (see module docstring).

    Defaults host/port/directory from Django settings (``CROWN_SFTP_*``) the same way Meyer and
    A-Tech do, falling back to the shared relay's own defaults. Per-company ``credentials`` must
    include ``sftp_user``/``sftp_password`` (``ftp_user``/``ftp_password`` as stored on
    CompanyProviders are accepted too, matching the relay_credential_fields key names).
    """

    def __init__(self, credentials: typing.Optional[typing.Dict] = None, require_credentials: bool = True):
        creds = dict(credentials or {})

        raw_server = creds.get("sftp_server") or creds.get("sftp_host") or creds.get("server_url") or ""
        self.sftp_server = (
            _normalize_sftp_server(raw_server)
            if str(raw_server or "").strip()
            else _normalize_sftp_server(_setting_str("CROWN_SFTP_HOST", _DEFAULT_CROWN_RELAY_HOST))
        )

        port_raw = creds.get("sftp_port")
        self.sftp_port = 0
        if port_raw is not None and str(port_raw).strip() != "":
            try:
                self.sftp_port = int(port_raw)
            except (TypeError, ValueError):
                self.sftp_port = 0
        if not (1 <= self.sftp_port <= 65535):
            self.sftp_port = _setting_int_port("CROWN_SFTP_PORT", _DEFAULT_CROWN_RELAY_PORT)

        # Accept both sftp_user/sftp_password (Meyer/A-Tech convention) and ftp_user/ftp_password
        # (what CompanyProviders.credentials actually stores for Crown — see
        # constants.PROVIDERS_CATALOG's relay_credential_fields for CROWN_AUTOMOTIVE).
        user = creds.get("sftp_user") or creds.get("ftp_user")
        password = creds.get("sftp_password") or creds.get("ftp_password")
        missing, str_fields = _require_non_empty_str({"sftp_user": user, "sftp_password": password}, "sftp_user", "sftp_password")

        self.sftp_user = str_fields.get("sftp_user", "")
        self.sftp_password = str_fields.get("sftp_password", "")
        self.sftp_directory = _coalesce_nonempty_str(
            creds, "sftp_directory", _setting_str("CROWN_SFTP_DIRECTORY", _DEFAULT_CROWN_RELAY_DIRECTORY)
        )

        if not self.sftp_server:
            missing.append("CROWN_SFTP_HOST (env) or sftp_server in credentials — relay host is not configured")
        if not self.sftp_directory:
            missing.append("CROWN_SFTP_DIRECTORY (env) or sftp_directory in credentials")

        if require_credentials and missing:
            raise ValueError(
                "Invalid Crown SFTP configuration — missing: {}. Company credentials must include "
                "sftp_user/ftp_user and sftp_password/ftp_password.".format(", ".join(missing))
            )

        self._transport = None
        self._sftp = None

    def _connect(self) -> None:
        try:
            sock = socket.create_connection(
                (self.sftp_server, self.sftp_port), timeout=DEFAULT_CONNECT_TIMEOUT_SECONDS
            )
            self._transport = paramiko.Transport(sock)
            self._transport.connect(username=self.sftp_user, password=self.sftp_password)
            self._sftp = paramiko.SFTPClient.from_transport(self._transport)
        except Exception as e:
            msg = "Failed to connect to Crown SFTP: {}".format(str(e))
            logger.error("{} {}".format(_LOG_PREFIX, msg))
            raise exceptions.CrownSFTPConnectionError(msg)

    def _disconnect(self) -> None:
        try:
            if self._sftp:
                self._sftp.close()
                self._sftp = None
            if self._transport:
                self._transport.close()
                self._transport = None
        except Exception as e:  # noqa: BLE001 — disconnect errors must not mask the real one
            logger.warning("{} Disconnect error: {}.".format(_LOG_PREFIX, str(e)))
            self._sftp = None
            self._transport = None

    def feed_present(self) -> bool:
        """
        True if this company's relay uploads folder holds at least one file. Connection failures
        propagate as CrownSFTPConnectionError rather than returning False, so callers can tell
        "not there yet" apart from "couldn't check" — same contract as Meyer/A-Tech's
        feed_present().
        """
        self._connect()
        try:
            try:
                entries = self._sftp.listdir_attr(self.sftp_directory)
            except FileNotFoundError:
                return False
            return any(not entry.filename.startswith(".") for entry in entries)
        finally:
            self._disconnect()
