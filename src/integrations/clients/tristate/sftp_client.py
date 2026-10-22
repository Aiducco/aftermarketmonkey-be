"""
SFTP client for Tri-State Enterprises' relay feed -- the files Tri-State pushes to OUR server,
not something we fetch from theirs. Same arrangement as Meyer, A-Tech, Crown and DLG: the
company authenticates with the relay sftp_user/sftp_password that
src.integrations.services.relay_sftp_provisioning issues it, and the distributor's rep is given
those same credentials to deliver into that one folder (see the TRI_STATE entry in
src.constants.PROVIDER_CATALOG for the copy the dealer sends their rep).

Deliberately separate from client.py (the PACE Customer API client): different transport,
different credential namespace (relay sftp_user/sftp_password in ``credentials.feed`` vs.
PACE's app_id/api_key on the order account), and a different job -- bulk catalog/pricing here,
per-item inventory and ordering there. Meyer draws exactly the same line between its client.py
and order_client.py.

PLACEHOLDER STATUS, 2026-10-22: Tri-State has not delivered a file yet, so nothing here has
been exercised against a real drop. The transport is real and complete (connect, stat,
download, cache) because it is just our own relay, which we can test against today -- what is
NOT yet known is what they will send: the file names (settings.TRISTATE_PRICING_REMOTE_FILE /
TRISTATE_INVENTORY_REMOTE_FILE are our own guess, env-overridable for exactly that reason),
whether it arrives as two files or one combined one, and the column layout. Hence
``list_relay_files`` below: when the first delivery lands, that call tells us what they
actually named it instead of leaving us guessing against a stat() that returns False.
"""
import logging
import os
import socket
import time
import typing
from urllib.parse import urlparse

import paramiko
from django.conf import settings

from src.integrations.clients.tristate import exceptions

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[TRISTATE-SFTP-CLIENT]"

DEFAULT_LOCAL_PRICING = "/tmp/tristate_pricing.csv"
DEFAULT_LOCAL_INVENTORY = "/tmp/tristate_inventory.csv"
DEFAULT_FILE_MAX_AGE_SECONDS = 6 * 60 * 60

# paramiko.Transport((host, port)) opens the socket with no timeout, so a bad host or a
# firewalled relay would hang the calling thread forever -- same reason Meyer's client sets
# this: feed_present() runs from the check_company_provider_connections cron.
DEFAULT_CONNECT_TIMEOUT_SECONDS = 15

_DEFAULT_RELAY_HOST = "5.161.121.143"
_DEFAULT_RELAY_PORT = 22
_DEFAULT_RELAY_DIRECTORY = "uploads"
_DEFAULT_PRICING_REMOTE_FILE = "Tri-State Pricing.csv"
_DEFAULT_INVENTORY_REMOTE_FILE = "Tri-State Inventory.csv"


def _setting_str(name: str, fallback: str) -> str:
    raw = getattr(settings, name, None)
    s = str(raw).strip() if raw is not None else ""
    return s if s else fallback


def _setting_int_port(name: str, fallback: int) -> int:
    raw = getattr(settings, name, None)
    if raw is None:
        return fallback
    try:
        port = int(raw)
    except (TypeError, ValueError):
        return fallback
    return port if 1 <= port <= 65535 else fallback


def _normalize_sftp_server(value: typing.Any) -> str:
    """Hostname, or URL (sftp/https) -- host part only."""
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
    return "/{}/{}".format(directory, filename) if directory else "/{}".format(filename)


def _coalesce_nonempty_str(creds: typing.Dict, key: str, fallback: str) -> str:
    value = creds.get(key)
    if value is not None and str(value).strip():
        return str(value).strip()
    return str(fallback or "").strip()


class TriStateSFTPClient:
    """
    Per-company ``credentials`` must include ``sftp_user`` and ``sftp_password`` -- the relay
    account, which for relay-provisioned providers is written into ``credentials.feed`` by
    src.api.services.integrations.connect_provider rather than typed in by the dealer.

    Host, port, folder and the two file names default from Django settings (``TRISTATE_SFTP_*``,
    ``TRISTATE_PRICING_REMOTE_FILE``, ``TRISTATE_INVENTORY_REMOTE_FILE``) and can each be
    overridden per connection: ``sftp_server`` (or ``sftp_host`` / ``server_url``),
    ``sftp_port``, ``sftp_directory``, ``pricing_remote_file``, ``inventory_remote_file``.
    Per-connection overrides matter more here than they do for Meyer -- the first real delivery
    may well land under a name nobody has told us yet.
    """

    def __init__(
        self,
        credentials: typing.Optional[typing.Dict] = None,
        local_pricing_path: typing.Optional[str] = None,
        local_inventory_path: typing.Optional[str] = None,
        file_max_age: int = DEFAULT_FILE_MAX_AGE_SECONDS,
    ):
        creds = dict(credentials or {})

        raw_server = creds.get("sftp_server") or creds.get("sftp_host") or creds.get("server_url") or ""
        if str(raw_server or "").strip():
            self.sftp_server = _normalize_sftp_server(raw_server)
        else:
            self.sftp_server = _normalize_sftp_server(_setting_str("TRISTATE_SFTP_HOST", _DEFAULT_RELAY_HOST))

        self.sftp_port = 0
        port_raw = creds.get("sftp_port")
        if port_raw is not None and str(port_raw).strip() != "":
            try:
                self.sftp_port = int(port_raw)
            except (TypeError, ValueError):
                self.sftp_port = 0
        if not 1 <= self.sftp_port <= 65535:
            self.sftp_port = _setting_int_port("TRISTATE_SFTP_PORT", _DEFAULT_RELAY_PORT)

        self.sftp_user = str(creds.get("sftp_user") or "").strip()
        self.sftp_password = str(creds.get("sftp_password") or "").strip()

        self.sftp_directory = _coalesce_nonempty_str(
            creds, "sftp_directory", _setting_str("TRISTATE_SFTP_DIRECTORY", _DEFAULT_RELAY_DIRECTORY)
        )
        self.pricing_remote_file = _coalesce_nonempty_str(
            creds, "pricing_remote_file", _setting_str("TRISTATE_PRICING_REMOTE_FILE", _DEFAULT_PRICING_REMOTE_FILE)
        )
        self.inventory_remote_file = _coalesce_nonempty_str(
            creds,
            "inventory_remote_file",
            _setting_str("TRISTATE_INVENTORY_REMOTE_FILE", _DEFAULT_INVENTORY_REMOTE_FILE),
        )

        missing: typing.List[str] = [
            key for key in ("sftp_user", "sftp_password") if not getattr(self, key)
        ]
        if not self.sftp_server:
            missing.append("TRISTATE_SFTP_HOST (env) or sftp_server in credentials")
        if not self.sftp_directory:
            missing.append("TRISTATE_SFTP_DIRECTORY (env) or sftp_directory in credentials")
        if missing:
            raise ValueError(
                "Invalid Tri-State SFTP configuration -- missing: {}. Company credentials must "
                "include sftp_user and sftp_password (the relay account, provisioned per company "
                "by relay_sftp_provisioning); relay host, port and folder come from Django "
                "settings (TRISTATE_SFTP_* env vars) unless overridden in "
                "credentials.".format(", ".join(missing))
            )

        self.local_pricing_path = (
            str(creds.get("local_pricing_path") or "").strip()
            or local_pricing_path
            or getattr(settings, "TRISTATE_PRICING_LOCAL_PATH", DEFAULT_LOCAL_PRICING)
        )
        self.local_inventory_path = (
            str(creds.get("local_inventory_path") or "").strip()
            or local_inventory_path
            or getattr(settings, "TRISTATE_INVENTORY_LOCAL_PATH", DEFAULT_LOCAL_INVENTORY)
        )
        self.file_max_age = file_max_age

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
            msg = "Failed to connect to the Tri-State relay SFTP: {}".format(e)
            logger.error("{} {}".format(_LOG_PREFIX, msg))
            raise exceptions.TriStateSFTPConnectionError(msg)

    def _disconnect(self) -> None:
        try:
            if self._sftp:
                self._sftp.close()
                self._sftp = None
            if self._transport:
                self._transport.close()
                self._transport = None
        except Exception as e:
            logger.warning("{} Disconnect error: {}.".format(_LOG_PREFIX, e))
            self._sftp = None
            self._transport = None

    def is_file_outdated(self, local_path: str) -> bool:
        if not os.path.exists(local_path):
            return True
        return (time.time() - os.path.getmtime(local_path)) > self.file_max_age

    def _download(self, remote_filename: str, local_path: str, force_download: bool) -> str:
        if not remote_filename:
            raise ValueError("Remote file name is required.")
        remote_path = _remote_join(self.sftp_directory, remote_filename)

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
            msg = "Tri-State file not found on the relay: {}".format(remote_path)
            logger.error("{} {}".format(_LOG_PREFIX, msg))
            raise exceptions.TriStateFileNotFoundError(msg)
        except exceptions.TriStateSFTPConnectionError:
            raise
        except Exception as e:
            if os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass
            msg = "Tri-State relay SFTP download failed: {}".format(e)
            logger.error("{} {}".format(_LOG_PREFIX, msg))
            raise exceptions.TriStateSFTPConnectionError(msg)
        finally:
            self._disconnect()

    def download_pricing_file(self, force_download: bool = False) -> str:
        return self._download(self.pricing_remote_file, self.local_pricing_path, force_download)

    def download_inventory_file(self, force_download: bool = False) -> str:
        return self._download(self.inventory_remote_file, self.local_inventory_path, force_download)

    def feed_present(self) -> bool:
        """
        True if both expected files exist in this company's relay folder. One connection,
        stat() only -- downloads nothing. Connection failures propagate as
        TriStateSFTPConnectionError rather than returning False, so callers can tell "nothing
        delivered yet" apart from "couldn't check" (see
        src.api.services.integrations._relay_feed_connection_status, which turns the first into
        WAITING and the second into FAILING).

        Contract matches Meyer's and A-Tech's feed_present() exactly -- that is what
        _RELAY_FEED_PRESENT_CLIENTS dispatches on.
        """
        self._connect()
        try:
            try:
                self._sftp.stat(_remote_join(self.sftp_directory, self.pricing_remote_file))
                self._sftp.stat(_remote_join(self.sftp_directory, self.inventory_remote_file))
                return True
            except FileNotFoundError:
                return False
        finally:
            self._disconnect()

    def list_relay_files(self) -> typing.List[typing.Dict[str, typing.Any]]:
        """
        Everything sitting in this company's relay folder, as
        ``[{"filename": ..., "size_bytes": ..., "modified_at": <epoch seconds>}, ...]``.

        Exists because the expected file names are still a guess (see this module's docstring):
        when Tri-State's first delivery lands under a name we didn't predict, feed_present()
        just says False and gives no hint why. This says what is actually there, which is what
        we need to set TRISTATE_PRICING_REMOTE_FILE / TRISTATE_INVENTORY_REMOTE_FILE correctly.
        Not part of the relay-check contract -- called by
        src.integrations.services.tristate.describe_tristate_relay_folder.
        """
        self._connect()
        try:
            entries = []
            for attrs in self._sftp.listdir_attr("/{}".format((self.sftp_directory or "").strip().strip("/"))):
                entries.append({
                    "filename": attrs.filename,
                    "size_bytes": attrs.st_size,
                    "modified_at": attrs.st_mtime,
                })
            return sorted(entries, key=lambda row: row["filename"].lower())
        finally:
            self._disconnect()
