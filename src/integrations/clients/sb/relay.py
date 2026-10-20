"""
Relay SFTP client for S&B's live-inventory file.

S&B publish their catalog, pricing and fitment as shared workbooks (see ``client.py``) but not
their stock. A dealer gets stock by emailing S&B with "Live Inventory" in the subject; S&B reply
with a file, the dealer forwards that to support@aftermarketscout.com, and support drops the
attachment in that company's own folder on the shared relay -- the same ``5.161.121.143:22`` /
``uploads`` endpoint Meyer, A-Tech, DLG, Crown and The Wheel Group use. The dealer enters nothing;
their connection carries the relay account we generated for them (``sftp_user`` / ``sftp_password``
-- see ``constants.PROVIDER_CATALOG`` -> ``relay_credential_fields`` for the ``SB`` entry).

**The file's format is unconfirmed.** S&B have never delivered one, so this client finds it by
name (``constants.SB_INVENTORY_FILENAMES``) and the parser in ``src.integrations.services.sb``
resolves its columns by header rather than by position -- see ``feed_spec.INVENTORY_SKU_COLUMNS``.
Everything about the catalog works without it; only stock quantities wait.

Every relay-provisioned provider shares one ``uploads/`` folder per company, so a presence check
has to be filename-specific or one provider's delivery reads as another's -- that exact bug was
confirmed live on 2026-10-13, when Crown's "any file" check read DLG's freshly-dropped
``dlg_inventory.csv`` as its own. ``feed_present()`` here only ever matches S&B's own names.
"""
import logging
import os
import time
import typing
from urllib.parse import urlparse

import paramiko
from django.conf import settings

from src import constants as src_constants
from src.integrations.clients.sb import exceptions

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[SB-RELAY-CLIENT]"

DEFAULT_FILE_MAX_AGE_SECONDS = 6 * 60 * 60
DEFAULT_CONNECT_TIMEOUT_SECONDS = 15
DEFAULT_LOCAL_INVENTORY_DIR = "/tmp"


def _normalize_sftp_server(value: typing.Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if "://" in text:
        return (urlparse(text).hostname or "").strip()
    return text


def _setting_str(name: str, fallback: str) -> str:
    raw = getattr(settings, name, None)
    value = str(raw).strip() if raw is not None else ""
    return value or fallback


def _setting_port(name: str, fallback: int) -> int:
    raw = getattr(settings, name, None)
    if raw is None:
        return fallback
    try:
        port = int(str(raw).strip())
    except (TypeError, ValueError):
        return fallback
    return port if 1 <= port <= 65535 else fallback


def _remote_join(directory: str, filename: str) -> str:
    directory = (directory or "").strip().strip("/")
    filename = (filename or "").strip().lstrip("/")
    if directory:
        return "/{}/{}".format(directory, filename)
    return "/{}".format(filename)


class SbRelayClient:
    """
    Finds and downloads a company's forwarded S&B live-inventory file from our relay.

    Credentials come from the calling ``CompanyProviders.credentials["feed"]`` (``sftp_user`` /
    ``sftp_password``, with the ``ftp_user`` / ``ftp_password`` spellings
    ``relay_credential_fields`` stores accepted too); host, port and folder are fixed in
    ``src.constants`` with Django-settings overrides, matching every other relay client.
    """

    def __init__(
        self,
        credentials: typing.Optional[typing.Dict] = None,
        local_dir: typing.Optional[str] = None,
        file_max_age: int = DEFAULT_FILE_MAX_AGE_SECONDS,
        require_credentials: bool = True,
    ):
        creds = dict(credentials or {})

        raw_server = creds.get("sftp_server") or creds.get("sftp_host") or creds.get("server_url")
        self.sftp_server = _normalize_sftp_server(raw_server) or _normalize_sftp_server(
            _setting_str("SB_SFTP_HOST", src_constants.SB_RELAY_SFTP_HOST)
        )

        creds_port = str(creds.get("sftp_port") or "").strip()
        if creds_port:
            try:
                self.sftp_port = int(creds_port)
            except ValueError:
                self.sftp_port = int(src_constants.SB_RELAY_SFTP_PORT)
        else:
            self.sftp_port = _setting_port("SB_SFTP_PORT", int(src_constants.SB_RELAY_SFTP_PORT))

        self.sftp_directory = _setting_str(
            "SB_SFTP_DIRECTORY", src_constants.SB_RELAY_SFTP_DIRECTORY
        ).strip()

        self.sftp_user = str(
            creds.get("sftp_user") or creds.get("ftp_user") or ""
        ).strip() or str(getattr(settings, "SB_RELAY_SFTP_USER", None) or "").strip()
        self.sftp_password = str(
            creds.get("sftp_password") or creds.get("ftp_password") or ""
        ).strip() or str(getattr(settings, "SB_RELAY_SFTP_PASSWORD", None) or "").strip()

        self.local_dir = (
            str(creds.get("local_feed_dir") or "").strip()
            or (local_dir or "").strip()
            or _setting_str("SB_INVENTORY_LOCAL_DIR", DEFAULT_LOCAL_INVENTORY_DIR)
        )
        self.file_max_age = file_max_age
        self.inventory_filenames = tuple(src_constants.SB_INVENTORY_FILENAMES)

        missing: typing.List[str] = []
        if not self.sftp_server:
            missing.append("sftp_server (SB_RELAY_SFTP_HOST fallback in src.constants)")
        if not self.sftp_directory:
            missing.append("SB_RELAY_SFTP_DIRECTORY in src.constants")
        if not self.inventory_filenames:
            missing.append("SB_INVENTORY_FILENAMES in src.constants")
        if not self.sftp_user:
            missing.append("sftp_user (company credentials, or SB_RELAY_SFTP_USER in settings)")
        if not self.sftp_password:
            missing.append(
                "sftp_password (company credentials, or SB_RELAY_SFTP_PASSWORD in settings)"
            )
        if require_credentials and missing:
            raise ValueError(
                "Invalid S&B relay SFTP configuration — missing: {}.".format(", ".join(missing))
            )

        self._transport = None
        self._sftp = None

    # ----------------------------------------------------------------------------------
    # Transport
    # ----------------------------------------------------------------------------------
    def _connect(self) -> None:
        try:
            self._transport = paramiko.Transport((self.sftp_server, self.sftp_port))
            self._transport.banner_timeout = DEFAULT_CONNECT_TIMEOUT_SECONDS
            self._transport.connect(username=self.sftp_user, password=self.sftp_password)
            self._sftp = paramiko.SFTPClient.from_transport(self._transport)
        except Exception as e:
            self._disconnect()
            raise exceptions.SbSFTPConnectionError(
                "Failed to connect to the S&B relay SFTP: {}".format(str(e))
            )

    def _disconnect(self) -> None:
        try:
            if self._sftp:
                self._sftp.close()
            if self._transport:
                self._transport.close()
        except Exception as e:
            logger.warning("{} Disconnect error: {}.".format(_LOG_PREFIX, str(e)))
        finally:
            self._sftp = None
            self._transport = None

    def _newest_matching_remote_file(self) -> typing.Optional[typing.Tuple[str, float]]:
        """
        ``(filename, mtime)`` of the newest file in the company's folder whose name is one of
        ``SB_INVENTORY_FILENAMES``, or None. Case-insensitive, because an attachment forwarded
        through a mail client arrives however the sender named it.
        """
        wanted = {name.lower() for name in self.inventory_filenames}
        newest: typing.Optional[typing.Tuple[str, float]] = None
        for attr in self._sftp.listdir_attr(_remote_join(self.sftp_directory, "")):
            name = (attr.filename or "").strip()
            if name.lower() not in wanted:
                continue
            mtime = float(attr.st_mtime or 0)
            if newest is None or mtime > newest[1]:
                newest = (name, mtime)
        return newest

    # ----------------------------------------------------------------------------------
    # Public API
    # ----------------------------------------------------------------------------------
    def feed_present(self) -> bool:
        """
        True when a real S&B inventory file exists in *this* company's relay folder.

        Connection failures propagate as SbSFTPConnectionError rather than returning False, so
        callers can tell "not there yet" apart from "couldn't check" — the same contract
        Meyer/A-Tech's ``feed_present()`` established and that
        ``src.api.services.integrations._relay_feed_connection_status`` relies on to choose
        between WAITING and FAILING.
        """
        self._connect()
        try:
            try:
                return self._newest_matching_remote_file() is not None
            except FileNotFoundError:
                # The company's uploads folder hasn't been created yet.
                return False
        finally:
            self._disconnect()

    def is_file_outdated(self, local_path: str) -> bool:
        if not os.path.exists(local_path):
            return True
        return (time.time() - os.path.getmtime(local_path)) > self.file_max_age

    def download_inventory_file(self, force_download: bool = False) -> str:
        """
        Download the newest S&B inventory file to the local dir and return its path.

        Raises SbFileNotFoundError when nothing has been delivered yet — which is the normal
        state for a brand-new connection, so callers log it and move on rather than failing the
        whole ingest.
        """
        self._connect()
        try:
            try:
                newest = self._newest_matching_remote_file()
            except FileNotFoundError:
                newest = None
            if not newest:
                raise exceptions.SbFileNotFoundError(
                    "No S&B inventory file on the relay (looked for {} in {!r}).".format(
                        ", ".join(self.inventory_filenames), self.sftp_directory,
                    )
                )

            remote_filename, _ = newest
            local_path = os.path.join(self.local_dir, remote_filename)
            if not force_download and not self.is_file_outdated(local_path):
                logger.info("{} Using cached file {}.".format(_LOG_PREFIX, local_path))
                return local_path

            remote_path = _remote_join(self.sftp_directory, remote_filename)
            tmp_path = "{}.tmp".format(local_path)
            try:
                os.makedirs(self.local_dir, exist_ok=True)
                self._sftp.get(remote_path, tmp_path)
                os.replace(tmp_path, local_path)
            except Exception as e:
                if os.path.exists(tmp_path):
                    try:
                        os.remove(tmp_path)
                    except OSError:
                        pass
                raise exceptions.SbSFTPConnectionError(
                    "S&B relay download failed for {}: {}".format(remote_path, str(e))
                )

            logger.info("{} Downloaded {} -> {}.".format(_LOG_PREFIX, remote_path, local_path))
            return local_path
        finally:
            self._disconnect()
