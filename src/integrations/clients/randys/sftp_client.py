"""
SFTP client for RANDYS Worldwide's inventory push.

Two things make this different from the Meyer/Tri-State relay clients it is otherwise modelled on:

* **The filename is timestamped.** RANDYS send ``Inventory_RWW_<YYMMDDHHMM>.csv``
  (e.g. ``Inventory_RWW_2610270310.csv``), so there is no fixed name to ``get()``. The folder is
  listed and the newest matching file wins -- S&B's relay client does the same for the same
  reason, and like S&B the match is prefix+suffix specific rather than "any file", because every
  relay provider shares one ``uploads/`` folder per company. A loose check here would happily
  match DLG's or Crown's file (that exact bug bit Crown on 2026-10-13).

* **The directory may be absolute.** Connections on our own relay are chrooted, so the folder is
  ``/uploads``. The primary connection (company 16) is served from RANDYS' own AWS box, which has
  no per-company chroot and keeps the drop at ``/home/sftpuser/uploads``. ``_remote_join``
  therefore passes an absolute configured directory through untouched and only prefixes a
  relative one -- so the outlier is a credentials value, not a second code path.

Host, port, directory and login all resolve per connection first
(``credentials["feed"]``), then Django settings, then ``src.constants``. Nothing about the
catalogue needs this client: it is inventory only.
"""
import logging
import os
import socket
import time
import typing

import paramiko
from django.conf import settings

from src import constants as src_constants
from src.integrations.clients.randys import exceptions

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[RANDYS-SFTP-CLIENT]"

# A bare paramiko.Transport((host, port)) never times out against a firewalled host, so the
# socket is created explicitly with a deadline -- same reason Meyer's client does it.
DEFAULT_CONNECT_TIMEOUT_SECONDS = 15
DEFAULT_FILE_MAX_AGE_SECONDS = 6 * 60 * 60


def _normalize_sftp_server(value: typing.Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    for prefix in ("sftp://", "ftp://", "ssh://", "https://", "http://"):
        if raw.lower().startswith(prefix):
            raw = raw[len(prefix):]
            break
    return raw.strip("/").split("/")[0].strip()


def _remote_join(directory: str, filename: str) -> str:
    """
    An absolute ``directory`` is used verbatim (RANDYS' own non-chrooted box); a relative one is
    anchored at the chroot root, matching every other relay client. See module docstring.
    """
    folder = (directory or "").strip()
    if not folder:
        return filename
    folder = folder.rstrip("/")
    if not folder.startswith("/"):
        folder = "/{}".format(folder)
    return "{}/{}".format(folder, filename) if filename else folder


def _coalesce_nonempty_str(*values: typing.Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


class RandysSFTPClient:
    """
    Reads the newest ``Inventory_RWW_*.csv`` from the connection's relay folder.

    Credentials (``CompanyProviders.credentials["feed"]``) may set ``sftp_server`` /
    ``sftp_host`` / ``server_url``, ``sftp_port``, ``sftp_directory``, ``sftp_user`` and
    ``sftp_password``. ``sftp_user``/``sftp_password`` fall back to
    ``settings.RANDYS_RELAY_SFTP_USER`` / ``RANDYS_RELAY_SFTP_PASSWORD`` only for a connection
    with no relay account of its own.
    """

    def __init__(
        self,
        credentials: typing.Optional[typing.Dict] = None,
        local_dir: typing.Optional[str] = None,
        file_max_age: int = DEFAULT_FILE_MAX_AGE_SECONDS,
        require_credentials: bool = True,
    ):
        creds = dict(credentials or {})

        self.sftp_server = _normalize_sftp_server(
            _coalesce_nonempty_str(
                creds.get("sftp_server"),
                creds.get("sftp_host"),
                creds.get("server_url"),
                getattr(settings, "RANDYS_SFTP_HOST", None),
                src_constants.RANDYS_RELAY_SFTP_HOST,
            )
        )

        port_raw = _coalesce_nonempty_str(
            creds.get("sftp_port"),
            getattr(settings, "RANDYS_SFTP_PORT", None),
            src_constants.RANDYS_RELAY_SFTP_PORT,
        )
        try:
            port = int(port_raw)
        except (TypeError, ValueError):
            port = int(src_constants.RANDYS_RELAY_SFTP_PORT)
        self.sftp_port = port if 1 <= port <= 65535 else int(src_constants.RANDYS_RELAY_SFTP_PORT)

        self.sftp_directory = _coalesce_nonempty_str(
            creds.get("sftp_directory"),
            getattr(settings, "RANDYS_SFTP_DIRECTORY", None),
            src_constants.RANDYS_RELAY_SFTP_DIRECTORY,
        )

        self.filename_prefix = src_constants.RANDYS_INVENTORY_FILENAME_PREFIX
        self.filename_suffix = src_constants.RANDYS_INVENTORY_FILENAME_SUFFIX
        # Pricing name is a placeholder until RANDYS deliver one -- see src.constants. Overridable
        # per connection so the real name can be set the moment the rep confirms it.
        self.pricing_filename_prefix = _coalesce_nonempty_str(
            creds.get("pricing_filename_prefix"),
            getattr(settings, "RANDYS_PRICING_FILENAME_PREFIX", None),
            src_constants.RANDYS_PRICING_FILENAME_PREFIX,
        )
        self.pricing_filename_suffix = _coalesce_nonempty_str(
            creds.get("pricing_filename_suffix"),
            getattr(settings, "RANDYS_PRICING_FILENAME_SUFFIX", None),
            src_constants.RANDYS_PRICING_FILENAME_SUFFIX,
        )

        self.sftp_user = _coalesce_nonempty_str(
            creds.get("sftp_user"),
            creds.get("ftp_user"),
            getattr(settings, "RANDYS_RELAY_SFTP_USER", None),
        )
        self.sftp_password = _coalesce_nonempty_str(
            creds.get("sftp_password"),
            creds.get("ftp_password"),
            getattr(settings, "RANDYS_RELAY_SFTP_PASSWORD", None),
        )

        self.local_dir = _coalesce_nonempty_str(
            creds.get("local_feed_dir"),
            local_dir,
            getattr(settings, "RANDYS_INVENTORY_LOCAL_DIR", None),
            "/tmp",
        )
        self.file_max_age = file_max_age

        missing: typing.List[str] = []
        if not self.sftp_server:
            missing.append("sftp_server (credentials, or RANDYS_RELAY_SFTP_HOST in src.constants)")
        if not self.sftp_directory:
            missing.append("sftp_directory (credentials, or RANDYS_RELAY_SFTP_DIRECTORY in src.constants)")
        if not self.sftp_user:
            missing.append("sftp_user (credentials, or RANDYS_RELAY_SFTP_USER in Django settings)")
        if not self.sftp_password:
            missing.append("sftp_password (credentials, or RANDYS_RELAY_SFTP_PASSWORD in Django settings)")
        if require_credentials and missing:
            raise ValueError(
                "Invalid RANDYS SFTP configuration -- missing: {}.".format(", ".join(missing))
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
            msg = "Failed to connect to RANDYS SFTP: {}".format(str(e))
            logger.error("{} {}".format(_LOG_PREFIX, msg))
            raise exceptions.RandysSFTPConnectionError(msg)

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

    @staticmethod
    def _matches_name(filename: str, prefix: str, suffix: str) -> bool:
        lowered = (filename or "").lower()
        return lowered.startswith((prefix or "").lower()) and lowered.endswith(
            (suffix or "").lower()
        )

    def _matches_inventory_name(self, filename: str) -> bool:
        return self._matches_name(filename, self.filename_prefix, self.filename_suffix)

    def _matches_pricing_name(self, filename: str) -> bool:
        return self._matches_name(
            filename, self.pricing_filename_prefix, self.pricing_filename_suffix
        )

    def _newest_remote_file(
        self, prefix: str, suffix: str
    ) -> typing.Optional[typing.Tuple[str, float]]:
        """
        Newest ``<prefix>*<suffix>`` in the folder as ``(filename, mtime)``, or None.

        Ordered by mtime rather than by the timestamp embedded in the name: the name's stamp is
        RANDYS' own clock, so a re-sent or corrected file would sort wrongly by it.

        The prefix+suffix match is specific on purpose. Every relay provider shares one
        ``uploads/`` folder per company, so an "any file" check happily matches a different
        distributor's drop -- that exact bug hit Crown on 2026-10-13 when it matched DLG's file.
        It is also what keeps the inventory and pricing feeds apart here.
        """
        folder = _remote_join(self.sftp_directory, "")
        entries = self._sftp.listdir_attr(folder)
        newest: typing.Optional[typing.Tuple[str, float]] = None
        for entry in entries:
            name = getattr(entry, "filename", "") or ""
            if not self._matches_name(name, prefix, suffix):
                continue
            mtime = float(getattr(entry, "st_mtime", 0) or 0)
            if newest is None or mtime > newest[1]:
                newest = (name, mtime)
        return newest

    def _newest_inventory_remote_file(self) -> typing.Optional[typing.Tuple[str, float]]:
        return self._newest_remote_file(self.filename_prefix, self.filename_suffix)

    def _newest_pricing_remote_file(self) -> typing.Optional[typing.Tuple[str, float]]:
        return self._newest_remote_file(
            self.pricing_filename_prefix, self.pricing_filename_suffix
        )

    def feed_present(self) -> bool:
        """
        True when an ``Inventory_RWW_*.csv`` exists in this connection's folder. One connection,
        listing only -- downloads nothing. A missing folder means the drop has never happened and
        is False; connection failures propagate so callers can tell "not there yet" from "could
        not check", the same contract Meyer's feed_present() originated.
        """
        self._connect()
        try:
            try:
                return self._newest_inventory_remote_file() is not None
            except FileNotFoundError:
                return False
        except exceptions.RandysSFTPConnectionError:
            raise
        except IOError:
            return False
        finally:
            self._disconnect()

    def list_relay_files(self) -> typing.List[typing.Dict[str, typing.Any]]:
        """
        Everything in the folder, for diagnosing a push that landed under an unexpected name --
        feed_present() returning False gives no hint on its own. Mirrors Tri-State's method.
        """
        self._connect()
        try:
            folder = _remote_join(self.sftp_directory, "")
            try:
                entries = self._sftp.listdir_attr(folder)
            except FileNotFoundError:
                return []
            rows = [
                {
                    "filename": getattr(e, "filename", "") or "",
                    "size_bytes": int(getattr(e, "st_size", 0) or 0),
                    "modified_at": float(getattr(e, "st_mtime", 0) or 0),
                    "matches_inventory_pattern": self._matches_inventory_name(
                        getattr(e, "filename", "") or ""
                    ),
                    "matches_pricing_pattern": self._matches_pricing_name(
                        getattr(e, "filename", "") or ""
                    ),
                }
                for e in entries
            ]
            return sorted(rows, key=lambda r: r["filename"].lower())
        finally:
            self._disconnect()

    def is_file_outdated(self, local_path: str) -> bool:
        if not os.path.exists(local_path):
            return True
        return (time.time() - os.path.getmtime(local_path)) > self.file_max_age

    def _download_newest(
        self,
        prefix: str,
        suffix: str,
        label: str,
        force_download: bool,
    ) -> str:
        """
        Download the newest ``<prefix>*<suffix>`` and return the local path.

        The local name mirrors the remote one, so a new RANDYS push (new timestamp) is always a
        cache miss and the staleness window only ever suppresses re-downloading the *same* file.
        Written to ``.tmp`` then ``os.replace``d, so a reader never sees a partial CSV.
        """
        self._connect()
        try:
            try:
                newest = self._newest_remote_file(prefix, suffix)
            except FileNotFoundError:
                newest = None
            if newest is None:
                msg = "No {}*{} ({}) found in {} on {}.".format(
                    prefix, suffix, label, _remote_join(self.sftp_directory, ""), self.sftp_server,
                )
                logger.warning("{} {}".format(_LOG_PREFIX, msg))
                raise exceptions.RandysFileNotFoundError(msg)

            remote_filename, _mtime = newest
            remote_path = _remote_join(self.sftp_directory, remote_filename)
            os.makedirs(self.local_dir, exist_ok=True)
            local_path = os.path.join(self.local_dir, remote_filename)

            if not force_download and not self.is_file_outdated(local_path):
                logger.info("{} Using cached file {}.".format(_LOG_PREFIX, local_path))
                return local_path

            tmp_path = local_path + ".tmp"
            try:
                self._sftp.get(remote_path, tmp_path)
                os.replace(tmp_path, local_path)
            except Exception:
                if os.path.exists(tmp_path):
                    try:
                        os.remove(tmp_path)
                    except OSError:
                        pass
                raise
            logger.info("{} Downloaded {} -> {}.".format(_LOG_PREFIX, remote_path, local_path))
            return local_path
        except (exceptions.RandysFileNotFoundError, exceptions.RandysSFTPConnectionError):
            raise
        except Exception as e:
            msg = "RANDYS SFTP download failed: {}".format(str(e))
            logger.error("{} {}".format(_LOG_PREFIX, msg))
            raise exceptions.RandysSFTPConnectionError(msg)
        finally:
            self._disconnect()

    def download_inventory_file(self, force_download: bool = False) -> str:
        return self._download_newest(
            self.filename_prefix, self.filename_suffix, "inventory", force_download
        )

    def download_pricing_file(self, force_download: bool = False) -> str:
        """
        Newest per-company pricing file. The name is a placeholder until RANDYS deliver one, so a
        RandysFileNotFoundError here is the expected state today, not a fault -- callers treat it
        as "no dealer cost yet" and leave cost null rather than guessing at one.
        """
        return self._download_newest(
            self.pricing_filename_prefix, self.pricing_filename_suffix, "pricing", force_download
        )

    def pricing_feed_present(self) -> bool:
        self._connect()
        try:
            try:
                return self._newest_pricing_remote_file() is not None
            except FileNotFoundError:
                return False
        except exceptions.RandysSFTPConnectionError:
            raise
        except IOError:
            return False
        finally:
            self._disconnect()
