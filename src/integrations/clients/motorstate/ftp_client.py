"""
FTP client for the Motor State per-account catalog/pricing feed.

Plain FTP on purpose. The server (Pure-FTPd) advertises ``AUTH TLS`` and the control channel
negotiates fine, but every passive data connection it hands back after ``PROT P`` is refused,
so an FTPS transfer can authenticate and then never move a byte. It also answers ``EPSV`` with
``500 Unknown command``; ftplib only reaches for EPSV over IPv6 and this host is IPv4-only, so
plain ``PASV`` is what gets used. Active mode is rejected outright (the server refuses to open
a connection to anything but the client's public IP, which NAT breaks).

Credentials live on ``CompanyProviders.credentials["feed"]`` and must carry ``ftp_user`` and
``ftp_password``. Host, port, directory and filename fall back to Django settings
(``MOTORSTATE_FTP_*``); the filename additionally falls back to the account number carried in
the login itself (``853809@motorstateftp.com`` -> ``853809.csv``), which is how Motor State
names every dealer's file.
"""
import ftplib
import logging
import os
import time
import typing
from urllib.parse import urlparse

from django.conf import settings

from src.integrations.clients.motorstate import exceptions
from src.integrations.clients.motorstate.feed_spec import DEFAULT_REMOTE_FEED_FILENAME

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[MOTOR-STATE-FTP-CLIENT]"

DEFAULT_LOCAL_FEED = "/tmp/motorstate_feed.csv"
DEFAULT_FILE_MAX_AGE_SECONDS = 6 * 60 * 60
DEFAULT_CONNECT_TIMEOUT_SECONDS = 30

_DEFAULT_MOTORSTATE_FTP_HOST = "ftp.motorstateftp.com"
_DEFAULT_MOTORSTATE_FTP_PORT = 21
# The account's file sits in the login's home directory; there is no subfolder.
_DEFAULT_MOTORSTATE_FTP_DIRECTORY = ""

# 1 MiB blocks: the feed is tens of MB and the default 8 KiB block size costs a pointless
# number of round trips through retrbinary's callback.
_RETR_BLOCK_SIZE = 1024 * 1024


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


def _normalize_host(value: typing.Any) -> str:
    """Hostname, or URL (ftp/ftps/https) -- host part only."""
    s = str(value or "").strip()
    if not s:
        return ""
    if "://" in s:
        return (urlparse(s).hostname or "").strip()
    return s


def _coalesce_nonempty_str(creds: typing.Dict, key: str, fallback: str) -> str:
    v = creds.get(key)
    if v is not None and str(v).strip():
        return str(v).strip()
    return str(fallback or "").strip()


def default_remote_filename_for_user(ftp_user: typing.Any) -> str:
    """``853809@motorstateftp.com`` -> ``853809.csv``.

    Motor State names each dealer's file after the account number that is also the local part
    of the FTP login, so the filename never has to be configured separately. Returns "" when
    the login has no usable local part, leaving the caller on the settings fallback.
    """
    local_part = str(ftp_user or "").strip().split("@", 1)[0].strip()
    return "{}.csv".format(local_part) if local_part else ""


class MotorStateFTPClient:
    """
    Downloads one account's feed file over plain FTP.

    Required ``credentials``: ``ftp_user``, ``ftp_password``.
    Optional: ``ftp_host`` (or ``ftp_server`` / ``server_url``), ``ftp_port``,
    ``ftp_directory``, ``ftp_remote_file`` (or ``remote_file``), ``local_feed_path``.
    """

    def __init__(
        self,
        credentials: typing.Optional[typing.Dict] = None,
        local_feed_path: typing.Optional[str] = None,
        file_max_age: int = DEFAULT_FILE_MAX_AGE_SECONDS,
        require_credentials: bool = True,
    ):
        creds = dict(credentials or {})

        raw_host = creds.get("ftp_host") or creds.get("ftp_server") or creds.get("server_url") or ""
        if str(raw_host or "").strip():
            self.ftp_host = _normalize_host(raw_host)
        else:
            self.ftp_host = _normalize_host(
                _setting_str("MOTORSTATE_FTP_HOST", _DEFAULT_MOTORSTATE_FTP_HOST)
            )

        port_raw = creds.get("ftp_port")
        self.ftp_port = 0
        if port_raw is not None and str(port_raw).strip() != "":
            try:
                self.ftp_port = int(port_raw)
            except (TypeError, ValueError):
                self.ftp_port = 0
        if not 1 <= self.ftp_port <= 65535:
            self.ftp_port = _setting_int_port("MOTORSTATE_FTP_PORT", _DEFAULT_MOTORSTATE_FTP_PORT)

        self.ftp_user = str(creds.get("ftp_user") or "").strip()
        self.ftp_password = str(creds.get("ftp_password") or "").strip()

        missing = [k for k, v in (("ftp_user", self.ftp_user), ("ftp_password", self.ftp_password)) if not v]

        self.ftp_directory = _coalesce_nonempty_str(
            creds,
            "ftp_directory",
            _setting_str("MOTORSTATE_FTP_DIRECTORY", _DEFAULT_MOTORSTATE_FTP_DIRECTORY),
        )
        self.ftp_remote_file = _coalesce_nonempty_str(
            creds,
            "ftp_remote_file",
            _coalesce_nonempty_str(
                creds,
                "remote_file",
                default_remote_filename_for_user(self.ftp_user)
                or _setting_str("MOTORSTATE_FEED_REMOTE_FILE", DEFAULT_REMOTE_FEED_FILENAME),
            ),
        )

        if not self.ftp_host:
            missing.append("MOTORSTATE_FTP_HOST (env) or ftp_host in credentials")
        if not self.ftp_remote_file:
            missing.append("ftp_remote_file in credentials (and ftp_user carries no account number)")

        self.local_feed_path = (
            str(creds.get("local_feed_path") or "").strip()
            or local_feed_path
            or getattr(settings, "MOTORSTATE_FEED_LOCAL_PATH", DEFAULT_LOCAL_FEED)
        )
        self.file_max_age = file_max_age

        if require_credentials and missing:
            raise ValueError(
                "Invalid Motor State FTP configuration -- missing: {}. Company credentials must "
                "include ftp_user and ftp_password; host, port, folder and filename come from "
                "Django settings (MOTORSTATE_FTP_*) or the account number in ftp_user.".format(
                    ", ".join(missing)
                )
            )

        self._ftp: typing.Optional[ftplib.FTP] = None

    # -- connection -------------------------------------------------------
    def _connect(self) -> None:
        try:
            ftp = ftplib.FTP(timeout=DEFAULT_CONNECT_TIMEOUT_SECONDS)
            ftp.connect(self.ftp_host, self.ftp_port)
            ftp.login(self.ftp_user, self.ftp_password)
            ftp.set_pasv(True)
            if self.ftp_directory:
                ftp.cwd(self.ftp_directory)
            self._ftp = ftp
        except Exception as e:
            msg = "Failed to connect to Motor State FTP: {}".format(str(e))
            logger.error("{} {}".format(_LOG_PREFIX, msg))
            raise exceptions.MotorStateFTPConnectionError(msg)

    def _disconnect(self) -> None:
        if not self._ftp:
            return
        try:
            self._ftp.quit()
        except Exception:
            # quit() sends QUIT and waits for a reply; a half-dead control channel raises here
            # and close() is the only way to release the socket.
            try:
                self._ftp.close()
            except Exception as e:
                logger.warning("{} Disconnect error: {}.".format(_LOG_PREFIX, str(e)))
        finally:
            self._ftp = None

    # -- transfer ---------------------------------------------------------
    def is_file_outdated(self, local_path: str) -> bool:
        if not os.path.exists(local_path):
            return True
        return (time.time() - os.path.getmtime(local_path)) > self.file_max_age

    def download_feed_file(self, force_download: bool = False) -> str:
        """Download this account's feed to the configured local path and return that path."""
        local_path = self.local_feed_path
        if not force_download and not self.is_file_outdated(local_path):
            logger.info("{} Using cached file {}.".format(_LOG_PREFIX, local_path))
            return local_path

        parent = os.path.dirname(local_path)
        if parent:
            os.makedirs(parent, exist_ok=True)

        tmp_path = local_path + ".tmp"
        try:
            self._connect()
            started = time.monotonic()
            with open(tmp_path, "wb") as fh:
                self._ftp.retrbinary(
                    "RETR {}".format(self.ftp_remote_file), fh.write, blocksize=_RETR_BLOCK_SIZE
                )
            os.replace(tmp_path, local_path)
            logger.info(
                "{} Downloaded {} -> {} ({} bytes in {:.1f}s).".format(
                    _LOG_PREFIX,
                    self.ftp_remote_file,
                    local_path,
                    os.path.getsize(local_path),
                    time.monotonic() - started,
                )
            )
            return local_path
        except ftplib.error_perm as e:
            # 550 is "no such file" as well as "permission denied"; the message is all the
            # server gives us to tell them apart.
            msg = "Motor State feed not available ({}): {}".format(self.ftp_remote_file, str(e))
            logger.error("{} {}".format(_LOG_PREFIX, msg))
            self._remove_quietly(tmp_path)
            raise exceptions.MotorStateFeedNotFoundError(msg)
        except exceptions.MotorStateFTPConnectionError:
            self._remove_quietly(tmp_path)
            raise
        except Exception as e:
            self._remove_quietly(tmp_path)
            msg = "Motor State FTP download failed: {}".format(str(e))
            logger.error("{} {}".format(_LOG_PREFIX, msg))
            raise exceptions.MotorStateFTPConnectionError(msg)
        finally:
            self._disconnect()

    @staticmethod
    def _remove_quietly(path: str) -> None:
        if os.path.exists(path):
            try:
                os.remove(path)
            except OSError:
                pass

    def feed_present(self) -> bool:
        """
        True if this account's feed file exists. SIZE only -- downloads nothing. Connection
        failures propagate rather than returning False, so callers can tell "not there yet"
        apart from "couldn't check".
        """
        self._connect()
        try:
            try:
                self._ftp.size(self.ftp_remote_file)
                return True
            except ftplib.error_perm:
                return False
        finally:
            self._disconnect()
