"""
FTP client for AllPro Distributing.

Plain FTP, not relay-provisioned: unlike Meyer/A-Tech/Crown/DLG/TWG, AllPro doesn't push a file to
our own relay -- each dealer logs into AllPro's own FTP server directly with a username/password
AllPro's rep issues per customer number (see PROVIDER_CATALOG's ALLPRO_DISTRIBUTING entry for the
request instructions shown to the user).

Only a connection/login test exists here so far -- no feed filename or format is known yet (no
sample file has ever been read; the one real attempt so far couldn't even resolve the host -- see
ALLPRO_FTP_HOST's own settings comment). Downloading and parsing a real feed is separate work for
once that's confirmed; building it now would mean guessing at a file we've never seen.
"""
import ftplib
import logging
import socket
import typing
from urllib.parse import urlparse

from django.conf import settings

from src.integrations.clients.allpro import exceptions

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[ALLPRO-FTP-CLIENT]"

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


def _normalize_host(value: typing.Any) -> str:
    """Hostname, or URL (ftp/ftps) -- host part only."""
    s = str(value or "").strip()
    if not s:
        return ""
    if "://" in s:
        return (urlparse(s).hostname or "").strip()
    return s


class AllProFTPClient:
    """
    Required ``credentials``: ``username``, ``password`` (the per-dealer login AllPro's rep
    issues). Optional overrides: ``ftp_host`` (or ``ftp_server``/``server_url``), ``ftp_port``.
    """

    def __init__(self, credentials: typing.Optional[typing.Dict] = None, require_credentials: bool = True):
        creds = dict(credentials or {})

        raw_host = creds.get("ftp_host") or creds.get("ftp_server") or creds.get("server_url") or ""
        self.ftp_host = (
            _normalize_host(raw_host)
            if str(raw_host or "").strip()
            else _normalize_host(_setting_str("ALLPRO_FTP_HOST", "adftp.hhenterprises.com"))
        )

        port_raw = creds.get("ftp_port")
        self.ftp_port = 0
        if port_raw is not None and str(port_raw).strip() != "":
            try:
                self.ftp_port = int(port_raw)
            except (TypeError, ValueError):
                self.ftp_port = 0
        if not (1 <= self.ftp_port <= 65535):
            self.ftp_port = _setting_int_port("ALLPRO_FTP_PORT", 21)

        self.username = str(creds.get("username") or "").strip()
        self.password = str(creds.get("password") or "").strip()

        missing = [k for k, v in (("username", self.username), ("password", self.password)) if not v]
        if not self.ftp_host:
            missing.append("ALLPRO_FTP_HOST (env) or ftp_host in credentials")

        if require_credentials and missing:
            raise ValueError(
                "Invalid AllPro FTP configuration — missing: {}. Request a username and password "
                "from Andrew.Avello@allprob2b.com (include your AllPro customer number).".format(
                    ", ".join(missing)
                )
            )

        self._ftp: typing.Optional[ftplib.FTP] = None

    def _connect(self) -> None:
        try:
            ftp = ftplib.FTP(timeout=DEFAULT_CONNECT_TIMEOUT_SECONDS)
            ftp.connect(self.ftp_host, self.ftp_port)
        except (socket.gaierror, socket.timeout, ConnectionRefusedError, OSError) as e:
            msg = "Could not reach AllPro's FTP server at {}:{}: {}".format(
                self.ftp_host, self.ftp_port, str(e)
            )
            logger.error("{} {}".format(_LOG_PREFIX, msg))
            raise exceptions.AllProFTPConnectionError(msg)

        try:
            ftp.login(self.username, self.password)
        except ftplib.error_perm as e:
            try:
                ftp.close()
            except Exception:  # noqa: BLE001
                pass
            msg = "AllPro rejected this username/password: {}".format(str(e))
            logger.error("{} {}".format(_LOG_PREFIX, msg))
            raise exceptions.AllProAuthError(msg)
        except Exception as e:
            try:
                ftp.close()
            except Exception:  # noqa: BLE001
                pass
            msg = "AllPro FTP login failed: {}".format(str(e))
            logger.error("{} {}".format(_LOG_PREFIX, msg))
            raise exceptions.AllProFTPConnectionError(msg)

        self._ftp = ftp

    def _disconnect(self) -> None:
        if not self._ftp:
            return
        try:
            self._ftp.quit()
        except Exception:  # noqa: BLE001 — a half-dead control channel raises here; close() releases it
            try:
                self._ftp.close()
            except Exception as e:  # noqa: BLE001
                logger.warning("{} Disconnect error: {}.".format(_LOG_PREFIX, str(e)))
        finally:
            self._ftp = None

    def test_connection(self) -> None:
        """Connect and log in, then disconnect. Raises on any failure; returns nothing on success."""
        self._connect()
        self._disconnect()
