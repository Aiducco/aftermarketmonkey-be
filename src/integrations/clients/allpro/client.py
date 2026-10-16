"""
FTP client for AllPro Distributing.

Plain FTP, not relay-provisioned: unlike Meyer/A-Tech/Crown/DLG/TWG, AllPro doesn't push a file to
our own relay -- each dealer logs into AllPro's own FTP server directly with a username/password
AllPro's rep issues per customer number (see PROVIDER_CATALOG's ALLPRO_DISTRIBUTING entry for the
request instructions shown to the user).

Three real files confirmed live 2026-10-15/16 against Trident's own apd-9010066 login:
  - ``APDLineCodes.txt`` (pipe-delimited, ``LineCode|Brand|AAIA``) -- the brand list, 739 rows,
    268 with a real (non-placeholder) AAIA code. One line_code can carry several different AAIA
    codes (e.g. "ACI" -> ACCESS/LOMAX/ADARAC) -- see AllProBrand's own docstring for why AAIA,
    not line_code, is the natural key.
  - ``APDInventory.txt`` (pipe-delimited, ``APD Full Part Number|Line Code|Vendor Part
    Number|AAIA|L|W|H|Wt|UPC|600 VA QOH|800 FL QOH|900 AL QOH|Total QOH``) -- the real
    catalog+inventory feed, confirmed live at 491,806 rows (only 8,314 with Total QOH > 0).
    ``APD Full Part Number`` = ``LineCode + VendorPartNumber`` concatenated with no separator
    (confirmed: "DAY"+"KJ71051BK" -> "DAYKJ71051BK").
  - ``MASTER_PRICEFILE.CSV`` (``"Unique APD Part Number","Brand Name","Cost"``) -- per-company
    cost. Its part-number column uses a DIFFERENT format than the inventory file's (colon-
    separated, e.g. "13F:BO2S71M") and only resolves against the inventory file's
    full_part_number for a real subset of rows once the colon is stripped (confirmed live:
    ~26% of a 300k-row sample) -- see src.integrations.services.allpro for the join and its
    honest gap-logging, same pattern as every other real-world distributor data-quality gap in
    this codebase.
"""
import csv
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
_RETR_BLOCKSIZE = 65536


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
            else _normalize_host(_setting_str("ALLPRO_FTP_HOST", "apdftp.hhenterprises.com"))
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

    def _iter_lines(self, remote_filename: str) -> typing.Iterator[str]:
        """
        Streams a remote file's lines -- confirmed necessary live (APDInventory.txt is ~33MB,
        MASTER_PRICEFILE.CSV ~26MB). ftplib's retrbinary callback can't yield directly (it isn't
        called from this generator's own frame), so chunks are collected into a list during the
        transfer and decoded/split into lines once it completes -- one file's bytes held at a
        time, same as downloading it to disk first would cost, just without the disk round trip.
        Connects and disconnects for just this one file; a caller needing more than one file
        calls this (or a wrapping iter_* method) once per file, each its own connection.
        """
        self._connect()
        try:
            chunks: typing.List[bytes] = []
            try:
                self._ftp.retrbinary("RETR {}".format(remote_filename), chunks.append, blocksize=_RETR_BLOCKSIZE)
            except ftplib.error_perm as e:
                msg = "AllPro FTP could not read {}: {}".format(remote_filename, str(e))
                logger.error("{} {}".format(_LOG_PREFIX, msg))
                raise exceptions.AllProFTPConnectionError(msg)

            data = b"".join(chunks)
            del chunks
            text = data.decode("utf-8", errors="replace")
            del data
            for line in text.splitlines():
                yield line
        finally:
            self._disconnect()

    def iter_line_codes(self) -> typing.Iterator[typing.Dict]:
        """Yields {line_code, name, aaia_code} from APDLineCodes.txt. aaia_code is uppercased;
        rows with a blank line_code or brand name are skipped."""
        lines = self._iter_lines("APDLineCodes.txt")
        header = next(lines, None)
        if header is None:
            return
        for line in lines:
            parts = line.split("|")
            if len(parts) < 3:
                continue
            line_code, name, aaia = (p.strip() for p in parts[:3])
            if not line_code or not name:
                continue
            yield {"line_code": line_code, "name": name, "aaia_code": aaia.upper()}

    _INVENTORY_COLUMNS = (
        "full_part_number", "line_code", "part_number", "aaia_code",
        "length", "width", "height", "weight", "upc",
        "warehouse_va_qty", "warehouse_fl_qty", "warehouse_al_qty", "total_qty",
    )

    def iter_inventory_records(self) -> typing.Iterator[typing.Dict]:
        """Yields one dict per APDInventory.txt row, keyed by _INVENTORY_COLUMNS in order.
        Numeric fields are left as raw strings -- parsing/validation is the caller's job (see
        src.integrations.services.allpro)."""
        lines = self._iter_lines("APDInventory.txt")
        header = next(lines, None)
        if header is None:
            return
        n = len(self._INVENTORY_COLUMNS)
        for line in lines:
            parts = line.split("|")
            if len(parts) < n:
                continue
            yield dict(zip(self._INVENTORY_COLUMNS, (p.strip() for p in parts[:n])))

    def iter_pricing_records(self) -> typing.Iterator[typing.Dict]:
        """Yields {raw_part_number, brand_name, cost} from MASTER_PRICEFILE.CSV (real CSV,
        comma-delimited with quoted fields -- unlike the other two pipe-delimited files)."""
        lines = self._iter_lines("MASTER_PRICEFILE.CSV")
        reader = csv.reader(lines)
        header = next(reader, None)
        if header is None:
            return
        for row in reader:
            if len(row) < 3:
                continue
            yield {"raw_part_number": row[0].strip(), "brand_name": row[1].strip(), "cost": row[2].strip()}
