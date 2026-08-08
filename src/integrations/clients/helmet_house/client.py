"""
Client for Helmet House's price and stock feed.

Helmet House publishes one FTP account holding their whole published catalog as a set of flat
files, all rewritten once a day (every live file carried the same timestamp when this was built).
``README.txt`` in the account root documents them; the ones that matter here are:

  * ``masterv.csv``  -- the complete catalog, 29 columns, **including the vendor (manufacturer)
    part number**. This is the only file the ingest needs: brand, description, dealer/retail/MAP
    price, per-warehouse stock, dimensions, UPC, photo filename, size/colour/model and status all
    live on the same row.
  * ``master.csv``   -- identical minus the Vendor P/N column. Used only as a fallback so an
    account that is somehow not given masterv.csv still ingests (master parts then key on Helmet
    House's own part number -- see master_parts._helmet_house_master_part_number).
  * ``xmitinv.csv`` / ``xmitparts.csv`` / ``xmitupc.csv`` / ``pmpro.csv`` -- narrower cuts of the
    same data, all redundant once masterv.csv is parsed. ``pmpro.csv`` additionally carries a
    stale ``Manufacturer`` column (it still labels the FXR block "TCX" and files 22k rows under
    "Misc"), so brand is taken from masterv.csv's Brand column and never from there.

Transport is **plain FTP on port 21** — their server offers no TLS, so unlike Keystone this cannot
be an implicit-FTPS connection. Nothing secret rides the wire beyond the login, which Helmet House
publishes as a single shared credential rather than issuing per dealer.
"""
import csv
import ftplib
import logging
import os
import time
import typing

from django.conf import settings

from src.integrations.clients.helmet_house import exceptions

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[HELMET-HOUSE-FTP-CLIENT]"

DEFAULT_FTP_HOST = "ftp.helmethouse.com"
DEFAULT_FTP_PORT = 21
DEFAULT_LOCAL_FILE_PATH = "/tmp/helmet_house_masterv.csv"
DEFAULT_FILE_MAX_AGE_SECONDS = 6 * 60 * 60  # 6 hours

# Preference order for the catalog file. masterv.csv is master.csv plus the Vendor P/N column, and
# that column is what lets a Helmet House part dedupe against the same part from another
# distributor, so it is always preferred when present.
CATALOG_FILENAMES = ("masterv.csv", "master.csv")

# Feed columns this client guarantees to callers. A file missing these is not the catalog file
# (e.g. an account pointed at xmitinv.csv), and failing here beats ingesting 40k blank rows.
REQUIRED_COLUMNS = ("Part Number", "Description", "Dealer", "Retail", "Status", "Brand")

# The server is reachable and answers promptly (a full 10 MB pull completed in seconds when this
# was built), but test_connection() runs synchronously inside an HTTP request, so the socket must
# never be able to hang the worker. Applies to connect and every subsequent command.
DEFAULT_CONNECT_TIMEOUT_SECONDS = 30

# Helmet House writes cp1252 punctuation (degree signs, smart quotes) into descriptions without
# declaring an encoding, so utf-8 is tried first and the file is decoded permissively after that.
_ENCODING_CANDIDATES = ("utf-8", "utf-8-sig", "cp1252", "latin-1")


class HelmetHouseFTPClient(object):
    """
    Downloads and parses Helmet House's catalog CSV. Returns raw row dicts keyed by the feed's own
    column headers; numeric coercion and brand normalisation are the service layer's job, matching
    the Keystone and Quadratec clients.
    """

    def __init__(
        self,
        credentials: typing.Optional[typing.Dict] = None,
        local_file_path: typing.Optional[str] = None,
        file_max_age: int = DEFAULT_FILE_MAX_AGE_SECONDS,
    ):
        creds = credentials or {}
        # Host/port are fixed in settings — the connect form only collects a login (see the
        # HELMHOUSE entry in PROVIDER_CATALOG) — but an override is still read from credentials so
        # a connection can be pointed at a different host without a deploy.
        self.ftp_host = (
            str(creds.get("ftp_host") or "").strip()
            or getattr(settings, "HELMET_HOUSE_FTP_HOST", DEFAULT_FTP_HOST)
        )
        self.ftp_port = int(
            creds.get("ftp_port") or getattr(settings, "HELMET_HOUSE_FTP_PORT", DEFAULT_FTP_PORT)
        )
        # Stripped because these are pasted by hand; a trailing space in the password is otherwise
        # sent verbatim and rejected as a bad login, which reads as "wrong credentials" to the user.
        self.ftp_user = str(creds.get("ftp_user") or "").strip()
        self.ftp_password = str(
            creds.get("ftp_password") or creds.get("ftp_pass") or ""
        ).strip()

        if not self.ftp_user or not self.ftp_password:
            raise ValueError("Invalid credentials. Missing ftp_user or ftp_password.")

        override = str(creds.get("catalog_filename") or "").strip()
        self.catalog_filenames = (override,) if override else CATALOG_FILENAMES

        self.local_file_path = local_file_path or getattr(
            settings, "HELMET_HOUSE_LOCAL_PATH", DEFAULT_LOCAL_FILE_PATH
        )
        self.file_max_age = file_max_age
        # Set by the last download so the service layer can record which file the rows came from.
        self.downloaded_filename: typing.Optional[str] = None

    # ----------------------------------------------------------------------------------
    # Transport
    # ----------------------------------------------------------------------------------
    def _connect(self) -> ftplib.FTP:
        try:
            ftp = ftplib.FTP()
            ftp.connect(
                host=self.ftp_host,
                port=self.ftp_port,
                timeout=DEFAULT_CONNECT_TIMEOUT_SECONDS,
            )
            ftp.login(user=self.ftp_user, passwd=self.ftp_password)
            ftp.set_pasv(True)
            logger.debug(
                "{} Connected to FTP server {}:{}.".format(
                    _LOG_PREFIX, self.ftp_host, self.ftp_port
                )
            )
            return ftp
        except ftplib.error_perm as e:
            msg = "Login rejected by Helmet House FTP server. Error: {}".format(str(e))
            logger.error("{} {}.".format(_LOG_PREFIX, msg))
            raise exceptions.HelmetHouseFTPAuthError(msg)
        except ftplib.all_errors as e:
            msg = "Failed to connect to Helmet House FTP server {}:{}. Error: {}".format(
                self.ftp_host, self.ftp_port, str(e)
            )
            logger.error("{} {}.".format(_LOG_PREFIX, msg))
            raise exceptions.HelmetHouseFTPConnectionError(msg)

    @staticmethod
    def _disconnect(ftp: typing.Optional[ftplib.FTP]) -> None:
        """
        Tear the socket down locally rather than sending QUIT and waiting for the server's 221 —
        any transfer has already completed by the time this runs, and the server notices the
        dropped connection on its own.
        """
        try:
            if ftp:
                ftp.close()
        except Exception as e:  # noqa: BLE001 -- a disconnect error must not mask the real one
            logger.warning("{} Error during disconnect: {}.".format(_LOG_PREFIX, str(e)))

    @staticmethod
    def _remote_size(ftp: ftplib.FTP, filename: str) -> typing.Optional[int]:
        """Byte size of a remote file, or None when the server refuses SIZE for it."""
        try:
            ftp.voidcmd("TYPE I")  # SIZE is refused in ASCII mode on most servers
            return ftp.size(filename)
        except (ftplib.error_perm, ftplib.error_temp, OSError):
            return None

    def _resolve_catalog_filename(self, ftp: ftplib.FTP) -> str:
        """
        First of ``catalog_filenames`` actually present on the server. Resolved against a real
        directory listing rather than trusting SIZE, so a server that refuses SIZE (or an account
        chrooted somewhere unexpected) still produces an accurate "which files are there" error.
        """
        try:
            available = {name.strip() for name in ftp.nlst()}
        except ftplib.all_errors as e:
            raise exceptions.HelmetHouseFTPConnectionError(
                "Connected to Helmet House FTP but could not list the feed directory: {}.".format(
                    str(e)
                )
            )
        # Helmet House's server answers NLST with bare filenames, but some servers return full
        # paths — compare on the basename so both shapes resolve.
        by_basename = {os.path.basename(name): name for name in available if name}
        for filename in self.catalog_filenames:
            if filename in by_basename:
                return by_basename[filename]
        raise exceptions.HelmetHouseFileNotFoundError(
            "None of the Helmet House catalog files {} are present on the FTP server. "
            "Found {} file(s) in the feed directory.".format(
                ", ".join(self.catalog_filenames), len(by_basename)
            )
        )

    def test_connection(self) -> None:
        """
        Connect-time check: log in and confirm a catalog file is actually there. Never downloads
        the ~10 MB file — an account that authenticates but is pointed at a directory without
        masterv.csv would otherwise look connected and never sync anything.
        """
        ftp = self._connect()
        try:
            self._resolve_catalog_filename(ftp)
        finally:
            self._disconnect(ftp)

    # ----------------------------------------------------------------------------------
    # Download
    # ----------------------------------------------------------------------------------
    def is_file_outdated(self) -> bool:
        if not os.path.exists(self.local_file_path):
            return True
        return (time.time() - os.path.getmtime(self.local_file_path)) > self.file_max_age

    def download_catalog_file(self, force_download: bool = False) -> str:
        """
        Download the catalog CSV and return the local path. The feed is rewritten once a day, so a
        local copy younger than ``file_max_age`` is reused unless ``force_download`` is set.
        """
        if not force_download and not self.is_file_outdated():
            logger.info(
                "{} Using existing local catalog file, not older than {} hours.".format(
                    _LOG_PREFIX, self.file_max_age // 3600
                )
            )
            return self.local_file_path

        # Download to a sibling temp path and move into place only once the transfer is verified,
        # so an interrupted pull can never leave a truncated file that the next run happily reuses.
        partial_path = "{}.partial".format(self.local_file_path)
        ftp = None
        try:
            ftp = self._connect()
            filename = self._resolve_catalog_filename(ftp)
            expected_size = self._remote_size(ftp, filename)

            with open(partial_path, "wb") as f:
                ftp.retrbinary("RETR {}".format(filename), f.write)

            downloaded_size = os.path.getsize(partial_path)
            if not downloaded_size:
                raise exceptions.HelmetHouseFTPConnectionError(
                    "Helmet House catalog file {} downloaded empty.".format(filename)
                )
            if expected_size is not None and downloaded_size != expected_size:
                raise exceptions.HelmetHouseFTPConnectionError(
                    "Download size mismatch for {}: got {} bytes, expected {}. File may be "
                    "incomplete.".format(filename, downloaded_size, expected_size)
                )

            os.replace(partial_path, self.local_file_path)
            self.downloaded_filename = os.path.basename(filename)
            logger.info(
                "{} Downloaded {} ({} bytes).".format(
                    _LOG_PREFIX, self.downloaded_filename, downloaded_size
                )
            )
            return self.local_file_path
        except exceptions.HelmetHouseException:
            raise
        except ftplib.error_perm as e:
            if "550" in str(e):
                raise exceptions.HelmetHouseFileNotFoundError(
                    "Helmet House catalog file not found on the FTP server: {}.".format(str(e))
                )
            raise exceptions.HelmetHouseFTPConnectionError(
                "Failed to download Helmet House catalog: {}.".format(str(e))
            )
        except ftplib.all_errors as e:
            msg = "Failed to download Helmet House catalog file: {}".format(str(e))
            logger.exception("{} {}.".format(_LOG_PREFIX, msg))
            raise exceptions.HelmetHouseFTPConnectionError(msg)
        finally:
            self._disconnect(ftp)
            if os.path.exists(partial_path):
                try:
                    os.remove(partial_path)
                except OSError:
                    pass

    # ----------------------------------------------------------------------------------
    # Parsing
    # ----------------------------------------------------------------------------------
    def _detect_encoding(self, path: str) -> str:
        """First encoding that decodes an 8 MiB sample cleanly."""
        try:
            with open(path, "rb") as f:
                sample = f.read(8 * 1024 * 1024)
        except OSError as e:
            raise exceptions.HelmetHouseDataValidationError(
                "Could not open Helmet House catalog CSV: {}.".format(str(e))
            )
        last_error: typing.Optional[Exception] = None
        for encoding in _ENCODING_CANDIDATES:
            try:
                sample.decode(encoding)
                return encoding
            except UnicodeDecodeError as e:
                last_error = e
        raise exceptions.HelmetHouseDataValidationError(
            "Unable to decode Helmet House CSV with {}: {}.".format(
                ", ".join(_ENCODING_CANDIDATES), last_error
            )
        )

    @staticmethod
    def _validate_header(fieldnames: typing.Optional[typing.Sequence[str]]) -> None:
        present = {(name or "").strip() for name in (fieldnames or [])}
        missing = [column for column in REQUIRED_COLUMNS if column not in present]
        if missing:
            raise exceptions.HelmetHouseDataValidationError(
                "Helmet House catalog CSV is missing expected column(s): {}. Found: {}.".format(
                    ", ".join(missing), ", ".join(sorted(present)) or "no columns"
                )
            )

    def iter_catalog_records(
        self, force_download: bool = False
    ) -> typing.Iterator[typing.Dict[str, str]]:
        """
        Stream the catalog CSV row-by-row as dicts keyed by the feed's own headers. Preferred over
        :func:`get_catalog_records` — the file is ~41k rows and holding it all in memory alongside
        the model instances built from it is avoidable.
        """
        path = self.download_catalog_file(force_download=force_download)
        encoding = self._detect_encoding(path)
        try:
            with open(path, "r", newline="", encoding=encoding, errors="replace") as f:
                reader = csv.DictReader(f)
                self._validate_header(reader.fieldnames)
                for row in reader:
                    yield {(k or "").strip(): v for k, v in row.items() if k is not None}
        except exceptions.HelmetHouseException:
            raise
        except Exception as e:
            msg = "Unable to stream the Helmet House catalog CSV. Error: {}".format(str(e))
            logger.exception("{} {}.".format(_LOG_PREFIX, msg))
            raise exceptions.HelmetHouseDataValidationError(msg)

    def get_catalog_records(
        self, force_download: bool = False
    ) -> typing.List[typing.Dict[str, str]]:
        """Whole catalog as a list. Loads the full file into memory — prefer the iterator."""
        records = list(self.iter_catalog_records(force_download=force_download))
        logger.info("{} Loaded catalog CSV with {} rows.".format(_LOG_PREFIX, len(records)))
        return records
