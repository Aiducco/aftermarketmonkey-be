import csv
import ftplib
import logging
import os
import time
import typing
import zipfile

from django.conf import settings

from src.integrations.clients.premier import exceptions

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[PREMIER-FTP-CLIENT]"

DEFAULT_FTP_HOST = "datafeed.pppwd.com"
DEFAULT_FTP_PORT = 21
DEFAULT_INVENTORY_ZIP_FILENAME = "premier_data_feed_master.zip"
DEFAULT_INVENTORY_CSV_FILENAME = "premier_data_feed_master.csv"
DEFAULT_FILE_MAX_AGE_SECONDS = 6 * 60 * 60  # 6 hours

# ftp.connect() had no timeout before, so a bad host/firewall could hang the calling thread
# indefinitely — matters now that test_connection() runs synchronously inside an HTTP request.
DEFAULT_CONNECT_TIMEOUT_SECONDS = 15


class PremierFTPClient:
    """
    FTP client for Premier Performance inventory data.
    Connects via plain FTP (port 21, passive mode).
    Host and port are fixed; only ftp_user and ftp_password are needed per company.
    """

    def __init__(
        self,
        credentials: typing.Optional[typing.Dict] = None,
        local_file_path: typing.Optional[str] = None,
        file_max_age: int = DEFAULT_FILE_MAX_AGE_SECONDS,
    ):
        creds = credentials or {}
        self.ftp_host = getattr(settings, "PREMIER_FTP_HOST", DEFAULT_FTP_HOST)
        self.ftp_port = int(getattr(settings, "PREMIER_FTP_PORT", DEFAULT_FTP_PORT))
        self.ftp_user = creds.get("ftp_user") or getattr(settings, "PREMIER_FTP_USER", "")
        self.ftp_pass = creds.get("ftp_password") or creds.get("ftp_pass") or getattr(
            settings, "PREMIER_FTP_PASSWORD", ""
        )

        if not self.ftp_user or not self.ftp_pass:
            raise ValueError("Invalid credentials. Missing ftp_user or ftp_password.")

        self.local_file_path = local_file_path or getattr(
            settings, "PREMIER_INVENTORY_LOCAL_PATH", "/tmp/premier_data_feed_master.csv"
        )
        self.file_max_age = file_max_age
        self._ftp_client: typing.Optional[ftplib.FTP] = None

    def _connect(self) -> ftplib.FTP:
        try:
            ftp = ftplib.FTP()
            ftp.connect(host=self.ftp_host, port=self.ftp_port, timeout=DEFAULT_CONNECT_TIMEOUT_SECONDS)
            ftp.login(user=self.ftp_user, passwd=self.ftp_pass)
            ftp.set_pasv(True)
            logger.debug(
                "{} Connected to FTP server {}:{}.".format(_LOG_PREFIX, self.ftp_host, self.ftp_port)
            )
            return ftp
        except ftplib.error_perm as e:
            # Server actively rejected the login (e.g. "530 Login or password incorrect!") —
            # distinct from an unreachable host/timeout, so callers can tell bad credentials
            # apart from a connectivity problem.
            msg = "Login rejected by FTP server. Error: {}".format(str(e))
            logger.error("{} {}.".format(_LOG_PREFIX, msg))
            raise exceptions.PremierFTPAuthError(msg)
        except ftplib.all_errors as e:
            msg = "Failed to connect to FTP server. Error: {}".format(str(e))
            logger.error("{} {}.".format(_LOG_PREFIX, msg))
            raise exceptions.PremierFTPConnectionError(msg)

    def _disconnect(self, ftp: typing.Optional[ftplib.FTP]) -> None:
        try:
            if ftp:
                ftp.quit()
                logger.debug("{} Disconnected from FTP server.".format(_LOG_PREFIX))
        except Exception as e:
            logger.warning("{} Error during disconnect: {}.".format(_LOG_PREFIX, str(e)))

    def test_connection(self) -> None:
        """Log in and immediately disconnect — validates credentials without downloading anything."""
        ftp = self._connect()
        self._disconnect(ftp)

    def is_file_outdated(self) -> bool:
        if not os.path.exists(self.local_file_path):
            return True
        file_mod_time = os.path.getmtime(self.local_file_path)
        return (time.time() - file_mod_time) > self.file_max_age

    def download_inventory_file(self, force_download: bool = False) -> str:
        """
        Download premier_data_feed_master.zip from FTP and extract the CSV.
        Returns path to local CSV file.
        """
        if not force_download and not self.is_file_outdated():
            logger.info("{} Using cached inventory file (not older than {} hours).".format(
                _LOG_PREFIX, self.file_max_age // 3600
            ))
            return self.local_file_path

        zip_path = os.path.splitext(self.local_file_path)[0] + ".zip"
        ftp = None
        try:
            ftp = self._connect()

            expected_size = None
            try:
                expected_size = ftp.size(DEFAULT_INVENTORY_ZIP_FILENAME)
            except (ftplib.error_perm, AttributeError):
                pass

            with open(zip_path, "wb") as zip_file:
                ftp.retrbinary("RETR {}".format(DEFAULT_INVENTORY_ZIP_FILENAME), zip_file.write)

            zip_size = os.path.getsize(zip_path)
            if expected_size is not None and zip_size != expected_size:
                if os.path.exists(zip_path):
                    os.remove(zip_path)
                msg = "Download size mismatch: got {} bytes, expected {} bytes.".format(
                    zip_size, expected_size
                )
                logger.error("{} {}".format(_LOG_PREFIX, msg))
                raise exceptions.PremierFTPConnectionError(msg)

            with zipfile.ZipFile(zip_path, "r") as zf:
                names = zf.namelist()
                csv_name = None
                for n in names:
                    if n.lower().endswith(".csv"):
                        csv_name = n
                        break
                if csv_name is None:
                    if os.path.exists(zip_path):
                        os.remove(zip_path)
                    raise exceptions.PremierFileNotFoundError(
                        "CSV not found in zip. Contents: {}".format(names)
                    )
                # Stream CSV directly from ZIP into local file — avoids reading all bytes into memory.
                with zf.open(csv_name) as csv_stream:
                    with open(self.local_file_path, "wb") as csv_file:
                        while True:
                            chunk = csv_stream.read(65536)
                            if not chunk:
                                break
                            csv_file.write(chunk)

            csv_size = os.path.getsize(self.local_file_path)
            if os.path.exists(zip_path):
                os.remove(zip_path)

            logger.info("{} Downloaded and extracted ({} bytes CSV).".format(
                _LOG_PREFIX, csv_size
            ))
            return self.local_file_path

        except ftplib.error_perm as e:
            if "550" in str(e):
                raise exceptions.PremierFileNotFoundError(
                    "File not found on FTP: {}".format(DEFAULT_INVENTORY_ZIP_FILENAME)
                )
            raise exceptions.PremierFTPConnectionError("Failed to download: {}".format(str(e)))
        except (exceptions.PremierFileNotFoundError, exceptions.PremierFTPConnectionError):
            raise
        except ftplib.all_errors as e:
            msg = "Failed to download inventory file: {}".format(str(e))
            logger.exception("{} {}.".format(_LOG_PREFIX, msg))
            raise exceptions.PremierFTPConnectionError(msg)
        finally:
            self._disconnect(ftp)
            if os.path.exists(zip_path):
                try:
                    os.remove(zip_path)
                except OSError:
                    pass

    def _detect_csv_encoding(self, path: str) -> str:
        """Detect encoding from an 8 MiB sample of the CSV. Returns the first successful encoding."""
        try:
            with open(path, "rb") as bf:
                sample = bf.read(8 * 1024 * 1024)
        except OSError as e:
            raise exceptions.PremierDataValidationError(
                "Could not open Premier CSV: {}".format(e)
            )
        last_error: typing.Optional[Exception] = None
        for encoding in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
            try:
                sample.decode(encoding)
                return encoding
            except UnicodeDecodeError as e:
                last_error = e
        raise exceptions.PremierDataValidationError(
            "Unable to decode CSV with utf-8-sig, utf-8, cp1252, or latin-1: {}".format(last_error)
        )

    def get_inventory_records(self, force_download: bool = False) -> typing.List[typing.Dict]:
        """
        Download the inventory file and return as list of dicts (one per row).
        Uses csv.DictReader to avoid pandas overhead; eliminates intermediate DataFrame.
        Use force_download=True to bypass cache when expecting updated/full data.

        NOTE: loads the entire CSV into memory. Prefer iter_inventory_records() for
        large files to keep memory bounded.
        """
        self.download_inventory_file(force_download=force_download)
        path = self.local_file_path
        try:
            enc = self._detect_csv_encoding(path)
            records: typing.List[typing.Dict] = []
            with open(path, "r", newline="", encoding=enc, errors="replace") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    records.append(dict(row))
            logger.info("{} Loaded inventory CSV with {} rows.".format(_LOG_PREFIX, len(records)))
            return records
        except exceptions.PremierDataValidationError:
            raise
        except Exception as e:
            msg = "Unable to fetch or process the CSV file. Error: {}".format(str(e))
            logger.exception("{} {}.".format(_LOG_PREFIX, msg))
            raise exceptions.PremierDataValidationError(msg)

    def iter_inventory_records(self, force_download: bool = False) -> typing.Iterator[typing.Dict]:
        """
        Stream the inventory CSV row-by-row as dicts — no full-file accumulation.

        Use this instead of get_inventory_records() for large catalogs. The caller
        receives one dict per row and can process + discard it immediately, keeping
        memory usage O(batch_size) rather than O(file_size).
        """
        self.download_inventory_file(force_download=force_download)
        path = self.local_file_path
        try:
            enc = self._detect_csv_encoding(path)
            with open(path, "r", newline="", encoding=enc, errors="replace") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    yield dict(row)
        except exceptions.PremierDataValidationError:
            raise
        except Exception as e:
            msg = "Unable to stream Premier CSV. Error: {}".format(str(e))
            logger.exception("{} {}.".format(_LOG_PREFIX, msg))
            raise exceptions.PremierDataValidationError(msg)
