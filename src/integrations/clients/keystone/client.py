import ftplib
import logging
import os
import ssl
import time
import typing
import zipfile

import pandas as pd

from django.conf import settings

from src.integrations.clients.keystone import exceptions

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[KEYSTONE-FTP-CLIENT]"

DEFAULT_FTP_HOST = "ftp.ekeystone.com"
DEFAULT_FTP_PORT = 990
DEFAULT_INVENTORY_ZIP_FILENAME = "Inventory.zip"
DEFAULT_INVENTORY_CSV_FILENAME = "Inventory.csv"
DEFAULT_FILE_MAX_AGE_SECONDS = 6 * 60 * 60  # 6 hours


class ImplicitFTP_TLS(ftplib.FTP_TLS):
    """FTP_TLS subclass that automatically wraps sockets in SSL to support implicit FTPS."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._sock = None

    @property
    def sock(self):
        """Return the socket."""
        return self._sock

    @sock.setter
    def sock(self, value):
        """When modifying the socket, ensure that it is ssl wrapped."""
        if value is not None and not isinstance(value, ssl.SSLSocket):
            value = self.context.wrap_socket(
                value,
                server_hostname=DEFAULT_FTP_HOST,
                session=self._sock.session if self._sock else None,
            )
        self._sock = value


class KeystoneFTPClient:
    """
    FTP client for Keystone inventory data.
    Connects via implicit FTPS (port 990).
    Credentials can be provided via dict (ftp_user, ftp_password) or from settings.
    """

    def __init__(
        self,
        credentials: typing.Optional[typing.Dict] = None,
        local_file_path: typing.Optional[str] = None,
        file_max_age: int = DEFAULT_FILE_MAX_AGE_SECONDS,
    ):
        creds = credentials or {}
        self.ftp_host = creds.get("ftp_host") or getattr(settings, "KEYSTONE_FTP_HOST", DEFAULT_FTP_HOST)
        self.ftp_port = creds.get("ftp_port") or getattr(settings, "KEYSTONE_FTP_PORT", DEFAULT_FTP_PORT)
        self.ftp_user = creds.get("ftp_user") or getattr(settings, "KEYSTONE_FTP_USER", "")
        self.ftp_pass = creds.get("ftp_password") or creds.get("ftp_pass") or getattr(
            settings, "KEYSTONE_FTP_PASSWORD", ""
        )

        if not self.ftp_user or not self.ftp_pass:
            raise ValueError("Invalid credentials. Missing ftp_user or ftp_password.")

        self.local_file_path = local_file_path or getattr(
            settings, "KEYSTONE_INVENTORY_LOCAL_PATH", "/tmp/keystone_inventory.csv"
        )
        self.file_max_age = file_max_age
        self._ftp_client: typing.Optional[ImplicitFTP_TLS] = None

    def _connect(self) -> ImplicitFTP_TLS:
        """Establish FTP connection."""
        try:
            ftp = ImplicitFTP_TLS()
            ftp.connect(host=self.ftp_host, port=self.ftp_port)
            ftp.login(user=self.ftp_user, passwd=self.ftp_pass)
            ftp.set_pasv(True)
            logger.debug(
                "{} Successfully connected to FTP server {}:{}".format(
                    _LOG_PREFIX, self.ftp_host, self.ftp_port
                )
            )
            return ftp
        except ftplib.all_errors as e:
            msg = "Failed to connect to FTP server. Error: {}".format(str(e))
            logger.error("{} {}.".format(_LOG_PREFIX, msg))
            raise exceptions.KeystoneFTPConnectionError(msg)

    def _disconnect(self, ftp: typing.Optional[ImplicitFTP_TLS]) -> None:
        """Close FTP connection."""
        try:
            if ftp:
                ftp.quit()
                logger.debug("{} Disconnected from FTP server.".format(_LOG_PREFIX))
        except Exception as e:
            logger.warning("{} Error during disconnect: {}.".format(_LOG_PREFIX, str(e)))

    def is_file_outdated(self) -> bool:
        """Check if the local inventory file is older than the allowed maximum age."""
        if not os.path.exists(self.local_file_path):
            return True
        file_mod_time = os.path.getmtime(self.local_file_path)
        current_time = time.time()
        return (current_time - file_mod_time) > self.file_max_age

    def download_inventory_file(self, force_download: bool = False) -> str:
        """
        Connect to the FTP server and download Inventory.zip, then extract Inventory.csv.
        Returns the path to the local CSV file.
        Always overwrites the local file when downloading and unpacking.
        Use force_download=True to bypass cache and ensure fresh data.
        """
        if not force_download and not self.is_file_outdated():
            logger.info("{} Using existing inventory file, not older than {} hours.".format(
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
                msg = (
                    "Download size mismatch: got {} bytes, expected {} bytes. "
                    "ZIP file may be incomplete.".format(zip_size, expected_size)
                )
                logger.error("{} {}".format(_LOG_PREFIX, msg))
                raise exceptions.KeystoneFTPConnectionError(msg)

            with zipfile.ZipFile(zip_path, "r") as zf:
                names = zf.namelist()
                csv_name = None
                try:
                    zf.getinfo(DEFAULT_INVENTORY_CSV_FILENAME)
                    csv_name = DEFAULT_INVENTORY_CSV_FILENAME
                except KeyError:
                    for n in names:
                        if n.lower().endswith("inventory.csv"):
                            csv_name = n
                            break
                if csv_name is None:
                    if os.path.exists(zip_path):
                        os.remove(zip_path)
                    raise exceptions.KeystoneFileNotFoundError(
                        "Inventory CSV not found in zip. Contents: {}".format(names)
                    )
                csv_data = zf.read(csv_name)

            with open(self.local_file_path, "wb") as csv_file:
                csv_file.write(csv_data)

            if os.path.exists(zip_path):
                os.remove(zip_path)

            logger.info(
                "{} Inventory downloaded from zip and extracted ({} bytes CSV).".format(
                    _LOG_PREFIX, len(csv_data)
                )
            )
            return self.local_file_path
        except ftplib.error_perm as e:
            if "550" in str(e):
                msg = "File not found on FTP server: {}".format(DEFAULT_INVENTORY_ZIP_FILENAME)
                raise exceptions.KeystoneFileNotFoundError(msg)
            raise exceptions.KeystoneFTPConnectionError("Failed to download: {}".format(str(e)))
        except exceptions.KeystoneFileNotFoundError:
            raise
        except ftplib.all_errors as e:
            msg = "Failed to download inventory file: {}".format(str(e))
            logger.exception("{} {}.".format(_LOG_PREFIX, msg))
            raise exceptions.KeystoneFTPConnectionError(msg)
        finally:
            self._disconnect(ftp)
            if os.path.exists(zip_path):
                try:
                    os.remove(zip_path)
                except OSError:
                    pass

    def get_inventory_dataframe(self, force_download: bool = False) -> pd.DataFrame:
        """
        Download the inventory file (if needed) and return as pandas DataFrame.
        Use force_download=True to bypass cache when expecting updated/full data.
        """
        self.download_inventory_file(force_download=force_download)
        try:
            # Try UTF-8 first, fall back to cp1252 (common for Excel exports)
            df = None
            for encoding in ("utf-8", "utf-8-sig", "cp1252", "latin-1"):
                try:
                    df = pd.read_csv(
                        self.local_file_path,
                        encoding=encoding,
                        on_bad_lines="warn",  # Log malformed rows instead of failing silently
                    )
                    break
                except UnicodeDecodeError:
                    continue
            if df is None:
                raise exceptions.KeystoneDataValidationError(
                    "Unable to decode CSV with utf-8, utf-8-sig, cp1252, or latin-1."
                )

            logger.info("{} Loaded inventory CSV with {} rows.".format(_LOG_PREFIX, len(df)))
            return df
        except exceptions.KeystoneDataValidationError:
            raise
        except Exception as e:
            msg = "Unable to fetch or process the CSV file. Error: {}".format(str(e))
            logger.exception("{} {}.".format(_LOG_PREFIX, msg))
            raise exceptions.KeystoneDataValidationError(msg)

    def get_inventory_records(self, force_download: bool = False) -> typing.List[typing.Dict]:
        """
        Download the inventory file and return as list of dicts (one per row).
        Use force_download=True to bypass cache when expecting updated/full data.
        """
        df = self.get_inventory_dataframe(force_download=force_download)
        return df.to_dict("records")
