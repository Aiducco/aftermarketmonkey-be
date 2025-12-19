import logging
import typing
import paramiko
import io
from datetime import datetime
import re

from src.integrations.clients.sdc import exceptions

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[SDC-SFTP-CLIENT]"


class SDCSFTPClient(object):
    def __init__(self, credentials: typing.Dict):
        self.sftp_server = credentials.get("sftp_server", "")
        self.sftp_port = credentials.get("sftp_port", 22)
        self.sftp_user = credentials.get("sftp_user", "")
        self.sftp_password = credentials.get("sftp_password", "")
        self.sftp_path = credentials.get("sftp_path", "")

        if not all([self.sftp_server, self.sftp_user, self.sftp_password, self.sftp_path]):
            raise ValueError("Invalid credentials parameter. Missing required SFTP credentials.")

        self._transport = None
        self._sftp = None

    def _connect(self) -> None:
        """Establish SFTP connection."""
        try:
            self._transport = paramiko.Transport((self.sftp_server, self.sftp_port))
            self._transport.connect(username=self.sftp_user, password=self.sftp_password)
            self._sftp = paramiko.SFTPClient.from_transport(self._transport)
            logger.debug(f"{_LOG_PREFIX} Successfully connected to SFTP server {self.sftp_server}:{self.sftp_port}")
        except Exception as e:
            msg = f"Failed to connect to SFTP server. Error: {str(e)}"
            logger.error(f"{_LOG_PREFIX} {msg}")
            raise exceptions.SDCSFTPConnectionError(msg)

    def _disconnect(self) -> None:
        """Close SFTP connection."""
        try:
            if self._sftp:
                self._sftp.close()
                self._sftp = None
            if self._transport:
                self._transport.close()
                self._transport = None
            logger.debug(f"{_LOG_PREFIX} Disconnected from SFTP server")
        except Exception as e:
            logger.warning(f"{_LOG_PREFIX} Error during disconnect: {str(e)}")
            # Reset to None even if close fails
            self._sftp = None
            self._transport = None

    def list_files(self, file_pattern: typing.Optional[str] = None) -> typing.List[str]:
        """
        List files in the SFTP path.
        
        Args:
            file_pattern: Optional regex pattern to filter files
            
        Returns:
            List of file names
        """
        try:
            self._connect()
            files = self._sftp.listdir(self.sftp_path)
            
            if file_pattern:
                pattern = re.compile(file_pattern)
                files = [f for f in files if pattern.match(f)]
            
            logger.debug(f"{_LOG_PREFIX} Found {len(files)} files in {self.sftp_path}")
            return files
        except Exception as e:
            msg = f"Failed to list files. Error: {str(e)}"
            logger.error(f"{_LOG_PREFIX} {msg}")
            raise exceptions.SDCException(msg)
        finally:
            self._disconnect()

    def get_file(self, filename: str) -> bytes:
        """
        Download a file from SFTP server.
        
        Args:
            filename: Name of the file to download
            
        Returns:
            File contents as bytes
        """
        try:
            self._connect()
            file_path = f"{self.sftp_path}/{filename}" if not self.sftp_path.endswith('/') else f"{self.sftp_path}{filename}"
            
            file_obj = io.BytesIO()
            self._sftp.getfo(file_path, file_obj)
            file_obj.seek(0)
            content = file_obj.read()
            
            logger.debug(f"{_LOG_PREFIX} Successfully downloaded file: {filename} ({len(content)} bytes)")
            return content
        except FileNotFoundError:
            msg = f"File not found: {filename}"
            logger.error(f"{_LOG_PREFIX} {msg}")
            raise exceptions.SDCFileNotFoundError(msg)
        except Exception as e:
            msg = f"Failed to download file {filename}. Error: {str(e)}"
            logger.error(f"{_LOG_PREFIX} {msg}")
            raise exceptions.SDCException(msg)
        finally:
            self._disconnect()

    def get_latest_product_file(self, brand_id: str) -> typing.Optional[typing.Tuple[str, bytes]]:
        """
        Get the latest product file for a brand.
        
        Args:
            brand_id: Brand ID (e.g., 'BBVR')
            
        Returns:
            Tuple of (filename, file_content) or None if not found
        """
        pattern = f"SDC_{brand_id}_BigCommerce_\\d{{8}}_\\d{{8}}\\.txt"
        files = self.list_files(pattern)
        
        if not files:
            logger.warning(f"{_LOG_PREFIX} No product files found for brand: {brand_id}")
            return None
        
        # Sort files by date (extract date from filename)
        def extract_date(filename: str) -> datetime:
            # Format: SDC_BBVR_BigCommerce_20251219_21331230.txt
            match = re.search(r'(\d{8})_\d{8}', filename)
            if match:
                date_str = match.group(1)
                return datetime.strptime(date_str, '%Y%m%d')
            return datetime.min
        
        sorted_files = sorted(files, key=extract_date, reverse=True)
        latest_file = sorted_files[0]
        
        logger.info(f"{_LOG_PREFIX} Found latest product file for brand {brand_id}: {latest_file}")
        file_content = self.get_file(latest_file)
        
        return (latest_file, file_content)

    def get_latest_fitment_file(self, brand_id: str) -> typing.Optional[typing.Tuple[str, bytes]]:
        """
        Get the latest fitment file for a brand.
        
        Args:
            brand_id: Brand ID (e.g., 'BBVR')
            
        Returns:
            Tuple of (filename, file_content) or None if not found
        """
        pattern = f"SDC_{brand_id}_BigCommerceFitment_\\d{{8}}_\\d{{8}}\\.txt"
        files = self.list_files(pattern)
        
        if not files:
            logger.warning(f"{_LOG_PREFIX} No fitment files found for brand: {brand_id}")
            return None
        
        # Sort files by date (extract date from filename)
        def extract_date(filename: str) -> datetime:
            # Format: SDC_BBVR_BigCommerceFitment_20251219_21331231.txt
            match = re.search(r'(\d{8})_\d{8}', filename)
            if match:
                date_str = match.group(1)
                return datetime.strptime(date_str, '%Y%m%d')
            return datetime.min
        
        sorted_files = sorted(files, key=extract_date, reverse=True)
        latest_file = sorted_files[0]
        
        logger.info(f"{_LOG_PREFIX} Found latest fitment file for brand {brand_id}: {latest_file}")
        file_content = self.get_file(latest_file)
        
        return (latest_file, file_content)

