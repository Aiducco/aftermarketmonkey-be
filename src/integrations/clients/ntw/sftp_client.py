"""
SFTP client for NTW's feed. NTW is TBC Corporation's wholesale arm and the feed lives on TBC's
MOVEit Transfer server -- one per-dealer account, landing a headerless CSV in that dealer's own
``Outbound`` folder four times a day (07:xx, 11:xx, 16:xx, 23:xx, confirmed across 30 days of
files).

We pull; nothing is pushed to our relay. That is the fallback arrangement, not the preferred one
-- TBC runs MOVEit Automation and could deliver to our relay like Meyer, A-Tech, Crown, DLG and
Tri-State do, which would mean holding no TBC credentials at all. Until they agree to that, this
client exists. See docs/NTW_INTEGRATION_PLAN.md.

Three things differ from the sibling SFTP clients and are deliberate:

* **Host keys are pinned.** TireRack's client (the closest model for this one) calls
  ``Transport.connect`` with no host-key verification at all, which means a DNS or routing
  compromise silently hands over a dealer's credentials. MOVEit's keys are stable and published
  below; an unexpected key raises rather than proceeding.
* **Host and port are not credential fields.** MOVEit is one tenant for every dealer, so they are
  constants with a per-connection override kept only for a migration or a test double.
* **MOVEit lands the account in ``/<Dealer Name>``,** not in the folder we want, and the dealer
  name differs per account -- so the Outbound folder is resolved relative to the SFTP home rather
  than from an absolute path we would have to store per connection.

MOVEit exposes no content hash over SFTP or its REST API, so the caller watermarks on the
filename: ``DELTI_<10-digit account>_<YYYYMMDDHHMMSS>.csv``, whose stamp is unique and monotonic.
"""
import io
import logging
import socket
import typing

import paramiko
from django.conf import settings

from src import constants
from src.integrations.clients.ntw import exceptions

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[NTW-SFTP-CLIENT]"

# paramiko.Transport((host, port)) opens the socket with no timeout, so a firewalled or wrong
# host would hang the calling thread forever -- same reason Meyer's and Tri-State's clients set
# this: feed_present() runs from the check_company_provider_connections cron.
DEFAULT_CONNECT_TIMEOUT_SECONDS = 15

# Captured live 2026-10-27 via ssh-keyscan. MOVEit Transfer serves both; either is accepted and
# anything else is refused. If TBC ever rekeys the server these must be updated in the same
# change that explains why -- a silent widening here is the one edit this module should not take.
TRUSTED_HOST_KEY_FINGERPRINTS = {
    "ssh-rsa": "SHA256:40J0JewCgGCupJ85pxc/dOsu0i2EBybJu6/jQRcWg7I",
    "ssh-ed25519": "SHA256:bgfnFVvKsZhMyANNZ0qWG19SX4QRzbKjYL1p0X1QYOM",
}


def _fingerprint(key: "paramiko.PKey") -> str:
    """OpenSSH-style ``SHA256:<base64>`` fingerprint, matching ssh-keyscan/ssh-keygen output."""
    import base64
    import hashlib

    digest = hashlib.sha256(key.asbytes()).digest()
    return "SHA256:" + base64.b64encode(digest).decode("ascii").rstrip("=")


class NtwSFTPClient:
    """
    Reads one dealer's NTW feed. ``credentials`` is the connection's ``credentials["feed"]``
    (see src.integrations.credentials.get_feed_credentials); only the login is required.
    """

    def __init__(self, credentials: typing.Dict) -> None:
        credentials = credentials or {}
        self.sftp_host = (
            credentials.get(constants.NTW_CREDENTIALS_SFTP_HOST)
            or getattr(settings, "NTW_SFTP_HOST", None)
            or constants.NTW_SFTP_DEFAULT_HOST
        )
        self.sftp_port = int(
            credentials.get(constants.NTW_CREDENTIALS_SFTP_PORT)
            or getattr(settings, "NTW_SFTP_PORT", None)
            or constants.NTW_SFTP_DEFAULT_PORT
        )
        self.sftp_user = credentials.get(constants.NTW_CREDENTIALS_SFTP_USER, "")
        self.sftp_password = credentials.get(constants.NTW_CREDENTIALS_SFTP_PASSWORD, "")

        if not all([self.sftp_host, self.sftp_user, self.sftp_password]):
            raise ValueError("NTW feed credentials are incomplete (need sftp_user and sftp_password).")

        self._transport = None
        self._sftp = None

    # -- connection ----------------------------------------------------------

    def _connect(self) -> None:
        if self._sftp is not None:
            return
        try:
            sock = socket.create_connection(
                (self.sftp_host, self.sftp_port), timeout=DEFAULT_CONNECT_TIMEOUT_SECONDS
            )
            self._transport = paramiko.Transport(sock)
            self._transport.start_client(timeout=DEFAULT_CONNECT_TIMEOUT_SECONDS)

            host_key = self._transport.get_remote_server_key()
            expected = TRUSTED_HOST_KEY_FINGERPRINTS.get(host_key.get_name())
            actual = _fingerprint(host_key)
            if expected is None or actual != expected:
                raise exceptions.NtwSFTPConnectionError(
                    "Unexpected SSH host key for {}: {} {} (expected {}). Refusing to send "
                    "credentials.".format(self.sftp_host, host_key.get_name(), actual, expected)
                )

            self._transport.auth_password(username=self.sftp_user, password=self.sftp_password)
            if not self._transport.is_authenticated():
                raise exceptions.NtwSFTPConnectionError(
                    "Authentication failed for {}@{}.".format(self.sftp_user, self.sftp_host)
                )
            self._sftp = paramiko.SFTPClient.from_transport(self._transport)
            logger.debug("{} Connected to {}:{}.".format(_LOG_PREFIX, self.sftp_host, self.sftp_port))
        except exceptions.NtwException:
            self._disconnect()
            raise
        except Exception as e:
            self._disconnect()
            msg = "Failed to connect to {}:{}. Error: {}".format(self.sftp_host, self.sftp_port, str(e))
            logger.error("{} {}".format(_LOG_PREFIX, msg))
            raise exceptions.NtwSFTPConnectionError(msg)

    def _disconnect(self) -> None:
        try:
            if self._sftp:
                self._sftp.close()
        except Exception:
            pass
        self._sftp = None
        try:
            if self._transport:
                self._transport.close()
        except Exception:
            pass
        self._transport = None

    # -- feed ----------------------------------------------------------------

    def _outbound_directory(self) -> str:
        """
        MOVEit drops the account in ``/<Dealer Name>``; the feed sits one level down in
        ``Outbound``. Resolved from the session's own cwd so the dealer's name never has to be
        stored, and falls back to the bare folder name when the server reports no cwd.
        """
        try:
            home = self._sftp.normalize(".")
        except Exception:
            home = ""
        directory = constants.NTW_SFTP_OUTBOUND_DIRECTORY
        return "{}/{}".format(home.rstrip("/"), directory) if home else directory

    def test_connection(self) -> None:
        """
        Connect and confirm the dealer's Outbound folder lists -- credentials plus the one thing
        that actually goes wrong on a new NTW account. Listing the home directory alone would
        pass for an account whose feed folder has not been created or permissioned yet, which is
        the state a dealer is in before TBC finishes provisioning. No download.
        """
        try:
            self._connect()
            self._sftp.listdir(self._outbound_directory())
        finally:
            self._disconnect()

    def list_feed_files(self) -> typing.List[typing.Tuple[str, int, float]]:
        """
        Every ``DELTI_*.csv`` in the dealer's Outbound folder as (filename, size, mtime), newest
        last. MOVEit keeps a 30-day rolling window (119 files at four a day, measured), which is
        what makes a catalog backfill from history possible -- a single file only lists what is in
        stock right now.
        """
        try:
            self._connect()
            directory = self._outbound_directory()
            entries = self._sftp.listdir_attr(directory)
        except exceptions.NtwException:
            raise
        except Exception as e:
            msg = "Failed to list {}. Error: {}".format(constants.NTW_SFTP_OUTBOUND_DIRECTORY, str(e))
            logger.error("{} {}".format(_LOG_PREFIX, msg))
            raise exceptions.NtwException(msg)
        finally:
            pass

        files = [
            (e.filename, e.st_size, e.st_mtime)
            for e in entries
            if e.filename.startswith(constants.NTW_FEED_FILENAME_PREFIX)
            and e.filename.endswith(constants.NTW_FEED_FILENAME_SUFFIX)
        ]
        # Sort by the stamp in the name, not mtime: MOVEit's upload time can reorder files
        # relative to the moment TBC generated them (measured: a 23:00 file uploaded at 08:20).
        files.sort(key=lambda f: f[0])
        return files

    def download_latest_feed(self) -> typing.Tuple[str, bytes]:
        """Download the newest feed file. Returns (filename, contents)."""
        try:
            self._connect()
            files = self.list_feed_files()
            if not files:
                msg = "No {}*.csv files in {}.".format(
                    constants.NTW_FEED_FILENAME_PREFIX, constants.NTW_SFTP_OUTBOUND_DIRECTORY
                )
                logger.error("{} {}".format(_LOG_PREFIX, msg))
                raise exceptions.NtwFileNotFoundError(msg)
            filename = files[-1][0]
            return filename, self._download(filename)
        except exceptions.NtwException:
            raise
        except Exception as e:
            msg = "Failed to download latest feed file. Error: {}".format(str(e))
            logger.error("{} {}".format(_LOG_PREFIX, msg))
            raise exceptions.NtwException(msg)
        finally:
            self._disconnect()

    def download_feed_file(self, filename: str) -> bytes:
        """Download one named feed file (used to backfill the catalog from the 30-day window)."""
        try:
            self._connect()
            return self._download(filename)
        except exceptions.NtwException:
            raise
        except Exception as e:
            msg = "Failed to download {}. Error: {}".format(filename, str(e))
            logger.error("{} {}".format(_LOG_PREFIX, msg))
            raise exceptions.NtwException(msg)
        finally:
            self._disconnect()

    def _download(self, filename: str) -> bytes:
        remote_path = "{}/{}".format(self._outbound_directory(), filename)
        file_obj = io.BytesIO()
        self._sftp.getfo(remote_path, file_obj)
        file_obj.seek(0)
        content = file_obj.read()
        logger.info("{} Downloaded {} ({} bytes).".format(_LOG_PREFIX, filename, len(content)))
        return content

    def feed_present(self) -> bool:
        """Cheap liveness check for check_company_provider_connections: can we see a feed file?"""
        try:
            return bool(self.list_feed_files())
        except exceptions.NtwException:
            return False
        finally:
            self._disconnect()
