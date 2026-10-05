"""
FTP transport for EDI mailboxes.

Alluvia hosts Rough Country's mailbox on plain FTP (``ftp://alluviaftp.alluviaplatform.com``),
so that is the default. ``use_tls`` switches to FTPS with explicit AUTH TLS and an encrypted
data channel, which is what we should be using — the payloads carry end-customer names and
street addresses, and plain FTP puts both, plus our password, on the wire in clear text. It is a
setting rather than a hardcoded choice so it can be flipped the moment Alluvia confirms FTPS is
available, without a code change (see ROUGH_COUNTRY_EDI_FTP_USE_TLS).

Two behaviors here exist specifically because FTP has no transactional semantics:

* **Uploads are written to a temporary name and renamed into place.** A partner polling the
  directory must never be able to pick up a half-written 850. The rename is atomic on the
  server side.
* **Downloads are never deleted, only moved.** Files are moved to the partner's Archive
  directory *after* the database work for that document has committed, so a crash mid-processing
  leaves the file where the next run will find it again. That makes delivery at-least-once,
  which is why every inbound document is deduplicated on its interchange control number before
  it is applied (see src.integrations.services.rough_country_edi).
"""
import ftplib
import io
import logging
import posixpath
import typing
from urllib.parse import urlparse

from src.integrations.edi import exceptions

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[EDI-FTP]"

_DEFAULT_TIMEOUT_SECONDS = 60
# EDI documents are kilobytes, not megabytes; a cap keeps a malformed/huge file from being read
# into memory unbounded.
_MAX_DOCUMENT_BYTES = 8 * 1024 * 1024


def normalize_host(value: typing.Any) -> str:
    """``ftp://alluviaftp.alluviaplatform.com`` -> ``alluviaftp.alluviaplatform.com``."""
    text = str(value or "").strip()
    if not text:
        return ""
    if "://" in text:
        return (urlparse(text).hostname or "").strip()
    return text.rstrip("/")


class EdiFtpTransport:
    """
    One connection to an EDI mailbox. Use as a context manager:

        with EdiFtpTransport(host, user, password) as ftp:
            for name in ftp.list_files("/Outbound/855"):
                ...
    """

    def __init__(
        self,
        host: str,
        user: str,
        password: str,
        port: int = 21,
        use_tls: bool = False,
        timeout: int = _DEFAULT_TIMEOUT_SECONDS,
        passive: bool = True,
    ) -> None:
        self.host = normalize_host(host)
        self.user = (user or "").strip()
        self.password = password or ""
        self.port = int(port or 21)
        self.use_tls = bool(use_tls)
        self.timeout = int(timeout or _DEFAULT_TIMEOUT_SECONDS)
        self.passive = bool(passive)
        self._ftp: typing.Optional[ftplib.FTP] = None

        missing = [
            name
            for name, value in (("host", self.host), ("user", self.user), ("password", self.password))
            if not value
        ]
        if missing:
            raise ValueError("EDI FTP configuration is incomplete — missing: {}.".format(", ".join(missing)))

    # -- connection ---------------------------------------------------------------------------

    def __enter__(self) -> "EdiFtpTransport":
        self.connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()

    def connect(self) -> None:
        try:
            if self.use_tls:
                ftp: ftplib.FTP = ftplib.FTP_TLS(timeout=self.timeout)
                ftp.connect(self.host, self.port, timeout=self.timeout)
                ftp.login(self.user, self.password)
                # Without prot_p the control channel is encrypted but every file still crosses
                # the wire in clear text.
                ftp.prot_p()
            else:
                ftp = ftplib.FTP(timeout=self.timeout)
                ftp.connect(self.host, self.port, timeout=self.timeout)
                ftp.login(self.user, self.password)
            ftp.set_pasv(self.passive)
        except ftplib.all_errors as e:
            raise exceptions.EdiTransportError(
                "Could not connect to {}:{} as {} ({}): {}.".format(
                    self.host, self.port, self.user, "FTPS" if self.use_tls else "FTP", e
                )
            )
        self._ftp = ftp
        logger.info(
            "%s Connected to %s:%s as %s (%s).",
            _LOG_PREFIX,
            self.host,
            self.port,
            self.user,
            "FTPS" if self.use_tls else "FTP",
        )

    def close(self) -> None:
        if self._ftp is None:
            return
        try:
            self._ftp.quit()
        except Exception:
            try:
                self._ftp.close()
            except Exception:
                pass
        self._ftp = None

    @property
    def ftp(self) -> ftplib.FTP:
        if self._ftp is None:
            raise exceptions.EdiTransportError("Not connected — call connect() first.")
        return self._ftp

    # -- operations ---------------------------------------------------------------------------

    def list_files(self, directory: str) -> typing.List[str]:
        """
        Filenames (not paths) in ``directory``, excluding anything that looks like a directory
        entry or a partner's own in-progress temp file. Returns [] for a directory that doesn't
        exist yet rather than raising — an empty Outbound folder is the normal state.
        """
        try:
            names = self.ftp.nlst(directory)
        except ftplib.error_perm as e:
            message = str(e)
            if message.startswith("550"):
                logger.info("%s Directory %s is empty or absent (%s).", _LOG_PREFIX, directory, message.strip())
                return []
            raise exceptions.EdiTransportError("Could not list {}: {}.".format(directory, e))
        except ftplib.all_errors as e:
            raise exceptions.EdiTransportError("Could not list {}: {}.".format(directory, e))

        files = []
        for name in names:
            base = posixpath.basename(name.strip())
            if not base or base in (".", ".."):
                continue
            if base.lower().endswith(".tmp") or base.startswith("."):
                continue
            files.append(base)
        return sorted(files)

    def read_text(self, path: str) -> str:
        buffer = io.BytesIO()

        def _write(chunk: bytes) -> None:
            if buffer.tell() + len(chunk) > _MAX_DOCUMENT_BYTES:
                raise exceptions.EdiTransportError(
                    "{} exceeds the {} byte cap for an EDI document.".format(path, _MAX_DOCUMENT_BYTES)
                )
            buffer.write(chunk)

        try:
            self.ftp.retrbinary("RETR {}".format(path), _write)
        except exceptions.EdiTransportError:
            raise
        except ftplib.all_errors as e:
            raise exceptions.EdiTransportError("Could not download {}: {}.".format(path, e))
        # X12 is 7-bit ASCII by definition; "replace" keeps one stray high byte from losing us
        # the whole document, which we would then never see or be able to diagnose.
        return buffer.getvalue().decode("ascii", errors="replace")

    def write_text(self, path: str, content: str) -> None:
        """Upload to ``<path>.tmp`` and rename onto ``path``, so the partner never reads a
        partially written document."""
        temp_path = "{}.tmp".format(path)
        payload = io.BytesIO(content.encode("ascii", errors="replace"))
        try:
            self.ftp.storbinary("STOR {}".format(temp_path), payload)
        except ftplib.all_errors as e:
            raise exceptions.EdiTransportError("Could not upload {}: {}.".format(temp_path, e))
        try:
            self.ftp.rename(temp_path, path)
        except ftplib.all_errors as e:
            raise exceptions.EdiTransportError(
                "Uploaded {} but could not rename it to {}: {}. The partner will not pick up a "
                ".tmp file, so this document has NOT been delivered.".format(temp_path, path, e)
            )
        logger.info("%s Uploaded %s (%s bytes).", _LOG_PREFIX, path, len(content))

    def move(self, source_path: str, destination_path: str) -> None:
        try:
            self.ftp.rename(source_path, destination_path)
        except ftplib.all_errors as e:
            raise exceptions.EdiTransportError(
                "Could not move {} to {}: {}.".format(source_path, destination_path, e)
            )
        logger.info("%s Moved %s -> %s.", _LOG_PREFIX, source_path, destination_path)
