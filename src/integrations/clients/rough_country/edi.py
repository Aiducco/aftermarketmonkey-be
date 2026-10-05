"""
Rough Country EDI mailbox (Alluvia-hosted FTP).

Scope today is deliberately narrow: collect the documents Rough Country has published for us
and move each one into its Archive folder, which is what Alluvia asked us to do. Generating
850s, acknowledging with 997s and applying documents to PurchaseOrder rows are separate,
later pieces — see docs/ROUGH_COUNTRY_EDI_PLAN.md.

Folder layout, as given by Alluvia. Note the names are from *their* point of view, so their
"Inbound" is our outbound:

    /Inbound/850    documents we send them
    /Inbound/997    functional acknowledgments we send them
    /Outbound/855   PO acknowledgments        -> /Archive/855
    /Outbound/856   advance ship notices      -> /Archive/856
    /Outbound/810   invoices                  -> /Archive/810

Ordering matters here. A file is archived only after it has been written to local disk AND
confirmed to be a complete interchange, because the archive move is the only thing that stops
us collecting it again — and moving a truncated file out of Outbound would lose it for good.
"""
import datetime
import logging
import os
import posixpath
import typing

from django.conf import settings

from src.integrations.edi import exceptions as edi_exceptions
from src.integrations.edi import transport as edi_transport
from src.integrations.edi import x12

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[ROUGH-COUNTRY-EDI]"

# Document types Rough Country publishes to us.
INBOUND_DOCUMENT_TYPES = ("855", "856", "810")

OUTBOUND_DIRECTORY_TEMPLATE = "/Outbound/{}"
ARCHIVE_DIRECTORY_TEMPLATE = "/Archive/{}"


class RoughCountryEdiClient:
    """Thin wrapper over EdiFtpTransport that knows Rough Country's folder layout."""

    def __init__(
        self,
        host: typing.Optional[str] = None,
        port: typing.Optional[int] = None,
        user: typing.Optional[str] = None,
        password: typing.Optional[str] = None,
        use_tls: typing.Optional[bool] = None,
        local_dir: typing.Optional[str] = None,
    ) -> None:
        self.host = (host or getattr(settings, "ROUGH_COUNTRY_EDI_FTP_HOST", "") or "").strip()
        self.port = int(port or getattr(settings, "ROUGH_COUNTRY_EDI_FTP_PORT", 21) or 21)
        self.user = (user or getattr(settings, "ROUGH_COUNTRY_EDI_FTP_USER", "") or "").strip()
        self.password = password or getattr(settings, "ROUGH_COUNTRY_EDI_FTP_PASSWORD", "") or ""
        self.use_tls = (
            bool(use_tls)
            if use_tls is not None
            else bool(getattr(settings, "ROUGH_COUNTRY_EDI_FTP_USE_TLS", False))
        )
        self.local_dir = (
            local_dir
            or getattr(settings, "ROUGH_COUNTRY_EDI_LOCAL_DIR", "")
            or "/tmp/rough_country_edi"
        )

        missing = [
            name
            for name, value in (
                ("ROUGH_COUNTRY_EDI_FTP_HOST", self.host),
                ("ROUGH_COUNTRY_EDI_FTP_USER", self.user),
                ("ROUGH_COUNTRY_EDI_FTP_PASSWORD", self.password),
            )
            if not value
        ]
        if missing:
            raise ValueError(
                "Rough Country EDI FTP is not configured — missing: {}.".format(", ".join(missing))
            )

    def _transport(self) -> edi_transport.EdiFtpTransport:
        return edi_transport.EdiFtpTransport(
            host=self.host,
            user=self.user,
            password=self.password,
            port=self.port,
            use_tls=self.use_tls,
        )

    def _local_path(self, document_type: str, filename: str) -> str:
        directory = os.path.join(self.local_dir, document_type)
        os.makedirs(directory, exist_ok=True)
        return os.path.join(directory, filename)

    def list_available(self) -> typing.Dict[str, typing.List[str]]:
        """{document_type: [filename, ...]} currently waiting in each Outbound folder."""
        available = {}
        with self._transport() as ftp:
            for document_type in INBOUND_DOCUMENT_TYPES:
                directory = OUTBOUND_DIRECTORY_TEMPLATE.format(document_type)
                available[document_type] = ftp.list_files(directory)
        return available

    def collect(
        self,
        document_types: typing.Optional[typing.Sequence[str]] = None,
        archive: bool = True,
        limit: typing.Optional[int] = None,
    ) -> typing.List[typing.Dict]:
        """
        Download every waiting document and (unless ``archive`` is False) move it to its Archive
        folder.

        Returns one result dict per file: ``document_type``, ``filename``, ``local_path``,
        ``archived`` and ``error``. A file that fails to download, or that arrives incomplete, is
        reported with ``error`` set and is deliberately left in Outbound so the next run picks it
        up again — a partial read of a file the partner is still writing is the expected way for
        that to happen, and it fixes itself on the retry.
        """
        wanted = [d for d in (document_types or INBOUND_DOCUMENT_TYPES) if d in INBOUND_DOCUMENT_TYPES]
        results: typing.List[typing.Dict] = []
        collected = 0

        with self._transport() as ftp:
            for document_type in wanted:
                source_dir = OUTBOUND_DIRECTORY_TEMPLATE.format(document_type)
                archive_dir = ARCHIVE_DIRECTORY_TEMPLATE.format(document_type)
                for filename in ftp.list_files(source_dir):
                    if limit is not None and collected >= limit:
                        return results
                    collected += 1
                    entry = {
                        "document_type": document_type,
                        "filename": filename,
                        "local_path": None,
                        "archived": False,
                        "error": None,
                    }
                    remote_path = posixpath.join(source_dir, filename)
                    try:
                        payload = ftp.read_text(remote_path)
                        _assert_complete(payload, remote_path)
                        local_path = self._local_path(document_type, filename)
                        with open(local_path, "w", encoding="ascii", errors="replace") as handle:
                            handle.write(payload)
                        entry["local_path"] = local_path
                    except (edi_exceptions.EdiTransportError, edi_exceptions.EdiParseError, OSError) as e:
                        entry["error"] = str(e)
                        logger.error("%s %s: %s", _LOG_PREFIX, remote_path, e)
                        results.append(entry)
                        continue

                    if archive:
                        try:
                            ftp.move(remote_path, posixpath.join(archive_dir, filename))
                            entry["archived"] = True
                        except edi_exceptions.EdiTransportError as e:
                            # We already have the file locally, so nothing is lost — but it will
                            # be collected again next run, which is why anything downstream of
                            # this must deduplicate rather than assume one delivery per file.
                            entry["error"] = "downloaded, but archiving failed: {}".format(e)
                            logger.error("%s %s: %s", _LOG_PREFIX, remote_path, e)
                    results.append(entry)

        return results


def _assert_complete(payload: str, remote_path: str) -> None:
    """
    Refuse to archive anything that isn't a whole interchange.

    FTP gives no signal about whether the partner had finished writing when we started reading,
    and a truncated X12 file parses perfectly right up to the byte it was cut at. parse_interchange
    checks for the IEA trailer specifically for this case.
    """
    try:
        x12.parse_interchange(payload)
    except edi_exceptions.EdiParseError as e:
        raise edi_exceptions.EdiParseError(
            "{} is not a complete X12 interchange ({}). Leaving it in Outbound to retry.".format(
                remote_path, e
            )
        )


def describe(payload: str) -> typing.Dict:
    """Summarise a downloaded document for logging/CLI output — sender, control number and the
    transaction sets it carries. Best-effort: never raises."""
    try:
        interchange = x12.parse_interchange(payload)
    except edi_exceptions.EdiParseError as e:
        return {"error": str(e)}
    return {
        "sender": interchange.sender_id,
        "receiver": interchange.receiver_id,
        "usage": interchange.usage_indicator,
        "interchange_control_number": interchange.control_number,
        "transaction_sets": [t.transaction_set_code for t in interchange.transactions()],
        "po_references": sorted({ref for t in interchange.transactions() for ref in _po_references(t)}),
    }


def _po_references(transaction: "x12.Transaction") -> typing.List[str]:
    """Our own PO number as each document type carries it — BAK03 on an 855, PRF01 on an 856,
    BIG04 on an 810. Read-only for now; this is what a later phase matches PurchaseOrders on."""
    references = []
    for seg in transaction.body():
        if seg.tag == "BAK" and seg.get(3):
            references.append(seg.get(3).strip())
        elif seg.tag == "PRF" and seg.get(1):
            references.append(seg.get(1).strip())
        elif seg.tag == "BIG" and seg.get(4):
            references.append(seg.get(4).strip())
    return references


def utc_stamp() -> str:
    return datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
