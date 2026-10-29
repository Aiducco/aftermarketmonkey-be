"""
Rough Country EDI mailbox (Alluvia-hosted FTP).

One mailbox per company. Alluvia issues every shop its own FTP login and its own ISA/GS
trading-partner id, so there is no global mailbox and no global credential: a client is always
built for one CompanyProviderOrderAccount, via :meth:`RoughCountryEdiClient.for_account`. The
global FTP settings this module started with existed only while Trident was the one test
account and have been removed.

Folder layout, as given by Alluvia. Note the names are from *their* point of view, so their
"Inbound" is our outbound:

    /Inbound/850    purchase orders we send them
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

from src import models as src_models
from src.integrations.clients.rough_country import edi_config
from src.integrations.edi import exceptions as edi_exceptions
from src.integrations.edi import transport as edi_transport
from src.integrations.edi import x12

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[ROUGH-COUNTRY-EDI]"

# Document types Rough Country publishes to us.
INBOUND_DOCUMENT_TYPES = ("855", "856", "810")
# Document types we publish to them.
OUTBOUND_DOCUMENT_TYPES = ("850", "997")

# Folder names are Alluvia's, from THEIR point of view, so their Inbound is what we write to.
OUTBOUND_DIRECTORY_TEMPLATE = "/Outbound/{}"
ARCHIVE_DIRECTORY_TEMPLATE = "/Archive/{}"
SEND_DIRECTORY_TEMPLATE = "/Inbound/{}"


class RoughCountryEdiClient:
    """
    Thin wrapper over EdiFtpTransport that knows Rough Country's folder layout, bound to one
    company's mailbox.

    Build it with :meth:`for_account` in anything that has a connection; the ``config``
    constructor is for tests and for the CLI, which resolves an account itself so it can print
    which environment it is about to talk to before doing anything.
    """

    def __init__(
        self,
        config: edi_config.RoughCountryEdiConfig,
        local_dir: typing.Optional[str] = None,
    ) -> None:
        self.config = config
        base_dir = local_dir or getattr(settings, "ROUGH_COUNTRY_EDI_LOCAL_DIR", "") or "/tmp/rough_country_edi"
        # Namespaced by our trading-partner id so two shops collecting at once cannot overwrite
        # each other's downloads. Alluvia's filenames carry a UUID, but relying on that to keep
        # two companies' files apart would be luck rather than design.
        self.local_dir = os.path.join(base_dir, config.sender_id)

    @classmethod
    def for_account(
        cls,
        company_provider: src_models.CompanyProviders,
        order_account: typing.Optional[src_models.CompanyProviderOrderAccount] = None,
    ) -> "RoughCountryEdiClient":
        """Raises ValueError when this account has no usable EDI credentials — the established
        order-adapter contract (see src.integrations.orders.registry.get_adapter)."""
        return cls(edi_config.for_account(company_provider, order_account))

    # Kept as properties so existing callers and log lines that read client.host/user keep
    # working now that the real source of truth is the config object.
    @property
    def host(self) -> str:
        return self.config.host

    @property
    def port(self) -> int:
        return self.config.port

    @property
    def user(self) -> str:
        return self.config.user

    @property
    def use_tls(self) -> bool:
        return self.config.use_tls

    def _transport(self) -> edi_transport.EdiFtpTransport:
        return edi_transport.EdiFtpTransport(
            host=self.config.host,
            user=self.config.user,
            password=self.config.password,
            port=self.config.port,
            use_tls=self.config.use_tls,
        )

    def _local_path(self, document_type: str, filename: str) -> str:
        directory = os.path.join(self.local_dir, document_type)
        os.makedirs(directory, exist_ok=True)
        return os.path.join(directory, filename)

    # -- sending ------------------------------------------------------------------------------

    def send_document(self, document_type: str, filename: str, payload: str) -> str:
        """
        Write one document into Rough Country's pickup folder and return the remote path.

        The transport uploads to a ``.tmp`` name and renames it into place, so Rough Country can
        never collect a half-written 850. A failed rename raises rather than returning quietly:
        they do not pick up ``.tmp`` files, so the document has NOT been delivered and the
        caller must not record it as sent.
        """
        if document_type not in OUTBOUND_DOCUMENT_TYPES:
            raise ValueError(
                "{} is not a document type we send (expected one of {}).".format(
                    document_type, ", ".join(OUTBOUND_DOCUMENT_TYPES)
                )
            )
        remote_path = posixpath.join(SEND_DIRECTORY_TEMPLATE.format(document_type), filename)
        with self._transport() as ftp:
            ftp.write_text(remote_path, payload)
        logger.info(
            "%s Sent %s to %s (%s).",
            _LOG_PREFIX,
            document_type,
            remote_path,
            self.config.describe(),
        )
        return remote_path

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

    # -- collect + apply ----------------------------------------------------------------------

    def process_waiting(
        self,
        handler: typing.Callable[[str, str, str], bool],
        document_types: typing.Optional[typing.Sequence[str]] = None,
        limit: typing.Optional[int] = None,
    ) -> typing.List[typing.Dict]:
        """
        Download each waiting document, hand it to ``handler(document_type, filename, payload)``
        and archive it only if the handler returns True.

        The ordering is the whole point. ``collect`` archives as soon as a file is downloaded,
        which is right when all we do is file it; when the document is also being applied to a
        PurchaseOrder, the archive move must happen only after that work has committed.
        Archiving first and crashing second would lose the document permanently — and since the
        archive move is the only thing that stops us collecting a file again, leaving it in place
        is what makes a retry possible. That makes delivery at-least-once, which is why the
        handler is required to deduplicate (see services/rough_country_edi.record_inbound).

        A handler that raises is reported and leaves the file in /Outbound; it is never allowed
        to abort the sweep, so one unparseable document cannot block every later one.
        """
        wanted = [d for d in (document_types or INBOUND_DOCUMENT_TYPES) if d in INBOUND_DOCUMENT_TYPES]
        results: typing.List[typing.Dict] = []
        processed = 0

        with self._transport() as ftp:
            for document_type in wanted:
                source_dir = OUTBOUND_DIRECTORY_TEMPLATE.format(document_type)
                archive_dir = ARCHIVE_DIRECTORY_TEMPLATE.format(document_type)
                for filename in ftp.list_files(source_dir):
                    if limit is not None and processed >= limit:
                        return results
                    processed += 1
                    entry = {
                        "document_type": document_type,
                        "filename": filename,
                        "applied": False,
                        "archived": False,
                        "error": None,
                    }
                    remote_path = posixpath.join(source_dir, filename)
                    try:
                        payload = ftp.read_text(remote_path)
                        _assert_complete(payload, remote_path)
                    except (edi_exceptions.EdiTransportError, edi_exceptions.EdiParseError) as e:
                        entry["error"] = str(e)
                        logger.error("%s %s: %s", _LOG_PREFIX, remote_path, e)
                        results.append(entry)
                        continue

                    try:
                        entry["applied"] = bool(handler(document_type, filename, payload))
                    except Exception as e:
                        # Deliberately broad: a handler failure must quarantine one document,
                        # not strand every document behind it. The file stays in /Outbound.
                        entry["error"] = "could not be applied: {}".format(e)
                        logger.exception("%s Failed applying %s.", _LOG_PREFIX, remote_path)
                        results.append(entry)
                        continue

                    if entry["applied"]:
                        try:
                            ftp.move(remote_path, posixpath.join(archive_dir, filename))
                            entry["archived"] = True
                        except edi_exceptions.EdiTransportError as e:
                            # Applied but not archived: harmless, because the next sweep
                            # deduplicates on the interchange control number.
                            entry["error"] = "applied, but archiving failed: {}".format(e)
                            logger.error("%s %s: %s", _LOG_PREFIX, remote_path, e)
                    results.append(entry)

        return results


def rough_country_sku(provider_part: src_models.ProviderPart) -> str:
    """
    Rough Country's real SKU for a ProviderPart -- NOT ``provider_external_id``.

    That column is a composite ``"{rc_brand_id}_{sku}"`` key (see
    ``master_parts._rough_country_provider_external_id``), needed only for our own uniqueness
    because the same SKU can recur under different Rough Country brands. A real part is stored as
    ``8_50120580`` while Rough Country calls it ``50120580``.

    Sending the composite key would break the integration in both directions, and did: the 850's
    ``VC`` element is how Rough Country identifies the item, and their 855/856/810 echo their own
    number back, so matching an inbound document to a PO line compares against this too.

    Same resolution order as ``premier._premier_item_number``, which has the identical composite-
    key problem: ``product_details``' "sku" entry carries the raw number directly (see
    master_parts' Rough Country sync), and splitting ``provider_external_id`` on its FIRST
    underscore is the fallback if that is ever missing or stale. First underscore, not last --
    Rough Country SKUs contain underscores of their own (``501065_C`` is a real part) and the
    brand-id prefix is always purely numeric.
    """
    for entry in provider_part.product_details or []:
        if isinstance(entry, dict) and entry.get("key") == "sku" and entry.get("value"):
            return str(entry["value"]).strip()
    external_id = (provider_part.provider_external_id or "").strip()
    prefix, separator, remainder = external_id.partition("_")
    if separator and prefix.isdigit() and remainder:
        return remainder.strip()
    return external_id


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
            "{} is not a complete X12 interchange ({}). Leaving it in Outbound to retry.".format(remote_path, e)
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
