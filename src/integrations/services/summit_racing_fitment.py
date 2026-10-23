"""
Fill ``MasterPartFitment`` for a brand from Summit Racing's applications tab.

Summit publishes, per part, the real vehicle combinations a part fits -- year range, make,
model, submodel and, for part types whose fitment varies by engine, engine size, drivetrain and
transmission. That is strictly richer than the make/model lists a facet endpoint would give, and
unlike those lists it is safe to store directly: each row is already one concrete application,
so nothing here cross-products anything.

Intended as a *gap filler*, not a primary source. Where a brand ships ACES (or a distributor
feed carries VCdb ids), that path gives BaseVehicleIDs and full catalog coverage; Summit gives
string make/model for the subset of a brand's catalog Summit happens to stock -- measured at
~31% for BDS. Reach for this when no feed is available for a brand at all.

Provenance note: rows are written with ``source_provider=NULL`` by default, matching the 3.4M
MasterPartFitment rows that already carry no provider. Summit is a retailer we scrape, not an
integrated provider, so inventing a ``Providers`` row for it would put it in UI surfaces that
list providers. Pass ``provider_kind`` to attribute the rows to an existing provider instead.
"""

from __future__ import annotations

import dataclasses
import logging
import math
import pathlib
import threading
import typing
from concurrent import futures

import pgbulk

import src.models as src_models
from src.integrations.clients.summit_racing import applications as summit_applications
from src.integrations.clients.summit_racing import client as summit_client
from src.integrations.clients.summit_racing import exceptions as summit_exceptions
from src.integrations.clients.summit_racing import sku_index as summit_sku_index
from src.integrations.clients.summit_racing import token as summit_token
from src.integrations.services import master_parts

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[summit-racing-fitment]"

# Summit renders 25 application rows per page and ignores every page-size override we tried
# (?pagesize=, ?pageSize=, and the HttpOnly search_s PageSize cookie all leave it at 25).
_ROWS_PER_PAGE = 25

_UNIQUE_FIELDS = [
    "master_part",
    "year_start",
    "year_end",
    "make",
    "model",
    "submodel",
    "engine",
    "drive_type",
]


@dataclasses.dataclass
class SweepResult:
    parts_considered: int = 0
    parts_resolved: int = 0
    parts_with_applications: int = 0
    parts_not_on_summit: int = 0
    parts_without_applications: int = 0
    parts_failed: int = 0
    rows_parsed: int = 0
    rows_written: int = 0
    requests_made: int = 0
    throttles: int = 0
    stopped_early_reason: str = ""

    def summary(self) -> str:
        return (
            "considered={} resolved={} with_apps={} no_apps={} not_on_summit={} failed={} "
            "rows_parsed={} rows_written={} requests={} throttles={}{}".format(
                self.parts_considered,
                self.parts_resolved,
                self.parts_with_applications,
                self.parts_without_applications,
                self.parts_not_on_summit,
                self.parts_failed,
                self.rows_parsed,
                self.rows_written,
                self.requests_made,
                self.throttles,
                " STOPPED_EARLY={}".format(self.stopped_early_reason) if self.stopped_early_reason else "",
            )
        )


@dataclasses.dataclass(frozen=True)
class _PartTask:
    master_part_id: int
    part_number: str
    sku: str


class _ClientPool:
    """One ``SummitRacingClient`` per worker thread.

    The client is not thread-safe (it paces itself and swaps its session on rotation), and
    sharing one would serialise every worker behind a single pace timer. Thread-local clients
    also mean each worker rotates its own exit IP when *it* gets throttled, leaving the others
    untouched.
    """

    def __init__(
        self,
        cookie_header: str,
        gateways: typing.Sequence[str],
        pace_seconds: float,
        timeout: float,
        mint_tokens: bool = False,
    ):
        self._cookie_header = cookie_header
        self._mint_tokens = mint_tokens
        self._gateways = list(gateways)
        self._pace_seconds = pace_seconds
        self._timeout = timeout
        self._local = threading.local()
        self._all: typing.List[summit_client.SummitRacingClient] = []
        self._lock = threading.Lock()
        self._next_gateway = 0

    def get(self) -> summit_client.SummitRacingClient:
        existing = getattr(self._local, "client", None)
        if existing is not None:
            return existing
        with self._lock:
            # Stagger starting gateways so N workers begin on N different exits rather than all
            # piling onto the first one and tripping its per-IP throttle together.
            start = self._next_gateway % len(self._gateways)
            self._next_gateway += 1
        ordered = self._gateways[start:] + self._gateways[:start]
        client = summit_client.SummitRacingClient(
            self._cookie_header,
            # Each worker mints on its own gateway, so its token and its exit IP stay paired --
            # the pairing a shared token across rotating exits cannot keep.
            token_provider=_mint_for_gateway if self._mint_tokens else None,
            gateways=ordered,
            pace_seconds=self._pace_seconds,
            timeout=self._timeout,
        )
        self._local.client = client
        with self._lock:
            self._all.append(client)
        return client

    def totals(self) -> typing.Tuple[int, int]:
        with self._lock:
            return (sum(c.request_count for c in self._all), sum(c.throttle_count for c in self._all))


def _mint_for_gateway(gateway: str) -> typing.Tuple[str, str]:
    minted = summit_token.mint_token(gateway=gateway)
    return minted.cookie_header, minted.user_agent


def fetch_part_applications(
    client: summit_client.SummitRacingClient,
    sku: str,
    *,
    max_pages: int,
) -> typing.Optional[typing.List[summit_applications.ApplicationRow]]:
    """All application rows for one SKU, or ``None`` when Summit has no such part.

    Page 1 carries the total, so the page count is known after one request rather than being
    discovered by walking until a page comes back empty.
    """
    first = client.get_applications_page(sku, page=1)
    total = summit_applications.parse_results_found(first)
    if total is None:
        raise summit_exceptions.SummitRacingPartNotFound(sku)
    if total == 0:
        return []

    rows = summit_applications.parse_applications(first)
    page_count = min(math.ceil(total / _ROWS_PER_PAGE), max_pages)
    for page in range(2, page_count + 1):
        rows.extend(summit_applications.parse_applications(client.get_applications_page(sku, page=page)))
    return rows


def sync_master_part_fitments_from_summit(
    *,
    brand_id: int,
    cookie_header: str,
    sku_prefix: str,
    sku_cache_path: typing.Optional[pathlib.Path] = None,
    refresh_sku_index: bool = False,
    gateways: typing.Optional[typing.Sequence[str]] = None,
    workers: int = 8,
    pace_seconds: float = 1.5,
    timeout: float = 60.0,
    only_missing: bool = True,
    limit: typing.Optional[int] = None,
    max_pages_per_part: int = 40,
    batch_size: int = 200,
    provider_kind: typing.Optional[int] = None,
    mint_tokens: bool = False,
    dry_run: bool = False,
    on_progress: typing.Optional[typing.Callable[[str], None]] = None,
) -> SweepResult:
    """Sweep one brand's parts and upsert whatever fitment Summit publishes for them."""
    result = SweepResult()
    emit = on_progress or (lambda message: None)

    provider = None
    if provider_kind is not None:
        provider = src_models.Providers.objects.filter(kind=provider_kind).first()
        if provider is None:
            raise ValueError("No Providers row with kind={}".format(provider_kind))

    skus = summit_sku_index.harvest_brand_skus(sku_prefix, cache_path=sku_cache_path, refresh=refresh_sku_index)
    if not skus:
        raise ValueError("No Summit SKUs found for prefix {!r} -- check the prefix.".format(sku_prefix))
    lookup = summit_sku_index.build_lookup(skus)
    emit("{} sku index: {} SKU(s), {} lookup key(s)".format(_LOG_PREFIX, len(skus), len(lookup)))

    tasks = list(_build_tasks(brand_id=brand_id, lookup=lookup, only_missing=only_missing, limit=limit, result=result))
    emit(
        "{} {} part(s) considered, {} resolved to a Summit SKU, {} not carried by Summit".format(
            _LOG_PREFIX, result.parts_considered, result.parts_resolved, result.parts_not_on_summit
        )
    )
    if not tasks:
        return result

    gateways = list(gateways) if gateways else [""]
    pool = _ClientPool(cookie_header, gateways, pace_seconds, timeout, mint_tokens=mint_tokens)
    pending_rows: typing.List[src_models.MasterPartFitment] = []
    stop = threading.Event()

    with futures.ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_task = {executor.submit(_run_one, pool, task, max_pages_per_part, stop): task for task in tasks}
        done_count = 0
        for future in futures.as_completed(future_to_task):
            task = future_to_task[future]
            done_count += 1
            try:
                rows, error = future.result()
            except Exception as e:  # noqa: BLE001 -- a worker crash must not abandon collected rows
                rows, error = None, e

            if isinstance(error, summit_exceptions.SummitRacingChallenged):
                # A dead cookie fails identically on every exit IP and for every remaining part.
                # Stop the sweep rather than converting the whole backlog into failures; the
                # operator needs a fresh token, and everything collected so far still gets
                # written below.
                if not result.stopped_early_reason:
                    result.stopped_early_reason = "cookie rejected by Imperva: {}".format(error)
                    emit("{} STOPPING: {}".format(_LOG_PREFIX, result.stopped_early_reason))
                stop.set()
                continue
            if isinstance(error, summit_exceptions.SummitRacingPartNotFound):
                result.parts_not_on_summit += 1
                continue
            if error is not None:
                result.parts_failed += 1
                logger.warning("%s part_number=%s sku=%s failed: %s", _LOG_PREFIX, task.part_number, task.sku, error)
                continue
            if rows is None:
                continue
            if not rows:
                result.parts_without_applications += 1
                continue

            result.parts_with_applications += 1
            result.rows_parsed += len(rows)
            pending_rows.extend(_to_fitment_models(task.master_part_id, rows, provider))

            if len(pending_rows) >= batch_size:
                result.rows_written += _flush(pending_rows, dry_run=dry_run)
                pending_rows = []

            if done_count % 100 == 0:
                requests_made, throttles = pool.totals()
                emit(
                    "{} progress {}/{} parts | rows_parsed={} rows_written={} requests={} throttles={}".format(
                        _LOG_PREFIX,
                        done_count,
                        len(tasks),
                        result.rows_parsed,
                        result.rows_written,
                        requests_made,
                        throttles,
                    )
                )

    result.rows_written += _flush(pending_rows, dry_run=dry_run)
    result.requests_made, result.throttles = pool.totals()
    return result


def _run_one(
    pool: _ClientPool,
    task: _PartTask,
    max_pages: int,
    stop: threading.Event,
) -> typing.Tuple[typing.Optional[typing.List[summit_applications.ApplicationRow]], typing.Optional[Exception]]:
    if stop.is_set():
        return None, None
    try:
        return fetch_part_applications(pool.get(), task.sku, max_pages=max_pages), None
    except Exception as e:  # noqa: BLE001 -- classified by the caller, which owns the counters
        return None, e


def _build_tasks(
    *,
    brand_id: int,
    lookup: typing.Dict[str, str],
    only_missing: bool,
    limit: typing.Optional[int],
    result: SweepResult,
) -> typing.Iterator[_PartTask]:
    """Stream the brand's parts by id, yielding only those Summit actually carries.

    Keyset-ordered and ``.values()``-projected so a 9k-part brand (or a 200k one) never
    materialises model instances, and so a re-run after an interruption covers the same set in
    the same order.
    """
    queryset = src_models.MasterPart.objects.filter(brand_id=brand_id)
    if only_missing:
        queryset = queryset.filter(fitments__isnull=True)
    queryset = queryset.order_by("id").values("id", "part_number")

    yielded = 0
    for row in queryset.iterator(chunk_size=2_000):
        result.parts_considered += 1
        sku = _resolve_sku(row["part_number"], lookup)
        if not sku:
            result.parts_not_on_summit += 1
            continue
        result.parts_resolved += 1
        yielded += 1
        yield _PartTask(master_part_id=row["id"], part_number=row["part_number"], sku=sku)
        if limit is not None and yielded >= limit:
            return


def _resolve_sku(part_number: str, lookup: typing.Dict[str, str]) -> typing.Optional[str]:
    for key in summit_sku_index.candidate_keys_for_part_number(part_number or ""):
        sku = lookup.get(key)
        if sku:
            return sku
    return None


def _to_fitment_models(
    master_part_id: int,
    rows: typing.Iterable[summit_applications.ApplicationRow],
    provider: typing.Optional[src_models.Providers],
) -> typing.List[src_models.MasterPartFitment]:
    out = []
    for row in rows:
        if not row.is_usable:
            continue
        out.append(
            src_models.MasterPartFitment(
                master_part_id=master_part_id,
                year_start=row.year_start,
                year_end=row.year_end,
                make=row.make,
                model=row.model,
                submodel=row.submodel,
                engine=row.engine,
                drive_type=row.drive_type,
                source_provider=provider,
            )
        )
    return out


def _flush(rows: typing.List[src_models.MasterPartFitment], *, dry_run: bool) -> int:
    """Upsert a batch. Idempotent: re-running the sweep rewrites the same keys, never duplicates.

    Summit renders one row per application *note*, so the same vehicle appears two or three
    times with different notes; MasterPartFitment has no notes column, which collapses those to
    one key. Deduping before the upsert is required -- Postgres rejects ON CONFLICT when a
    conflict key repeats inside a single statement.
    """
    if not rows:
        return 0
    deduped = master_parts._dedupe_master_part_fitments_for_upsert(rows, context="Summit Racing applications")
    if dry_run:
        return len(deduped)
    pgbulk.upsert(
        src_models.MasterPartFitment,
        deduped,
        unique_fields=_UNIQUE_FIELDS,
        update_fields=["source_provider"],
    )
    return len(deduped)
