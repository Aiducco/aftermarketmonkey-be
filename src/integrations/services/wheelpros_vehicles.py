"""
Crawl Wheel Pros' Vehicle API into ``wheelpros_vehicles`` / ``wheelpros_vehicle_axles``.

Spec: https://developer.wheelpros.com/assets/specs/vehicle-api/openapi/api.html
Transport: :class:`src.integrations.clients.wheelpros.vehicle_client.WheelProsVehicleApiClient`.
Entry point: the ``fetch_wheelpros_vehicles`` management command.

Shape of the API
----------------
Six endpoints, four listings and two details, arranged as a tree that can only be walked
downward -- there is no bulk export and no pagination, so the whole catalogue is reached one
node at a time::

    /v1/years                                              -> [2026, 2025, ...]
    /v1/years/{y}/makes                                    -> ["Ford", ...]
    /v1/years/{y}/makes/{mk}/models                        -> ["F-150", ...]
    /v1/years/{y}/makes/{mk}/models/{md}                   -> the model's fitment payload
    /v1/years/{y}/makes/{mk}/models/{md}/submodels         -> ["Raptor", ...]
    /v1/years/{y}/makes/{mk}/models/{md}/submodels/{sub}   -> the submodel's fitment payload

The two detail endpoints return the same object; the submodel one adds ``subModel``. Both land
in the same table -- see :class:`src.models.WheelProsVehicle`.

Cost, and why the unit of work is a model
-----------------------------------------
A full pass costs, in requests::

    1  +  Y  +  (Y x M)  +  sum over models of (2 + S)

-- one for the year list, one per year for makes, one per year/make for models, then per model
two calls (its detail and its submodel list) plus one per submodel. The tail dominates: it is
proportional to the number of distinct year/make/model/submodel combinations, which for a
full-history automotive catalogue runs to the high hundreds of thousands. This is a long crawl,
measured in hours at any neighbourly rate, so it is built to be interrupted:

* the unit of work is one **(year, make, model)** -- its detail, its submodel list, and every
  submodel detail, fetched together and written together;
* every finished unit is appended to a JSONL checkpoint and skipped on restart, so a crash costs
  one model rather than the crawl;
* the write is an upsert keyed on (year, make, model, submodel), so re-running is idempotent and
  ``--no-resume`` is merely slow, never wrong.

Run it year by year (``--year 2024 --year 2025``) or make by make (``--make Ford``) if you would
rather not hold one process open for the whole thing; the checkpoint is shared, so the slices
compose into a full pass.

Threads fetch, the main thread writes
-------------------------------------
Workers only do HTTP and parsing; every DB write happens on the thread that consumes the results.
That keeps the crawl single-writer (no lock contention, no connection-per-thread) and means the
checkpoint can only ever record a unit whose rows are already committed -- never the reverse.

Parsing
-------
Wheel Pros sends **every** axle value as a string, measurements included, and those strings are
display strings: ``'5x114.3'``, ``'+35'``, ``'18"'``, ``'N/A'``. :func:`_decimal` is deliberately
tolerant of that and returns ``None`` rather than raising, because one odd value must not kill a
six-hour crawl. Anything that is a code or a size expression stays text. The untouched payload is
stored alongside on ``raw_payload``/``raw_axle``, so **a parser fix never requires a re-crawl** --
re-derive the columns from the blobs instead.
"""
import concurrent.futures
import dataclasses
import decimal
import json
import logging
import pathlib
import threading
import time
import typing

import pgbulk
from django.conf import settings
from django.utils import timezone

from src import enums as src_enums
from src import models as src_models
from src.integrations import credentials as credentials_helper
from src.integrations.clients.wheelpros import exceptions as wheelpros_exceptions
from src.integrations.clients.wheelpros.vehicle_client import WheelProsVehicleApiClient

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[WHEELPROS-VEHICLES]"

DEFAULT_CONCURRENCY = 6
DEFAULT_RATE_PER_SECOND = 8.0
DEFAULT_BATCH_SIZE = 500

# Values Wheel Pros uses for "we do not publish this". Stored as NULL, never as 0 or "".
_NOT_AVAILABLE = frozenset({"", "na", "n/a", "none", "null", "-", "--", "n\\a", "tbd", "unknown"})

# Column -> where it lives in the axle payload. A tuple is a path through the nested
# diameter/caliper/offset/lug sub-objects. Kept as data rather than thirty lines of getattr
# soup so the mapping can be read against the spec in one glance.
_AXLE_TEXT_FIELDS: typing.Final[dict[str, tuple[str, ...]]] = {
    "code": ("code",),
    "bolt_pattern_mm": ("boltPatternMm",),
    "hub_code": ("hubCode",),
    "nut_bolt": ("nutBolt",),
    "oe_hex": ("oeHexTx",),
    "oe_tire": ("oeTireTx",),
    "pressure_sensor": ("vehiclePressureSensor",),
    "sensor_part_number_oe": ("sensorPartNumberOe",),
    "lug_nut_size": ("lug", "lugNutSizeTx"),
    "lug_style_am": ("lug", "amLugStyle"),
}

# Column -> (path, max_digits, decimal_places). The bounds are the model's own, and are enforced
# here rather than left to Postgres: a value that overflows must become NULL and a warning, not
# a DataError that aborts a crawl thousands of models in.
_AXLE_DECIMAL_FIELDS: typing.Final[dict[str, tuple[tuple[str, ...], int, int]]] = {
    "center_bore_mm": (("centerBoreMm",), 8, 2),
    "hub_clearance_mm": (("hubClearanceMm",), 8, 2),
    "oe_width_in": (("oeWidthIn",), 6, 2),
    "max_width_in": (("maxWidthIn",), 6, 2),
    "max_bs": (("maxBs",), 6, 2),
    "max_fs": (("maxFs",), 6, 2),
    "min_wheel_load": (("minWheelLoad",), 10, 2),
    "y_factor": (("yFactor",), 8, 3),
    "y_factor_25": (("yFactor25",), 8, 3),
    "y_factor_50": (("yFactor50",), 8, 3),
    "oe_diameter_in": (("diameter", "oeDiameterIn"), 6, 2),
    "min_diameter_in": (("diameter", "minDiameterIn"), 6, 2),
    "max_diameter_in": (("diameter", "maxDiameterIn"), 6, 2),
    "caliper_peak_depth": (("caliper", "peakDepth"), 8, 2),
    "caliper_depth_90mm": (("caliper", "depth90mm"), 8, 2),
    "caliper_depth_100mm": (("caliper", "depth100mm"), 8, 2),
    "caliper_depth_106mm": (("caliper", "depth106mm"), 8, 2),
    "caliper_depth_119mm": (("caliper", "depth119mm"), 8, 2),
    "caliper_depth_134mm": (("caliper", "depth134mm"), 8, 2),
    "caliper_depth_160mm": (("caliper", "depth160mm"), 8, 2),
    "oe_offset_mm": (("offset", "oeOffset"), 7, 2),
    "offset_min_mm": (("offset", "offsetMinMm"), 7, 2),
    "offset_max_mm": (("offset", "offsetMaxMm"), 7, 2),
    "lift_offset_min_mm": (("offset", "liftOffsetMinMm"), 7, 2),
    "lift_offset_max_mm": (("offset", "liftOffsetMaxMm"), 7, 2),
}

_VEHICLE_UPDATE_FIELDS = [
    "external_id",
    "staggered",
    "vehicle_types",
    "raw_payload",
    "scraped_at",
    "updated_at",
]

_AXLE_UPDATE_FIELDS = (
    ["raw_axle", "updated_at", "lug_count"]
    + list(_AXLE_TEXT_FIELDS)
    + list(_AXLE_DECIMAL_FIELDS)
)


class WheelProsVehicleCrawlError(Exception):
    """The crawl cannot proceed — no usable credentials, or the account is not entitled."""


# -- work units ---------------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class ModelRef:
    """One unit of work: everything the crawl fetches and writes atomically."""

    year: int
    make: str
    model: str
    # Which ?type= listing surfaced it. A model reachable from both the wheel and the tire
    # catalogue is crawled once, carrying both labels.
    vehicle_types: tuple[str, ...] = ()

    @property
    def key(self) -> str:
        """Checkpoint identity. Case-folded because the API's path params are case-insensitive
        and its listings are not consistent about casing between calls — without this, a resumed
        run would re-crawl models it had already finished under a different spelling."""
        return "{}|{}|{}".format(self.year, self.make.strip().lower(), self.model.strip().lower())

    def __str__(self) -> str:
        return "{} {} {}".format(self.year, self.make, self.model)


@dataclasses.dataclass
class CrawlStats:
    years_seen: int = 0
    makes_seen: int = 0
    models_listed: int = 0
    models_done: int = 0
    models_skipped: int = 0
    models_missing: int = 0
    models_failed: int = 0
    vehicles_written: int = 0
    axles_written: int = 0
    requests_made: int = 0


# -- parsing ------------------------------------------------------------------------------------


def _clean(value: object) -> typing.Optional[str]:
    """A trimmed string, or ``None`` for anything Wheel Pros means as "not published"."""
    if value is None or isinstance(value, (dict, list)):
        return None
    text = str(value).strip()
    if not text or text.lower() in _NOT_AVAILABLE:
        return None
    return text


def _dig(payload: typing.Optional[dict], path: tuple[str, ...]) -> object:
    """Walk ``path`` through nested dicts, tolerating a missing or non-dict level."""
    current: object = payload
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _decimal(
    value: object, *, max_digits: int, decimal_places: int, field: str
) -> typing.Optional[decimal.Decimal]:
    """
    Parse one of Wheel Pros' numeric-ish strings, or return ``None``.

    Handles the shapes actually seen in a display string: a leading sign (``'+35'``), a trailing
    unit (``'18"'``, ``'114.3 mm'``), thousands separators, and a value that is simply text.
    Returns ``None`` rather than raising — a single unparseable field must cost one column, not
    the crawl.

    Values that would overflow the column are dropped with a warning for the same reason: the
    raw string is still on ``raw_axle``, so nothing is lost and the parser can be fixed later
    without re-fetching.
    """
    text = _clean(value)
    if text is None:
        return None

    # Keep only what can belong to a single decimal number, then require that what is left still
    # parses — this rejects "5x114.3" (a bolt pattern, not a measurement) instead of silently
    # turning it into 5114.3.
    cleaned = text.replace(",", "").replace('"', "").replace("mm", "").replace("in", "").strip()
    try:
        parsed = decimal.Decimal(cleaned)
    except (decimal.InvalidOperation, ValueError):
        logger.debug("%s unparseable decimal for %s: %r", _LOG_PREFIX, field, text)
        return None
    if not parsed.is_finite():
        return None

    quantized = parsed.quantize(decimal.Decimal(1).scaleb(-decimal_places), rounding=decimal.ROUND_HALF_UP)
    if abs(quantized) >= decimal.Decimal(10) ** (max_digits - decimal_places):
        logger.warning("%s %s=%r overflows the column; storing NULL", _LOG_PREFIX, field, text)
        return None
    return quantized


def _int(value: object, *, field: str) -> typing.Optional[int]:
    parsed = _decimal(value, max_digits=9, decimal_places=0, field=field)
    return int(parsed) if parsed is not None else None


def _bool(value: object) -> typing.Optional[bool]:
    """``properties.staggered`` is a real boolean in the spec, but tolerate the string form too.
    Anything else is unknown — NULL, not False."""
    if isinstance(value, bool):
        return value
    text = _clean(value)
    if text is None:
        return None
    lowered = text.lower()
    if lowered in ("true", "yes", "y", "1"):
        return True
    if lowered in ("false", "no", "n", "0"):
        return False
    return None


# -- payload -> rows ------------------------------------------------------------------------------


def build_vehicle(
    payload: dict, *, ref: ModelRef, submodel: str = "", scraped_at=None
) -> src_models.WheelProsVehicle:
    """
    Map a detail payload onto an unsaved :class:`src.models.WheelProsVehicle`.

    Identity comes from the payload where the payload has it, and from the request otherwise:
    Wheel Pros echoes ``make``/``model``/``subModel`` in its own canonical casing, which is what
    should be stored, but a payload that omits one must still produce a row addressable by the
    tuple that was requested.
    """
    year = _int(payload.get("year"), field="year") or ref.year
    make = _clean(payload.get("make")) or ref.make
    model = _clean(payload.get("model")) or ref.model
    resolved_submodel = _clean(payload.get("subModel")) or submodel or ""

    return src_models.WheelProsVehicle(
        external_id=_int(payload.get("id"), field="id"),
        year=year,
        make=make[:128],
        model=model[:255],
        submodel=resolved_submodel[:255],
        staggered=_bool(_dig(payload, ("properties", "staggered"))),
        vehicle_types=list(ref.vehicle_types),
        raw_payload=payload,
        scraped_at=scraped_at or timezone.now(),
    )


def build_axles(payload: dict, *, vehicle_id: int) -> list[src_models.WheelProsVehicleAxle]:
    """One unsaved axle row per populated position. A vehicle whose payload carries no axle data
    yields none, rather than two rows of NULLs that would be indistinguishable from a vehicle
    Wheel Pros genuinely has no fitment for."""
    axles = payload.get("axles")
    if not isinstance(axles, dict):
        return []

    rows = []
    for position in (
        src_models.WheelProsVehicleAxle.Position.FRONT,
        src_models.WheelProsVehicleAxle.Position.REAR,
    ):
        axle = axles.get(position.value)
        if not isinstance(axle, dict) or not axle:
            continue

        row = src_models.WheelProsVehicleAxle(
            vehicle_id=vehicle_id,
            position=position.value,
            raw_axle=axle,
            lug_count=_int(_dig(axle, ("lug", "lugCnt")), field="lugCnt"),
        )
        for field, path in _AXLE_TEXT_FIELDS.items():
            text = _clean(_dig(axle, path))
            setattr(row, field, text[: src_models.WheelProsVehicleAxle._meta.get_field(field).max_length] if text else None)
        for field, (path, max_digits, decimal_places) in _AXLE_DECIMAL_FIELDS.items():
            setattr(
                row,
                field,
                _decimal(_dig(axle, path), max_digits=max_digits, decimal_places=decimal_places, field=field),
            )
        rows.append(row)
    return rows


# -- rate limiting --------------------------------------------------------------------------------


class _TokenBucket:
    """Process-wide request throttle shared by the worker threads."""

    def __init__(self, rate_per_second: float) -> None:
        if rate_per_second <= 0:
            raise ValueError("rate_per_second must be positive")
        self._interval = 1.0 / rate_per_second
        self._lock = threading.Lock()
        self._next_at = 0.0

    def acquire(self) -> None:
        with self._lock:
            now = time.monotonic()
            wait = max(0.0, self._next_at - now)
            # Anchor the next slot on the one just handed out, so a burst of threads spaces out
            # instead of all sleeping to the same instant.
            self._next_at = max(now, self._next_at) + self._interval
        if wait:
            time.sleep(wait)


class _ThrottledClient:
    """Wraps the API client so every call passes the shared rate limiter first. Keeps the
    throttle out of the client (which knows nothing about crawls) and out of the crawl body
    (which would otherwise have to remember to acquire before each of six call sites)."""

    def __init__(self, client: WheelProsVehicleApiClient, bucket: typing.Optional[_TokenBucket]) -> None:
        self._client = client
        self._bucket = bucket

    def __getattr__(self, name: str):
        attribute = getattr(self._client, name)
        if not callable(attribute):
            return attribute

        def throttled(*args, **kwargs):
            if self._bucket is not None:
                self._bucket.acquire()
            return attribute(*args, **kwargs)

        return throttled


# -- checkpoint -------------------------------------------------------------------------------------


class _Checkpoint:
    """
    Append-only JSONL record of finished units, written by the main thread only.

    A unit is recorded *after* its rows are committed, so the checkpoint can never claim work the
    database does not have. The reverse — committed rows with no checkpoint line — is harmless:
    the upsert makes re-crawling that unit a no-op.
    """

    def __init__(self, path: typing.Optional[pathlib.Path]) -> None:
        self.path = path
        self._handle = None

    def __enter__(self) -> "_Checkpoint":
        if self.path is not None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self._handle = self.path.open("a", encoding="utf-8")
        return self

    def __exit__(self, *exc_info) -> None:
        if self._handle is not None:
            self._handle.close()
            self._handle = None

    def completed_keys(self) -> set[str]:
        if self.path is None or not self.path.exists():
            return set()
        keys: set[str] = set()
        with self.path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    keys.add(json.loads(line)["key"])
                except (ValueError, KeyError):
                    # A line torn by a hard kill mid-write. Ignore it — that unit is simply
                    # re-crawled, which the upsert makes free.
                    logger.warning("%s unreadable checkpoint line; ignoring it", _LOG_PREFIX)
        return keys

    def record(self, ref: ModelRef, *, status: str, vehicles: int) -> None:
        if self._handle is None:
            return
        self._handle.write(
            json.dumps(
                {
                    "key": ref.key,
                    "year": ref.year,
                    "make": ref.make,
                    "model": ref.model,
                    "status": status,
                    "vehicles": vehicles,
                    "at": timezone.now().isoformat(),
                }
            )
            + "\n"
        )
        self._handle.flush()


# -- credentials --------------------------------------------------------------------------------


def get_vehicle_api_client(
    company_provider_id: typing.Optional[int] = None,
    environment: typing.Optional[str] = None,
) -> WheelProsVehicleApiClient:
    """
    Build a client from a Wheel Pros connection's order credentials.

    The Vehicle API has no credentials of its own: it authenticates with the same Product Data
    Portal username/password as the Orders API, which this codebase stores on
    ``CompanyProviderOrderAccount`` (see :mod:`src.integrations.credentials`). Vehicle reference
    data is account-agnostic — every entitled account sees the same catalogue — so any connection
    with usable credentials will do, exactly as ``fetch_and_save_wheelpros_warehouses`` picks one
    for GET /warehouses/v1. ``company_provider_id`` pins a specific one.
    """
    queryset = src_models.CompanyProviders.objects.filter(
        provider__kind=src_enums.BrandProviderKind.WHEELPROS.value, active=True
    )
    if company_provider_id is not None:
        queryset = queryset.filter(id=company_provider_id)

    for company_provider in queryset.order_by("-primary", "id"):
        order_credentials = credentials_helper.get_order_credentials(company_provider)
        if order_credentials.get("username") and order_credentials.get("password"):
            logger.info(
                "%s Using CompanyProviders id=%s (company_id=%s) for Vehicle API auth.",
                _LOG_PREFIX,
                company_provider.id,
                company_provider.company_id,
            )
            return WheelProsVehicleApiClient(
                credentials=order_credentials,
                environment=environment or getattr(settings, "WHEELPROS_VEHICLE_ENVIRONMENT", "production"),
            )

    raise WheelProsVehicleCrawlError(
        "No active Wheel Pros connection with API username/password found{}. Configure order "
        "credentials on the connection first — the Vehicle API reuses them.".format(
            " for company_provider_id={}".format(company_provider_id) if company_provider_id else ""
        )
    )


# -- discovery ------------------------------------------------------------------------------------


def discover_models(
    client,
    *,
    years: typing.Optional[typing.Sequence[int]] = None,
    makes: typing.Optional[typing.Sequence[str]] = None,
    vehicle_types: typing.Sequence[typing.Optional[str]] = (None,),
    stats: typing.Optional[CrawlStats] = None,
    progress: typing.Optional[typing.Callable[[str], None]] = None,
) -> list[ModelRef]:
    """
    Walk years -> makes -> models and return the crawl's work list.

    Runs before the fan-out, on one thread, because it is cheap relative to the detail phase
    (one call per year plus one per year/make, against one or more per model) and because
    knowing the total up front is what makes progress reporting and ``--limit-models`` possible.

    ``vehicle_types`` of ``(None,)`` — the default — asks for the unfiltered union in a single
    pass. Pass ``("wheel", "tire")`` instead to learn which catalogue each model belongs to; that
    doubles the listing calls and records the answer on ``ModelRef.vehicle_types``.
    """
    stats = stats or CrawlStats()
    emit = progress or (lambda message: None)

    wanted_makes = {make.strip().lower() for make in makes if make.strip()} if makes else None
    # (year, make, model) -> the ?type= labels it was found under.
    found: dict[tuple[int, str, str], set[str]] = {}
    order: list[tuple[int, str, str]] = []

    for vehicle_type in vehicle_types:
        label = vehicle_type or "all"

        available_years = sorted(client.get_years(vehicle_type), reverse=True)
        selected_years = [year for year in available_years if year in set(years)] if years else available_years
        if years:
            missing = sorted(set(years) - set(available_years))
            for year in missing:
                emit("  warning: {} is not a year Wheel Pros lists (type={})".format(year, label))
        stats.years_seen = max(stats.years_seen, len(selected_years))

        for year in selected_years:
            year_makes = client.get_makes(year, vehicle_type)
            if wanted_makes:
                year_makes = [make for make in year_makes if make.strip().lower() in wanted_makes]
            stats.makes_seen += len(year_makes)

            for make in year_makes:
                make_models = client.get_models(year, make, vehicle_type)
                for model in make_models:
                    # Case-folded so the same model listed as "F-150" under one call and "F-150 "
                    # under another is one unit of work, not two.
                    key = (year, make.strip().lower(), model.strip().lower())
                    if key not in found:
                        found[key] = set()
                        order.append(key)
                        # Keep the first spelling seen; it is what the request path will use, and
                        # the payload overrides it at write time anyway.
                        found[key].add("__display__:{}|{}".format(make, model))
                    if vehicle_type:
                        found[key].add(vehicle_type)
                emit(
                    "  {} {} — {} models ({} distinct so far, type={})".format(
                        year, make, len(make_models), len(found), label
                    )
                )

    refs = []
    for key in order:
        year = key[0]
        labels = found[key]
        display = next(label for label in labels if label.startswith("__display__:"))
        make, model = display[len("__display__:") :].split("|", 1)
        refs.append(
            ModelRef(
                year=year,
                make=make,
                model=model,
                vehicle_types=tuple(sorted(label for label in labels if not label.startswith("__display__:"))),
            )
        )
    stats.models_listed = len(refs)
    return refs


# -- one unit of work -------------------------------------------------------------------------------


@dataclasses.dataclass
class _FetchedModel:
    ref: ModelRef
    # (submodel name, payload) — "" for the model-level record. Ordered model-first.
    payloads: list[tuple[str, dict]]
    missing: bool = False


def fetch_model(client, ref: ModelRef, *, max_submodels: typing.Optional[int] = None) -> _FetchedModel:
    """
    Fetch one model's detail, its submodel list, and every submodel's detail.

    A 404 on the model itself means the listing named something the detail endpoint does not
    have — treated as "nothing to write here", not as a failure, because it is Wheel Pros' data
    being inconsistent rather than anything a retry would fix. A 404 on an individual submodel is
    skipped the same way, leaving the rest of the model intact.
    """
    payloads: list[tuple[str, dict]] = []

    try:
        model_payload = client.get_model_info(ref.year, ref.make, ref.model)
    except wheelpros_exceptions.WheelProsVehicleNotFound:
        return _FetchedModel(ref=ref, payloads=[], missing=True)
    if isinstance(model_payload, dict) and model_payload:
        payloads.append(("", model_payload))

    submodels = client.get_submodels(ref.year, ref.make, ref.model)
    if max_submodels is not None:
        submodels = submodels[:max_submodels]

    for submodel in submodels:
        try:
            submodel_payload = client.get_submodel_info(ref.year, ref.make, ref.model, submodel)
        except wheelpros_exceptions.WheelProsVehicleNotFound:
            logger.info("%s no detail for %s / %s; skipping it", _LOG_PREFIX, ref, submodel)
            continue
        if isinstance(submodel_payload, dict) and submodel_payload:
            payloads.append((submodel, submodel_payload))

    return _FetchedModel(ref=ref, payloads=payloads, missing=not payloads)


# -- persistence ------------------------------------------------------------------------------------


def persist_model(fetched: _FetchedModel) -> tuple[int, int]:
    """
    Upsert one unit's vehicles and their axles. Returns (vehicles, axles) written.

    Two steps, because the axles need their parent's primary key: upsert the vehicle rows, read
    back the ids for exactly the tuples just written, then upsert the axles against them. The
    read-back is one indexed query over a handful of rows (one model plus its submodels), which
    is cheaper than it looks and far more robust than depending on what a bulk upsert does or
    does not return.
    """
    scraped_at = timezone.now()
    vehicles = [
        build_vehicle(payload, ref=fetched.ref, submodel=submodel, scraped_at=scraped_at)
        for submodel, payload in fetched.payloads
    ]
    if not vehicles:
        return 0, 0

    # A payload that echoed a different spelling could collide with another in the same batch;
    # last one wins, matching what the upsert would do anyway, but without tripping Postgres'
    # "cannot affect row a second time".
    deduplicated = {(v.year, v.make, v.model, v.submodel): v for v in vehicles}
    vehicles = list(deduplicated.values())

    pgbulk.upsert(
        src_models.WheelProsVehicle,
        vehicles,
        unique_fields=["year", "make", "model", "submodel"],
        update_fields=_VEHICLE_UPDATE_FIELDS,
    )

    written = src_models.WheelProsVehicle.objects.filter(
        year__in={v.year for v in vehicles},
        make__in={v.make for v in vehicles},
        model__in={v.model for v in vehicles},
        submodel__in={v.submodel for v in vehicles},
    ).values_list("year", "make", "model", "submodel", "id")
    # The filter is a cross-product of the four value sets, so it can match rows this unit did
    # not write (a sibling submodel of another model, say). Keying the map by the full tuple and
    # looking up only the tuples just built means those extras are simply never consulted.
    identity_to_id = {(row[0], row[1], row[2], row[3]): row[4] for row in written}

    axles: list[src_models.WheelProsVehicleAxle] = []
    for vehicle in vehicles:
        vehicle_id = identity_to_id.get((vehicle.year, vehicle.make, vehicle.model, vehicle.submodel))
        if vehicle_id is None:
            # Should not happen -- the row was just upserted -- but a missing id must never
            # become an axle row pointing at nothing.
            logger.warning("%s no id read back for %s; skipping its axles", _LOG_PREFIX, vehicle)
            continue
        axles.extend(build_axles(vehicle.raw_payload or {}, vehicle_id=vehicle_id))

    if axles:
        pgbulk.upsert(
            src_models.WheelProsVehicleAxle,
            axles,
            unique_fields=["vehicle", "position"],
            update_fields=_AXLE_UPDATE_FIELDS,
        )

    return len(vehicles), len(axles)


# -- the crawl ----------------------------------------------------------------------------------------


def run_crawl(
    client,
    refs: typing.Sequence[ModelRef],
    *,
    concurrency: int = DEFAULT_CONCURRENCY,
    rate_per_second: float = DEFAULT_RATE_PER_SECOND,
    checkpoint_path: typing.Optional[pathlib.Path] = None,
    resume: bool = True,
    dry_run: bool = False,
    max_submodels: typing.Optional[int] = None,
    progress: typing.Optional[typing.Callable[[str], None]] = None,
    stats: typing.Optional[CrawlStats] = None,
) -> CrawlStats:
    """
    Fetch every ref concurrently and write the results from this thread.

    Results are consumed as they arrive rather than gathered, so memory stays flat over a crawl
    of any length and the checkpoint advances continuously instead of at the end.
    """
    stats = stats or CrawlStats()
    emit = progress or (lambda message: None)
    throttled = _ThrottledClient(client, _TokenBucket(rate_per_second) if rate_per_second else None)

    with _Checkpoint(None if dry_run else checkpoint_path) as checkpoint:
        pending = list(refs)
        if resume:
            done = checkpoint.completed_keys()
            if done:
                pending = [ref for ref in pending if ref.key not in done]
                stats.models_skipped = len(refs) - len(pending)
                emit("resume: skipping {} models already in the checkpoint".format(stats.models_skipped))

        if not pending:
            return stats

        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
            futures = {
                pool.submit(fetch_model, throttled, ref, max_submodels=max_submodels): ref for ref in pending
            }
            for index, future in enumerate(concurrent.futures.as_completed(futures), start=1):
                ref = futures[future]
                try:
                    fetched = future.result()
                except wheelpros_exceptions.WheelProsVehiclePermissionError:
                    # Not entitled: every remaining call would fail the same way. Abandon the
                    # rest rather than burn the whole work list against a wall.
                    for remaining in futures:
                        remaining.cancel()
                    raise
                except Exception as exc:
                    stats.models_failed += 1
                    # Deliberately NOT checkpointed — the next run retries it.
                    logger.warning("%s failed on %s: %s", _LOG_PREFIX, ref, exc)
                    emit("  failed: {} ({})".format(ref, exc))
                    continue

                if fetched.missing:
                    stats.models_missing += 1
                    checkpoint.record(ref, status="missing", vehicles=0)
                    continue

                if dry_run:
                    vehicles_written, axles_written = len(fetched.payloads), 0
                else:
                    try:
                        vehicles_written, axles_written = persist_model(fetched)
                    except Exception as exc:
                        stats.models_failed += 1
                        logger.exception("%s write failed for %s: %s", _LOG_PREFIX, ref, exc)
                        emit("  write failed: {} ({})".format(ref, exc))
                        continue
                    checkpoint.record(ref, status="ok", vehicles=vehicles_written)

                stats.models_done += 1
                stats.vehicles_written += vehicles_written
                stats.axles_written += axles_written

                if index % 100 == 0 or index == len(pending):
                    emit(
                        "  {}/{} models — {} vehicles, {} axles, {} requests".format(
                            index, len(pending), stats.vehicles_written, stats.axles_written, client.requests_made
                        )
                    )

    stats.requests_made = client.requests_made
    return stats
