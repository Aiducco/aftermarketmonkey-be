"""
Run one registered source and land what it produced in ``raw_tire_specs``.

Every source goes through here, whatever read it. Loaders differ in how bytes arrive and in
nothing else; identity, change detection, warnings, counting and the run log are the same problem
for a scraped site and a mailed spreadsheet, and having one copy of them is what keeps a new brand
cheap.

The three decisions this module owns
------------------------------------
**Identity.** ``(source, external_key)`` is the row. ``external_key`` is the brand's own article
number when they publish one -- that is the only identifier with a real guarantee behind it. When
they publish none, it is a digest of what actually identifies the tire (brand, part number, size,
model, sub-model, load range), so that re-running the same file updates rows instead of doubling
them. See :func:`derive_external_key`, including the note on what changing that digest costs.

**Change.** A row is written only when its values moved. Manufacturer files are re-sent whole and
mostly unchanged, and rewriting 40,000 identical rows every quarter makes ``updated_at`` useless
for the one question it is good for -- what changed. Rows that are unchanged still get their
``last_seen_*`` stamped, which is a much cheaper write and is what makes "gone from the source"
answerable.

**Absence.** A row the newest full run did not see is a row the brand withdrew. That is only true
of a *full* run, so ``--prune`` refuses to act on a limited one: deleting a brand's catalog
because somebody passed ``--limit 50`` is exactly the kind of destructive surprise a landing zone
should not be able to spring.

Nothing here writes to ``tire_specs``. See ``docs/BRAND_TIRE_DATA_INITIATIVE.md``.
"""
import collections
import dataclasses
import hashlib
import json
import logging
import typing

from django.db import transaction
from django.utils import timezone

from src import models as src_models
from src.integrations.brand_data import base, mapping
from src.integrations.brand_data import registry as brand_registry

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[BRAND-DATA]"

BATCH_SIZE = 500

# Columns a source can fill, plus the ones we derive from what it filled. Every write goes through
# this list, so a column added to the model but not here is simply never written -- asserted below
# rather than left to be discovered as a permanently NULL column.
MAPPED_COLUMNS = tuple(sorted(mapping.COLUMN_KINDS))
DERIVED_COLUMNS = ("brand_key", "part_number_key", "model_key", "parsed_size")
VALUE_COLUMNS = MAPPED_COLUMNS + DERIVED_COLUMNS
PAYLOAD_COLUMNS = ("raw", "attributes", "warnings", "content_hash")

_model_fields = {field.name for field in src_models.RawTireSpec._meta.get_fields()}
_missing = [column for column in VALUE_COLUMNS + PAYLOAD_COLUMNS if column not in _model_fields]
assert not _missing, f"ingest writes columns raw_tire_specs does not have: {_missing}"


@dataclasses.dataclass
class RunStats:
    """What one run did. Mirrored onto the ``TireBrandSourceRun`` row and printed by the command."""

    source_slug: str = ""
    status: str = src_models.TireBrandSourceRun.STATUS_RUNNING
    dry_run: bool = False
    seen: int = 0
    created: int = 0
    updated: int = 0
    unchanged: int = 0
    skipped: int = 0
    with_warnings: int = 0
    pruned: int = 0
    input_label: str = ""
    fingerprint: str = ""
    error: str = ""
    warning_counts: typing.Counter = dataclasses.field(default_factory=collections.Counter)
    unmapped_columns: typing.Counter = dataclasses.field(default_factory=collections.Counter)

    @property
    def written(self) -> int:
        return self.created + self.updated

    def as_json(self) -> typing.Dict[str, typing.Any]:
        return {
            "warnings": dict(self.warning_counts.most_common(25)),
            # What the source published that no column takes. The first thing to read when a brand
            # turns out to carry a field we wanted: it is already here, in ``attributes``.
            "unmapped_columns": dict(self.unmapped_columns.most_common(40)),
            "pruned": self.pruned,
        }


def run_source(
    source: src_models.TireBrandSource,
    *,
    file_override: str = "",
    limit: typing.Optional[int] = None,
    dry_run: bool = False,
    skip_unchanged: bool = False,
    prune: bool = False,
    progress: typing.Callable[[str], None] = lambda message: None,
) -> RunStats:
    """
    Pull one source end to end. Raises :class:`base.BrandDataError` for anything the operator can fix.

    ``dry_run`` reads and maps and writes no spec rows -- the run row is still recorded, because a
    dry run that found the file empty is exactly as worth knowing about as a real one.
    """
    stats = RunStats(source_slug=source.slug, dry_run=dry_run)
    field_map = (source.config or {}).get("field_map") or {}
    mapping.validate_field_map(field_map)
    if not field_map:
        raise base.SourceConfigError(
            f"{source.slug}: config has no 'field_map'. Without one nothing in the source's records "
            f"reaches a column -- see src/integrations/brand_data/mapping.py for the shape."
        )

    loader = brand_registry.resolve(source)
    ctx = base.LoaderContext(
        source=source,
        config=dict(source.config or {}),
        file_override=file_override,
        limit=limit,
        progress=progress,
    )

    if skip_unchanged:
        skipped_run = _skip_if_unchanged(source, loader, ctx, stats)
        if skipped_run is not None:
            return skipped_run

    run = src_models.TireBrandSourceRun.objects.create(source=source, dry_run=dry_run)
    source.last_run_at = run.started_at
    source.save(update_fields=["last_run_at", "updated_at"])

    try:
        _consume(source, run, loader, ctx, field_map, stats, dry_run=dry_run, progress=progress)
        if prune and not dry_run:
            stats.pruned = _prune(source, run, limit=limit, progress=progress)
        stats.status = src_models.TireBrandSourceRun.STATUS_SUCCESS
    except base.BrandDataError as exc:
        stats.status = src_models.TireBrandSourceRun.STATUS_FAILED
        stats.error = str(exc)
        _finish(source, run, ctx, stats)
        raise
    except Exception as exc:  # noqa: BLE001 - the run row must record it before it propagates
        stats.status = src_models.TireBrandSourceRun.STATUS_FAILED
        stats.error = f"{type(exc).__name__}: {exc}"
        _finish(source, run, ctx, stats)
        raise

    _finish(source, run, ctx, stats)
    logger.info(
        "%s %s: seen=%s created=%s updated=%s unchanged=%s skipped=%s",
        _LOG_PREFIX,
        source.slug,
        stats.seen,
        stats.created,
        stats.updated,
        stats.unchanged,
        stats.skipped,
    )
    return stats


def run_due(
    *,
    sources: typing.Optional[typing.Iterable[src_models.TireBrandSource]] = None,
    progress: typing.Callable[[str], None] = lambda message: None,
    **kwargs: typing.Any,
) -> typing.List[RunStats]:
    """
    Run several sources, in registry priority order, without letting one failure stop the rest.

    A brand's portal being down is not a reason to skip the twelve brands after it, so failures
    are collected onto their own stats row and the loop continues. The command reports them
    together at the end and exits non-zero.
    """
    if sources is None:
        sources = src_models.TireBrandSource.objects.filter(status=src_models.TireBrandSource.STATUS_ACTIVE).exclude(
            handler=""
        )
    results = []
    for source in sorted(sources, key=lambda row: (row.priority, row.brand_name)):
        progress(f"{source.brand_name} ({source.slug})")
        try:
            results.append(run_source(source, progress=progress, **kwargs))
        except base.BrandDataError as exc:
            progress(f"  FAILED: {exc}")
            results.append(
                RunStats(
                    source_slug=source.slug,
                    status=src_models.TireBrandSourceRun.STATUS_FAILED,
                    error=str(exc),
                )
            )
    return results


# ---------------------------------------------------------------------------------------------
# The pull
# ---------------------------------------------------------------------------------------------
def _consume(
    source: src_models.TireBrandSource,
    run: src_models.TireBrandSourceRun,
    loader: base.BrandLoader,
    ctx: base.LoaderContext,
    field_map: typing.Mapping[str, typing.Any],
    stats: RunStats,
    *,
    dry_run: bool,
    progress: typing.Callable[[str], None],
) -> None:
    batch: typing.Dict[str, typing.Tuple[mapping.MappedRow, base.SourceRecord]] = {}
    mapped_keys = _mapped_source_keys(field_map)

    for record in loader(ctx):
        stats.seen += 1
        row = mapping.map_record(record.payload, field_map)
        if not row.identifies_a_tire:
            stats.skipped += 1
            continue

        key = derive_external_key(record.key, row.values)
        if key is None:
            stats.skipped += 1
            continue

        for warning in row.warnings:
            stats.warning_counts[warning.split(":")[0]] += 1
        if row.warnings:
            stats.with_warnings += 1
        for column in row.attributes:
            if column not in row.used_keys and column not in mapped_keys:
                stats.unmapped_columns[column] += 1

        # Last row wins on a duplicate key inside one file: a brand that lists the same article
        # twice is listing a correction, not a second tire.
        batch[key] = (row, record)
        if len(batch) >= BATCH_SIZE:
            _write_batch(source, run, batch, stats, dry_run=dry_run)
            progress(f"  {stats.seen} records read, {stats.written} written")
            batch = {}

    if batch:
        _write_batch(source, run, batch, stats, dry_run=dry_run)


def _mapped_source_keys(field_map: typing.Mapping[str, typing.Any]) -> typing.Set[str]:
    """Every source key the map names, in any of the three spec forms -- so 'unmapped' means it."""
    keys: typing.Set[str] = set()
    for spec in field_map.values():
        if isinstance(spec, str):
            keys.add(spec)
        elif isinstance(spec, (list, tuple)):
            keys.update(str(item) for item in spec)
        elif isinstance(spec, dict):
            paths = spec.get("paths") or ([spec["path"]] if spec.get("path") else [])
            keys.update(str(item) for item in paths)
    return keys


def derive_external_key(
    record_key: typing.Optional[str], values: typing.Mapping[str, typing.Any]
) -> typing.Optional[str]:
    """
    The identity of one row within one source.

    A brand's own article number is used whenever there is one, because it is the only identifier
    the brand itself promises to keep stable. Failing that -- and most spreadsheets have no such
    column -- the key is a digest of the fields that actually distinguish one SKU from another in
    a tire line.

    **Changing the digest's inputs re-keys every row of every source that relies on it**, which
    orphans the old rows rather than updating them. If it ever has to change, re-run the affected
    sources with ``--prune`` in the same pass so the orphans go with it.

    Returns None when the record carries nothing that identifies anything, which is the signature
    of a totals row or a page footer that reached the mapper.
    """
    explicit = record_key or values.get("external_key")
    if explicit:
        return str(explicit)[:255]

    parts = [
        str(values.get(column) or "")
        for column in (
            "brand_key",
            "part_number_key",
            "size_display",
            "model_key",
            "sub_model",
            "load_range",
            "sidewall_style",
        )
    ]
    if not any(parts):
        return None
    digest = hashlib.sha1("|".join(parts).upper().encode("utf-8")).hexdigest()[:32]
    # Prefixed so that a key we made is never mistaken for one the brand published -- the two
    # behave differently when the source's own numbering changes.
    return f"d:{digest}"


def content_hash(values: typing.Mapping[str, typing.Any], raw: typing.Any, warnings: typing.Sequence[str]) -> str:
    """
    A digest of everything a re-run could change on the row.

    Covers the mapped values *and* the raw record: a mapping fix changes the values and so writes,
    a source correcting a field nobody maps still updates ``raw`` so the correction is not lost,
    and a file re-sent unchanged writes nothing.
    """
    payload = {
        "values": {column: _stable(values.get(column)) for column in VALUE_COLUMNS},
        "raw": raw,
        "warnings": list(warnings),
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode("utf-8")).hexdigest()


def _stable(value: typing.Any) -> typing.Any:
    return str(value) if value is not None and not isinstance(value, (str, int, float, bool, dict, list)) else value


def _write_batch(
    source: src_models.TireBrandSource,
    run: src_models.TireBrandSourceRun,
    batch: typing.Dict[str, typing.Tuple[mapping.MappedRow, base.SourceRecord]],
    stats: RunStats,
    *,
    dry_run: bool,
) -> None:
    now = timezone.now()
    prepared = {}
    for key, (row, record) in batch.items():
        values = {column: row.values.get(column) for column in VALUE_COLUMNS}
        values["external_key"] = key
        raw = record.payload if isinstance(record.payload, (dict, list)) else {"value": str(record.payload)}
        prepared[key] = (
            values,
            {
                "raw": raw,
                "attributes": row.attributes,
                "warnings": row.warnings,
                "content_hash": content_hash(values, raw, row.warnings),
            },
        )

    if dry_run:
        existing_hashes = dict(
            src_models.RawTireSpec.objects.filter(source=source, external_key__in=prepared).values_list(
                "external_key", "content_hash"
            )
        )
        for key, (_, payload) in prepared.items():
            if key not in existing_hashes:
                stats.created += 1
            elif existing_hashes[key] != payload["content_hash"]:
                stats.updated += 1
            else:
                stats.unchanged += 1
        return

    with transaction.atomic():
        existing = {
            row.external_key: row
            for row in src_models.RawTireSpec.objects.select_for_update().filter(
                source=source, external_key__in=list(prepared)
            )
        }

        creates, updates, unchanged_ids = [], [], []
        for key, (values, payload) in prepared.items():
            current = existing.get(key)
            if current is None:
                creates.append(
                    src_models.RawTireSpec(
                        source=source,
                        last_seen_run=run,
                        last_seen_at=now,
                        last_changed_at=now,
                        **values,
                        **payload,
                    )
                )
                continue
            if current.content_hash == payload["content_hash"]:
                unchanged_ids.append(current.pk)
                continue
            for column, value in {**values, **payload}.items():
                setattr(current, column, value)
            current.last_seen_run = run
            current.last_seen_at = now
            current.last_changed_at = now
            updates.append(current)

        if creates:
            src_models.RawTireSpec.objects.bulk_create(creates, batch_size=BATCH_SIZE)
            stats.created += len(creates)
        if updates:
            src_models.RawTireSpec.objects.bulk_update(
                updates,
                list(VALUE_COLUMNS) + list(PAYLOAD_COLUMNS) + ["last_seen_run", "last_seen_at", "last_changed_at"],
                batch_size=BATCH_SIZE,
            )
            stats.updated += len(updates)
        if unchanged_ids:
            # Deliberately not a bulk_update of whole rows: "we looked and it had not moved" is
            # two columns, and writing forty to say it is what makes updated_at meaningless.
            src_models.RawTireSpec.objects.filter(pk__in=unchanged_ids).update(last_seen_run=run, last_seen_at=now)
            stats.unchanged += len(unchanged_ids)


def _prune(
    source: src_models.TireBrandSource,
    run: src_models.TireBrandSourceRun,
    *,
    limit: typing.Optional[int],
    progress: typing.Callable[[str], None],
) -> int:
    """Delete rows this run did not see. Only meaningful after a complete pull -- see the module docstring."""
    if limit is not None:
        raise base.SourceConfigError("--prune cannot run with --limit: a partial pull says nothing about absence")
    stale = src_models.RawTireSpec.objects.filter(source=source).exclude(last_seen_run=run)
    count = stale.count()
    if count:
        progress(f"  pruning {count} rows the source no longer lists")
        stale.delete()
    return count


def _skip_if_unchanged(
    source: src_models.TireBrandSource,
    loader: base.BrandLoader,
    ctx: base.LoaderContext,
    stats: RunStats,
) -> typing.Optional[RunStats]:
    """
    Stop before pulling when the input is byte-identical to the last success.

    Only possible for loaders that can fingerprint their input without reading all of it -- a file
    can be hashed, an API cannot be asked whether it changed. Loaders advertise the ability by
    hanging a ``fingerprint`` callable on the loader function; the rest simply do not skip.
    """
    fingerprinter = getattr(loader, "fingerprint", None)
    if fingerprinter is None:
        return None
    fingerprint = fingerprinter(ctx)
    if not fingerprint:
        return None
    last = (
        src_models.TireBrandSourceRun.objects.filter(source=source, status=src_models.TireBrandSourceRun.STATUS_SUCCESS)
        .order_by("-started_at")
        .first()
    )
    if last is None or last.input_fingerprint != fingerprint:
        return None

    run = src_models.TireBrandSourceRun.objects.create(
        source=source,
        status=src_models.TireBrandSourceRun.STATUS_SKIPPED,
        finished_at=timezone.now(),
        input_label=ctx.input_label,
        input_fingerprint=fingerprint,
    )
    source.last_run_at = run.started_at
    source.save(update_fields=["last_run_at", "updated_at"])
    stats.status = src_models.TireBrandSourceRun.STATUS_SKIPPED
    stats.fingerprint = fingerprint
    stats.input_label = ctx.input_label
    return stats


def _finish(
    source: src_models.TireBrandSource,
    run: src_models.TireBrandSourceRun,
    ctx: base.LoaderContext,
    stats: RunStats,
) -> None:
    stats.input_label = ctx.input_label
    stats.fingerprint = ctx.fingerprint

    run.status = stats.status
    run.finished_at = timezone.now()
    run.input_label = ctx.input_label
    run.input_fingerprint = ctx.fingerprint
    run.rows_seen = stats.seen
    run.rows_created = stats.created
    run.rows_updated = stats.updated
    run.rows_unchanged = stats.unchanged
    run.rows_skipped = stats.skipped
    run.rows_with_warnings = stats.with_warnings
    run.error = stats.error
    run.stats = stats.as_json()
    run.save()

    if stats.status == src_models.TireBrandSourceRun.STATUS_SUCCESS and not stats.dry_run:
        source.last_success_at = run.finished_at
        source.last_row_count = stats.seen
        source.save(update_fields=["last_success_at", "last_row_count", "updated_at"])
