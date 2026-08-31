"""
Pull a manufacturer's own tire data into ``raw_tire_specs``.

The machinery lives in ``src/integrations/brand_data/``; read
``docs/BRAND_TIRE_DATA_INITIATIVE.md`` before adding a source. Nothing here writes to
``tire_specs`` -- this command collects, and the merge is a separate step.

Typical runs:

    # what the registry can run at all
    manage.py ingest_brand_tire_specs --list

    # read and map a brand's sheet, write nothing, see what did not parse
    manage.py ingest_brand_tire_specs --source michelin --dry-run

    # a new file from the same brand, this quarter's edition
    manage.py ingest_brand_tire_specs --source michelin --file resources/brand_data/michelin/2026-q3.xlsx

    # the real thing, dropping rows the brand no longer lists
    manage.py ingest_brand_tire_specs --source michelin --prune

    # everything active, skipping sources whose file has not changed
    manage.py ingest_brand_tire_specs --all --skip-unchanged

A run that fails leaves a ``failed`` row in ``tire_brand_source_runs`` with the error on it; with
``--all`` one source failing does not stop the others, and the command exits non-zero at the end.
"""
from django.core.management.base import BaseCommand, CommandError

from src import models as src_models
from src.integrations.brand_data import base, ingest
from src.integrations.brand_data import registry as brand_registry


class Command(BaseCommand):
    help = (
        "Pull tire specifications published by the manufacturers themselves into raw_tire_specs, "
        "from the sources registered in tire_brand_sources. Idempotent; safe to re-run."
    )

    def add_arguments(self, parser):
        parser.add_argument("--source", action="append", dest="slugs", default=None, help="Source slug. Repeatable.")
        parser.add_argument("--brand", default="", help="Every source for this brand name.")
        parser.add_argument("--all", action="store_true", help="Every active source with a handler.")
        parser.add_argument("--list", action="store_true", help="Print the runnable registry and exit.")
        parser.add_argument(
            "--file",
            default="",
            help="Read this file instead of the path in the source's config. File-based sources only.",
        )
        parser.add_argument("--limit", type=int, default=None, help="Stop after N records. Implies no --prune.")
        parser.add_argument("--dry-run", action="store_true", help="Read and map; write no spec rows.")
        parser.add_argument(
            "--skip-unchanged",
            action="store_true",
            help="Skip a source whose input is byte-identical to its last success. File sources only.",
        )
        parser.add_argument(
            "--prune",
            action="store_true",
            help="Delete rows this run did not see -- the brand's withdrawals. Refused with --limit.",
        )

    def handle(self, *args, **options):
        if options["list"]:
            self._list()
            return

        sources = self._sources(options)
        if not sources:
            raise CommandError("no sources selected -- try --list, or --all")
        if options["file"] and len(sources) > 1:
            raise CommandError("--file names one file, so it can only be used with one source")

        results = ingest.run_due(
            sources=sources,
            file_override=options["file"],
            limit=options["limit"],
            dry_run=options["dry_run"],
            skip_unchanged=options["skip_unchanged"],
            prune=options["prune"],
            progress=self.stdout.write,
        )
        self._report(results, dry_run=options["dry_run"])

        failures = [result for result in results if result.status == src_models.TireBrandSourceRun.STATUS_FAILED]
        if failures:
            raise CommandError(f"{len(failures)} of {len(results)} source(s) failed -- see above")

    def _sources(self, options):
        queryset = src_models.TireBrandSource.objects.all()
        if options["slugs"]:
            sources = list(queryset.filter(slug__in=options["slugs"]))
            missing = set(options["slugs"]) - {source.slug for source in sources}
            if missing:
                raise CommandError(f"no such source(s): {', '.join(sorted(missing))}")
            return sources
        if options["brand"]:
            return list(queryset.filter(brand_name__iexact=options["brand"]))
        if options["all"]:
            return list(queryset.filter(status=src_models.TireBrandSource.STATUS_ACTIVE).exclude(handler=""))
        return []

    def _list(self):
        rows = src_models.TireBrandSource.objects.order_by("priority", "brand_name")
        if not rows:
            self.stdout.write("The registry is empty. Seed it: manage.py seed_tire_brand_sources --from-catalog")
            return
        self.stdout.write(f"loaders registered: {', '.join(brand_registry.loader_names())}")
        self.stdout.write("")
        self.stdout.write(f"  {'slug':<32} {'brand':<24} {'method':<8} {'handler':<12} {'status':<9} last success")
        for source in rows:
            last = source.last_success_at.strftime("%Y-%m-%d") if source.last_success_at else "never"
            marker = " " if source.is_runnable else "-"
            self.stdout.write(
                f"{marker} {source.slug:<32} {source.brand_name[:23]:<24} {source.method:<8} "
                f"{(source.handler or '-'):<12} {source.status:<9} {last}"
            )
        self.stdout.write("")
        self.stdout.write("'-' marks a source this command cannot run: no handler, or retired.")

    def _report(self, results, *, dry_run):
        verb = "would write" if dry_run else "wrote"
        self.stdout.write("")
        for result in results:
            if result.status == src_models.TireBrandSourceRun.STATUS_FAILED:
                self.stdout.write(f"  {result.source_slug}: FAILED -- {result.error}")
                continue
            if result.status == src_models.TireBrandSourceRun.STATUS_SKIPPED:
                self.stdout.write(f"  {result.source_slug}: skipped, input unchanged since last success")
                continue
            self.stdout.write(
                f"  {result.source_slug}: {result.seen} records, {verb} "
                f"{result.created} new / {result.updated} changed, {result.unchanged} unchanged"
            )
            if result.skipped:
                self.stdout.write(f"      {result.skipped} records identified no tire and were dropped")
            if result.pruned:
                self.stdout.write(f"      {result.pruned} rows pruned (no longer listed by the brand)")
            if result.with_warnings:
                self.stdout.write(f"      {result.with_warnings} rows carry a warning:")
                for warning, count in result.warning_counts.most_common(8):
                    self.stdout.write(f"        {count:>7}  {warning}")
            if result.unmapped_columns:
                unmapped = ", ".join(column for column, _ in result.unmapped_columns.most_common(12))
                self.stdout.write(f"      published but not mapped: {unmapped}")
            self.stdout.write(f"      read: {result.input_label}")
