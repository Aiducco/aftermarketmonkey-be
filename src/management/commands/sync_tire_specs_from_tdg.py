"""
Merge the TDG catalog into ``tire_specs``, behind SimpleTire.

See ``src/integrations/services/tdg_sync.py`` for the precedence rule and for the two TDG columns
that are deliberately never read (its warranty figure is kilometres despite the column name, and
its ``max_load_lb`` is empty on every row).

**Nothing is written unless --apply is passed.** A dry run costs nothing -- no API, two table scans.

Order matters. Run this *after* ``sync_tire_specs_from_simpletire``: a row SimpleTire already owns
only accepts TDG values where it is empty, so running TDG first would let the thinner catalog
claim fields the better one was going to fill.

    manage.py sync_tire_specs_from_tdg                      # what would change
    manage.py sync_tire_specs_from_tdg --brands MICHELIN    # the brands simpletire never had
    manage.py sync_tire_specs_from_tdg --apply
"""
import csv
import pathlib

from django.core.management.base import BaseCommand, CommandError

from src.integrations.services import tdg_sync, tire_enrichment


class Command(BaseCommand):
    help = "Match tire_specs against tdg_products and merge what TDG is authoritative for. Read-only unless --apply."

    def add_arguments(self, parser):
        parser.add_argument("--brands", default=None, help="Comma-separated brand names. Omit for the whole catalog.")
        parser.add_argument("--limit", type=int, default=None, help="Stop after this many tire_specs.")
        parser.add_argument("--apply", action="store_true", help="Write the merge.")
        parser.add_argument("--report", default=None, metavar="PATH", help="Per-row CSV of proposed changes.")

    def handle(self, *args, **options):
        brand_ids = None
        if options["brands"]:
            names = [n.strip() for n in options["brands"].split(",") if n.strip()]
            resolved = tire_enrichment.resolve_brand_ids(names)
            unknown = sorted(set(names) - set(resolved))
            if unknown:
                raise CommandError("Unknown brand(s): {}".format(", ".join(unknown)))
            brand_ids = list(resolved.values())

        report_rows = []
        report_path = pathlib.Path(options["report"]) if options["report"] else None

        def collect(spec, found, updates):
            report_rows.append(
                {
                    "master_part_id": spec.master_part_id,
                    "brand": spec.master_part.brand.name if spec.master_part.brand_id else "",
                    "part_number": spec.master_part.part_number,
                    "size_display": spec.size_display,
                    "spec_source_before": spec.spec_source,
                    "matched": "" if found is None else "tier{}".format(found.tier),
                    "tdg_id": "" if found is None else found.row["id"],
                    "changes": "; ".join("{}: {} -> {}".format(f, getattr(spec, f), v) for f, v in updates.items()),
                }
            )

        stats = tdg_sync.run(
            brand_ids=brand_ids,
            apply_changes=options["apply"],
            limit=options["limit"],
            on_result=collect if report_path else None,
        )

        if report_path and report_rows:
            with report_path.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=list(report_rows[0]))
                writer.writeheader()
                writer.writerows(report_rows)
            self.stdout.write("Report: {} ({} rows)".format(report_path, len(report_rows)))

        self._report(stats, applied=options["apply"])

    def _report(self, stats, *, applied):
        self.stdout.write("\nMatching")
        self.stdout.write("  tire_specs scanned      {}".format(stats.scanned))
        pct = 100 * stats.matched / stats.scanned if stats.scanned else 0
        self.stdout.write("  matched                 {} ({:.1f}%)".format(stats.matched, pct))
        for tier in sorted(stats.by_tier):
            label = {1: "brand + part number", 2: "part number + size", 3: "brand + model + size"}[tier]
            self.stdout.write("    tier {} {:<22}{}".format(tier, label, stats.by_tier[tier]))
        self.stdout.write("  unmatched               {}".format(stats.unmatched))

        self.stdout.write("\nWhat the match is worth")
        self.stdout.write("  no catalog had these    {}  (TDG becomes their source)".format(stats.new_to_any_catalog))
        self.stdout.write("  SimpleTire already had  {}  (TDG may only fill gaps)".format(stats.behind_simpletire))

        self.stdout.write("\nProposed changes")
        self.stdout.write("  rows with a change      {}".format(stats.changed))
        for field, count in sorted(stats.field_changes.items(), key=lambda kv: -kv[1]):
            self.stdout.write("    {:<24}{}".format(field, count))

        if stats.unmapped_load_ranges:
            self.stdout.write(self.style.WARNING("\nLoad ranges left alone (not in load_range_ply)"))
            for value, count in sorted(stats.unmapped_load_ranges.items(), key=lambda kv: -kv[1])[:8]:
                self.stdout.write("    {:<24}{}".format(value, count))
        if stats.run_flat_conflicts:
            self.stdout.write(
                self.style.WARNING(
                    "\nrun-flat: {} rows where TDG contradicts an answer we already had "
                    "(ours kept -- it came from an explicit RF in the distributor's title)".format(
                        stats.run_flat_conflicts
                    )
                )
            )

        if stats.samples:
            self.stdout.write("\nSamples")
            for line in stats.samples:
                self.stdout.write("  {}".format(line))

        if applied:
            self.stdout.write(self.style.SUCCESS("\nWritten: {} rows".format(stats.written)))
        else:
            self.stdout.write(self.style.WARNING("\nNothing written -- pass --apply to commit."))
