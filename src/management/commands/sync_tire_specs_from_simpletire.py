"""
Merge manufacturer-grade specs from the scraped SimpleTire catalog into ``tire_specs``.

See ``src/integrations/services/simpletire_sync.py`` for the match tiers and, more importantly,
for why each field is or is not taken from them.

**Nothing is written unless --apply is passed.** A bare run is a full dry run: it matches, builds
every update and reports exactly what would change, touching nothing. Unlike the enrichment
command there is no cost to a dry run -- no API is called, the whole thing is two table scans.

    # what would change, catalog-wide
    manage.py sync_tire_specs_from_simpletire

    # one brand, with a CSV of every proposed field change
    manage.py sync_tire_specs_from_simpletire --brands NITTO --report /tmp/nitto-sync.csv

    # commit
    manage.py sync_tire_specs_from_simpletire --apply

Run ``reparse_tire_sizes`` *after* this, never in the middle of it: rows written here are stamped
``spec_source='simpletire'`` and the reparse honours that stamp, but a row that has not been
stamped yet is still fair game for the parser.
"""
import csv
import pathlib

from django.core.management.base import BaseCommand, CommandError

from src.integrations.services import simpletire_sync, tire_enrichment


class Command(BaseCommand):
    help = (
        "Match tire_specs against simpletire_skus and merge the catalog-authoritative fields. "
        "Read-only unless --apply is given."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--brands",
            default=None,
            help="Comma-separated brand names, matched case-insensitively. Omit for the whole catalog.",
        )
        parser.add_argument("--limit", type=int, default=None, help="Stop after this many tire_specs.")
        parser.add_argument(
            "--apply",
            action="store_true",
            help="Write the merge. Without it, nothing is written.",
        )
        parser.add_argument(
            "--report",
            default=None,
            metavar="PATH",
            help="Write a per-row CSV of proposed changes, including the rows that did not match.",
        )

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
                    "matched": "" if found is None else "tier{}".format(found.tier),
                    "simpletire_sku": "" if found is None else found.sku["id"],
                    "their_model": "" if found is None else (found.sku.get("product_line_name") or ""),
                    "our_model": spec.model_name or "",
                    "changes": "; ".join("{}: {} -> {}".format(f, getattr(spec, f), v) for f, v in updates.items()),
                }
            )

        stats = simpletire_sync.run(
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
        matched_pct = 100 * stats.matched / stats.scanned if stats.scanned else 0
        self.stdout.write("  matched                 {} ({:.1f}%)".format(stats.matched, matched_pct))
        for tier in sorted(stats.by_tier):
            label = {1: "brand + part number", 2: "part number + size", 3: "brand + model + size"}[tier]
            self.stdout.write("    tier {} {:<22}{}".format(tier, label, stats.by_tier[tier]))
        self.stdout.write("  unmatched               {}".format(stats.unmatched))

        self.stdout.write("\nProposed changes")
        self.stdout.write("  rows with a change      {}".format(stats.changed))
        self.stdout.write("  rows already identical  {}".format(stats.unchanged))
        for field, count in sorted(stats.field_changes.items(), key=lambda kv: -kv[1]):
            self.stdout.write("    {:<24}{}".format(field, count))

        if stats.unmapped_load_ranges:
            self.stdout.write(self.style.WARNING("\nLoad ranges left alone (not in load_range_ply)"))
            for value, count in sorted(stats.unmapped_load_ranges.items(), key=lambda kv: -kv[1])[:10]:
                self.stdout.write("    {:<24}{}".format(value, count))
        if stats.unmapped_categories:
            self.stdout.write(self.style.WARNING("\nCategories with no mapping (tread_category left alone)"))
            for value, count in sorted(stats.unmapped_categories.items(), key=lambda kv: -kv[1])[:10]:
                self.stdout.write("    {:<24}{}".format(value, count))
        if stats.category_conflicts:
            total = sum(stats.category_conflicts.values())
            self.stdout.write(
                self.style.WARNING("\nSame-axis category conflicts: {} (ours kept, review these)".format(total))
            )
            for value, count in sorted(stats.category_conflicts.items(), key=lambda kv: -kv[1])[:10]:
                self.stdout.write("    {:<24}{}".format(value.replace("category-conflict:", ""), count))

        if stats.samples:
            self.stdout.write("\nSamples")
            for line in stats.samples:
                self.stdout.write("  {}".format(line))

        if applied:
            self.stdout.write(self.style.SUCCESS("\nWritten: {} rows".format(stats.written)))
        else:
            self.stdout.write(self.style.WARNING("\nNothing written -- pass --apply to commit."))
