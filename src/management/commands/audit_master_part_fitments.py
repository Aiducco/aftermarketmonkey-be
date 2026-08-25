"""
Report ``MasterPartFitment`` coverage per brand, split by whether the brand's parts were ever
supposed to have fitment.

The rules live in src/integrations/utils/fitment_expectation.py and the scan in
src/integrations/services/fitment_audit.py. This command only formats the result.

**Writes nothing, ever.** There is no --apply. The audit's staging lives in session TEMP tables
that disappear with the connection.

Typical runs:
    # headline plus the worst brands
    manage.py audit_master_part_fitments

    # every brand, machine-readable, for a spreadsheet
    manage.py audit_master_part_fitments --csv /tmp/fitment_audit.csv --top 0
"""
import csv

from django.core.management.base import BaseCommand

from src.integrations.services import fitment_audit


class Command(BaseCommand):
    help = (
        "Audit master part fitment coverage per brand. Separates brands that are missing "
        "fitment from brands that should not have any (tires, wheels, apparel, tools). "
        "Read-only."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--top",
            type=int,
            default=40,
            help="How many brands to print per section. 0 prints all of them.",
        )
        parser.add_argument(
            "--csv",
            default=None,
            help="Write the full per-brand table to this path. Every brand, not just --top.",
        )

    def handle(self, *args, **options):
        result = fitment_audit.run()
        top = options["top"] or None

        self.stdout.write("\nParts by expectation")
        self.stdout.write("{:<16} {:>9} {:>9} {:>9} {:>9}".format(
            "expectation", "parts", "fitted", "gap", "gap w/src"))
        for expectation, rows in _by_expectation(result["part_totals"]).items():
            self.stdout.write("{:<16} {:>9,} {:>9,} {:>9,} {:>9,}".format(
                expectation, rows["parts"], rows["fitted"], rows["gap"], rows["gap_capable"]))

        labelled = [(fitment_audit.classify_brand(row), row) for row in result["brands"]]
        counts = {}
        for label, row in labelled:
            entry = counts.setdefault(label, {"brands": 0, "parts": 0, "missing": 0})
            entry["brands"] += 1
            entry["parts"] += row[2]
            # The brand's own gap, not its size. A brand landing in pipeline_gap because 200 of
            # its 40,000 parts are missing fitment should not read as 40,000 missing parts.
            entry["missing"] += row[7] + row[8]

        self.stdout.write("\nBrands by verdict")
        self.stdout.write("{:<16} {:>9} {:>11} {:>11}".format(
            "verdict", "brands", "parts", "missing"))
        for label in ("ok", "not_applicable", "pipeline_gap", "sourcing_gap", "mixed", "unknown"):
            entry = counts.get(label)
            if entry:
                self.stdout.write("{:<16} {:>9,} {:>11,} {:>11,}".format(
                    label, entry["brands"], entry["parts"], entry["missing"]))

        self._section(
            "Pipeline gaps -- we pull a fitment-carrying feed for these and still have none",
            [row for label, row in labelled if label == "pipeline_gap"],
            key=lambda row: row[7],
            column="missing",
            value=lambda row: row[7],
            top=top,
        )
        self._section(
            "Sourcing gaps -- vehicle-specific, but no feed we pull carries fitment for them",
            [row for label, row in labelled if label == "sourcing_gap"],
            key=lambda row: row[8],
            column="missing",
            value=lambda row: row[8],
            top=top,
        )
        self._section(
            "Correctly empty -- nothing in these catalogs has vehicle fitment",
            [row for label, row in labelled if label == "not_applicable"],
            key=lambda row: row[2],
            column="parts",
            value=lambda row: row[2],
            top=top,
        )
        self._section(
            "Unclassified -- no signal decided. Not a gap and not an exemption.",
            [row for label, row in labelled if label == "unknown"],
            key=lambda row: row[2],
            column="parts",
            value=lambda row: row[2],
            top=top,
        )

        self.stdout.write("\nWhich rule decided what")
        self.stdout.write("{:<36} {:<16} {:>9} {:>9}".format(
            "source", "expectation", "parts", "fitted"))
        for source, expectation, parts, fitted in result["by_source"][:30]:
            self.stdout.write("{:<36} {:<16} {:>9,} {:>9,}".format(
                source, expectation, parts, fitted))

        if options["csv"]:
            self._write_csv(options["csv"], labelled)
            self.stdout.write(self.style.SUCCESS(
                "\nWrote {} brands to {}".format(len(labelled), options["csv"])))

    def _section(self, title, rows, key, column, value, top):
        rows = sorted(rows, key=key, reverse=True)
        self.stdout.write("\n{} ({:,} brands)".format(title, len(rows)))
        if not rows:
            return
        self.stdout.write("  {:>9} {:>9} {:>9}  brand".format(column, "parts", "fitted"))
        for row in rows[:top] if top else rows:
            self.stdout.write("  {:>9,} {:>9,} {:>9,}  {}".format(value(row), row[2], row[3], row[1]))

    def _write_csv(self, path, labelled):
        with open(path, "w", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow([
                "brand_id", "brand", "verdict", "parts", "fitted", "coverage_pct",
                "expected_parts", "not_applicable_parts", "unknown_parts",
                "missing_with_capable_provider", "missing_without_capable_provider",
                "parts_via_fitment_capable_provider",
            ])
            for label, row in sorted(labelled, key=lambda item: item[1][2], reverse=True):
                (brand_id, name, parts, fitted, expect_parts, na_parts,
                 unknown_parts, bug_parts, sourcing_parts, capable) = row
                writer.writerow([
                    brand_id, name, label, parts, fitted, round(100 * fitted / parts, 2),
                    expect_parts, na_parts, unknown_parts, bug_parts, sourcing_parts, capable,
                ])


def _by_expectation(part_totals):
    """Fold the (expectation, has_fit, fit_capable, count) grid into one row per expectation."""
    out = {}
    for expectation, has_fit, fit_capable, count in part_totals:
        entry = out.setdefault(expectation, {"parts": 0, "fitted": 0, "gap": 0, "gap_capable": 0})
        entry["parts"] += count
        if has_fit:
            entry["fitted"] += count
        else:
            entry["gap"] += count
            if fit_capable:
                entry["gap_capable"] += count
    return dict(sorted(out.items(), key=lambda item: -item[1]["parts"]))
