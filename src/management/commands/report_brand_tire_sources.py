"""
Where manufacturer tire data stands: what each source delivered, and which brands still have none.

Two questions, and they are different ones. The source report says whether what we set up is
still working -- a source whose row count fell, or whose last success is months old, has broken
quietly. The brand report says what is still missing, ordered by how many of that brand's tires no
reseller catalog has confirmed, which is the number that should decide what to chase next.

    manage.py report_brand_tire_sources
    manage.py report_brand_tire_sources --brands --min-tires 50
    manage.py report_brand_tire_sources --brands --csv /tmp/brand-tire-gaps.csv
"""
import csv

from django.core.management.base import BaseCommand

from src.integrations.brand_data import coverage


class Command(BaseCommand):
    help = "Report on tire_brand_sources: what each has delivered, and which brands have no source yet."

    def add_arguments(self, parser):
        parser.add_argument("--brands", action="store_true", help="Also report brand coverage from our catalog.")
        parser.add_argument("--min-tires", type=int, default=25, help="Brand report: ignore smaller brands.")
        parser.add_argument("--csv", default="", help="Write the brand report to this path as well.")

    def handle(self, *args, **options):
        self._sources()
        if options["brands"] or options["csv"]:
            self._brands(min_tires=options["min_tires"], csv_path=options["csv"])

    def _sources(self):
        rows = coverage.source_coverage()
        self.stdout.write("")
        self.stdout.write(f"{len(rows)} registered source(s)")
        self.stdout.write("")
        self.stdout.write(f"  {'slug':<30} {'status':<9} {'rows':>8} {'no size':>8} {'warned':>8}  last run")
        for row in rows:
            source = row.source
            if row.last_run is None:
                last = "never"
            else:
                last = f"{row.last_run.started_at:%Y-%m-%d} {row.last_run.status}"
            self.stdout.write(
                f"  {source.slug[:29]:<30} {source.status:<9} {row.raw_rows:>8} "
                f"{row.rows_unparsed_size:>8} {row.rows_with_warnings:>8}  {last}"
            )
        self.stdout.write("")
        self.stdout.write(
            "'no size' is rows whose size string our parser could not read -- a column we have "
            "misidentified, or a size notation src.domain.tire_size does not cover yet."
        )

    def _brands(self, *, min_tires, csv_path):
        brands = coverage.catalog_brands(min_tires=min_tires)
        covered = [brand for brand in brands if brand.has_source]
        self.stdout.write("")
        self.stdout.write(f"{len(covered)} of {len(brands)} tire brand(s) with >= {min_tires} tires have a source")
        self.stdout.write("")
        self.stdout.write(f"  {'brand':<36} {'tires':>8} {'unconfirmed':>12} {'raw rows':>9}  source")
        for brand in brands[:40]:
            slugs = ", ".join(source.slug for source in brand.sources) or "-"
            self.stdout.write(
                f"  {brand.brand_name[:35]:<36} {brand.tires:>8} {brand.unvalidated:>12} "
                f"{brand.raw_rows:>9}  {slugs}"
            )
        if len(brands) > 40:
            self.stdout.write(f"  ... and {len(brands) - 40} more")

        if csv_path:
            with open(csv_path, "w", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(["brand", "tires", "unconfirmed_by_reseller_catalog", "raw_rows", "sources"])
                for brand in brands:
                    writer.writerow(
                        [
                            brand.brand_name,
                            brand.tires,
                            brand.unvalidated,
                            brand.raw_rows,
                            ";".join(source.slug for source in brand.sources),
                        ]
                    )
            self.stdout.write("")
            self.stdout.write(f"  wrote {csv_path}")
