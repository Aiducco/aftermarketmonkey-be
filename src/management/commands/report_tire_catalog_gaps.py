"""
Which tires no external catalog has validated, and why.

Read-only. Run it after a crawl to see what the crawl actually bought, and to decide where the
next one should point.

    manage.py report_tire_catalog_gaps
    manage.py report_tire_catalog_gaps --brands 12      # brands listed per reason
"""
from django.core.management.base import BaseCommand

from src.integrations.services import tire_gap_report


class Command(BaseCommand):
    help = "Report which tires are not validated by SimpleTire or TDG, grouped by why they were missed."

    def add_arguments(self, parser):
        parser.add_argument("--brands", type=int, default=8, help="Brands to list under each reason.")

    def handle(self, *args, **options):
        report = tire_gap_report.build()
        total = report.total_specs

        self.stdout.write("\nTire specs                {:>7}".format(total))
        self.stdout.write(
            self.style.SUCCESS(
                "  validated by a catalog  {:>7}  {:.1f}%".format(report.validated, 100 * report.validated / total)
            )
        )
        self.stdout.write(
            "  not validated           {:>7}  {:.1f}%".format(report.unvalidated, 100 * report.unvalidated / total)
        )

        for reason in tire_gap_report.REASON_ORDER:
            count = report.by_reason[reason]
            if not count:
                continue
            self.stdout.write("\n  {:<48} {:>6}  {:>4.1f}% of all tires".format(reason, count, 100 * count / total))
            self.stdout.write("     -> {}".format(tire_gap_report.REASON_ACTION[reason]))
            for brand, n in report.by_reason_brand[reason].most_common(options["brands"]):
                share = 100 * n / report.brand_totals[brand]
                self.stdout.write("        {:<32} {:>5}  ({:.0f}% of the brand)".format(brand[:32], n, share))
