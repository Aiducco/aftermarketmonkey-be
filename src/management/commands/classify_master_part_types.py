"""
Classify master parts as wheel / tire / part. See
src/integrations/services/product_type_classification.py for the machinery and
src/integrations/utils/product_type.py for the rules themselves.

Writes nothing unless --apply is passed. A bare run stages every verdict and reports what it
would do, which is the intended way to inspect a rule change before it touches 3.2M rows.

Typical runs:
    # see what would happen, write nothing
    manage.py classify_master_part_types

    # one distributor, small bite, still read-only
    manage.py classify_master_part_types --sources wheelpros --limit 20000

    # commit it
    manage.py classify_master_part_types --apply
"""
from django.core.management.base import BaseCommand, CommandError

from src.integrations.services import product_type_classification as classification


class Command(BaseCommand):
    help = (
        "Populate MasterPart.product_type (wheel/tire/part) from distributor signals. "
        "Read-only unless --apply is given. A master part whose best-tier distributors disagree "
        "is left NULL and reported as a conflict rather than guessed at."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--sources",
            default=None,
            help=(
                "Comma-separated subset of sources to collect. Default: all. Available: {}.".format(
                    ", ".join(sorted(classification.SOURCES_BY_NAME))
                )
            ),
        )
        parser.add_argument(
            "--limit",
            type=int,
            default=None,
            help="Max provider_parts rows to scan per source. Omit for the whole catalog.",
        )
        parser.add_argument(
            "--apply",
            action="store_true",
            help="Write the resolved verdicts to master_parts. Without it, nothing is written.",
        )

    def handle(self, *args, **options):
        source_names = None
        if options["sources"]:
            source_names = [name.strip() for name in options["sources"].split(",") if name.strip()]
            unknown = sorted(set(source_names) - set(classification.SOURCES_BY_NAME))
            if unknown:
                raise CommandError(
                    "Unknown source(s): {}. Available: {}.".format(
                        ", ".join(unknown), ", ".join(sorted(classification.SOURCES_BY_NAME))
                    )
                )

        result = classification.run(
            source_names=source_names,
            limit_per_source=options["limit"],
            apply_changes=options["apply"],
        )

        self.stdout.write("\nPer-source collection")
        self.stdout.write("{:<22} {:>10} {:>10} {:>10} {:>10}".format(
            "source", "scanned", "staged", "unjoined", "undecided"))
        for name, stats in sorted(result["per_source"].items()):
            self.stdout.write("{:<22} {:>10} {:>10} {:>10} {:>10}".format(
                name, stats["scanned"], stats["staged"], stats["unjoined"], stats["undecided"]))

        resolution = result["resolution"]
        self.stdout.write("\nResolved (unanimous at best tier)")
        for product_type, agreed, conflicted in resolution["by_type"]:
            self.stdout.write("  {:<8} {:>10}".format(product_type, agreed))
        self.stdout.write("  {:<8} {:>10}   <- left NULL on purpose".format(
            "conflict", resolution["conflicts"]))
        self.stdout.write("  {:<8} {:>10}".format("total mp", resolution["total_master_parts"]))

        if result["conflicts"]:
            self.stdout.write("\nConflicts (best-tier distributors disagree)")
            for master_part_id, brand, part_number, detail in result["conflicts"]:
                self.stdout.write("  {} {} {} -- {}".format(master_part_id, brand, part_number, detail))

        if result["applied"] is None:
            self.stdout.write(self.style.WARNING(
                "\nDry run -- nothing written. Re-run with --apply to commit."))
            return

        self.stdout.write(self.style.SUCCESS("\nUpdated {} master parts.".format(result["applied"])))
        gaps = result.get("gaps") or {}
        self.stdout.write("\nStill unclassified: {}".format(gaps.get("unclassified")))
        self.stdout.write("  by brand:")
        for brand, count in gaps.get("by_brand", []):
            self.stdout.write("    {:<40} {}".format(brand, count))
        self.stdout.write("  by distributor:")
        for provider, count in gaps.get("by_provider", []):
            self.stdout.write("    {:<40} {}".format(provider, count))
