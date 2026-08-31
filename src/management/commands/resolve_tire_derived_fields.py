"""
Fill max_load_lb / max_speed_mph / ply_rating from the load index, speed rating and load range
already on each row.

See ``src/integrations/services/tire_derived.py``. These are lookups, not published facts, and
until the catalog merges they were only ever resolved from what the parser could read out of a
distributor title -- so a rating supplied by a catalog left its derived figure empty.

**Nothing is written unless --apply is passed.**

    manage.py resolve_tire_derived_fields             # what would fill
    manage.py resolve_tire_derived_fields --apply
    manage.py resolve_tire_derived_fields --overwrite --apply   # also correct existing values
"""
from django.core.management.base import BaseCommand, CommandError

from src.integrations.services import tire_derived, tire_enrichment


class Command(BaseCommand):
    help = "Resolve the three derived tire figures from the values already on each spec row."

    def add_arguments(self, parser):
        parser.add_argument("--brands", default=None, help="Comma-separated brand names.")
        parser.add_argument(
            "--propagate-season",
            action="store_true",
            help=(
                "Also fill an empty season_category from the other sizes of the same model. A "
                "tire model has one season, but only the SKUs a catalog matched received it."
            ),
        )
        parser.add_argument("--apply", action="store_true", help="Write the fills.")
        parser.add_argument(
            "--overwrite",
            action="store_true",
            help=(
                "Also replace values that are already set. Off by default: where a catalog "
                "published max_load_lb directly it is the manufacturer's figure for that exact "
                "SKU and beats our lookup."
            ),
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

        stats = tire_derived.run(brand_ids=brand_ids, apply_changes=options["apply"], overwrite=options["overwrite"])

        self.stdout.write("\nScanned                   {}".format(stats.scanned))
        self.stdout.write("Rows with a change        {}".format(stats.changed))
        self.stdout.write("\nFilled (was empty)")
        for field, count in sorted(stats.filled.items(), key=lambda kv: -kv[1]):
            self.stdout.write("  {:<22}{}".format(field, count))
        if stats.overwritten:
            self.stdout.write("\nOverwritten (was set, disagreed)")
            for field, count in sorted(stats.overwritten.items(), key=lambda kv: -kv[1]):
                self.stdout.write("  {:<22}{}".format(field, count))
        if stats.unresolvable:
            total = sum(stats.unresolvable.values())
            self.stdout.write(self.style.WARNING("\nInput present but no lookup row for it: {}".format(total)))
            for value, count in sorted(stats.unresolvable.items(), key=lambda kv: -kv[1])[:10]:
                self.stdout.write("  {:<22}{}".format(value, count))
        if stats.samples:
            self.stdout.write("\nSamples")
            for line in stats.samples:
                self.stdout.write("  {}".format(line))

        if options["propagate_season"]:
            filled, ambiguous = tire_derived.propagate_season_by_model(
                brand_ids=brand_ids, apply_changes=options["apply"]
            )
            self.stdout.write("\nSeason propagated across sizes of the same model")
            self.stdout.write("  rows filled             {}".format(filled))
            if ambiguous:
                self.stdout.write("  models left alone       {} (their sizes disagree on the season)".format(ambiguous))

        if options["apply"]:
            self.stdout.write(self.style.SUCCESS("\nWritten: {} rows".format(stats.written)))
        else:
            self.stdout.write(self.style.WARNING("\nNothing written -- pass --apply to commit."))
