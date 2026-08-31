"""
Merge spellings of the same tire model within a brand.

See ``src/integrations/services/tire_model_names.py`` for what counts as a spelling variant and,
more importantly, what does not -- ``Terra Grappler`` and ``Terra Grappler G2`` are different
tires, and so are ``Ultra Grip Performance`` and ``UltraGrip Performance+``.

**Nothing is written unless --apply is passed.** Run it after
``sync_tire_specs_from_simpletire``, not before: the canonical spelling is chosen as whichever one
a matched catalog row uses, so the merge is only as good as the catalog names already present.

    manage.py consolidate_tire_model_names                 # what would merge
    manage.py consolidate_tire_model_names --brands PIRELLI --apply
"""
from django.core.management.base import BaseCommand, CommandError

from src.integrations.services import tire_enrichment, tire_model_names


class Command(BaseCommand):
    help = "Consolidate tire_specs.model_name spelling variants within each brand. Read-only unless --apply."

    def add_arguments(self, parser):
        parser.add_argument("--brands", default=None, help="Comma-separated brand names. Omit for the whole catalog.")
        parser.add_argument("--apply", action="store_true", help="Write the renames.")
        parser.add_argument("--show", type=int, default=40, help="How many groups to print. Default 40.")

    def handle(self, *args, **options):
        brand_ids = None
        if options["brands"]:
            names = [n.strip() for n in options["brands"].split(",") if n.strip()]
            resolved = tire_enrichment.resolve_brand_ids(names)
            unknown = sorted(set(names) - set(resolved))
            if unknown:
                raise CommandError("Unknown brand(s): {}".format(", ".join(unknown)))
            brand_ids = list(resolved.values())

        stats, groups = tire_model_names.run(brand_ids=brand_ids, apply_changes=options["apply"])

        self.stdout.write("{:<26} {:<34} {:>6}  {}".format("brand", "canonical", "rows", "absorbs"))
        for group in groups[: options["show"]]:
            self.stdout.write(
                "{:<26} {:<34} {:>6}  {}".format(
                    group.brand_name[:26], group.canonical[:34], group.rows, ", ".join(sorted(group.variants))[:64]
                )
            )
        if len(groups) > options["show"]:
            self.stdout.write("  ... and {} more groups".format(len(groups) - options["show"]))

        self.stdout.write("\nGroups                    {}".format(stats.groups))
        self.stdout.write("Rows renamed              {}".format(stats.rows_changed))
        self.stdout.write("Distinct (brand, model)   {} -> {}".format(stats.distinct_before, stats.distinct_after))
        if options["apply"]:
            self.stdout.write(self.style.SUCCESS("\nWritten: {} rows".format(stats.written)))
        else:
            self.stdout.write(self.style.WARNING("\nNothing written -- pass --apply to commit."))
