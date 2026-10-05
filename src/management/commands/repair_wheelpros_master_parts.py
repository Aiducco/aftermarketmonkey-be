"""
Move Premier's "Wheel Pros" master parts onto the manufacturer that made them.

See ``src/integrations/services/wheelpros_brand_repair.py``. Run
``resolve_premier_wheelpros_brands --apply`` FIRST -- this repairs what already exists, that stops
the next ingest recreating it.

    manage.py repair_wheelpros_master_parts             # report only
    manage.py repair_wheelpros_master_parts --limit 50  # a small bite, still read-only
    manage.py repair_wheelpros_master_parts --apply
"""
from django.core.management.base import BaseCommand

from src.integrations.services import wheelpros_brand_repair


class Command(BaseCommand):
    help = "Rename or merge WHEEL PROS master parts onto their real brand. Read-only unless --apply."

    def add_arguments(self, parser):
        parser.add_argument("--apply", action="store_true", help="Write the renames and merges.")
        parser.add_argument("--limit", type=int, default=None, help="Stop after this many master parts.")

    def handle(self, *args, **options):
        stats = wheelpros_brand_repair.run(apply_changes=options["apply"], limit=options["limit"])

        self.stdout.write("\nWHEEL PROS master parts an override can place   {}".format(stats.resolvable))
        self.stdout.write("  rename (no row at the target brand)           {}".format(stats.renamed))
        self.stdout.write("  merge into the existing correct part          {}".format(stats.merged))

        if options["apply"]:
            self.stdout.write("\nMoved")
            self.stdout.write("  provider_parts repointed                    {}".format(stats.provider_parts_moved))
            self.stdout.write("  provider_parts dropped (survivor had it)    {}".format(stats.provider_parts_dropped))
            self.stdout.write("  master_part_data repointed                  {}".format(stats.data_moved))
            self.stdout.write("  master_part_data dropped                    {}".format(stats.data_dropped))
            self.stdout.write("\nTire specs")
            self.stdout.write("  moved to the surviving part                 {}".format(stats.specs_moved))
            self.stdout.write("  survivor's kept (better source)             {}".format(stats.specs_kept_survivor))
            self.stdout.write("  survivor's replaced (loser better sourced)  {}".format(stats.specs_replaced_survivor))
            self.stdout.write(self.style.SUCCESS("\nDuplicate master parts deleted: {}".format(stats.losers_deleted)))
            for reason, count in sorted(stats.skipped.items()):
                self.stdout.write(self.style.WARNING("  {:<40}{}".format(reason, count)))
        else:
            if stats.samples:
                self.stdout.write("\nSamples")
                for line in stats.samples:
                    self.stdout.write("  {}".format(line))
            self.stdout.write(self.style.WARNING("\nNothing written -- pass --apply to commit."))
