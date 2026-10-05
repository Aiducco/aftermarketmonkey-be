"""
Give Premier's "Wheel Pros" bucket rows their real manufacturer.

Premier resells through Wheel Pros and files many distinct makers -- American Racing, Niche,
Rotiform, Falken, Moto Metal, Nitto -- under one vendor-name brand rather than the manufacturer.
Left alone, every one of those rows creates a MasterPart branded "WHEEL PROS", which is a
distributor, not a tyre or wheel maker, and which no external catalog can match.

Writes ``PremierParts.brand_override``, which the master-parts ingest prefers over the feed-level
brand mapping. Nothing else changes: Premier's own ``brand_id`` is untouched, and the override is
a nullable column, so clearing it restores today's behaviour exactly.

**Run this before repairing master_parts.** The override is what makes the *next* ingest build the
part under the right brand; repair the master parts first and the next sync recreates them wrongly.

    manage.py resolve_premier_wheelpros_brands              # report only
    manage.py resolve_premier_wheelpros_brands --apply
    manage.py resolve_premier_wheelpros_brands --apply --use-leading-phrase
"""
from django.core.management.base import BaseCommand

from src.integrations.services import premier


class Command(BaseCommand):
    help = "Resolve the real manufacturer for Premier's 'Wheel Pros' bucket. Read-only unless --apply."

    def add_arguments(self, parser):
        parser.add_argument("--apply", action="store_true", help="Write PremierParts.brand_override.")
        parser.add_argument(
            "--use-leading-phrase",
            action="store_true",
            help=(
                "Also run the free-text leading-phrase cascade. Off by default: it is the weakest "
                "of the three signals and the only one where a wheel style name can collide with "
                "an unrelated brand."
            ),
        )

    def handle(self, *args, **options):
        stats = premier.resolve_wheelpros_bucket_brands(
            dry_run=not options["apply"], use_leading_phrase=options["use_leading_phrase"]
        )
        if not stats:
            self.stdout.write("Nothing to resolve.")
            return

        self.stdout.write("\nBucket rows scanned        {}".format(stats["candidates"]))
        self.stdout.write("  by a resolved sibling    {}".format(stats.get("sibling_matches", 0)))
        self.stdout.write("  by the model code        {}".format(stats.get("marque_matches", 0)))
        phrase = stats.get("exact_matches", 0) + stats.get("compact_matches", 0) + stats.get("fuzzy_matches", 0)
        if phrase:
            self.stdout.write("  by the leading phrase    {}".format(phrase))
        self.stdout.write(self.style.SUCCESS("  resolved                 {}".format(stats["resolved"])))
        self.stdout.write("  left as Wheel Pros       {}".format(stats["unresolved"]))

        if options["apply"]:
            self.stdout.write(
                self.style.SUCCESS("\nOverrides written. Now repair master_parts (see the module docstring).")
            )
        else:
            self.stdout.write(self.style.WARNING("\nNothing written -- pass --apply to commit."))
