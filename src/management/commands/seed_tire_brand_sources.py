"""
Fill the registry of manufacturer tire-data sources: declared ones from a file, planned ones from
our own catalog.

    # every tire brand we carry that nobody has found a source for yet, as a planned row
    manage.py seed_tire_brand_sources --from-catalog --dry-run
    manage.py seed_tire_brand_sources --from-catalog

    # a source somebody has actually arranged
    manage.py seed_tire_brand_sources --from-json resources/brand_data/sources/michelin.json

Both are idempotent and neither overwrites work: seeding a brand that already has a source skips
it, and a declaration only writes the fields it names. See ``src/integrations/brand_data/seeds.py``
for the declaration shape and what is validated before anything is written.
"""
from django.core.management.base import BaseCommand, CommandError

from src.integrations.brand_data import base, seeds


class Command(BaseCommand):
    help = (
        "Seed tire_brand_sources -- declared sources from a JSON file, and a planned row for every "
        "tire brand in our catalog that has no source yet."
    )

    def add_arguments(self, parser):
        parser.add_argument("--from-json", default="", help="Path to a source declarations file.")
        parser.add_argument(
            "--from-catalog",
            action="store_true",
            help="Add a planned row per tire brand we carry that has no source.",
        )
        parser.add_argument(
            "--min-tires",
            type=int,
            default=25,
            help="With --from-catalog: ignore brands with fewer tires than this (default: 25).",
        )
        parser.add_argument("--limit", type=int, default=None, help="With --from-catalog: only the top N gaps.")
        parser.add_argument("--dry-run", action="store_true", help="Say what would be written; write nothing.")

    def handle(self, *args, **options):
        if not options["from_json"] and not options["from_catalog"]:
            raise CommandError("nothing to do -- pass --from-json and/or --from-catalog")

        if options["from_json"]:
            try:
                declarations = seeds.load_declarations(options["from_json"])
                result = seeds.upsert_sources(declarations, dry_run=options["dry_run"])
            except base.BrandDataError as exc:
                raise CommandError(str(exc)) from exc
            self._report("declared", result, dry_run=options["dry_run"])

        if options["from_catalog"]:
            result, missing = seeds.plan_from_catalog(
                min_tires=options["min_tires"],
                limit=options["limit"],
                dry_run=options["dry_run"],
            )
            self.stdout.write("")
            self.stdout.write(
                f"{len(missing)} brand(s) with >= {options['min_tires']} tires and no source. "
                f"Biggest gaps first -- 'unconfirmed' is tires no reseller catalog has matched:"
            )
            self.stdout.write("")
            for brand in missing[:25]:
                self.stdout.write(
                    f"  {brand.brand_name[:34]:<36} {brand.tires:>7} tires  {brand.unvalidated:>7} unconfirmed"
                )
            if len(missing) > 25:
                self.stdout.write(f"  ... and {len(missing) - 25} more")
            self._report("planned", result, dry_run=options["dry_run"])

    def _report(self, label, result, *, dry_run):
        verb = "would create" if dry_run else "created"
        self.stdout.write("")
        self.stdout.write(
            f"  {label}: {verb} {len(result.created)}, updated {len(result.updated)}, "
            f"unchanged {len(result.unchanged)}"
        )
        for slug in result.updated[:20]:
            self.stdout.write(f"      updated {slug}")
        if result.linked:
            self.stdout.write(f"      linked to a catalog brand: {len(result.linked)}")
