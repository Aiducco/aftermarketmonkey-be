"""
Fill MasterPart.image_url for tires from the catalog each one was matched to.

See ``src/integrations/services/tire_images.py``. This is the only command in the tire pipeline
that writes to ``master_parts``, and it is restricted to rows that have a tire spec, have no image
already, and have a recorded catalog match -- it copies a URL and does nothing else.

**Nothing is written unless --apply is passed.**

Run it after both spec merges, so every tire that can be matched already is.

    manage.py backfill_tire_images                 # how many would fill, and from where
    manage.py backfill_tire_images --apply
"""
from django.core.management.base import BaseCommand, CommandError

from src.integrations.services import tire_enrichment, tire_images


class Command(BaseCommand):
    help = "Copy product images from the matched SimpleTire/TDG rows onto tires that have none."

    def add_arguments(self, parser):
        parser.add_argument("--brands", default=None, help="Comma-separated brand names.")
        parser.add_argument("--limit", type=int, default=None)
        parser.add_argument("--apply", action="store_true", help="Write the image urls.")

    def handle(self, *args, **options):
        brand_ids = None
        if options["brands"]:
            names = [n.strip() for n in options["brands"].split(",") if n.strip()]
            resolved = tire_enrichment.resolve_brand_ids(names)
            unknown = sorted(set(names) - set(resolved))
            if unknown:
                raise CommandError("Unknown brand(s): {}".format(", ".join(unknown)))
            brand_ids = list(resolved.values())

        stats = tire_images.run(brand_ids=brand_ids, apply_changes=options["apply"], limit=options["limit"])

        self.stdout.write("\nTires scanned             {}".format(stats.scanned))
        self.stdout.write("  already had an image    {}".format(stats.already_had_one))
        self.stdout.write("  no catalog match        {}".format(stats.no_catalog_match))
        self.stdout.write("  matched, catalog has no image  {}".format(stats.catalog_has_none))
        if stats.placeholder_skipped:
            self.stdout.write(
                self.style.WARNING(
                    "  placeholder refused     {}  (a generic sidewall, not this product)".format(
                        stats.placeholder_skipped
                    )
                )
            )
        self.stdout.write(self.style.SUCCESS("  would fill              {}".format(stats.filled)))
        for source, count in sorted(stats.by_source.items(), key=lambda kv: -kv[1]):
            self.stdout.write("    from {:<18}{}".format(source, count))

        if stats.samples:
            self.stdout.write("\nSamples")
            for line in stats.samples:
                self.stdout.write("  {}".format(line))

        if options["apply"]:
            self.stdout.write(self.style.SUCCESS("\nWritten: {} master_parts".format(stats.written)))
        else:
            self.stdout.write(self.style.WARNING("\nNothing written -- pass --apply to commit."))
