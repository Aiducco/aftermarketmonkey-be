"""
Build the Meilisearch wheels index.

See ``src/search/wheels_index.py`` for the projection and ``src/search/index_builder.py`` for the
rebuild-and-swap machinery.

    manage.py index_wheels_meilisearch --setup        # create / configure, index nothing
    manage.py index_wheels_meilisearch --rebuild      # full build into staging, verify, swap
    manage.py index_wheels_meilisearch --rebuild --brands KMC   # one brand, refuses to swap

A rebuild is refused unless the staged document count matches exactly what the database says it
should be. A short index that quietly replaces a good one looks like inventory disappearing.
"""
from django.core.management.base import BaseCommand, CommandError

from src.integrations.services import tire_enrichment
from src.search import wheels_index


class Command(BaseCommand):
    help = "Create, configure and rebuild the Meilisearch wheels index."

    def add_arguments(self, parser):
        parser.add_argument("--setup", action="store_true", help="Create and configure the index.")
        parser.add_argument("--rebuild", action="store_true", help="Full rebuild into staging, then swap.")
        parser.add_argument("--brands", default=None, help="Scope a rebuild to these brands (will not swap).")
        parser.add_argument("--batch-size", type=int, default=wheels_index.REINDEX_BATCH_SIZE)

    def handle(self, *args, **options):
        if not (options["setup"] or options["rebuild"]):
            raise CommandError("Nothing to do: pass --setup and/or --rebuild.")
        if not wheels_index.is_configured():
            raise CommandError("Meilisearch is not configured (MEILISEARCH_HOST / MEILISEARCH_MASTER_KEY).")

        brand_ids = None
        if options["brands"]:
            names = [n.strip() for n in options["brands"].split(",") if n.strip()]
            resolved = tire_enrichment.resolve_brand_ids(names)
            unknown = sorted(set(names) - set(resolved))
            if unknown:
                raise CommandError("Unknown brand(s): {}".format(", ".join(unknown)))
            brand_ids = list(resolved.values())
            if options["rebuild"]:
                raise CommandError(
                    "--brands with --rebuild would swap in an index containing only those brands. "
                    "Use --brands with --setup to inspect, or rebuild the whole index."
                )

        if options["setup"]:
            ok = wheels_index.setup_index()
            self.stdout.write(
                self.style.SUCCESS("Index '{}' configured.".format(wheels_index.SPEC.name))
                if ok
                else self.style.ERROR("Setup failed -- see the log.")
            )

        if options["rebuild"]:
            expected = wheels_index.indexable_count()
            self.stdout.write("Rebuilding '{}': {} documents expected...".format(wheels_index.SPEC.name, expected))
            live, expected = wheels_index.reindex(batch_size=options["batch_size"])
            if live == expected:
                self.stdout.write(self.style.SUCCESS("Live: {} documents.".format(live)))
            else:
                raise CommandError(
                    "Rebuild did NOT swap: staged/live {} against {} expected. The previous index is "
                    "untouched and staging was kept. See the log.".format(live, expected)
                )
