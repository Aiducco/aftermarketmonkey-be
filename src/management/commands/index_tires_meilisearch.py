"""
Build and maintain the ``tires_v1`` Meilisearch index.

**Separate from the parts index by construction.** This command and
``src/search/tires_index.py`` never call anything that mutates ``parts`` or ``vehicles``, and the
tires module refuses to run at all if its index name collides with either -- see
``tires_index._assert_not_a_shared_index``. ``setup_meilisearch`` and
``index_parts_meilisearch`` are untouched by any of this.

Typical runs:

    # configure the index without writing documents
    manage.py index_tires_meilisearch --setup

    # see what a document looks like, contact nothing
    manage.py index_tires_meilisearch --dry-run --limit 3

    # incremental: upsert one brand's tires into the live index
    manage.py index_tires_meilisearch --brands NITTO

    # full rebuild: stage everything, verify the count, then swap atomically
    manage.py index_tires_meilisearch --rebuild
"""
import json

from django.core.management.base import BaseCommand, CommandError

from src.integrations.services import tire_enrichment
from src.search import tires_index


class Command(BaseCommand):
    help = (
        "Project tire_specs into the tires Meilisearch index. --rebuild does a verified "
        "zero-downtime swap; without it, documents are upserted into the live index."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--brands",
            default=None,
            help="Comma-separated brand names to index (e.g. NITTO). Omit for every tire.",
        )
        parser.add_argument(
            "--rebuild",
            action="store_true",
            help=(
                "Full rebuild into a staging index, verified against the expected document "
                "count, then swapped in atomically. Refuses to swap on a count mismatch."
            ),
        )
        parser.add_argument(
            "--setup", action="store_true", help="Create/configure the index and exit. Writes no documents."
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Print projected documents and exit. Meilisearch is never contacted.",
        )
        parser.add_argument("--limit", type=int, default=5, help="Documents to print with --dry-run. Default 5.")
        parser.add_argument(
            "--batch-size", type=int, default=tires_index.REINDEX_BATCH_SIZE, help="Documents per upload request."
        )

    def handle(self, *args, **options):
        brand_ids = None
        if options["brands"]:
            names = [name.strip() for name in options["brands"].split(",") if name.strip()]
            resolved = tire_enrichment.resolve_brand_ids(names)
            unknown = sorted(set(names) - set(resolved))
            if unknown:
                raise CommandError("Unknown brand(s): {}".format(", ".join(unknown)))
            brand_ids = list(resolved.values())

        if options["dry_run"]:
            return self._dry_run(brand_ids=brand_ids, limit=options["limit"])

        if not tires_index.is_configured():
            raise CommandError("Meilisearch is not configured (MEILISEARCH_HOST is empty).")

        if options["setup"]:
            if not tires_index.setup_index():
                raise CommandError("Index setup failed; see the log.")
            self.stdout.write(self.style.SUCCESS("Configured index '{}'.".format(tires_index.INDEX_NAME_TIRES)))
            return

        if options["rebuild"]:
            if brand_ids:
                # A brand-scoped staging build contains only that brand, so swapping it in would
                # delete every other brand's tires from the live index.
                raise CommandError(
                    "--rebuild cannot be combined with --brands: the swap would replace the whole "
                    "index with one brand. Use --brands on its own for an incremental upsert."
                )
            indexed, expected = tires_index.reindex(batch_size=options["batch_size"])
            if indexed != expected:
                raise CommandError(
                    "Refused to swap: staged {} documents but expected {}. The live index is "
                    "unchanged and staging was kept for inspection.".format(indexed, expected)
                )
            self.stdout.write(
                self.style.SUCCESS("Rebuilt '{}': {} documents live.".format(tires_index.INDEX_NAME_TIRES, indexed))
            )
            return

        tires_index.setup_index()
        expected = tires_index.indexable_count(brand_ids)
        written = 0
        for batch in tires_index.iter_documents(brand_ids=brand_ids, batch_size=options["batch_size"]):
            written += tires_index.upsert_documents(batch)
            self.stdout.write("  upserted {}/{}".format(written, expected))
        self.stdout.write(
            self.style.SUCCESS("Upserted {} documents into '{}'.".format(written, tires_index.INDEX_NAME_TIRES))
        )

    def _dry_run(self, *, brand_ids, limit):
        expected = tires_index.indexable_count(brand_ids)
        self.stdout.write("{} tires would be indexed into '{}'.\n".format(expected, tires_index.INDEX_NAME_TIRES))
        shown = 0
        for batch in tires_index.iter_documents(brand_ids=brand_ids, batch_size=max(limit, 1)):
            for document in batch:
                self.stdout.write(json.dumps(document, indent=2, default=str))
                shown += 1
                if shown >= limit:
                    return
            if shown >= limit:
                return
