"""
Re-derive the parser-owned half of ``tire_specs`` after a parser change. No LLM calls.

See ``src/integrations/services/tire_reparse.py`` for which fields are touched and which are
deliberately not. In short: the 12 size fields and the 3 resolved from the lookup tables are
recomputed; the 18 identification fields the LLM produced are never written.

Read-only unless ``--apply``.

    manage.py reparse_tire_sizes                        # what would change, catalog-wide
    manage.py reparse_tire_sizes --brands TOYO
    manage.py reparse_tire_sizes --apply --reindex      # commit, then push changed docs
"""
from django.core.management.base import BaseCommand, CommandError

from src.integrations.services import tire_enrichment, tire_reparse


class Command(BaseCommand):
    help = (
        "Recompute tire_specs size fields (and max_load_lb / max_speed_mph / ply_rating) from the "
        "current parser, leaving every LLM-derived field untouched. Read-only unless --apply."
    )

    def add_arguments(self, parser):
        parser.add_argument("--brands", default=None, help="Comma-separated brand names. Omit for the whole catalog.")
        parser.add_argument("--apply", action="store_true", help="Write the recomputed size fields.")
        parser.add_argument(
            "--reindex",
            action="store_true",
            help="After applying, upsert the changed documents into the tires index. Requires --apply.",
        )

    def handle(self, *args, **options):
        if options["reindex"] and not options["apply"]:
            raise CommandError("--reindex requires --apply; there would be nothing new to index.")

        brand_ids = None
        if options["brands"]:
            names = [n.strip() for n in options["brands"].split(",") if n.strip()]
            resolved = tire_enrichment.resolve_brand_ids(names)
            unknown = sorted(set(names) - set(resolved))
            if unknown:
                raise CommandError("Unknown brand(s): {}".format(", ".join(unknown)))
            brand_ids = list(resolved.values())

        stats = tire_reparse.run(brand_ids=brand_ids, apply_changes=options["apply"])

        self.stdout.write("\nRe-derived from the current parser")
        self.stdout.write("  specs scanned            {:,}".format(stats.scanned))
        self.stdout.write("  unchanged                {:,}".format(stats.unchanged))
        self.stdout.write(
            "  CHANGED                  {:,}  ({:.1f}%)".format(
                stats.changed, 100 * stats.changed / stats.scanned if stats.scanned else 0
            )
        )

        if stats.field_changes:
            self.stdout.write("\nBy field")
            for field, count in sorted(stats.field_changes.items(), key=lambda kv: -kv[1]):
                self.stdout.write("  {:<24}{:>8,}".format(field, count))

        if stats.samples:
            self.stdout.write("\nExamples")
            for sample in stats.samples:
                self.stdout.write("  {}".format(sample))

        if stats.now_unparseable:
            # Reported, never deleted: a parser change that loses coverage should show up as a
            # number, not as inventory quietly vanishing.
            self.stdout.write(
                self.style.WARNING(
                    "\n  {:,} spec(s) whose titles NO LONGER parse -- left in place, not deleted.".format(
                        stats.now_unparseable
                    )
                )
            )
            self.stdout.write(
                "     master_part_ids: {}{}".format(
                    ", ".join(str(i) for i in stats.unparseable_master_part_ids[:10]),
                    " ..." if len(stats.unparseable_master_part_ids) > 10 else "",
                )
            )

        if not options["apply"]:
            self.stdout.write(self.style.WARNING("\nNothing written -- pass --apply to commit."))
            return

        if options["reindex"] and stats.changed_master_part_ids:
            self.stdout.write("\n  reindexed {:,} document(s)".format(self._reindex(stats.changed_master_part_ids)))

        self.stdout.write(self.style.SUCCESS("\nDone."))

    def _reindex(self, master_part_ids):
        from src.search import tires_index

        if not tires_index.is_configured():
            self.stdout.write(self.style.WARNING("  Meilisearch not configured; skipped reindex."))
            return 0
        wanted = set(master_part_ids)
        written = 0
        for batch in tires_index.iter_documents(batch_size=tires_index.REINDEX_BATCH_SIZE):
            changed = [doc for doc in batch if doc["id"] in wanted]
            if changed:
                written += tires_index.upsert_documents(changed)
        return written
