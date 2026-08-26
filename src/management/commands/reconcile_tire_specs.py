"""
Settle per-SKU disagreements across the sizes of one tire model.

Run after every enrichment batch. See ``src/integrations/services/tire_reconciliation.py`` for
what each pass does and, importantly, for why the model-name pass is far narrower than the
original plan called for.

Writes nothing without ``--apply``.

    # what would change, and the review queue
    manage.py reconcile_tire_specs --brands NITTO

    # commit, then push the changed documents into the search index
    manage.py reconcile_tire_specs --brands NITTO --apply --reindex
"""
from django.core.management.base import BaseCommand, CommandError

from src.integrations.services import tire_enrichment, tire_reconciliation


class Command(BaseCommand):
    help = (
        "Reconcile tread_category by majority vote per model, and collapse model-name spellings "
        "that differ only in case. Read-only unless --apply is given."
    )

    def add_arguments(self, parser):
        parser.add_argument("--brands", default=None, help="Comma-separated brand names. Omit for every tire.")
        parser.add_argument("--apply", action="store_true", help="Write the changes.")
        parser.add_argument(
            "--reindex",
            action="store_true",
            help="After applying, upsert the changed documents into the tires index. Requires --apply.",
        )
        parser.add_argument(
            "--min-agreement",
            type=float,
            default=tire_reconciliation.DEFAULT_MIN_AGREEMENT,
            help=(
                "Minimum share of the vote the winning category must hold before it overwrites a "
                "model's other sizes. Below it, the model is left exactly as answered and listed "
                "for review -- a 50/50 split resolved by tiebreaker is a coin flip written as "
                "fact. Default {:.2f}.".format(tire_reconciliation.DEFAULT_MIN_AGREEMENT)
            ),
        )
        parser.add_argument(
            "--split-threshold",
            type=float,
            default=2.0,
            help="Exit non-zero if the post-reconciliation split rate exceeds this percentage. Default 2.0.",
        )

    def handle(self, *args, **options):
        if options["reindex"] and not options["apply"]:
            raise CommandError("--reindex requires --apply; there would be nothing new to index.")

        brand_ids = None
        if options["brands"]:
            names = [name.strip() for name in options["brands"].split(",") if name.strip()]
            resolved = tire_enrichment.resolve_brand_ids(names)
            unknown = sorted(set(names) - set(resolved))
            if unknown:
                raise CommandError("Unknown brand(s): {}".format(", ".join(unknown)))
            brand_ids = list(resolved.values())

        report = tire_reconciliation.run(
            brand_ids=brand_ids, apply_changes=options["apply"], min_agreement=options["min_agreement"]
        )

        self.stdout.write("\nBefore reconciliation")
        self.stdout.write("  models                  {}".format(len(report.votes)))
        self.stdout.write(
            "  with a split vote       {} ({:.1f}%)".format(len(report.split_votes), 100 * report.split_rate)
        )

        if report.split_votes:
            self.stdout.write("\nReview queue (a close split is a real ambiguity; a lopsided one was noise)")
            for vote in sorted(report.split_votes, key=lambda v: -v.total):
                marker = "  <-- close" if vote.winner_share < 0.7 else ""
                self.stdout.write(
                    "  {:<26} {} -> {} ({:.0f}% agreed){}".format(
                        vote.model_name[:26], dict(vote.counts), vote.winner, 100 * vote.winner_share, marker
                    )
                )

        if report.undecided:
            self.stdout.write(
                self.style.WARNING(
                    "\nLeft for review -- winner below the {:.0%} agreement bar, NOT overwritten".format(
                        options["min_agreement"]
                    )
                )
            )
            for vote in sorted(report.undecided, key=lambda v: -v.total):
                self.stdout.write(
                    "  {:<26} {} -> would have been {} ({:.0f}%)".format(
                        vote.model_name[:26], dict(vote.counts), vote.winner, 100 * vote.winner_share
                    )
                )

        self.stdout.write("\nChanges")
        self.stdout.write("  tread_category rewritten {}".format(report.categories_changed))
        self.stdout.write("  model_name canonicalised {}".format(report.names_changed))

        if not options["apply"]:
            self.stdout.write(self.style.WARNING("\nNothing written -- pass --apply to commit."))
            return

        # Re-tally against what is now in the table. This is the acceptance number: under 2%
        # means per-SKU classification plus this pass was the right trade.
        after = tire_reconciliation.collect_votes(brand_ids)
        # Deliberately-undecided models stay split; that is the intended outcome, not a failure
        # to converge, so they are excluded from the rate the threshold gates on.
        undecided_keys = {(v.brand_id, v.model_name.lower()) for v in report.undecided}
        split_after = [
            vote for vote in after if vote.is_split and (vote.brand_id, vote.model_name.lower()) not in undecided_keys
        ]
        rate = 100 * len(split_after) / len(after) if after else 0.0
        self.stdout.write("\nAfter reconciliation")
        self.stdout.write("  models with a split vote {} ({:.2f}%)".format(len(split_after), rate))

        if options["reindex"] and report.touched_master_part_ids:
            written = self._reindex(report.touched_master_part_ids)
            self.stdout.write("  documents reindexed      {}".format(written))

        if rate > options["split_threshold"]:
            raise CommandError(
                "Split rate {:.2f}% exceeds the {:.2f}% threshold -- reconciliation did not "
                "converge; inspect the review queue above.".format(rate, options["split_threshold"])
            )
        self.stdout.write(self.style.SUCCESS("\nReconciled."))

    def _reindex(self, master_part_ids):
        """Push only the changed documents. A full rebuild is not warranted for a category flip."""
        from src.api.services import tire_search
        from src.search import tires_index

        if not tires_index.is_configured():
            self.stdout.write(self.style.WARNING("  Meilisearch not configured; skipped reindex."))
            return 0

        wanted = set(master_part_ids)
        written = 0
        for batch in tires_index.iter_documents(batch_size=tires_index.REINDEX_BATCH_SIZE):
            changed = [document for document in batch if document["id"] in wanted]
            if changed:
                written += tires_index.upsert_documents(changed)
        # Model names moved, so the cached brand/facet reference data may be stale.
        tire_search.invalidate_reference_cache()
        return written
