"""
Merge duplicate MasterPart rows, on the server rather than over a laptop's connection.

The merge logic lives in ``scripts/merge_normalized_part_number_duplicates.py``; this is a thin
runner around it so the whole job is one command instead of a Django-shell session. Running it
where the database is matters a lot: the row-by-row path is latency-bound, measured at ~0.4
groups/sec from a laptop versus ~17/sec for the set-based path, so a run that takes hours
remotely finishes in minutes on the server.

Two passes, either or both selectable:

- **part-number**: rows whose part numbers differ only in case, whitespace or punctuation
  ('MS 96587' vs 'MS96587').
- **barcode**: rows sharing a brand and a validated GTIN whose part numbers are not string
  related at all -- brand prefixes ('TOY357280' vs '357280'), placeholder part numbers, leading
  zeros. Graded by corroborating evidence; ``--max-evidence-tier`` sets how weak a grade is
  accepted, defaulting to 4 so that groups linked by the barcode *alone* are never auto-merged.

Both passes send each group down whichever path suits it: set-based where the rows share no
provider, row-by-row where a distributor's link sits on both and one has to be deleted rather
than repointed, or where both rows hold MasterPartData and its fields need merging. Groups the
fast path defers are re-run through the careful one in the same invocation, looping until the
deferred set stops shrinking -- otherwise they quietly accumulate.

Dry run by default. Nothing is written unless ``--apply`` is passed.

    python manage.py merge_duplicate_master_parts                    # preview everything
    python manage.py merge_duplicate_master_parts --apply
    python manage.py merge_duplicate_master_parts --pass barcode --apply
    python manage.py merge_duplicate_master_parts --apply --review-csv /tmp/review.csv
"""
import collections
import runpy
import time
import typing

from django.core.management.base import BaseCommand

from src.integrations.utils import part_numbers as pn_util

_SCRIPT = "scripts/merge_normalized_part_number_duplicates.py"
_MAX_ROUNDS = 5


class Command(BaseCommand):
    help = "Merge duplicate MasterPart rows (part-number and/or barcode anchored)."

    def add_arguments(self, parser):
        parser.add_argument(
            "--pass", dest="which", choices=["part-number", "barcode", "both"], default="both",
            help="Which pass to run (default: both).",
        )
        parser.add_argument(
            "--apply", action="store_true",
            help="Actually merge. Without this the command only reports what it would do.",
        )
        parser.add_argument(
            "--max-evidence-tier", type=int, default=pn_util.GTIN_EVIDENCE_PREFIX_SUFFIX,
            help=(
                "Weakest evidence grade to accept on the barcode pass: 1 sku bridge, "
                "2 placeholder, 3 leading zeros, 4 brand prefix/suffix, 5 barcode alone. "
                "Default 4. Raising it to 5 merges on the barcode with nothing corroborating "
                "it, which a single bad barcode in one feed would turn into a wrong merge."
            ),
        )
        parser.add_argument(
            "--review-csv", dest="review_csv", default=None,
            help="Write the groups that were NOT merged, with their evidence, to this path.",
        )
        parser.add_argument(
            "--chunk-size", type=int, default=400,
            help="Groups per transaction on the set-based path (default: 400).",
        )

    def handle(self, *args, **options):
        ns = runpy.run_path(_SCRIPT, run_name="merge_command")
        # Fails loudly if a table has started referencing master_parts that the merge does not
        # know how to relocate -- before anything is written, rather than partway through.
        ns["assert_all_master_part_references_handled"]()

        apply_changes = options["apply"]
        if not apply_changes:
            self.stdout.write(self.style.WARNING("DRY RUN -- nothing will be written. Pass --apply to merge."))

        started = time.time()
        totals = collections.Counter()
        which = options["which"]

        if which in ("part-number", "both"):
            report = ns["find_merge_candidates"]()
            self.stdout.write("\nPART-NUMBER PASS: {} mergeable, {} held for review".format(
                len(report.auto_mergeable), len(report.needs_review)))
            self._run(ns, report, options, apply_changes, totals, label="part-number",
                      rederive=lambda: ns["find_merge_candidates"]())

        if which in ("barcode", "both"):
            report = ns["find_gtin_merge_candidates"](max_tier=options["max_evidence_tier"])
            self.stdout.write("\nBARCODE PASS: {} mergeable, {} held for review".format(
                len(report.auto_mergeable), len(report.needs_review)))
            for tier, count in collections.Counter(g.tier for g in report.auto_mergeable).most_common():
                self.stdout.write("    {:52s} {}".format(tier, count))
            if options["review_csv"]:
                ns["export_review_csv"](report, options["review_csv"])
            self._run(ns, report, options, apply_changes, totals, label="barcode",
                      rederive=lambda: ns["find_gtin_merge_candidates"](
                          max_tier=options["max_evidence_tier"]))

        self.stdout.write(self.style.SUCCESS(
            "\nmerged={merged} deleted={deleted} failed={failed} in {mins:.1f} min".format(
                merged=totals["merged"], deleted=totals["deleted"], failed=totals["failed"],
                mins=(time.time() - started) / 60,
            )
        ))

    def _run(self, ns, report, options, apply_changes, totals, label, rederive):
        """Merge one pass's groups, re-running whatever the fast path defers."""
        if not apply_changes:
            for group in report.auto_mergeable[:5]:
                self.stdout.write("  would merge: {} -> {}".format(
                    [r.part_number for r in group.rows], group.reason))
            return

        # Re-derive candidates each round rather than reusing the first scan: merging changes
        # which rows remain, and a merge can expose a group that was previously ambiguous.
        # Loops until a round merges nothing, so deferred groups cannot quietly accumulate.
        pending = list(report.auto_mergeable)
        for round_no in range(1, _MAX_ROUNDS + 1):
            if not pending:
                break
            bulk, rowwise = self._split(pending)
            merged_before = totals["merged"]
            deferred_ids: typing.Set[int] = set()

            if bulk:
                result = ns["merge_groups_bulk"](bulk, dry_run=False, chunk_size=options["chunk_size"])
                totals["merged"] += result["merged"]
                totals["deleted"] += result["deleted"]
                totals["failed"] += len(result["failed"])
                deferred_ids = set(result["fallback"])
                self.stdout.write("  [{} r{}] bulk merged={} deleted={} deferred={} failed={}".format(
                    label, round_no, result["merged"], result["deleted"],
                    len(deferred_ids), len(result["failed"])))

            # Groups the set-based path could not finish (both rows hold MasterPartData) plus
            # groups that always needed the careful path.
            careful = rowwise + [g for g in bulk if any(r.id in deferred_ids for r in g.rows)]
            if careful:
                result = ns["merge_batch"](careful, dry_run=False)
                totals["merged"] += len(result["merged"])
                totals["failed"] += len(result["failed"])
                self.stdout.write("  [{} r{}] row-by-row merged={} failed={}".format(
                    label, round_no, len(result["merged"]), len(result["failed"])))

            if totals["merged"] == merged_before:
                self.stdout.write("  [{}] round {} merged nothing -- stopping".format(label, round_no))
                break
            pending = rederive().auto_mergeable
            self.stdout.write("  [{}] {} groups still mergeable after round {}".format(
                label, len(pending), round_no))

    @staticmethod
    def _split(groups):
        """Set-based path where no provider repeats across rows; careful path otherwise."""
        bulk, rowwise = [], []
        for group in groups:
            seen = collections.Counter()
            for row in group.rows:
                for provider_id in row.provider_ids:
                    seen[provider_id] += 1
            (rowwise if any(count > 1 for count in seen.values()) else bulk).append(group)
        return bulk, rowwise
