"""
Populate ``tire_specs`` from distributor titles: one LLM call per tire.

See ``src/integrations/services/tire_enrichment.py`` for the machinery and
``src/domain/tire_size.py`` for the size parser that is the source of truth for dimensions.

**Nothing is written unless --apply is passed**, but the LLM *is* still called without it -- a
bare run is a dry run of the write, not of the spend, and its point is letting you read real
responses before they touch the catalog. Use ``--preview`` for the genuinely free inspection: it
builds the prompts and prints them without calling anything.

Typical runs:

    # free: what would we send, and does the parser agree with the titles?
    manage.py enrich_tire_specs --brands NITTO --preview 5

    # the 200-row pilot, writing nothing, with a CSV to grade by hand
    manage.py enrich_tire_specs --brands NITTO --limit 200 --report /tmp/nitto-pilot.csv

    # commit one brand
    manage.py enrich_tire_specs --brands NITTO --apply

    # retry only the ones the model could not identify last time
    manage.py enrich_tire_specs --brands NITTO --mode incomplete --apply
"""
import csv
import json
import pathlib

from django.core.management.base import BaseCommand, CommandError

from src.integrations.llm import azure_llm
from src.integrations.services import tire_enrichment


class Command(BaseCommand):
    help = (
        "Enrich master parts into tire_specs with one LLM call per tire. Read-only unless "
        "--apply is given. Candidates are master parts whose distributor titles decode to a tire "
        "size; the model confirms or denies with is_tire."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--brands",
            default=None,
            help="Comma-separated brand names, matched case-insensitively (e.g. NITTO). Omit for the whole catalog.",
        )
        parser.add_argument(
            "--mode",
            default=tire_enrichment.MODE_MISSING,
            choices=list(tire_enrichment.MODES),
            help=(
                "missing: master parts with no tire_specs row (default). "
                "incomplete: rows the model failed to identify -- no model_name or no tread_category. "
                "all: re-enrich everything, including rows that are already complete."
            ),
        )
        parser.add_argument("--limit", type=int, default=None, help="Max candidates to enrich. Omit for all of them.")
        parser.add_argument(
            "--workers",
            type=int,
            default=4,
            help="Concurrent LLM calls. Default 4.",
        )
        parser.add_argument(
            "--apply",
            action="store_true",
            help="Write tire_specs and stamp MasterPart.product_type. Without it, nothing is written.",
        )
        parser.add_argument(
            "--include-rejected",
            action="store_true",
            help="Re-offer master parts a previous run's model said were not tires.",
        )
        parser.add_argument(
            "--preview",
            type=int,
            default=None,
            metavar="N",
            help="Build and print N prompts, then exit. Calls no API and costs nothing.",
        )
        parser.add_argument(
            "--report",
            default=None,
            metavar="PATH",
            help="Write a per-candidate CSV for hand grading. Includes rejections.",
        )

    def handle(self, *args, **options):
        brand_names = None
        if options["brands"]:
            brand_names = [name.strip() for name in options["brands"].split(",") if name.strip()]
            unknown = sorted(set(brand_names) - set(tire_enrichment.resolve_brand_ids(brand_names)))
            if unknown:
                raise CommandError("Unknown brand(s): {}".format(", ".join(unknown)))

        if options["preview"] is not None:
            return self._preview(brand_names=brand_names, options=options)

        report_rows = []
        report_path = pathlib.Path(options["report"]) if options["report"] else None

        def collect(candidate, validated, rejection):
            report_rows.append(
                {
                    "master_part_id": candidate.master_part_id,
                    "brand": candidate.brand_name,
                    "size_display": candidate.parsed.size_display,
                    "notation": candidate.parsed.notation,
                    "overall_diameter_in": candidate.parsed.overall_diameter_in,
                    "titles": " | ".join(candidate.titles),
                    "size_variants": " | ".join(candidate.size_variants),
                    "rejection": rejection or "",
                    "is_tire": "" if validated is None else validated.is_tire,
                    "model_name": (validated.model_name or "") if validated else "",
                    "sub_model": (validated.sub_model or "") if validated else "",
                    "tread_category": (validated.tread_category or "") if validated else "",
                    "vehicle_class": (validated.vehicle_class or "") if validated else "",
                    "is_3pmsf": "" if not validated or validated.is_3pmsf is None else validated.is_3pmsf,
                    "confidence": (validated.confidence if validated else "") or "",
                    "search_aliases": " | ".join(validated.search_aliases) if validated else "",
                    "reason": (validated.reason or "") if validated else "",
                }
            )

        try:
            stats = tire_enrichment.run(
                brand_names=brand_names,
                mode=options["mode"],
                limit=options["limit"],
                max_workers=options["workers"],
                apply_changes=options["apply"],
                include_rejected=options["include_rejected"],
                on_result=collect if report_path else None,
            )
        except ValueError as exc:
            raise CommandError(str(exc))

        if report_path and report_rows:
            with report_path.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=list(report_rows[0]))
                writer.writeheader()
                writer.writerows(report_rows)
            self.stdout.write("Report: {} ({} rows)".format(report_path, len(report_rows)))

        self._report(stats, applied=options["apply"])

    def _preview(self, *, brand_names, options):
        """Build prompts without calling anything. The point is checking what the model will see
        -- especially whether the titles we collected actually name the product."""
        system_prompt = tire_enrichment.build_system_prompt()
        self.stdout.write(
            "System prompt: {} chars, ~{} tokens (fixed for the run, cacheable)\n".format(
                len(system_prompt), azure_llm.estimate_tokens(system_prompt)
            )
        )

        shown = 0
        input_tokens = 0
        for candidate in tire_enrichment.iter_candidates(
            brand_ids=list(tire_enrichment.resolve_brand_ids(brand_names).values()) if brand_names else None,
            mode=options["mode"],
            limit=options["preview"],
            include_rejected=options["include_rejected"],
        ):
            payload = json.dumps(tire_enrichment.build_user_payload(candidate), indent=2)
            input_tokens += azure_llm.estimate_tokens(payload)
            self.stdout.write("\n" + "-" * 78)
            self.stdout.write(payload)
            if len(candidate.size_variants) > 1:
                self.stdout.write(
                    self.style.WARNING(
                        "  size conflict across providers: {}".format(", ".join(candidate.size_variants))
                    )
                )
            shown += 1

        if not shown:
            self.stdout.write(self.style.WARNING("\nNo candidates matched."))
            return

        per_call = azure_llm.estimate_cost(system_prompt, "", tire_enrichment.LLM_MAX_TOKENS)
        self.stdout.write("\n" + "-" * 78)
        self.stdout.write("{} previewed. Average user payload ~{} tokens.".format(shown, input_tokens // shown))
        self.stdout.write(
            "Worst case per call (uncached system prompt, output at the {}-token cap): ${:.4f}".format(
                tire_enrichment.LLM_MAX_TOKENS, per_call["total_cost_usd_worst_case"]
            )
        )

    def _report(self, stats, *, applied):
        self.stdout.write("\nCandidate selection")
        self.stdout.write("  master parts scanned    {}".format(stats.scanned))
        self.stdout.write("  no parseable size       {}".format(stats.no_size))
        self.stdout.write("  size conflict flagged   {}".format(stats.size_conflict))

        self.stdout.write("\nLLM")
        self.stdout.write("  calls                   {}".format(stats.called))
        self.stdout.write("  transport errors        {}".format(stats.llm_errors))
        for reason, count in sorted(stats.rejected.items()):
            self.stdout.write("  rejected: {:<14}{}".format(reason, count))
        self.stdout.write("  not a tire              {}".format(stats.not_a_tire))

        accepted = stats.called - stats.llm_errors - sum(stats.rejected.values()) - stats.not_a_tire
        self.stdout.write("\nAccepted ({})".format(accepted))
        if accepted:
            self.stdout.write(
                "  with tread_category     {} ({:.1f}%)".format(
                    stats.with_category, 100 * stats.with_category / accepted
                )
            )
            self.stdout.write(
                "  with model_name         {} ({:.1f}%)".format(
                    stats.with_model_name, 100 * stats.with_model_name / accepted
                )
            )

        if applied:
            self.stdout.write("\nWritten")
            self.stdout.write("  tire_specs upserted     {}".format(stats.written))
            self.stdout.write("  product_type -> tire    {}".format(stats.product_type_set))
            if stats.product_type_conflict:
                self.stdout.write(
                    self.style.WARNING(
                        "  distributor disagrees   {} (left as the distributor filed them)".format(
                            stats.product_type_conflict
                        )
                    )
                )
        else:
            self.stdout.write(
                self.style.WARNING(
                    "\nNothing written -- pass --apply to commit ({} specs were built).".format(
                        accepted - stats.not_a_tire if accepted else 0
                    )
                )
            )
