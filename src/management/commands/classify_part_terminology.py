"""
Classify master parts into PCdb part terminologies with an LLM. See
src/integrations/services/part_terminology_classification.py for the machinery, the two prompts,
and the four production-confirmed fixes the pipeline depends on.

Writes nothing unless --apply is passed, matching classify_master_part_types. A bare run does the
full classification and reports what it would write, which is the intended way to sanity-check a
model or endpoint change before it touches ml_part_terminology_classification.

The endpoint is any OpenAI-compatible server -- Ollama, vLLM, LM Studio all expose one. Defaults
come from QWEN_API_BASE_URL / QWEN_API_KEY / QWEN_MODEL_NAME; the --llm-* flags override them for
a single run, which is the easy way to try a different model without editing .env.

Typical runs:
    # 20 parts against a local Ollama, write nothing, print every verdict
    manage.py classify_part_terminology --limit 20 --show-rows \
        --llm-base-url http://localhost:11434/v1 --llm-model qwen2.5:32b

    # a few brands, evenly sampled, still read-only
    manage.py classify_part_terminology --brand-ids 2951,3564,5914 --limit 300

    # commit it
    manage.py classify_part_terminology --brand-ids 2951 --apply
"""
from django.core.management.base import BaseCommand, CommandError

from src.integrations.services import part_terminology_classification as terminology


class Command(BaseCommand):
    help = (
        "Classify master parts into PCdb part terminologies via a two-stage LLM pipeline. "
        "Read-only unless --apply is given. Every part gets a row, including the ones the model "
        "refuses to classify -- 'unclassifiable' is an answer, not an absence."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--brand-ids",
            default=None,
            help="Comma-separated MasterPart.brand_id filter. Omit for the whole catalog.",
        )
        parser.add_argument(
            "--limit",
            type=int,
            default=None,
            help=(
                "Cap total parts processed. With several --brand-ids the limit is spread evenly "
                "across them rather than spent on whichever brand sorts first."
            ),
        )
        parser.add_argument(
            "--batch-size",
            type=int,
            default=50,
            help=(
                "Parts per LLM call, both stages (default: 50). Lower this if the model's context "
                "window can't hold the largest pooled candidate list."
            ),
        )
        parser.add_argument(
            "--max-workers",
            type=int,
            default=8,
            help="Concurrent LLM calls (default: 8).",
        )
        parser.add_argument(
            "--reclassify",
            action="store_true",
            help="Include parts that already have a classification row. Default: skip them.",
        )
        parser.add_argument(
            "--llm-base-url",
            default=None,
            help="OpenAI-compatible endpoint, e.g. http://localhost:11434/v1. Default: QWEN_API_BASE_URL.",
        )
        parser.add_argument(
            "--llm-api-key",
            default=None,
            help="Endpoint API key. Default: QWEN_API_KEY. Most self-hosted servers ignore it.",
        )
        parser.add_argument(
            "--llm-model",
            default=None,
            help="Model name the endpoint expects, e.g. qwen2.5:32b. Default: QWEN_MODEL_NAME.",
        )
        parser.add_argument(
            "--max-candidates",
            type=int,
            default=terminology.MAX_STAGE2_CANDIDATES,
            help=(
                "Largest Stage 2 candidate pool to send (default: {}). A handful of PCdb "
                "subcategories pool to more terms than any context window holds -- the worst is "
                "11,601 terms, about 488k tokens. A pool over this falls back to the single "
                "category/subcategory pair Stage 1 chose, which fits in 95 of the 96 cases.".format(
                    terminology.MAX_STAGE2_CANDIDATES
                )
            ),
        )
        parser.add_argument(
            "--taxonomy-examples",
            type=int,
            default=3,
            help=(
                "Real terminology names shown as examples beside each subcategory in Stage 1's "
                "taxonomy (default: 3). Raising it to 6 fixed a whole product family that kept "
                "routing to a sibling catch-all subcategory, but cost 50%% more taxonomy tokens "
                "per Stage 1 call and did not move the aggregate on a diverse sample -- worth "
                "trying when a specific family routes consistently wrong."
            ),
        )
        parser.add_argument(
            "--reasoning-effort",
            default=terminology.DEFAULT_REASONING_EFFORT,
            help=(
                "Reasoning budget for models that have one (gpt-oss, o-series): low/medium/high. "
                "Default: {}. Pass an empty string to omit the parameter entirely, which is "
                "required for non-reasoning models served by vLLM -- they reject it.".format(
                    terminology.DEFAULT_REASONING_EFFORT
                )
            ),
        )
        parser.add_argument(
            "--show-rows",
            action="store_true",
            help="Print every per-part verdict. Intended for small --limit runs.",
        )
        parser.add_argument(
            "--apply",
            action="store_true",
            help="Write the verdicts to ml_part_terminology_classification. Without it, nothing is written.",
        )

    def handle(self, *args, **options):
        brand_ids = None
        if options["brand_ids"]:
            try:
                brand_ids = [int(value.strip()) for value in options["brand_ids"].split(",") if value.strip()]
            except ValueError:
                raise CommandError("--brand-ids must be a comma-separated list of integers.")
            if not brand_ids:
                raise CommandError("--brand-ids was given but empty.")

        if options["batch_size"] < 1:
            raise CommandError("--batch-size must be at least 1.")
        if options["max_workers"] < 1:
            raise CommandError("--max-workers must be at least 1.")

        result = terminology.run(
            brand_ids=brand_ids,
            limit=options["limit"],
            batch_size=options["batch_size"],
            max_workers=options["max_workers"],
            include_classified=options["reclassify"],
            apply_changes=options["apply"],
            base_url=options["llm_base_url"],
            api_key=options["llm_api_key"],
            model=options["llm_model"],
            reasoning_effort=options["reasoning_effort"] or None,
            examples_per_subcategory=options["taxonomy_examples"],
            max_candidates=options["max_candidates"],
        )

        if not result["parts"]:
            self.stdout.write(self.style.WARNING(
                "No parts matched. Everything selected may already be classified -- "
                "pass --reclassify to redo them."))
            return

        self.stdout.write("\nRun")
        self.stdout.write("  {:<22} {}".format("model", result["model"]))
        self.stdout.write("  {:<22} {:.1f}s".format("elapsed", result["elapsed"]))
        self.stdout.write("  {:<22} {}".format("parts", result["parts"]))

        self.stdout.write("\nStage 1 routing")
        self.stdout.write("  {:<22} {:>8}".format("routed", result["routed"]))
        self.stdout.write("  {:<22} {:>8}   across {} pair(s)".format("", "", result["pairs"]))
        self.stdout.write("  {:<22} {:>8}".format("unroutable", result["unroutable"]))
        if result["narrowed_pool"]:
            self.stdout.write("  {:<22} {:>8}   <- pool too big; judged on their own pair only".format(
                "narrowed to pair", result["narrowed_pool"]))
        if result["oversized_pool"]:
            self.stdout.write("  {:<22} {:>8}   <- too big even unpooled; not sent".format(
                "oversized", result["oversized_pool"]))

        self.stdout.write("\nVerdicts")
        self.stdout.write("  {:<22} {:>8}".format("classified", result["classified"]))
        self.stdout.write("  {:<22} {:>8}   <- the model looked and refused".format(
            "unclassifiable", result["unclassifiable"]))
        self.stdout.write("  {:<22} {:>8}   <- the pipeline itself failed".format(
            "error", result["errors"]))

        if options["show_rows"]:
            self.stdout.write("\nPer-part")
            for row in sorted(result["rows"], key=lambda r: r["master_part_id"]):
                self.stdout.write("  {} {:<15} term={:<8} {}/{}".format(
                    row["master_part_id"],
                    row["status"],
                    row["part_terminology_id"] if row["part_terminology_id"] is not None else "-",
                    row["category"] or "-",
                    row["subcategory"] or "-",
                ))
                if row["reasoning"]:
                    self.stdout.write("      {}".format(row["reasoning"]))

        if not result["applied"]:
            self.stdout.write(self.style.WARNING(
                "\nDry run -- nothing written. Re-run with --apply to commit."))
            return

        self.stdout.write(self.style.SUCCESS(
            "\nWrote {} row(s) to ml_part_terminology_classification.".format(result["written"])))
