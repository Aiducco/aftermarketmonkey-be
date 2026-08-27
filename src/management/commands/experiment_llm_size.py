"""
Measure how well the LLM reads a tire size, against the parser that currently does it.

**Writes nothing.** The parser stays the source of truth whatever this reports; the output is a
number to decide with, not a migration. See ``src/integrations/services/tire_size_experiment.py``
for why the prompt forbids deriving and why a null counts as an abstention rather than a miss.

    manage.py experiment_llm_size --brands TOYO --limit 150
    manage.py experiment_llm_size --brands TOYO --limit 150 --provider anthropic
    manage.py experiment_llm_size --brands TOYO --limit 300 --report /tmp/size.csv
"""
import concurrent.futures
import csv
import pathlib

from django.core.management.base import BaseCommand, CommandError

from src.integrations.llm import anthropic_llm, azure_llm
from src.integrations.services import tire_enrichment, tire_size_experiment


class Command(BaseCommand):
    help = "Ask the LLM to read tire sizes and score it against src.domain.tire_size. Writes nothing."

    def add_arguments(self, parser):
        parser.add_argument("--brands", required=True, help="Comma-separated brand names.")
        parser.add_argument("--limit", type=int, default=150, help="Rows to test. Default 150.")
        parser.add_argument("--workers", type=int, default=10)
        parser.add_argument("--seed", type=int, default=1, help="Same seed tests the same rows.")
        parser.add_argument(
            "--provider",
            default="azure",
            choices=["azure", "anthropic"],
            help="Which model reads the sizes. Default azure (the one that did enrichment).",
        )
        parser.add_argument("--model", default=None, help="Override the model/deployment.")
        parser.add_argument("--report", default=None, help="CSV of every disagreement.")

    def handle(self, *args, **options):
        names = [n.strip() for n in options["brands"].split(",") if n.strip()]
        resolved = tire_enrichment.resolve_brand_ids(names)
        unknown = sorted(set(names) - set(resolved))
        if unknown:
            raise CommandError("Unknown brand(s): {}".format(", ".join(unknown)))

        candidates = tire_size_experiment.sample_candidates(
            brand_ids=list(resolved.values()), limit=options["limit"], seed=options["seed"]
        )
        if not candidates:
            raise CommandError("No candidates found.")

        if options["provider"] == "anthropic":
            llm_module = anthropic_llm
            model = options["model"] or anthropic_llm.model_name()
            cli = anthropic_llm.client()
        else:
            llm_module = azure_llm
            model = options["model"] or azure_llm.deployment()
            cli = azure_llm.client()

        self.stdout.write("Testing {} rows with {} against src.domain.tire_size.\n".format(len(candidates), model))

        with concurrent.futures.ThreadPoolExecutor(max_workers=options["workers"]) as pool:
            results = list(pool.map(lambda c: tire_size_experiment.ask(cli, c, llm_module, model), candidates))

        errors = [r for r in results if r.error]
        if errors:
            self.stdout.write(self.style.WARNING("  {} call(s) failed, e.g. {}".format(len(errors), errors[0].error)))

        self.stdout.write(self.style.MIGRATE_HEADING("Agreement with the parser, per field"))
        self.stdout.write(
            "   {:<22}{:>8}{:>10}{:>12}{:>14}{:>14}".format(
                "field", "agree", "disagree", "agreement", "LLM abstained", "parser abstained"
            )
        )
        worst = []
        for field in tire_size_experiment.COMPARED_FIELDS:
            agree = sum(1 for r in results if r.verdict(field) is True)
            disagree = sum(1 for r in results if r.verdict(field) is False)
            llm_null = sum(1 for r in results if r.abstained(field) == "llm")
            parser_null = sum(1 for r in results if r.abstained(field) == "parser")
            compared = agree + disagree
            rate = 100 * agree / compared if compared else 0.0
            if compared:
                worst.append((rate, field))
            self.stdout.write(
                "   {:<22}{:>8}{:>10}{:>11.1f}%{:>14}{:>14}".format(field, agree, disagree, rate, llm_null, parser_null)
            )

        # A row is only usable if EVERY size field matches -- a spec with one wrong number is a
        # wrong spec, so per-field rates flatter the result.
        complete = sum(
            1
            for r in results
            if r.llm is not None and all(r.verdict(f) is not False for f in tire_size_experiment.COMPARED_FIELDS)
        )
        self.stdout.write(
            "\n   rows with NO disagreement on any field: {}/{}  ({:.1f}%)".format(
                complete, len(results), 100 * complete / len(results)
            )
        )

        missed = [r for r in results if r.llm is not None and r.llm.get("is_size_present") is False]
        if missed:
            self.stdout.write(
                self.style.WARNING("\n   {} row(s) where the LLM found no size but the parser did:".format(len(missed)))
            )
            for r in missed[:6]:
                self.stdout.write("      {} | {}".format(r.parser["size_display"], r.titles[0][:62]))

        self.stdout.write(self.style.MIGRATE_HEADING("\nWorst fields, with examples"))
        for _rate, field in sorted(worst)[:4]:
            bad = [r for r in results if r.verdict(field) is False][:4]
            if not bad:
                continue
            self.stdout.write("   {}".format(field))
            for r in bad:
                self.stdout.write(
                    "      parser={!s:<16} llm={!s:<16} {}".format(
                        r.parser.get(field), r.llm.get(field), r.titles[0][:56]
                    )
                )

        if options["report"]:
            path = pathlib.Path(options["report"])
            with path.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.writer(handle)
                writer.writerow(["master_part_id", "field", "parser", "llm", "titles"])
                for r in results:
                    for field in tire_size_experiment.COMPARED_FIELDS:
                        if r.verdict(field) is False:
                            writer.writerow(
                                [
                                    r.master_part_id,
                                    field,
                                    r.parser.get(field),
                                    (r.llm or {}).get(field),
                                    " | ".join(r.titles),
                                ]
                            )
            self.stdout.write("\nDisagreement CSV: {}".format(path))

        self.stdout.write(self.style.WARNING("\nNothing was written. The parser remains the source of truth."))
