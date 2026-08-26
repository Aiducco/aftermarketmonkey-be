"""
Independently audit the LLM-derived fields on ``tire_specs``, using a different vendor's model.

Read-only. It writes nothing and changes nothing -- it produces a number.

See ``src/integrations/services/tire_audit.py`` for why the auditor is a different model family
and why it is shown the evidence rather than our answer. Agreement is a lower bound on
correctness: both models can share a misconception.

    manage.py audit_tire_specs --per-band 60              # ~180 rows across three confidence bands
    manage.py audit_tire_specs --only-3pmsf --per-band 40 # the field with legal exposure
    manage.py audit_tire_specs --brands MICHELIN --report /tmp/audit.csv
"""
import concurrent.futures
import csv
import pathlib

from django.core.management.base import BaseCommand, CommandError

from src.integrations.llm import anthropic_llm
from src.integrations.services import tire_audit, tire_enrichment

# The auditor is asked the original question, never shown our answer -- see the service docstring.
# Built from the same tread vocabulary so a disagreement is a real difference of opinion and not
# one model choosing from a different list.
_AUDIT_PREAMBLE = """You identify tires from distributor catalogue listings. You will be given a
brand, the distributor titles for one tire, and its size.

Answer independently and from your own knowledge of the tire market. Return only JSON:

{
  "model_name": "<the manufacturer's product name, brand and size removed>" or null,
  "tread_category": "<one code from the list below>" or null,
  "vehicle_class": "passenger" | "light_truck" | "trailer" | "commercial" | "motorcycle" | "atv_utv" | null,
  "is_3pmsf": true | false | null
}

RULES
- If you cannot identify the product, return null rather than guessing from the brand.
- is_3pmsf is the Three-Peak Mountain Snowflake certification. Return true ONLY if you know this
  specific model carries that marking. Never infer it from "all weather" or "winter capable"
  marketing language. Return null when unsure -- null is a valid and often correct answer.
- tread_category must be one of the codes below, or null.

TREAD CATEGORY CODES
"""


class Command(BaseCommand):
    help = (
        "Blind re-derivation of tire_specs LLM fields by a second, different-vendor model. "
        "Reports agreement per field and per confidence band. Writes nothing."
    )

    def add_arguments(self, parser):
        parser.add_argument("--per-band", type=int, default=50, help="Rows per confidence band (3 bands). Default 50.")
        parser.add_argument("--brands", default=None, help="Comma-separated brand names. Omit for the whole catalog.")
        parser.add_argument(
            "--only-3pmsf", action="store_true", help="Audit only rows asserting severe-snow certification."
        )
        parser.add_argument("--workers", type=int, default=8, help="Concurrent auditor calls. Default 8.")
        parser.add_argument("--seed", type=int, default=1, help="Sample seed; the same seed audits the same rows.")
        parser.add_argument("--model", default=None, help="Override the auditor model.")
        parser.add_argument("--report", default=None, help="Write a per-row CSV of every disagreement.")

    def handle(self, *args, **options):
        brand_ids = None
        if options["brands"]:
            names = [n.strip() for n in options["brands"].split(",") if n.strip()]
            resolved = tire_enrichment.resolve_brand_ids(names)
            unknown = sorted(set(names) - set(resolved))
            if unknown:
                raise CommandError("Unknown brand(s): {}".format(", ".join(unknown)))
            brand_ids = list(resolved.values())

        sampled = tire_audit.sample(
            per_band=options["per_band"],
            brand_ids=brand_ids,
            only_3pmsf=options["only_3pmsf"],
            seed=options["seed"],
        )
        rows = tire_audit.build_audit_rows(sampled)
        if not rows:
            raise CommandError("No rows sampled.")

        # The vocabulary block is shared with enrichment so both models choose from one list.
        from src import models as src_models

        vocabulary = "\n".join(
            "  {:<12} {} -- {}".format(c.code, c.label, c.description) for c in src_models.TreadCategory.objects.all()
        )
        system_prompt = _AUDIT_PREAMBLE + vocabulary

        auditor = options["model"] or anthropic_llm.model_name()
        self.stdout.write(
            "Auditing {} rows with {} (enrichment used {}).".format(
                len(rows), auditor, tire_enrichment.azure_llm.deployment()
            )
        )

        cli = anthropic_llm.client()
        with concurrent.futures.ThreadPoolExecutor(max_workers=options["workers"]) as pool:
            done = list(pool.map(lambda r: tire_audit.audit_one(cli, r, system_prompt, auditor), rows))

        errors = [r for r in done if r.error]
        if errors:
            self.stdout.write(
                self.style.WARNING("  {} auditor call(s) failed: {}".format(len(errors), errors[0].error))
            )

        results = tire_audit.tally(done)
        self.stdout.write(self.style.MIGRATE_HEADING("\nAgreement with an independent model"))
        self.stdout.write(
            "   {:<18}{:>8}{:>10}{:>12}{:>14}{:>16}".format(
                "field", "agree", "disagree", "agreement", "we abstained", "auditor abstained"
            )
        )
        for field in tire_audit.AUDITED_FIELDS:
            r = results[field]
            self.stdout.write(
                "   {:<18}{:>8,}{:>10,}{:>11.1f}%{:>14,}{:>16,}".format(
                    field, r.agree, r.disagree, 100 * r.agreement, r.we_abstained, r.auditor_abstained
                )
            )
        self.stdout.write(
            "      Abstentions are NULL on one side only. NULL means unknown, which the prompt "
            "rewards over guessing, so it is not counted as a disagreement."
        )

        self.stdout.write(self.style.MIGRATE_HEADING("\ntread_category agreement by confidence band"))
        for band, r in sorted(tire_audit.tally_by_band(done, "tread_category").items()):
            self.stdout.write(
                "   {:<10}{:>6,} agree {:>6,} disagree   {:>6.1f}%".format(band, r.agree, r.disagree, 100 * r.agreement)
            )

        self.stdout.write(self.style.MIGRATE_HEADING("\nis_3pmsf disagreements (the ones that matter)"))
        snow = [r for r in done if r.agrees_on("is_3pmsf") is False]
        if not snow:
            self.stdout.write("   none -- the two models never contradicted each other on it")
        missed = [r for r in done if r.ours.get("is_3pmsf") is None and (r.theirs or {}).get("is_3pmsf") is True]
        if missed:
            self.stdout.write(
                "   {} row(s) where we abstained but the auditor asserted certified "
                "(candidates for a second pass, not errors):".format(len(missed))
            )
            for row in missed[:8]:
                self.stdout.write(
                    "      {:<16}{:<30}{}".format(
                        row.brand_name[:15], (row.ours.get("model_name") or "")[:29], row.size_display
                    )
                )
        for row in snow[:12]:
            self.stdout.write(
                "   {:<16}{:<28}ours={!s:<6} auditor={!s:<6} {}".format(
                    row.brand_name[:15],
                    (row.ours.get("model_name") or "")[:27],
                    row.ours.get("is_3pmsf"),
                    row.theirs.get("is_3pmsf"),
                    row.size_display,
                )
            )

        self.stdout.write(self.style.MIGRATE_HEADING("\ntread_category disagreements"))
        for row in [r for r in done if r.agrees_on("tread_category") is False][:12]:
            self.stdout.write(
                "   {:<16}{:<28}ours={:<12} auditor={}".format(
                    row.brand_name[:15],
                    (row.ours.get("model_name") or "")[:27],
                    str(row.ours.get("tread_category")),
                    row.theirs.get("tread_category"),
                )
            )

        if options["report"]:
            path = pathlib.Path(options["report"])
            with path.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.writer(handle)
                writer.writerow(["master_part_id", "brand", "size", "band", "field", "ours", "auditor", "titles"])
                for row in done:
                    for field in tire_audit.AUDITED_FIELDS:
                        if row.agrees_on(field) is False:
                            writer.writerow(
                                [
                                    row.master_part_id,
                                    row.brand_name,
                                    row.size_display,
                                    row.band,
                                    field,
                                    row.ours.get(field),
                                    (row.theirs or {}).get(field),
                                    " | ".join(row.titles),
                                ]
                            )
            self.stdout.write("\nDisagreement CSV: {}".format(path))

        self.stdout.write(
            self.style.WARNING(
                "\nAgreement is a LOWER BOUND on correctness. Both models can share a misconception, "
                "and neither has seen the tire."
            )
        )
