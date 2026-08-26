"""
Report on the quality of ``tire_specs``. Read-only -- it never writes.

See ``src/integrations/services/tire_validation.py`` for what each check means and why.

    manage.py validate_tire_specs                    # whole catalog
    manage.py validate_tire_specs --brands NITTO     # one brand
    manage.py validate_tire_specs --strict            # exit non-zero if a consistency check fails

``--strict`` is what a nightly cron should use: the consistency checks are all bugs-not-gaps, so a
non-zero exit means something regressed rather than that data is merely missing.
"""
from django.core.management.base import BaseCommand, CommandError

from src.integrations.services import tire_enrichment, tire_validation


class Command(BaseCommand):
    help = "Report tire_specs data quality: correctness against distributor figures, internal consistency, coverage."

    def add_arguments(self, parser):
        parser.add_argument("--brands", default=None, help="Comma-separated brand names. Omit for the whole catalog.")
        parser.add_argument("--brand-limit", type=int, default=40, help="Rows in the per-brand table. Default 40.")
        parser.add_argument(
            "--strict",
            action="store_true",
            help="Exit non-zero if any internal-consistency check fails. For cron.",
        )

    def handle(self, *args, **options):
        brand_ids = None
        label = "whole catalog"
        if options["brands"]:
            names = [n.strip() for n in options["brands"].split(",") if n.strip()]
            resolved = tire_enrichment.resolve_brand_ids(names)
            unknown = sorted(set(names) - set(resolved))
            if unknown:
                raise CommandError("Unknown brand(s): {}".format(", ".join(unknown)))
            brand_ids = list(resolved.values())
            label = ", ".join(names)

        self.stdout.write(self.style.MIGRATE_HEADING("\nTire spec validation — {}".format(label)))

        # 1. Outside evidence first: this is the only check that can prove the parser wrong.
        self.stdout.write(self.style.MIGRATE_HEADING("\n1. Correctness against distributor-stated figures"))
        cross, nominal = tire_validation.overall_diameter_cross_check(brand_ids)
        self._render(cross, invert=True)
        if nominal:
            self.stdout.write(
                "      {} numeric-notation row(s) excluded -- their diameter is nominal by "
                "construction, not measured. See tire_size's module docstring.".format(len(nominal))
            )
            for line in nominal[:4]:
                self.stdout.write("        ~ {}".format(line))

        # 2. Internal consistency.
        self.stdout.write(self.style.MIGRATE_HEADING("\n2. Internal consistency (every one should be zero)"))
        consistency = tire_validation.consistency_checks(brand_ids)
        for check in consistency:
            self._render(check)

        # 3. Coverage.
        self.stdout.write(self.style.MIGRATE_HEADING("\n3. Coverage"))
        for field, populated, total in tire_validation.coverage(brand_ids):
            pct = (100 * populated / total) if total else 0
            self.stdout.write("   {:<26}{:>8,} / {:<8,} {:>6.1f}%".format(field, populated, total, pct))

        advisory = tire_validation.possible_abbreviations(brand_ids)
        if advisory:
            self.stdout.write(
                "\n   advisory: {} model_name value(s) look unexpanded (many are real names): {}".format(
                    len(advisory), ", ".join(advisory[:8])
                )
            )

        # 4. Review queue.
        splits = tire_validation.split_votes()
        self.stdout.write(self.style.MIGRATE_HEADING("\n4. Models still split on a category (review queue)"))
        if not splits:
            self.stdout.write(self.style.SUCCESS("   none — reconciliation converged"))
        else:
            for brand, model, categories, sizes in splits:
                self.stdout.write(
                    "   {:<22}{:<30}{} categories over {} sizes".format(
                        brand[:21], (model or "")[:29], categories, sizes
                    )
                )

        # 5. Per brand.
        self.stdout.write(self.style.MIGRATE_HEADING("\n5. Per brand"))
        self.stdout.write(
            "   {:<28}{:>7}{:>8}{:>10}{:>10}{:>8}".format("brand", "specs", "models", "model %", "categ %", "disputed")
        )
        for name, specs, with_model, with_category, disputed, models in tire_validation.per_brand(
            options["brand_limit"]
        ):
            self.stdout.write(
                "   {:<28}{:>7,}{:>8,}{:>9.1f}%{:>9.1f}%{:>8,}".format(
                    name[:27], specs, models, 100 * with_model / specs, 100 * with_category / specs, disputed
                )
            )

        failed = [c for c in consistency if not c.ok]
        if failed:
            self.stdout.write(
                self.style.ERROR(
                    "\n{} consistency check(s) failing: {}".format(len(failed), ", ".join(c.name for c in failed))
                )
            )
            if options["strict"]:
                raise CommandError("Consistency checks failed.")
        else:
            self.stdout.write(self.style.SUCCESS("\nAll internal-consistency checks pass."))

    def _render(self, check, invert=False):
        if check.total == 0:
            self.stdout.write("   {:<44}{}".format(check.name, "no data to compare"))
            return
        agree = check.total - check.failures
        if invert:
            body = "{:>8,} / {:<8,} agree  ({:.2f}% disagree)".format(agree, check.total, 100 * check.rate)
        else:
            body = "{:>8,} failing of {:,}".format(check.failures, check.total)
        style = self.style.SUCCESS if check.ok else self.style.WARNING
        self.stdout.write("   {:<44}{}".format(check.name, style(body)))
        if check.detail:
            self.stdout.write("      {}".format(check.detail))
        for sample in check.samples:
            self.stdout.write("        - {}".format(sample))
