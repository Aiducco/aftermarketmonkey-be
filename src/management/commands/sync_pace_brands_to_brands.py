"""
Map ``pace_brands`` rows to ``Brands``, populating brand_pace_brand_mapping.

Three phases, same technique and order as the TSE/AllPro/Keystone/DLG syncs -- see
src.integrations.utils.brand_matching:

  1. exact AAIA code     PaceBrand.aaia_brand_code == Brands.aaia_code
  2. compact key         punctuation/spacing-insensitive name identity
                         ("AUTOMETER" == "AUTO METER", "WD40" == "WD-40")
  3. fuzzy word-prefix   same chain with truncation on either side
                         ("BAK IND" ~ "BAK INDUSTRIES", "DIRTY LIFE" ~ "DIRTY LIFE WHEELS")

WHY THIS IS NEEDED

PACE's brand codes do not line up with ours. Only 366 of PACE's 628 real AAIA codes (58%) match
a Brands.aaia_code outright, and the two systems demonstrably assign different codes to the same
company -- Putco BCJH vs FKLF, Covercraft BBLQ vs FQMN, Competition Clutch GPTP vs BXVR. Without
a mapping, "how much of PACE do we already carry" has to fall back to matching part numbers
alone, which overcounts badly: 55% part-number-only against 25% on brand+part.

FUZZY MATCHING IS RESTRICTED TO THE REAL-AAIA REGIME, ON PURPOSE

1,028 of the 1,656 pace_brands rows are AAM member-local jobber line codes, not AAIA codes (see
PaceBrand.has_real_aaia_code), and 1,022 of those have a name of three characters or less --
"DOR", "STA", "RAY", "SEA", "SPC". Word-prefix alignment accepts a >=3-character token as a
prefix match, so those names match almost anything beginning with those letters: "SEA" would
resolve against "Sealed Power" or "Seat Saver" with equal confidence, and "STA" against
"Standard Motor Products", "Stampede" or "Stage 8". Some of those guesses would even be right,
which is what makes it dangerous rather than merely noisy.

So line-code brands get phase 1 only. Resolving them properly needs a line-code alias table
(DOR -> Dorman), which is a separate curated job, not something to infer from a 3-letter string.

Nothing is created here. The AllPro/TSE equivalents add a Brands row for whatever stays
unresolved; this does not, because a newly created Brands row has no master_parts behind it and
so cannot contribute to the coverage count this mapping exists to produce. Pass
``--create-missing`` to opt into that phase explicitly.

    python manage.py sync_pace_brands_to_brands
    python manage.py sync_pace_brands_to_brands --dry-run
"""
import logging
import typing

from django.core.management.base import BaseCommand
from django.db import transaction

from src import models as src_models
from src.integrations.utils.brand_matching import (
    best_fuzzy_brand_match,
    brands_by_compact_key,
    brands_by_first_token_upper,
    normalize_compact_key,
    normalize_upper_words,
)

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[SYNC-PACE-BRANDS]"

# Hand-verified collisions the matcher resolves confidently but wrongly -- found by reviewing
# all 84 phase-3 matches, which is what that phase's report exists for. Compared after
# upper+strip on the PaceBrand name. Left unmapped rather than wrongly linked, same device as
# tristate.py's _FUZZY_MATCH_EXCLUSIONS.
_FUZZY_MATCH_EXCLUSIONS = frozenset({
    # "GAT" is Gates (belts, hoses, tensioners). Word-prefix alignment accepts a >=3-character
    # token as a prefix, so it lands on "GATHER GRILLS" -- two unrelated companies sharing
    # three letters. The same trap the line-code regime is excluded from wholesale; BDLK slips
    # through because it carries a real AAIA code with a 3-letter name.
    "GAT",
    # Both of these resolve to Brands #3978 "RAM", and tristate.py already excludes the first
    # for exactly this reason: "Ram Mounts" (phone/tablet mounting hardware) and "RAM Trailer
    # Parts" (towing equipment) are almost certainly two different real-world brands sharing
    # one ambiguous single-word canonical entry. PACE carries both, so both are excluded.
    "RAM MOUNTS USA",
    "RAM TRAILER PARTS",
    # Shop consumables, not ICON Vehicle Dynamics (suspension) -- "Iconic" prefix-matches
    # "ICON" and lands on #3063, which the genuine ICON offroad line also resolves to.
    "ICONIC SHOP SUPPLIES",
})


class Command(BaseCommand):
    help = (
        "Map pace_brands to Brands (exact AAIA code, compact key, fuzzy word-prefix) into "
        "brand_pace_brand_mapping. Fuzzy phases are restricted to real-AAIA brands."
    )

    def add_arguments(self, parser):
        parser.add_argument("--dry-run", action="store_true",
                            help="Report what would be mapped, write nothing.")
        parser.add_argument("--create-missing", action="store_true",
                            help="Also create a Brands row for anything left unresolved "
                                 "(real-AAIA only). Off by default.")
        parser.add_argument("--show", type=int, default=15,
                            help="Sample rows to print per phase (default 15).")

    def handle(self, *args, **options):
        dry_run = options["dry_run"]
        show = options["show"]

        mapped_ids = set(
            src_models.BrandPaceBrandMapping.objects.values_list("pace_brand_id", flat=True)
        )
        unmapped = list(
            src_models.PaceBrand.objects.exclude(id__in=mapped_ids).order_by("aaia_brand_code")
        )
        if not unmapped:
            self.stdout.write("Every pace_brands row is already mapped. Nothing to do.")
            return

        real = [p for p in unmapped if p.has_real_aaia_code]
        line = [p for p in unmapped if not p.has_real_aaia_code]
        self.stdout.write("unmapped pace_brands: %s  (%s real AAIA, %s line-code)%s" % (
            format(len(unmapped), ","), format(len(real), ","), format(len(line), ","),
            " [DRY RUN]" if dry_run else ""))
        self.stdout.flush()

        resolved: typing.Dict[int, typing.Tuple[src_models.Brands, str]] = {}

        # ---- Phase 1: exact AAIA code. Applies to BOTH regimes -- a line code that happens to
        # exist in Brands.aaia_code is an exact identity, not a guess.
        codes = {(p.aaia_brand_code or "").strip().upper() for p in unmapped}
        by_code: typing.Dict[str, src_models.Brands] = {}
        for b in src_models.Brands.objects.filter(aaia_code__in=codes).order_by("id"):
            key = (b.aaia_code or "").strip().upper()
            if key and key not in by_code:
                by_code[key] = b
        for p in unmapped:
            b = by_code.get((p.aaia_brand_code or "").strip().upper())
            if b:
                resolved[p.id] = (b, "aaia_code")
        self._report("phase 1 -- exact AAIA code", resolved, "aaia_code", unmapped, show)

        # ---- Phase 2: compact key. Real-AAIA only (see module docstring).
        todo = [p for p in real if p.id not in resolved]
        if todo:
            compact = brands_by_compact_key()
            for p in todo:
                if (p.name or "").strip().upper() in _FUZZY_MATCH_EXCLUSIONS:
                    continue
                key = normalize_compact_key(p.name or "")
                if not key:
                    continue
                b = compact.get(key)
                if b:
                    resolved[p.id] = (b, "compact_key")
        self._report("phase 2 -- compact key", resolved, "compact_key", unmapped, show)

        # ---- Phase 3: fuzzy word-prefix. Real-AAIA only.
        todo = [p for p in real if p.id not in resolved]
        if todo:
            first_token = brands_by_first_token_upper()
            fallback: typing.Optional[list] = None
            for p in todo:
                if (p.name or "").strip().upper() in _FUZZY_MATCH_EXCLUSIONS:
                    continue
                parts = normalize_upper_words(p.name or "").split()
                candidates = list(first_token.get(parts[0], ())) if parts else []
                if not candidates:
                    if fallback is None:
                        fallback = list(
                            src_models.Brands.objects.only("id", "name", "aaia_code").order_by("id")
                        )
                    candidates = fallback
                b = best_fuzzy_brand_match(p.name or "", candidates)
                if b:
                    resolved[p.id] = (b, "fuzzy")
        self._report("phase 3 -- fuzzy word-prefix", resolved, "fuzzy", unmapped, show)

        # ---- Phase 4 (opt-in only)
        created = 0
        leftover_real = [p for p in real if p.id not in resolved
                         and (p.name or "").strip().upper() not in _FUZZY_MATCH_EXCLUSIONS]
        if options["create_missing"] and leftover_real and not dry_run:
            with transaction.atomic():
                for p in leftover_real:
                    b = src_models.Brands.objects.create(
                        name=(p.name or "").strip() or p.aaia_brand_code,
                        aaia_code=p.aaia_brand_code or None,
                    )
                    resolved[p.id] = (b, "created")
                    created += 1
            self.stdout.write(self.style.WARNING("phase 4 -- created %s new Brands row(s)" % created))

        if not dry_run:
            rows = [
                src_models.BrandPaceBrandMapping(
                    brand=b, pace_brand_id=pid, match_phase=phase)
                for pid, (b, phase) in resolved.items()
            ]
            with transaction.atomic():
                src_models.BrandPaceBrandMapping.objects.bulk_create(rows, ignore_conflicts=True)

        self.stdout.write("")
        self.stdout.write(self.style.SUCCESS("%s of %s pace_brands resolved%s" % (
            format(len(resolved), ","), format(len(unmapped), ","),
            " (dry run -- nothing written)" if dry_run else "")))
        by_phase: typing.Dict[str, int] = {}
        for _b, phase in resolved.values():
            by_phase[phase] = by_phase.get(phase, 0) + 1
        for phase in ("aaia_code", "compact_key", "fuzzy", "created"):
            if by_phase.get(phase):
                self.stdout.write("    %-12s %s" % (phase, format(by_phase[phase], ",")))
        self.stdout.write("  unresolved: %s real AAIA, %s line-code" % (
            format(len(leftover_real), ","),
            format(len([p for p in line if p.id not in resolved]), ",")))
        if not options["create_missing"] and leftover_real:
            self.stdout.write("  (pass --create-missing to add Brands rows for the real-AAIA leftovers)")
        logger.info("%s resolved=%s by_phase=%s created=%s",
                    _LOG_PREFIX, len(resolved), by_phase, created)

    def _report(self, title, resolved, phase, unmapped, show):
        """Print what this phase resolved, so fuzzy results in particular can be eyeballed."""
        hits = [(p, resolved[p.id][0]) for p in unmapped
                if p.id in resolved and resolved[p.id][1] == phase]
        self.stdout.write("")
        self.stdout.write("%s -- %s matched" % (title, format(len(hits), ",")))
        for p, b in hits[:show]:
            flag = "" if (p.name or "").strip().lower() == (b.name or "").strip().lower() else "  <- differs"
            self.stdout.write("    %-6s %-32s -> Brands #%-6s %-32s%s" % (
                p.aaia_brand_code, (p.name or "")[:32], b.id, (b.name or "")[:32], flag))
        if len(hits) > show:
            self.stdout.write("    ... %s more" % format(len(hits) - show, ","))
        self.stdout.flush()
