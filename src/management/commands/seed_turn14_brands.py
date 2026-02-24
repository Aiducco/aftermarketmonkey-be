"""
One-time utility to seed Brands, BrandProviders, and BrandTurn14BrandMapping
for all Turn14Brand records. Run after fetch_and_save_turn_14_brands.

Matches Brands by: 1) aaia_code (split by comma, take first), 2) name (case-insensitive).
Uses bulk operations.
"""
from django.core.management.base import BaseCommand

from src import enums as src_enums
from src import models as src_models


class Command(BaseCommand):
    help = (
        "One-time seed: create Brands, BrandProviders, and BrandTurn14BrandMapping "
        "for each Turn14Brand. Matches by aaia_code or name. Uses bulk operations."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Log new brands that would be created without creating them.",
        )
        parser.add_argument(
            "--yes",
            action="store_true",
            help="Skip confirmation prompt when creating new brands.",
        )

    def _normalize_aaia(self, aaia_code):
        """Split by comma, take first part. E.g. 'BBSC,BBSC' -> 'BBSC'."""
        if not aaia_code or not str(aaia_code).strip():
            return None
        return str(aaia_code).strip().split(",")[0].strip() or None

    def _find_brand_for_turn14(self, t14_brand, brands_by_aaia, brands_by_name):
        """Find existing Brand: first by aaia_code, then by name (case-insensitive)."""
        aaia_primary = self._normalize_aaia(t14_brand.aaia_code)
        if aaia_primary and aaia_primary in brands_by_aaia:
            return brands_by_aaia[aaia_primary]
        name_upper = (t14_brand.name or "").strip().upper()
        if name_upper and name_upper in brands_by_name:
            return brands_by_name[name_upper]
        return None

    def handle(self, *args, **options):
        dry_run = options.get("dry_run", False)
        skip_confirm = options.get("yes", False)

        self.stdout.write("Starting Turn14 brands seed (bulk)...")

        turn14_provider = src_models.Providers.objects.filter(
            kind=src_enums.BrandProviderKind.TURN_14.value,
        ).first()
        if not turn14_provider:
            self.stdout.write(
                self.style.ERROR("Turn14 provider not found. Aborting.")
            )
            return

        turn14_brands = list(
            src_models.Turn14Brand.objects.all().only("id", "name", "external_id", "aaia_code")
        )
        if not turn14_brands:
            self.stdout.write(
                self.style.WARNING(
                    "No Turn14Brand records found. Run fetch_and_save_turn_14_brands first."
                )
            )
            return

        # Build lookup: existing Brands by aaia_code (normalized) and by name (uppercase)
        all_brands = list(src_models.Brands.objects.all())
        brands_by_aaia = {}
        brands_by_name = {}
        for b in all_brands:
            if b.aaia_code:
                aaia_norm = self._normalize_aaia(b.aaia_code)
                if aaia_norm and aaia_norm not in brands_by_aaia:
                    brands_by_aaia[aaia_norm] = b
            if b.name:
                brands_by_name[(b.name or "").strip().upper()] = b

        # 1. Determine which Brands we need (find or create)
        turn14_to_brand = {}
        new_brand_names = set()

        for t14 in turn14_brands:
            brand = self._find_brand_for_turn14(t14, brands_by_aaia, brands_by_name)
            if brand:
                turn14_to_brand[t14.id] = brand
            else:
                name = (t14.name or "").strip()
                if name:
                    new_brand_names.add(name)
                    turn14_to_brand[t14.id] = None  # will create

        # Create new Brands (dedupe by uppercase name)
        new_brand_names = list(dict.fromkeys(n for n in new_brand_names if n and n.upper() not in brands_by_name))
        new_brands = []
        new_brands_log = []
        for n in new_brand_names:
            t14_match = next((t for t in turn14_brands if (t.name or "").strip() == n), None)
            aaia = self._normalize_aaia(t14_match.aaia_code if t14_match else None)
            new_brands.append(
                src_models.Brands(
                    name=n,
                    status=src_enums.BrandProviderStatus.ACTIVE.value,
                    status_name=src_enums.BrandProviderStatus.ACTIVE.name,
                    aaia_code=aaia,
                )
            )
            new_brands_log.append({
                "name": n,
                "aaia_code": aaia,
                "turn14_external_id": t14_match.external_id if t14_match else None,
            })

        if new_brands_log:
            self.stdout.write("")
            self.stdout.write(self.style.WARNING("NEW Turn14 brands to be created ({}):".format(len(new_brands_log))))
            for i, entry in enumerate(new_brands_log, 1):
                self.stdout.write("  {}. name={!r} aaia_code={!r} turn14_external_id={!r}".format(
                    i, entry["name"], entry["aaia_code"], entry["turn14_external_id"]
                ))
            self.stdout.write("")
        else:
            self.stdout.write("  No new brands to create (all Turn14 brands matched existing).")

        if dry_run:
            self.stdout.write(self.style.WARNING("Dry run. No changes made. Run without --dry-run to create."))
            return

        if new_brands and not skip_confirm:
            confirm = input("Create {} new brand(s)? [y/N]: ".format(len(new_brands)))
            if confirm.lower() not in ("y", "yes"):
                self.stdout.write(self.style.WARNING("Aborted."))
                return

        if new_brands:
            src_models.Brands.objects.bulk_create(new_brands)
            for b in new_brands:
                brands_by_name[(b.name or "").strip().upper()] = b
                if b.aaia_code:
                    brands_by_aaia[b.aaia_code] = b

        # Resolve turn14_to_brand for newly created (now in brands_by_name)
        for t14 in turn14_brands:
            if turn14_to_brand.get(t14.id) is None:
                turn14_to_brand[t14.id] = self._find_brand_for_turn14(t14, brands_by_aaia, brands_by_name)
            if turn14_to_brand.get(t14.id) is None:
                self.stdout.write(self.style.WARNING("  No Brand for Turn14Brand: {} (aaia={})".format(t14.name, t14.aaia_code)))

        # 2. Bulk create BrandProviders
        brand_ids_to_link = {b.id for b in turn14_to_brand.values() if b}
        existing_bp = set(
            src_models.BrandProviders.objects.filter(
                provider=turn14_provider,
                brand_id__in=brand_ids_to_link,
            ).values_list("brand_id", flat=True)
        )
        bp_to_create = [
            src_models.BrandProviders(brand=b, provider=turn14_provider)
            for b in turn14_to_brand.values()
            if b and b.id not in existing_bp
        ]
        if bp_to_create:
            src_models.BrandProviders.objects.bulk_create(bp_to_create)

        self.stdout.write("  BrandProviders: {} created".format(len(bp_to_create)))

        # 3. Bulk create BrandTurn14BrandMapping
        existing_mappings = set(
            src_models.BrandTurn14BrandMapping.objects.filter(
                turn14_brand__in=turn14_brands,
            ).values_list("brand_id", "turn14_brand_id")
        )
        mappings_to_create = []
        for t14 in turn14_brands:
            brand = turn14_to_brand.get(t14.id)
            if not brand:
                continue
            if (brand.id, t14.id) not in existing_mappings:
                mappings_to_create.append(
                    src_models.BrandTurn14BrandMapping(brand=brand, turn14_brand=t14)
                )

        if mappings_to_create:
            src_models.BrandTurn14BrandMapping.objects.bulk_create(mappings_to_create)

        self.stdout.write("  BrandTurn14BrandMapping: {} created".format(len(mappings_to_create)))

        self.stdout.write(
            self.style.SUCCESS(
                "Done. Brands: {} new, BrandProviders: {} new, Mappings: {} new.".format(
                    len(new_brands), len(bp_to_create), len(mappings_to_create)
                )
            )
        )
