"""
Seed BrandMeyerBrandMapping (and BrandProviders / CompanyBrands) from a CSV mapping file.

Expected columns: meyer_brand, matched_brand (required); optional score, match_type.

Rows with a non-empty matched_brand link MeyerBrand -> existing Brands (case-insensitive name).
Rows with empty matched_brand (no_match) default to creating a Brands row from the Meyer name
and linking it; use --skip-no-match-brands to leave those unprocessed.

Run after fetch_meyer_feeds so MeyerBrand rows exist.
"""
import os

from django.core.management.base import BaseCommand

from src.integrations.services import meyer


class Command(BaseCommand):
    help = (
        "Apply Meyer brand mappings from CSV (meyer_brand, matched_brand, …) to Brands / mappings."
    )

    def add_arguments(self, parser):
        default_csv = os.path.join(
            os.path.expanduser("~"),
            "Downloads",
            "Meyer brand mapping.csv",
        )
        parser.add_argument(
            "--csv",
            dest="csv_path",
            default=default_csv,
            help="Path to mapping CSV (default: ~/Downloads/Meyer brand mapping.csv).",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Log actions only; no database writes.",
        )
        parser.add_argument(
            "--skip-no-match-brands",
            action="store_true",
            help="For rows with empty matched_brand, do not create Brands (default: create and link).",
        )

    def handle(self, *args, **options):
        csv_path = options["csv_path"]
        dry_run = options["dry_run"]
        create_no_match = not options["skip_no_match_brands"]

        if not os.path.isfile(csv_path):
            self.stdout.write(
                self.style.ERROR("CSV not found: {}".format(csv_path))
            )
            return

        if dry_run:
            self.stdout.write(self.style.WARNING("Dry run — no writes."))
        self.stdout.write("Using mapping CSV: {}".format(csv_path))
        if create_no_match:
            self.stdout.write("No-match rows: will create catalog Brand + mapping (use --skip-no-match-brands to skip).")
        else:
            self.stdout.write("No-match rows: skipped (--skip-no-match-brands).")

        try:
            stats = meyer.apply_meyer_brand_mappings_from_csv(
                csv_path,
                dry_run=dry_run,
                create_brands_for_no_match=create_no_match,
            )
        except Exception as e:
            self.stdout.write(self.style.ERROR("Error: {}".format(str(e))))
            raise

        self.stdout.write(
            "Rows read: {}, {}: {}, skipped_no_meyer: {}, "
            "skipped_no_catalog_brand: {}, skipped_no_match_row: {}.".format(
                stats.get("rows"),
                "would link (dry run)" if dry_run else "rows linked",
                stats.get("would_mappings") if dry_run else stats.get("linked"),
                stats.get("skipped_no_meyer"),
                stats.get("skipped_no_catalog_brand"),
                stats.get("skipped_no_match_row"),
            )
        )
        if dry_run:
            self.stdout.write(
                "  [DRY RUN] would_mappings: {}, would_create_brands: {}, "
                "would_bp: {}, would_cb: {}.".format(
                    stats.get("would_mappings"),
                    stats.get("would_create_brands"),
                    stats.get("would_bp"),
                    stats.get("would_cb"),
                )
            )
        else:
            self.stdout.write(
                "Created brands: {}, mappings: {}, brand_providers: {}, company_brands: {}.".format(
                    stats.get("created_brands"),
                    stats.get("created_mappings"),
                    stats.get("created_brand_providers"),
                    stats.get("created_company_brands"),
                )
            )
            nu = stats.get("new_mappings_brand_provider_reused") or 0
            if nu:
                self.stdout.write(
                    "Note: {} new mapping(s) reused an existing Meyer BrandProviders row for that catalog Brand "
                    "(several MeyerBrand rows can point at one Brand; BrandProviders is unique per brand+provider).".format(
                        nu
                    )
                )

        self.stdout.write(self.style.SUCCESS("Done."))
