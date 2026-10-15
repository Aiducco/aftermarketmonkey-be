"""
One-time seed of TSEBrand from the bundled tristate_brand_codes.json (146 brands Tri-State
actually carries, curated from their own public catalog -- see that file's own history for where
it came from), then auto-maps to Brands by AAIA code. Re-running is safe/idempotent (upsert on
aaia_code).

This bootstraps the initial 146-brand set with no credentials needed. get_brands itself now
works (access was granted after following up with Tri-State's rep) and returns the full real
brand list -- run src.integrations.services.tristate.refresh_tse_brands_from_api afterward
(needs a connected company's credentials) to authoritatively backfill warehouse_code/name on
these rows; it doesn't create new TSEBrand rows for brands outside this curated set.

    python manage.py seed_tse_brands
"""
import json
import os

from django.core.management.base import BaseCommand

from src import models as src_models
from src.integrations.services import tristate as tristate_services


def _brand_codes_path() -> str:
    return os.path.join(
        os.path.dirname(__file__), "data", "tristate_brand_codes.json"
    )


class Command(BaseCommand):
    help = "Seed TSEBrand from the bundled tristate_brand_codes.json and map to Brands by AAIA code."

    def handle(self, *args, **options):
        with open(_brand_codes_path()) as f:
            brand_codes = json.load(f)

        created = 0
        updated = 0
        for entry in brand_codes:
            code = (entry.get("code") or "").strip().upper()
            name = (entry.get("name") or "").strip()
            if not code or not name:
                continue
            _, was_created = src_models.TSEBrand.objects.update_or_create(
                aaia_code=code, defaults={"name": name},
            )
            if was_created:
                created += 1
            else:
                updated += 1

        self.stdout.write(
            "Seeded {} TSEBrand rows ({} created, {} updated).".format(created + updated, created, updated)
        )

        mapped = tristate_services.sync_unmapped_tse_brands_to_brands()
        self.stdout.write(self.style.SUCCESS("Mapped {} TSEBrand rows to Brands by AAIA code.".format(mapped)))
