"""
Refresh BrandProviders.part_count from ProviderPart -- the one real aggregation over the
multi-million-row provider_parts table this needs, done here on its own schedule instead of live
on every request (see that field's own docstring in src/models.py for the production timeout this
fixes: ShopMonkey's brand picker GROUP BY-ing provider_parts directly).

Also backfills any (provider, brand) pair real ProviderPart data has but brand_providers doesn't
track yet -- a real gap is better closed than silently skipped.

    manage.py refresh_brand_provider_part_counts
"""
import logging

import pgbulk
from django.core.management.base import BaseCommand
from django.db.models import Count

from src import models as src_models

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[REFRESH-BRAND-PROVIDER-PART-COUNTS]"
_UPSERT_BATCH = 5000


class Command(BaseCommand):
    help = "Refresh BrandProviders.part_count from ProviderPart (platform-wide aggregation, not per-request)."

    def handle(self, *args, **options):
        self.stdout.write("Aggregating ProviderPart by (provider, brand)...")
        rows = list(
            src_models.ProviderPart.objects.values("provider_id", "master_part__brand_id")
            .annotate(part_count=Count("id"))
            .iterator(chunk_size=20000)
        )
        self.stdout.write("Found {} distinct (provider, brand) pairs.".format(len(rows)))

        instances = [
            src_models.BrandProviders(
                provider_id=r["provider_id"],
                brand_id=r["master_part__brand_id"],
                part_count=r["part_count"],
            )
            for r in rows
            if r["master_part__brand_id"]
        ]

        written = 0
        for start in range(0, len(instances), _UPSERT_BATCH):
            batch = instances[start:start + _UPSERT_BATCH]
            pgbulk.upsert(
                src_models.BrandProviders,
                batch,
                unique_fields=["brand", "provider"],
                update_fields=["part_count", "updated_at"],
                returning=False,
            )
            written += len(batch)

        self.stdout.write(self.style.SUCCESS(
            "Done. Upserted part_count for {} (provider, brand) pairs.".format(written)
        ))
