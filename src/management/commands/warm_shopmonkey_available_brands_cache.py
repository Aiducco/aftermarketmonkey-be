"""
Precomputes the ShopMonkey inventory brand-picker's slow brand/provider/part_count aggregation
(see shopmonkey_sync.refresh_available_brands_cache) for every company with
FEATURE_SHOPMONKEY_INVENTORY_SYNC enabled, so a live request to GET /shopmonkey/inventory/
available-brands/ always hits a warm cache instead of running the 4.6M+-row aggregate query
itself (confirmed live to take 30s+ and 502 at Trident's real data volume).

Scheduled every 4 hours, same cadence as sync_shopmonkey_inventory -- brand/part-count data only
changes when a distributor's catalog re-syncs, not minute to minute.

    manage.py warm_shopmonkey_available_brands_cache
"""
from django.core.management.base import BaseCommand

from src import models as src_models
from src.api.services import feature_flags as feature_flags_services
from src.integrations.services import shopmonkey_sync as shopmonkey_sync_services


class Command(BaseCommand):
    help = "Precompute and cache the ShopMonkey available-brands list for every feature-flagged company."

    def handle(self, *args, **options):
        company_ids = list(
            src_models.CompanyFeatureFlags.objects.filter(
                feature=feature_flags_services.FEATURE_SHOPMONKEY_INVENTORY_SYNC, enabled=True
            ).values_list("company_id", flat=True)
        )
        if not company_ids:
            self.stdout.write("No companies have the feature flag enabled -- nothing to warm.")
            return

        for company_id in company_ids:
            count = shopmonkey_sync_services.refresh_available_brands_cache(company_id)
            self.stdout.write(f"company_id={company_id}: cached {count} brands")

        self.stdout.write(self.style.SUCCESS(f"Done. Warmed {len(company_ids)} compan{'y' if len(company_ids) == 1 else 'ies'}."))
