"""
Scheduled (every 4 hours) ShopMonkey inventory sync -- pushes every enabled
ShopMonkeySyncBrand's parts (for every company with FEATURE_SHOPMONKEY_INVENTORY_SYNC on;
Trident Motorsports only today) into ShopMonkey. See
src.integrations.services.shopmonkey_sync for the actual sync logic.

    manage.py sync_shopmonkey_inventory
"""
from django.core.management.base import BaseCommand

from src.integrations.services import shopmonkey_sync as shopmonkey_sync_services


class Command(BaseCommand):
    help = "Sync every enabled ShopMonkeySyncBrand's parts into ShopMonkey."

    def handle(self, *args, **options):
        self.stdout.write("Starting ShopMonkey inventory sync...")
        shopmonkey_sync_services.sync_all_enabled_brands()
        self.stdout.write(self.style.SUCCESS("Done."))
