from django.core.management.base import BaseCommand

from src.integrations.services import confirmed_purchase_order_sync


class Command(BaseCommand):
    help = (
        "Refresh distributor order data for CONFIRMED purchase orders: checked on every run "
        "within the first hour after submission, then at most once a day after that. See "
        "confirmed_purchase_order_sync's module docstring for the exact policy. Turn14 and "
        "Keystone are implemented so far; other distributors are skipped with a log line."
    )

    def handle(self, *args, **options):
        result = confirmed_purchase_order_sync.refresh_confirmed_purchase_orders()
        self.stdout.write(
            self.style.SUCCESS(
                "Checked {} purchase order(s), skipped {} (not due yet).".format(
                    result["checked"], result["skipped"]
                )
            )
        )
