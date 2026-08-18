"""
Idempotently creates (or finds) Stripe Products + monthly/annual recurring Prices for the
Tracker/Hunter/Pack plans and the Extra Seat add-on, using settings.STRIPE_SECRET_KEY (LIVE mode
— no test-mode branch, matches every other Stripe call in src/api/services/billing.py).

Does NOT edit conf/settings_base.py itself — prints the resulting product/price ids at the end;
paste them into the matching PLANS[...]/EXTRA_SEAT_ADDON entries by hand before deploying.

Pack's Price here is created identically to Tracker/Hunter's — the per-location
"adjustable_quantity" behavior is a Stripe Checkout line-item property, not a Price property, so
it's applied later in billing.create_checkout_session, never here.

Run: python manage.py provision_stripe_billing_catalog
"""
import stripe
from django.conf import settings
from django.core.management.base import BaseCommand

# plan_id -> (display name, description, monthly cents, annual cents)
_CATALOG = [
    ("tracker", "Tracker", "AftermarketScout Tracker plan", 4900, 48000),
    ("hunter", "Hunter", "AftermarketScout Hunter plan", 9900, 96000),
    ("pack", "Pack", "AftermarketScout Pack plan (per location)", 7900, 78000),
]
_EXTRA_SEAT = ("extra_seat", "Extra Seat", "AftermarketScout additional team seat add-on", 900, 9000)


class Command(BaseCommand):
    help = (
        "Idempotently create/find Stripe Products + monthly/annual Prices for the plan catalog "
        "and the extra-seat add-on. Prints resulting IDs to paste into settings_base.PLANS / "
        "EXTRA_SEAT_ADDON. LIVE mode — uses the configured STRIPE_SECRET_KEY as-is."
    )

    def handle(self, *args, **options):
        api_key = getattr(settings, "STRIPE_SECRET_KEY", None)
        if not api_key:
            self.stderr.write(self.style.ERROR("STRIPE_SECRET_KEY not configured — aborting."))
            return

        stripe.api_key = api_key

        results = {}
        for plan_id, name, description, monthly_cents, annual_cents in _CATALOG + [_EXTRA_SEAT]:
            self.stdout.write(f"Provisioning {plan_id}...")
            product = self._find_or_create_product(plan_id, name, description)
            monthly_price = self._find_or_create_price(product.id, plan_id, "month", monthly_cents)
            annual_price = self._find_or_create_price(product.id, plan_id, "year", annual_cents)
            results[plan_id] = {
                "product_id": product.id,
                "price_monthly": monthly_price.id,
                "price_annual": annual_price.id,
            }
            self.stdout.write(self.style.SUCCESS(f"  {plan_id}: {results[plan_id]}"))

        self.stdout.write("\n--- Paste into conf/settings_base.py ---")
        for plan_id, ids in results.items():
            self.stdout.write(f"{plan_id}: {ids}")

    def _find_or_create_product(self, plan_id: str, name: str, description: str):
        # Product.list is read-your-writes consistent; Product.search is NOT (it's backed by an
        # eventually-consistent index that can lag ~1 minute behind a just-created object) — using
        # search as the primary lookup caused this command to create duplicate live Products on a
        # quick re-run. list() is the only lookup used here for that reason.
        for product in stripe.Product.list(active=True, limit=100).auto_paging_iter():
            if product.metadata.get("plan_id") == plan_id:
                return product
        return stripe.Product.create(name=name, description=description, metadata={"plan_id": plan_id})

    def _find_or_create_price(self, product_id: str, plan_id: str, interval: str, unit_amount: int):
        for price in stripe.Price.list(product=product_id, active=True, limit=100).auto_paging_iter():
            if price.recurring and price.recurring.interval == interval and price.unit_amount == unit_amount:
                return price
        return stripe.Price.create(
            product=product_id,
            currency="usd",
            unit_amount=unit_amount,
            recurring={"interval": interval},
            metadata={"plan_id": plan_id},
        )
