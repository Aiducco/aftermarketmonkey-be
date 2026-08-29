import decimal
import enum

from django.contrib.auth import models as auth_models
from django.contrib.postgres import fields as pg_fields
from django.core.serializers.json import DjangoJSONEncoder
from django.db import models as django_db_models
from django.utils import timezone


class Company(django_db_models.Model):
    name = django_db_models.CharField(max_length=255)
    slug = django_db_models.CharField(max_length=255)
    status = django_db_models.PositiveSmallIntegerField()
    status_name = django_db_models.CharField(max_length=255)

    # Onboarding / B2B fields (Step 2)
    business_type = django_db_models.JSONField(
        default=list, blank=True
    )  # list[str], e.g. ["retail_store", "dealership"]
    country = django_db_models.CharField(max_length=64, null=True, blank=True)
    state_province = django_db_models.CharField(max_length=128, null=True, blank=True)
    city = django_db_models.CharField(max_length=128, null=True, blank=True)
    postal_code = django_db_models.CharField(max_length=32, null=True, blank=True)
    tax_id = django_db_models.CharField(max_length=64, null=True, blank=True)

    # Onboarding progress: 0=not_started, 1=account_created, 2=company_details, 3=personalization, 4=complete
    onboarding_step = django_db_models.PositiveSmallIntegerField(default=0, null=True, blank=True)

    # Stripe billing
    stripe_customer_id = django_db_models.CharField(max_length=255, null=True, blank=True)
    # Local subscription state — kept in sync via Stripe webhooks
    subscription_plan = django_db_models.CharField(max_length=32, null=True, blank=True)
    subscription_id = django_db_models.CharField(max_length=255, null=True, blank=True)
    subscription_status = django_db_models.CharField(max_length=32, null=True, blank=True)
    subscription_period_end = django_db_models.DateTimeField(null=True, blank=True)
    # Set only when subscription_plan/status above were granted manually (comp'd trial, not a
    # real Stripe subscription) -- lets demote_expired_trials.py find these rows without
    # touching companies on a real paid or Stripe-trialing subscription. Cleared as soon as a
    # real Stripe subscription is synced onto this company (see billing._sync_company_subscription).
    manual_trial_granted_at = django_db_models.DateTimeField(null=True, blank=True)

    # Dedicated SFTP relay account (one per company) — auto-provisioned in the background so
    # relay-based distributors (Meyer, A-Tech, etc.) can be connected with one click instead of
    # the company emailing support for credentials.
    relay_sftp_username = django_db_models.CharField(max_length=64, null=True, blank=True, unique=True)
    relay_sftp_password = django_db_models.CharField(max_length=128, null=True, blank=True)
    relay_sftp_provisioned_at = django_db_models.DateTimeField(null=True, blank=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "company"
        unique_together = ["slug"]


class CompanyOnboardingPreferences(django_db_models.Model):
    """
    Step 3 personalization: preferred distributors, categories, and optional credentials.
    """

    company = django_db_models.OneToOneField(
        Company, on_delete=django_db_models.CASCADE, related_name="onboarding_preferences"
    )
    # Provider IDs (e.g. Turn14=1, Keystone=3)
    preferred_distributor_ids = django_db_models.JSONField(default=list, blank=True)
    # E.g. ["Suspension/Lift Kits", "Tonneau Covers", "Lighting", "Exterior Armor", "Performance Tuning"]
    top_categories = django_db_models.JSONField(default=list, blank=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "company_onboarding_preferences"


class CompanyLocation(django_db_models.Model):
    """
    A shop/warehouse address book entry for a company — e.g. "Main Warehouse", "Store #2".
    Lets the checkout flow offer "ship to one of my locations" instead of typing the address
    every time; fields mirror PurchaseOrder.ship_to_* so a location can be copied straight
    into a quote's ship_to payload.
    """

    company = django_db_models.ForeignKey(Company, on_delete=django_db_models.CASCADE, related_name="locations")

    label = django_db_models.CharField(max_length=100)

    name = django_db_models.CharField(max_length=255)
    attention = django_db_models.CharField(max_length=255, null=True, blank=True)
    address1 = django_db_models.CharField(max_length=255)
    address2 = django_db_models.CharField(max_length=255, null=True, blank=True)
    city = django_db_models.CharField(max_length=128)
    state = django_db_models.CharField(max_length=64)
    postal_code = django_db_models.CharField(max_length=32)
    country = django_db_models.CharField(max_length=64)
    phone = django_db_models.CharField(max_length=32, null=True, blank=True)

    is_primary = django_db_models.BooleanField(default=False)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "company_locations"


class CompanyDestinations(django_db_models.Model):
    status = django_db_models.PositiveSmallIntegerField()
    status_name = django_db_models.CharField(max_length=255)
    destination_type = django_db_models.PositiveSmallIntegerField()
    destination_type_name = django_db_models.CharField(max_length=255)
    credentials = django_db_models.JSONField()

    company = django_db_models.ForeignKey(Company, on_delete=django_db_models.CASCADE, related_name="destinations")

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "company_destinations"


class Providers(django_db_models.Model):
    name = django_db_models.CharField(max_length=255)
    status = django_db_models.PositiveSmallIntegerField()
    status_name = django_db_models.CharField(max_length=255)
    type = django_db_models.PositiveSmallIntegerField()
    type_name = django_db_models.CharField(max_length=255)

    kind = django_db_models.PositiveSmallIntegerField()
    kind_name = django_db_models.CharField(max_length=255)

    coming_soon = django_db_models.BooleanField(default=False)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "providers"
        unique_together = ["kind"]


class CompanyProviders(django_db_models.Model):
    company = django_db_models.ForeignKey(Company, on_delete=django_db_models.CASCADE, related_name="company_providers")
    provider = django_db_models.ForeignKey(
        Providers, on_delete=django_db_models.CASCADE, related_name="brand_providers"
    )

    credentials = django_db_models.JSONField()

    primary = django_db_models.BooleanField(default=False)
    active = django_db_models.BooleanField(default=True)

    # Set to True once the first successful pricing sync completes for this connection.
    # False means the initial data ingest is still pending or in progress — the frontend
    # should show a "Ingesting data..." / "Setting up..." state instead of empty results.
    # Existing rows are migrated to True so only newly-connected providers start as False.
    initial_sync_completed = django_db_models.BooleanField(default=False)

    # Live connectivity/sync status (see src.enums.CompanyProviderConnectionStatus). Null
    # until first checked. Refreshed periodically by check_company_provider_connections for
    # rows where initial_sync_completed is False; set to CONNECTED directly once that flips
    # True. Exposed on the integrations catalog and connection detail endpoints.
    status = django_db_models.PositiveSmallIntegerField(null=True, blank=True)
    status_name = django_db_models.CharField(max_length=32, null=True, blank=True)
    status_reason = django_db_models.TextField(null=True, blank=True)
    status_checked_at = django_db_models.DateTimeField(null=True, blank=True)

    # Order-placement connectivity status (see src.enums.CompanyProviderOrderConnectionStatus).
    # Independent of the feed status above — a company can have a working feed with no order
    # credentials configured (null here), or order credentials that validate fine but sit in
    # WAITING until the feed itself reaches CONNECTED. Null until order credentials are entered.
    order_status = django_db_models.PositiveSmallIntegerField(null=True, blank=True)
    order_status_name = django_db_models.CharField(max_length=32, null=True, blank=True)
    order_status_reason = django_db_models.TextField(null=True, blank=True)
    order_status_checked_at = django_db_models.DateTimeField(null=True, blank=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    # High-water mark for the master-pricing-layer propagation step (e.g.
    # sync_provider_pricing_from_meyer_for_company): rows in the provider's raw pricing table
    # with updated_at >= this only get touched by their real value changing (see the
    # hand-written upsert in meyer.py's _flush_buf), so this lets that propagation step process
    # just what changed since last time instead of the whole raw pricing table every cycle.
    # Null means "never propagated" -- process everything. Only wired for Meyer so far.
    pricing_propagation_watermark = django_db_models.DateTimeField(null=True, blank=True)

    class Meta:
        db_table = "company_providers"
        unique_together = ["company", "provider"]


class CompanyProviderOrderAccount(django_db_models.Model):
    """
    A named order-placement credential set for a CompanyProviders connection — e.g. a company
    that has one Keystone account for its own shop's orders and a second, separate Keystone
    account it uses for drop-shipping. Every connection with order credentials configured has
    exactly one row here with ``is_default=True`` (the account get_order_credentials resolves to
    when no account is explicitly requested — see src.integrations.credentials); this is the
    single source of truth for order credentials, including for connections that only ever have
    one account, which is still represented by its own is_default row rather than living
    directly on CompanyProviders.credentials. Credentials entered through a connection's normal
    "Ordering" section (connect_provider/update_connection) are written here, to the is_default
    row — CompanyProviders.credentials never carries an "order" key.

    ``is_default`` is enforced at the application layer the same way CompanyLocation.is_primary
    is: at most one True per company_provider, and clearing/deleting the default promotes the
    next-oldest active account (see integrations_services._promote_next_default_order_account).
    """

    company_provider = django_db_models.ForeignKey(
        CompanyProviders, on_delete=django_db_models.CASCADE, related_name="order_accounts"
    )

    label = django_db_models.CharField(max_length=100)
    credentials = django_db_models.JSONField()

    # Which channel this account currently places orders through — see src.enums.OrderMethod
    # (API=1, EMAIL=2). Defaults to API so every account that existed before this field keeps
    # its exact current behavior. Switching is non-destructive: `credentials` can hold both API
    # and email keys (e.g. account_number/security_key alongside rep_email/cc_email) at once, so
    # toggling back and forth never loses previously-entered values — see
    # src.integrations.orders.registry.get_adapter().
    order_method = django_db_models.PositiveSmallIntegerField(default=1)
    order_method_name = django_db_models.CharField(max_length=16, default="API")

    is_default = django_db_models.BooleanField(default=False)
    active = django_db_models.BooleanField(default=True)

    # Order-placement connectivity status for THIS account (src.enums.CompanyProviderOrderConnectionStatus)
    # -- the source of truth for order status, including the default account's; CompanyProviders.
    # order_status/... mirrors whichever account is currently is_default, for consumers that only
    # know that older single-value shape (see integrations_services._refresh_default_order_status).
    order_status = django_db_models.PositiveSmallIntegerField(null=True, blank=True)
    order_status_name = django_db_models.CharField(max_length=32, null=True, blank=True)
    order_status_reason = django_db_models.TextField(null=True, blank=True)
    order_status_checked_at = django_db_models.DateTimeField(null=True, blank=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "company_provider_order_accounts"
        unique_together = ["company_provider", "label"]


class ShopManagementProviders(django_db_models.Model):
    """Global catalog of connectable shop-management systems (ShopMonkey, ...). Deliberately
    separate from Providers/BrandProviderKind — a shop-management system isn't a parts source
    and shouldn't share that model's distributor-oriented flows or kind namespace."""

    name = django_db_models.CharField(max_length=255)
    status = django_db_models.PositiveSmallIntegerField()
    status_name = django_db_models.CharField(max_length=255)

    kind = django_db_models.PositiveSmallIntegerField()
    kind_name = django_db_models.CharField(max_length=255)

    coming_soon = django_db_models.BooleanField(default=False)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "shop_management_providers"
        unique_together = ["kind"]


class CompanyShopManagementProviders(django_db_models.Model):
    """Per-tenant connection to a shop-management system. One credentials namespace only
    (e.g. {"api_key": "..."}) — unlike CompanyProviders there's no separate feed/order split,
    since there's a single credential set and (for now) a single capability."""

    company = django_db_models.ForeignKey(
        Company, on_delete=django_db_models.CASCADE, related_name="shop_management_providers"
    )
    provider = django_db_models.ForeignKey(
        ShopManagementProviders, on_delete=django_db_models.CASCADE, related_name="company_connections"
    )

    credentials = django_db_models.JSONField()

    active = django_db_models.BooleanField(default=True)

    # See src.enums.ShopManagementConnectionStatus. Null until first checked.
    status = django_db_models.PositiveSmallIntegerField(null=True, blank=True)
    status_name = django_db_models.CharField(max_length=32, null=True, blank=True)
    status_reason = django_db_models.TextField(null=True, blank=True)
    status_checked_at = django_db_models.DateTimeField(null=True, blank=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "company_shop_management_providers"
        unique_together = ["company", "provider"]


class IntegrationRequest(django_db_models.Model):
    company = django_db_models.ForeignKey(
        Company, on_delete=django_db_models.CASCADE, related_name="integration_requests"
    )
    provider = django_db_models.ForeignKey(Providers, on_delete=django_db_models.CASCADE, related_name="requests")
    created_at = django_db_models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "integration_requests"
        unique_together = ["company", "provider"]


class CustomIntegrationRequest(django_db_models.Model):
    company = django_db_models.ForeignKey(
        Company, on_delete=django_db_models.CASCADE, related_name="custom_integration_requests"
    )
    distributor_name = django_db_models.CharField(max_length=255)
    created_at = django_db_models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "custom_integration_requests"
        unique_together = ["company", "distributor_name"]


class Brands(django_db_models.Model):
    name = django_db_models.CharField(max_length=255)
    status = django_db_models.PositiveSmallIntegerField()
    status_name = django_db_models.CharField(max_length=255)
    aaia_code = django_db_models.CharField(max_length=255, null=True)

    data = django_db_models.JSONField(null=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "brands"
        unique_together = ["name"]


class BrandProviders(django_db_models.Model):
    brand = django_db_models.ForeignKey(Brands, on_delete=django_db_models.CASCADE, related_name="providers")
    provider = django_db_models.ForeignKey(Providers, on_delete=django_db_models.CASCADE, related_name="providers")

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "brand_providers"
        unique_together = ["brand", "provider"]


class CompanyDestinationPartsPreferences(django_db_models.Model):
    company_destination = django_db_models.ForeignKey(
        CompanyDestinations, on_delete=django_db_models.CASCADE, related_name="parts_preferences"
    )
    preferences = django_db_models.JSONField()

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "company_destination_parts_preferences"
        unique_together = ["company_destination"]


class CompanyDestinationParts(django_db_models.Model):
    company_destination = django_db_models.ForeignKey(
        CompanyDestinations, on_delete=django_db_models.CASCADE, related_name="parts"
    )
    part_unique_key = django_db_models.CharField(max_length=255)
    source_data = django_db_models.JSONField()
    source_external_id = django_db_models.TextField()
    destination_data = django_db_models.JSONField(null=True)
    destination_external_id = django_db_models.TextField(null=True)
    brand = django_db_models.ForeignKey(Brands, on_delete=django_db_models.CASCADE, related_name="parts")

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "company_destination_parts"
        # unique_together = ["company_destination"]


class UserProfile(django_db_models.Model):
    user = django_db_models.OneToOneField(auth_models.User, on_delete=django_db_models.CASCADE, related_name="profile")
    company = django_db_models.ForeignKey(
        Company,
        on_delete=django_db_models.CASCADE,
        related_name="user_profiles",
        null=True,
        blank=True,
    )
    is_company_admin = django_db_models.BooleanField(default=False)
    # Job function within the company (owner, parts_manager, service_advisor, technician, other).
    # Free CharField; allowed values enforced at the schema layer (see onboarding.USER_ROLES).
    role = django_db_models.CharField(max_length=32, null=True, blank=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "auth_user_profile"


class Turn14Brand(django_db_models.Model):
    external_id = django_db_models.CharField(max_length=255)
    name = django_db_models.CharField(max_length=255)
    dropship = django_db_models.BooleanField(default=False)
    price_groups = django_db_models.JSONField(null=True)
    logo = django_db_models.TextField(null=True)
    aaia_code = django_db_models.CharField(max_length=255)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "turn14_brands"
        unique_together = ["external_id"]


class Turn14Location(django_db_models.Model):
    """Turn14 warehouse locations from GET /v1/locations API."""

    external_id = django_db_models.CharField(max_length=32)
    name = django_db_models.CharField(max_length=255)
    street = django_db_models.CharField(max_length=255, blank=True)
    city = django_db_models.CharField(max_length=255, blank=True)
    state = django_db_models.CharField(max_length=64, blank=True)
    country = django_db_models.CharField(max_length=64, blank=True)
    zip_code = django_db_models.CharField(max_length=32, blank=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "turn14_locations"
        unique_together = ["external_id"]


class MeyerLocation(django_db_models.Model):
    """Meyer warehouse locations from the Order API's GET /Warehouses (see
    fetch_and_save_meyer_locations) — decodes a shipping quote's bare warehouse code
    (e.g. "053") into a human-readable place, the same role Turn14Location plays for Turn14.
    Meyer's Warehouses response is narrower than Turn14's locations (no name/street/zip), just
    LocationCode/City/State/Country."""

    external_id = django_db_models.CharField(max_length=32)  # Meyer's "LocationCode"
    city = django_db_models.CharField(max_length=255, blank=True)
    state = django_db_models.CharField(max_length=64, blank=True)
    country = django_db_models.CharField(max_length=64, blank=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "meyer_locations"
        unique_together = ["external_id"]


class WheelProsWarehouse(django_db_models.Model):
    """Wheel Pros warehouses from the Orders API's GET /warehouses/v1 — decodes a tracking/order
    response's bare warehouseCode into a human-readable ship-from location, the same role
    Turn14Location/MeyerLocation play for their distributors."""

    external_id = django_db_models.CharField(max_length=32)  # Wheel Pros' "id"
    name = django_db_models.CharField(max_length=255, blank=True)
    city = django_db_models.CharField(max_length=255, blank=True)
    state = django_db_models.CharField(max_length=64, blank=True)
    country = django_db_models.CharField(max_length=64, blank=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "wheelpros_warehouses"
        unique_together = ["external_id"]


class CompanyBrands(django_db_models.Model):
    company = django_db_models.ForeignKey(Company, on_delete=django_db_models.CASCADE, related_name="brands")
    brand = django_db_models.ForeignKey(Brands, on_delete=django_db_models.CASCADE, related_name="brands")
    status = django_db_models.PositiveSmallIntegerField()
    status_name = django_db_models.CharField(max_length=255)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "company_brands"
        unique_together = ["company", "brand"]


class CompanyBrandDestination(django_db_models.Model):
    company_brand = django_db_models.ForeignKey(
        CompanyBrands, on_delete=django_db_models.CASCADE, related_name="destinations"
    )
    destination = django_db_models.ForeignKey(
        CompanyDestinations, on_delete=django_db_models.CASCADE, related_name="company_brands"
    )

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "company_brand_destination"
        unique_together = ["company_brand", "destination"]


class CompanyDestinationExecutionRun(django_db_models.Model):
    company_brand_destination = django_db_models.ForeignKey(
        CompanyBrandDestination, on_delete=django_db_models.CASCADE, related_name="execution_runs"
    )
    status = django_db_models.PositiveSmallIntegerField()
    status_name = django_db_models.CharField(max_length=255)
    products_processed = django_db_models.IntegerField(default=0)
    products_created = django_db_models.IntegerField(default=0)
    products_updated = django_db_models.IntegerField(default=0)
    products_failed = django_db_models.IntegerField(default=0)
    error_message = django_db_models.TextField(null=True)
    message = django_db_models.TextField(null=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)
    completed_at = django_db_models.DateTimeField(null=True)

    class Meta:
        db_table = "company_destination_execution_run"


class CompanyDestinationPartsHistory(django_db_models.Model):
    destination_part = django_db_models.ForeignKey(
        CompanyDestinationParts, on_delete=django_db_models.CASCADE, related_name="history"
    )
    execution_run = django_db_models.ForeignKey(
        CompanyDestinationExecutionRun, on_delete=django_db_models.CASCADE, related_name="history_records", null=True
    )
    data = django_db_models.JSONField()
    changes = django_db_models.JSONField(null=True)
    synced = django_db_models.BooleanField(default=False)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "company_destination_parts_history"
        # unique_together = ["company_destination"]


class BrandTurn14BrandMapping(django_db_models.Model):
    brand = django_db_models.ForeignKey(
        Brands, on_delete=django_db_models.CASCADE, related_name="turn14_brand_mappings"
    )
    turn14_brand = django_db_models.ForeignKey(
        Turn14Brand, on_delete=django_db_models.CASCADE, related_name="brand_mappings"
    )

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "brand_turn14_brand_mapping"
        unique_together = ["brand", "turn14_brand"]


class Turn14Items(django_db_models.Model):
    external_id = django_db_models.CharField(max_length=255)
    brand = django_db_models.ForeignKey(Turn14Brand, on_delete=django_db_models.CASCADE, related_name="items")
    product_name = django_db_models.CharField(max_length=255, null=True)
    part_number = django_db_models.CharField(max_length=255, null=True)
    mfr_part_number = django_db_models.CharField(max_length=255, null=True)
    part_description = django_db_models.TextField(null=True)
    category = django_db_models.CharField(max_length=255, null=True)
    subcategory = django_db_models.CharField(max_length=255, null=True)
    external_brand_id = django_db_models.IntegerField(null=True)
    brand_name = django_db_models.CharField(max_length=255, null=True)
    price_group_id = django_db_models.IntegerField(null=True)
    price_group = django_db_models.CharField(max_length=255, null=True)
    active = django_db_models.BooleanField(default=False)
    born_on_date = django_db_models.DateField(null=True)
    regular_stock = django_db_models.BooleanField(default=False)
    powersports_indicator = django_db_models.BooleanField(default=False)
    dropship_controller_id = django_db_models.IntegerField(null=True)
    air_freight_prohibited = django_db_models.BooleanField(default=False)
    not_carb_approved = django_db_models.BooleanField(default=False)
    carb_acknowledgement_required = django_db_models.BooleanField(default=False)
    ltl_freight_required = django_db_models.BooleanField(default=False)
    prop_65 = django_db_models.CharField(max_length=255, null=True)
    epa = django_db_models.CharField(max_length=255, null=True)
    units_per_sku = django_db_models.IntegerField(null=True)
    clearance_item = django_db_models.BooleanField(default=False)
    thumbnail = django_db_models.TextField(null=True)
    barcode = django_db_models.CharField(max_length=255, null=True)
    dimensions = django_db_models.JSONField(null=True)
    warehouse_availability = django_db_models.JSONField(null=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "turn14_items"
        unique_together = ["external_id"]


class Turn14BrandData(django_db_models.Model):
    external_id = django_db_models.CharField(max_length=255)
    brand = django_db_models.ForeignKey(Turn14Brand, on_delete=django_db_models.CASCADE, related_name="brand_data")
    type = django_db_models.CharField(max_length=255, null=True)
    files = django_db_models.JSONField(null=True)
    descriptions = django_db_models.JSONField(null=True)
    relationships = django_db_models.JSONField(null=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "turn14_brand_data"
        unique_together = ["external_id"]


class Turn14BrandPricing(django_db_models.Model):
    external_id = django_db_models.CharField(max_length=255)
    brand = django_db_models.ForeignKey(Turn14Brand, on_delete=django_db_models.CASCADE, related_name="brand_pricing")
    company = django_db_models.ForeignKey(
        Company, on_delete=django_db_models.CASCADE, related_name="turn14_brand_pricing"
    )
    type = django_db_models.CharField(max_length=255, null=True)
    purchase_cost = django_db_models.DecimalField(max_digits=10, decimal_places=2, null=True)
    has_map = django_db_models.BooleanField(default=False)
    can_purchase = django_db_models.BooleanField(default=False)
    pricelists = django_db_models.JSONField(null=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "turn14_brand_pricing"
        unique_together = ["company", "external_id"]


class Turn14BrandInventory(django_db_models.Model):
    external_id = django_db_models.CharField(max_length=255)
    brand = django_db_models.ForeignKey(
        Turn14Brand, on_delete=django_db_models.CASCADE, related_name="brand_inventory", null=True
    )
    type = django_db_models.CharField(max_length=255, null=True)
    inventory = django_db_models.JSONField(null=True)
    manufacturer = django_db_models.JSONField(null=True)
    eta = django_db_models.JSONField(null=True)
    relationships = django_db_models.JSONField(null=True)
    total_inventory = django_db_models.IntegerField(null=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "turn14_brand_inventory"
        unique_together = ["external_id"]


class Turn14ItemFitment(django_db_models.Model):
    """
    Raw per-part/per-vehicle fitment pairs from GET /v1/items/fitment/brand/{brand_id}.
    vehicle_id is Turn14's own vehicle config id, not yet resolved to year/make/model —
    that mapping requires a VCDB dataset we don't have yet. Kept as a flat (item, vehicle_id)
    pair for now; each row is one id pulled out of the response's nested vehicle_ids arrays.
    """

    item_external_id = django_db_models.CharField(max_length=255)
    brand = django_db_models.ForeignKey(Turn14Brand, on_delete=django_db_models.CASCADE, related_name="item_fitments")
    vehicle_id = django_db_models.PositiveIntegerField(db_index=True)
    late_models_only = django_db_models.BooleanField(default=False)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "turn14_item_fitments"
        unique_together = ["item_external_id", "vehicle_id"]


class VcdbVehicle(django_db_models.Model):
    """
    Flattened Auto Care Association VCdb vehicle reference data: VehicleID joined against
    BaseVehicle/Make/Model/SubModel into a single year/make/model/submodel row. Populated by
    the `import_vcdb_vehicles` management command from the AutoCare VCdb JSON dataset.
    Standalone lookup table — not wired into Turn14ItemFitment or any other model.

    ``engine``/``drive_type`` are filled in only when VCdb's VehicleToEngineConfig/
    VehicleToDriveType join has exactly one option for this VehicleID — left blank ("") when a
    vehicle has multiple engine/drivetrain options, since a bare VehicleID can't disambiguate
    which one applies (real ACES fitment data pins that down with extra qualifier IDs on the
    `<App>` row, not the VehicleID alone). Roughly 76% of VCdb vehicles have an unambiguous
    engine and 89% an unambiguous drive type.
    """

    vehicle_id = django_db_models.PositiveIntegerField(unique=True)
    base_vehicle_id = django_db_models.PositiveIntegerField(db_index=True)
    year = django_db_models.PositiveSmallIntegerField(db_index=True)
    make = django_db_models.CharField(max_length=128, db_index=True)
    model = django_db_models.CharField(max_length=128, db_index=True)
    submodel = django_db_models.CharField(max_length=255, blank=True, default="")
    region_id = django_db_models.PositiveSmallIntegerField(null=True, blank=True)
    engine = django_db_models.CharField(max_length=255, blank=True, default="")
    drive_type = django_db_models.CharField(max_length=64, blank=True, default="")

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "vcdb_vehicles"
        indexes = [
            django_db_models.Index(fields=["year", "make", "model"], name="vcdb_veh_ymm_idx"),
        ]

    def __str__(self):
        return f"{self.year} {self.make} {self.model} {self.submodel}".strip()


class BigCommerceParts(django_db_models.Model):
    external_id = django_db_models.CharField(max_length=255)
    sku = django_db_models.TextField(max_length=255)
    raw_data = django_db_models.JSONField(null=True)
    external_brand_id = django_db_models.CharField(max_length=255, null=True)
    company_destination = django_db_models.ForeignKey(
        CompanyDestinations, on_delete=django_db_models.CASCADE, related_name="bigcommerce_parts"
    )

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "bigcommerce_parts"
        unique_together = ["external_id", "sku", "company_destination"]


class BigCommerceBrands(django_db_models.Model):
    external_id = django_db_models.CharField(max_length=255)
    name = django_db_models.TextField(max_length=255)
    brand = django_db_models.ForeignKey(Brands, on_delete=django_db_models.CASCADE, related_name="bigcommerce_brands")
    company_destination = django_db_models.ForeignKey(
        CompanyDestinations, on_delete=django_db_models.CASCADE, related_name="bigcommerce_brands"
    )

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "bigcommerce_brands"
        unique_together = ["external_id", "brand", "company_destination"]


class BigCommerceCategories(django_db_models.Model):
    external_id = django_db_models.IntegerField()
    name = django_db_models.CharField(max_length=255)
    parent_id = django_db_models.IntegerField(default=0)
    tree_id = django_db_models.IntegerField(default=1)
    company_destination = django_db_models.ForeignKey(
        CompanyDestinations, on_delete=django_db_models.CASCADE, related_name="bigcommerce_categories"
    )

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "bigcommerce_categories"
        unique_together = ["external_id", "company_destination", "tree_id"]


class SDCBrands(django_db_models.Model):
    external_id = django_db_models.CharField(max_length=255)
    name = django_db_models.TextField(max_length=255)
    aaia_code = django_db_models.CharField(max_length=255)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "sdc_brands"
        unique_together = ["external_id", "name"]


class SDCPartFitment(django_db_models.Model):
    sku = django_db_models.TextField(max_length=255)
    brand = django_db_models.ForeignKey(SDCBrands, on_delete=django_db_models.CASCADE, related_name="fitment_brands")
    year = django_db_models.IntegerField()
    make = django_db_models.TextField(max_length=255)
    model = django_db_models.TextField(max_length=255)
    category_pcdb = django_db_models.CharField(max_length=255, null=True)
    subcategory_pcdb = django_db_models.CharField(max_length=255, null=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "sdc_part_fitment"
        unique_together = ["sku", "brand", "year", "make", "model"]


class BrandSDCBrandMapping(django_db_models.Model):
    brand = django_db_models.ForeignKey(Brands, on_delete=django_db_models.CASCADE, related_name="sdc_brand_mappings")
    sdc_brand = django_db_models.ForeignKey(
        SDCBrands, on_delete=django_db_models.CASCADE, related_name="brand_mappings"
    )

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "brand_sdc_brand_mapping"
        unique_together = ["brand", "sdc_brand"]


class SDCParts(django_db_models.Model):
    part_number = django_db_models.CharField(max_length=255)
    brand = django_db_models.ForeignKey(SDCBrands, on_delete=django_db_models.CASCADE, related_name="parts")
    brand_label = django_db_models.CharField(max_length=255, null=True)
    gtin = django_db_models.CharField(max_length=255, null=True)
    category_pcdb = django_db_models.CharField(max_length=255, null=True)
    life_cycle_status = django_db_models.CharField(max_length=255, null=True)
    country_of_origin = django_db_models.CharField(max_length=255, null=True)
    warranty = django_db_models.TextField(null=True)
    long_description = django_db_models.TextField(null=True)
    extended_description = django_db_models.TextField(null=True)
    application_summary = django_db_models.TextField(null=True)
    features_and_benefits = django_db_models.TextField(null=True)
    marketing_description = django_db_models.TextField(null=True)
    title = django_db_models.CharField(max_length=255, null=True)
    keywords = django_db_models.TextField(null=True)
    product_attributes = django_db_models.TextField(null=True)
    jobber_usd = django_db_models.DecimalField(max_digits=10, decimal_places=2, null=True)
    retail_usd = django_db_models.DecimalField(max_digits=10, decimal_places=2, null=True)
    map_usd = django_db_models.DecimalField(max_digits=10, decimal_places=2, null=True)
    unilateral_usd = django_db_models.DecimalField(max_digits=10, decimal_places=2, null=True)
    primary_image = django_db_models.TextField(null=True)
    additional_image = django_db_models.TextField(null=True)
    installation_instructions = django_db_models.TextField(null=True)
    logo = django_db_models.TextField(null=True)
    video_random = django_db_models.TextField(null=True)
    video_installation = django_db_models.TextField(null=True)
    length_for_case = django_db_models.DecimalField(max_digits=10, decimal_places=2, null=True)
    width_for_case = django_db_models.DecimalField(max_digits=10, decimal_places=2, null=True)
    height_for_case = django_db_models.DecimalField(max_digits=10, decimal_places=2, null=True)
    weight_for_case = django_db_models.DecimalField(max_digits=10, decimal_places=2, null=True)
    inventory = django_db_models.IntegerField(null=True)
    external_brand_id = django_db_models.CharField(max_length=255, null=True)
    part_terminology_label = django_db_models.CharField(max_length=255, null=True)
    quantity_per_application = django_db_models.CharField(max_length=255, null=True)
    hazardous_material = django_db_models.CharField(max_length=255, null=True)
    condition = django_db_models.CharField(max_length=255, null=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "sdc_parts"
        unique_together = ["part_number", "brand"]


class KeystoneBrand(django_db_models.Model):
    external_id = django_db_models.CharField(max_length=255)
    name = django_db_models.CharField(max_length=255)
    aaia_code = django_db_models.CharField(max_length=255, null=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "keystone_brands"
        unique_together = [["external_id"]]


class BrandKeystoneBrandMapping(django_db_models.Model):
    brand = django_db_models.ForeignKey(
        Brands, on_delete=django_db_models.CASCADE, related_name="keystone_brand_mappings"
    )
    keystone_brand = django_db_models.ForeignKey(
        KeystoneBrand, on_delete=django_db_models.CASCADE, related_name="brand_mappings"
    )

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "brand_keystone_brand_mapping"
        unique_together = ["brand", "keystone_brand"]


class KeystoneParts(django_db_models.Model):
    vcpn = django_db_models.CharField(max_length=255)
    brand = django_db_models.ForeignKey(KeystoneBrand, on_delete=django_db_models.CASCADE, related_name="parts")
    vendor_code = django_db_models.CharField(max_length=255, null=True)
    part_number = django_db_models.CharField(max_length=255, null=True)
    manufacturer_part_no = django_db_models.CharField(max_length=255, null=True)
    long_description = django_db_models.TextField(null=True)
    jobber_price = django_db_models.DecimalField(max_digits=10, decimal_places=2, null=True)
    cost = django_db_models.DecimalField(max_digits=10, decimal_places=2, null=True)
    upsable = django_db_models.BooleanField(default=False)
    core_charge = django_db_models.DecimalField(max_digits=10, decimal_places=2, null=True)
    case_qty = django_db_models.IntegerField(null=True)
    is_non_returnable = django_db_models.BooleanField(default=False)
    prop65_toxicity = django_db_models.CharField(max_length=255, null=True)
    upc_code = django_db_models.CharField(max_length=255, null=True)
    weight = django_db_models.DecimalField(max_digits=10, decimal_places=2, null=True)
    height = django_db_models.DecimalField(max_digits=10, decimal_places=2, null=True)
    length = django_db_models.DecimalField(max_digits=10, decimal_places=2, null=True)
    width = django_db_models.DecimalField(max_digits=10, decimal_places=2, null=True)
    aaia_code = django_db_models.CharField(max_length=255, null=True)
    is_hazmat = django_db_models.BooleanField(default=False)
    is_chemical = django_db_models.BooleanField(default=False)
    ups_ground_assessorial = django_db_models.DecimalField(max_digits=10, decimal_places=2, null=True)
    us_ltl = django_db_models.DecimalField(max_digits=10, decimal_places=2, null=True)
    east_qty = django_db_models.IntegerField(null=True)
    midwest_qty = django_db_models.IntegerField(null=True)
    california_qty = django_db_models.IntegerField(null=True)
    southeast_qty = django_db_models.IntegerField(null=True)
    pacific_nw_qty = django_db_models.IntegerField(null=True)
    texas_qty = django_db_models.IntegerField(null=True)
    great_lakes_qty = django_db_models.IntegerField(null=True)
    florida_qty = django_db_models.IntegerField(null=True)
    total_qty = django_db_models.IntegerField(null=True)
    kit_components = django_db_models.TextField(null=True)
    is_kit = django_db_models.BooleanField(default=False)
    raw_data = django_db_models.JSONField(null=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "keystone_parts"
        unique_together = ["vcpn", "brand"]


class KeystoneCompanyPricing(django_db_models.Model):
    """
    Per-company Keystone FTP pricing for a catalog row (KeystoneParts).
    Catalog fields live on KeystoneParts; cost/jobber/core come from each company's inventory file.
    """

    part = django_db_models.ForeignKey(
        KeystoneParts, on_delete=django_db_models.CASCADE, related_name="company_pricing"
    )
    company = django_db_models.ForeignKey(
        Company, on_delete=django_db_models.CASCADE, related_name="keystone_company_pricing"
    )
    jobber_price = django_db_models.DecimalField(max_digits=10, decimal_places=2, null=True)
    cost = django_db_models.DecimalField(max_digits=10, decimal_places=2, null=True)
    core_charge = django_db_models.DecimalField(max_digits=10, decimal_places=2, null=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "keystone_company_pricing"
        unique_together = ["part", "company"]


class PremierBrand(django_db_models.Model):
    """Brand / manufacturer from the Premier Performance data feed (Brand column)."""

    external_id = django_db_models.CharField(max_length=255)
    name = django_db_models.CharField(max_length=255)
    line_code = django_db_models.CharField(max_length=64, null=True, blank=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "premier_brands"
        unique_together = [["external_id"]]


class BrandPremierBrandMapping(django_db_models.Model):
    brand = django_db_models.ForeignKey(
        "Brands", on_delete=django_db_models.CASCADE, related_name="premier_brand_mappings"
    )
    premier_brand = django_db_models.ForeignKey(
        PremierBrand, on_delete=django_db_models.CASCADE, related_name="brand_mappings"
    )

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "brand_premier_brand_mapping"
        unique_together = ["brand", "premier_brand"]


class PremierParts(django_db_models.Model):
    """Catalog row from the Premier Performance master data feed."""

    premier_part_number = django_db_models.CharField(max_length=255)
    brand = django_db_models.ForeignKey(PremierBrand, on_delete=django_db_models.CASCADE, related_name="parts")
    mfg_part_number = django_db_models.CharField(max_length=255, null=True)
    long_description = django_db_models.TextField(null=True)
    external_long_description = django_db_models.TextField(null=True)
    length = django_db_models.DecimalField(max_digits=10, decimal_places=3, null=True)
    width = django_db_models.DecimalField(max_digits=10, decimal_places=3, null=True)
    height = django_db_models.DecimalField(max_digits=10, decimal_places=3, null=True)
    weight = django_db_models.DecimalField(max_digits=10, decimal_places=3, null=True)
    upc_code = django_db_models.CharField(max_length=255, null=True)
    usa_item_availability = django_db_models.IntegerField(null=True)
    core_charge = django_db_models.DecimalField(max_digits=10, decimal_places=4, null=True)
    jobber_price = django_db_models.DecimalField(max_digits=10, decimal_places=4, null=True)
    map_price = django_db_models.DecimalField(max_digits=10, decimal_places=4, null=True)
    retail_price = django_db_models.DecimalField(max_digits=10, decimal_places=4, null=True)
    inventory_status = django_db_models.CharField(max_length=64, null=True)
    nv_qty = django_db_models.IntegerField(null=True)
    ky_qty = django_db_models.IntegerField(null=True)
    mfg_qty = django_db_models.IntegerField(null=True)
    wa_qty = django_db_models.IntegerField(null=True)
    image_url = django_db_models.TextField(null=True)
    ships_ltl = django_db_models.BooleanField(default=False)
    item_with_cores = django_db_models.BooleanField(default=False)
    prop65_carcinogen = django_db_models.BooleanField(default=False)
    prop65_reproductive_harm = django_db_models.BooleanField(default=False)
    approved_line = django_db_models.BooleanField(default=False)
    california_legal = django_db_models.BooleanField(default=False)
    line_code = django_db_models.CharField(max_length=64, null=True)
    pies_ems_code = django_db_models.CharField(max_length=64, null=True)
    drop_ship_fee = django_db_models.DecimalField(max_digits=10, decimal_places=4, null=True)
    canada_map = django_db_models.DecimalField(max_digits=10, decimal_places=4, null=True)
    canada_msrp = django_db_models.DecimalField(max_digits=10, decimal_places=4, null=True)
    canada_jobber = django_db_models.DecimalField(max_digits=12, decimal_places=2, null=True)
    part_category = django_db_models.CharField(max_length=255, null=True)
    part_subcategory = django_db_models.CharField(max_length=255, null=True)
    part_terminology = django_db_models.CharField(max_length=255, null=True)
    freight_cost = django_db_models.DecimalField(max_digits=10, decimal_places=3, null=True)
    minimum_order_qty = django_db_models.IntegerField(null=True)
    drop_shippable_from_mfg = django_db_models.BooleanField(default=False)
    vendor_enhanced_emissions_code = django_db_models.CharField(max_length=255, null=True)
    is_kit = django_db_models.BooleanField(default=False)
    kit_component_list = django_db_models.TextField(null=True)
    raw_data = django_db_models.JSONField(null=True)

    # Per-PART brand correction, distinct from the per-feed-brand BrandPremierBrandMapping --
    # confirmed live that Premier's own feed lumps many real manufacturers (Nitto, Falken, Moto
    # Metal, Niche, American Racing, Toyo, Performance Replicas, ...) under one vendor-name
    # PremierBrand ("Wheel Pros", the wholesaler Premier resells through), rather than the true
    # manufacturer. When set, this OVERRIDES the brand a BrandPremierBrandMapping would otherwise
    # give this specific row (see premier.resolve_wheelpros_bucket_brands, which derives it from
    # this row's own long_description, and master_parts._ingest_premier_parts_for_mapped_brands,
    # which prefers it over the feed-brand-level mapping).
    brand_override = django_db_models.ForeignKey(
        Brands,
        on_delete=django_db_models.SET_NULL,
        null=True,
        blank=True,
        related_name="premier_brand_overrides",
    )

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "premier_parts"
        unique_together = ["premier_part_number", "brand"]


class PremierCompanyPricing(django_db_models.Model):
    """
    Per-company Premier FTP pricing for a catalog row (PremierParts).
    Catalog fields live on PremierParts; cost/jobber/map/core come from each company's feed.
    """

    part = django_db_models.ForeignKey(PremierParts, on_delete=django_db_models.CASCADE, related_name="company_pricing")
    company = django_db_models.ForeignKey(
        Company, on_delete=django_db_models.CASCADE, related_name="premier_company_pricing"
    )
    customer_price = django_db_models.DecimalField(max_digits=10, decimal_places=4, null=True)
    jobber_price = django_db_models.DecimalField(max_digits=10, decimal_places=4, null=True)
    map_price = django_db_models.DecimalField(max_digits=10, decimal_places=4, null=True)
    core_charge = django_db_models.DecimalField(max_digits=10, decimal_places=4, null=True)
    customer_cad_price = django_db_models.DecimalField(max_digits=10, decimal_places=4, null=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "premier_company_pricing"
        unique_together = ["part", "company"]


class MeyerBrand(django_db_models.Model):
    """Manufacturer / brand label from Meyer pricing feed (MFG column)."""

    external_id = django_db_models.CharField(max_length=512)
    name = django_db_models.CharField(max_length=512)
    aaia_code = django_db_models.CharField(max_length=255, null=True, blank=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "meyer_brands"
        unique_together = [["external_id"]]


class BrandMeyerBrandMapping(django_db_models.Model):
    brand = django_db_models.ForeignKey(Brands, on_delete=django_db_models.CASCADE, related_name="meyer_brand_mappings")
    meyer_brand = django_db_models.ForeignKey(
        MeyerBrand, on_delete=django_db_models.CASCADE, related_name="brand_mappings"
    )

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "brand_meyer_brand_mapping"
        unique_together = ["brand", "meyer_brand"]


class MeyerParts(django_db_models.Model):
    """
    Meyer catalog row: pricing from Meyer Pricing file; availability from Meyer Inventory
    (joined on Meyer Part / Item Number per brand).
    """

    brand = django_db_models.ForeignKey(MeyerBrand, on_delete=django_db_models.CASCADE, related_name="parts")
    meyer_part = django_db_models.CharField(max_length=255)
    mfg_item_number = django_db_models.CharField(max_length=255, null=True, blank=True)
    description = django_db_models.TextField(null=True, blank=True)
    jobber_price = django_db_models.DecimalField(max_digits=14, decimal_places=5, null=True, blank=True)
    cost = django_db_models.DecimalField(max_digits=14, decimal_places=5, null=True, blank=True)
    core_charge = django_db_models.DecimalField(max_digits=14, decimal_places=5, null=True, blank=True)
    upc = django_db_models.CharField(max_length=64, null=True, blank=True)
    map_price = django_db_models.DecimalField(max_digits=14, decimal_places=5, null=True, blank=True)
    length = django_db_models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    width = django_db_models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    height = django_db_models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    weight = django_db_models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    category = django_db_models.CharField(max_length=255, null=True, blank=True)
    sub_category = django_db_models.CharField(max_length=255, null=True, blank=True)
    is_ltl = django_db_models.BooleanField(default=False)
    is_discontinued = django_db_models.BooleanField(default=False)
    is_oversize = django_db_models.BooleanField(default=False)
    addtl_handling_charge = django_db_models.BooleanField(default=False)
    available_qty = django_db_models.DecimalField(max_digits=14, decimal_places=2, null=True, blank=True)
    mfg_qty_available = django_db_models.IntegerField(null=True, blank=True)
    inventory_ltl = django_db_models.IntegerField(null=True, blank=True)
    is_stocking = django_db_models.BooleanField(default=False)
    is_special_order = django_db_models.BooleanField(default=False)
    raw_data = django_db_models.JSONField(null=True, blank=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "meyer_parts"
        unique_together = ["meyer_part", "brand"]


class MeyerCompanyPricing(django_db_models.Model):
    """
    Per-company Meyer pricing for a catalog row (MeyerParts).
    Catalog/non-price fields live on MeyerParts; prices come from each company's SFTP pricing file.
    """

    part = django_db_models.ForeignKey(
        MeyerParts,
        on_delete=django_db_models.CASCADE,
        related_name="company_pricing",
    )
    company = django_db_models.ForeignKey(
        Company,
        on_delete=django_db_models.CASCADE,
        related_name="meyer_company_pricing",
    )
    jobber_price = django_db_models.DecimalField(max_digits=14, decimal_places=5, null=True, blank=True)
    cost = django_db_models.DecimalField(max_digits=14, decimal_places=5, null=True, blank=True)
    core_charge = django_db_models.DecimalField(max_digits=14, decimal_places=5, null=True, blank=True)
    map_price = django_db_models.DecimalField(max_digits=14, decimal_places=5, null=True, blank=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "meyer_company_pricing"
        unique_together = [["part", "company"]]


class AtechBrand(django_db_models.Model):
    """Distributor brand for A-Tech (linked from SKU prefix via AtechPrefixBrand)."""

    external_id = django_db_models.CharField(max_length=512)
    name = django_db_models.CharField(max_length=512)
    aaia_code = django_db_models.CharField(max_length=255, null=True, blank=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "atech_brands"
        unique_together = [["external_id"]]


class BrandAtechBrandMapping(django_db_models.Model):
    """Links catalog ``Brands`` to ``AtechBrand`` (for master parts and company pricing fan-out)."""

    brand = django_db_models.ForeignKey(
        Brands,
        on_delete=django_db_models.CASCADE,
        related_name="atech_brand_mappings",
    )
    atech_brand = django_db_models.ForeignKey(
        AtechBrand,
        on_delete=django_db_models.CASCADE,
        related_name="brand_mappings",
    )

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "brand_atech_brand_mapping"
        unique_together = [["brand", "atech_brand"]]


class AtechPrefixBrand(django_db_models.Model):
    """
    Manual mapping: SKU prefix (part number segment before '-', stored uppercase) -> AtechBrand.
    Example: prefix ACC for ACC-35370 -> AtechBrand whose ``name`` is the catalog label you want.
    """

    prefix = django_db_models.CharField(max_length=64)
    atech_brand = django_db_models.ForeignKey(
        AtechBrand,
        on_delete=django_db_models.CASCADE,
        related_name="prefix_mappings",
    )

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    def save(self, *args, **kwargs):
        self.prefix = (self.prefix or "").strip().upper()
        super().save(*args, **kwargs)

    class Meta:
        db_table = "atech_prefix_brand"
        unique_together = [["prefix"]]


class AtechParts(django_db_models.Model):
    """
    Row from A-Tech combined relay feed: pricing, per-DC amounts, fees, GTIN.
    ``feed_part_number`` is the full distributor line (e.g. ACC-35370); ``part_number`` and
    ``mfr_part_number`` store the suffix after the known prefix and hyphen (e.g. 35370).
    ``brand`` may be null when no ``AtechPrefixBrand`` mapping exists yet; ``brand_prefix`` is
    always the token before the first hyphen in the feed line (e.g. ACC).
    """

    brand = django_db_models.ForeignKey(
        AtechBrand,
        on_delete=django_db_models.CASCADE,
        related_name="parts",
        null=True,
        blank=True,
    )
    brand_prefix = django_db_models.CharField(max_length=64, blank=True, default="")
    feed_part_number = django_db_models.CharField(max_length=255)
    part_number = django_db_models.CharField(max_length=255)
    mfr_part_number = django_db_models.CharField(max_length=255, null=True, blank=True)
    description = django_db_models.TextField(null=True, blank=True)
    cost = django_db_models.DecimalField(max_digits=14, decimal_places=5, null=True, blank=True)
    retail_price = django_db_models.DecimalField(max_digits=14, decimal_places=5, null=True, blank=True)
    jobber_price = django_db_models.DecimalField(max_digits=14, decimal_places=5, null=True, blank=True)
    qty_tallmadge = django_db_models.IntegerField(null=True, blank=True)
    qty_sparks = django_db_models.IntegerField(null=True, blank=True)
    qty_mcdonough = django_db_models.IntegerField(null=True, blank=True)
    qty_arlington = django_db_models.IntegerField(null=True, blank=True)
    core_charge = django_db_models.DecimalField(max_digits=14, decimal_places=5, null=True, blank=True)
    fee_hazmat = django_db_models.DecimalField(max_digits=14, decimal_places=5, null=True, blank=True)
    fee_truck_us = django_db_models.DecimalField(max_digits=14, decimal_places=5, null=True, blank=True)
    fee_handling_ground = django_db_models.DecimalField(max_digits=14, decimal_places=5, null=True, blank=True)
    fee_handling_air = django_db_models.DecimalField(max_digits=14, decimal_places=5, null=True, blank=True)
    gtin = django_db_models.CharField(max_length=64, null=True, blank=True)
    image_url = django_db_models.TextField(null=True, blank=True)
    raw_data = django_db_models.JSONField(null=True, blank=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "atech_parts"
        unique_together = [["feed_part_number"]]


class AtechCompanyPricing(django_db_models.Model):
    """
    Per-company A-Tech prices for an ``AtechParts`` row (same column layout as the SFTP feed).
    Catalog / inventory columns remain on ``AtechParts``; amounts here come from each company's feed pull.
    """

    part = django_db_models.ForeignKey(
        AtechParts,
        on_delete=django_db_models.CASCADE,
        related_name="company_pricing",
    )
    company = django_db_models.ForeignKey(
        Company,
        on_delete=django_db_models.CASCADE,
        related_name="atech_company_pricing",
    )
    cost = django_db_models.DecimalField(max_digits=14, decimal_places=5, null=True, blank=True)
    retail_price = django_db_models.DecimalField(max_digits=14, decimal_places=5, null=True, blank=True)
    jobber_price = django_db_models.DecimalField(max_digits=14, decimal_places=5, null=True, blank=True)
    core_charge = django_db_models.DecimalField(max_digits=14, decimal_places=5, null=True, blank=True)
    fee_hazmat = django_db_models.DecimalField(max_digits=14, decimal_places=5, null=True, blank=True)
    fee_truck_us = django_db_models.DecimalField(max_digits=14, decimal_places=5, null=True, blank=True)
    fee_handling_ground = django_db_models.DecimalField(max_digits=14, decimal_places=5, null=True, blank=True)
    fee_handling_air = django_db_models.DecimalField(max_digits=14, decimal_places=5, null=True, blank=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "atech_company_pricing"
        unique_together = [["part", "company"]]


class DlgBrand(django_db_models.Model):
    """Feed brand label from DLG ``dlg_inventory.csv`` (``Brand`` column)."""

    external_id = django_db_models.CharField(max_length=512)
    name = django_db_models.CharField(max_length=512)
    aaia_code = django_db_models.CharField(max_length=255, null=True, blank=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "dlg_brands"
        unique_together = [["external_id"]]


class BrandDlgBrandMapping(django_db_models.Model):
    brand = django_db_models.ForeignKey(
        Brands,
        on_delete=django_db_models.CASCADE,
        related_name="dlg_brand_mappings",
    )
    dlg_brand = django_db_models.ForeignKey(
        DlgBrand,
        on_delete=django_db_models.CASCADE,
        related_name="brand_mappings",
    )

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "brand_dlg_brand_mapping"
        unique_together = [["brand", "dlg_brand"]]


class DlgParts(django_db_models.Model):
    """
    Row from ``dlg_inventory.csv``: ``Name`` = part / SKU, ``Display Name`` = description,
    ``Available On Hand`` = qty, ``Units`` = sell unit, ``Base Price`` = list/base price.
    """

    brand = django_db_models.ForeignKey(
        DlgBrand,
        on_delete=django_db_models.CASCADE,
        related_name="parts",
    )
    part_number = django_db_models.CharField(max_length=255)
    display_name = django_db_models.TextField(null=True, blank=True)
    available_on_hand = django_db_models.IntegerField(null=True, blank=True)
    units = django_db_models.CharField(max_length=64, null=True, blank=True)
    base_price = django_db_models.DecimalField(max_digits=14, decimal_places=5, null=True, blank=True)
    raw_data = django_db_models.JSONField(null=True, blank=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "dlg_parts"
        unique_together = [["part_number", "brand"]]


class DlgCompanyPricing(django_db_models.Model):
    """
    Per-company DLG pricing for a ``DlgParts`` row (from that company’s ``dlg_inventory.csv``).
    Catalog/inventory fields stay on ``DlgParts``; ``base_price`` here is the company-specific amount.
    """

    part = django_db_models.ForeignKey(
        DlgParts,
        on_delete=django_db_models.CASCADE,
        related_name="company_pricing",
    )
    company = django_db_models.ForeignKey(
        Company,
        on_delete=django_db_models.CASCADE,
        related_name="dlg_company_pricing",
    )
    base_price = django_db_models.DecimalField(max_digits=14, decimal_places=5, null=True, blank=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "dlg_company_pricing"
        unique_together = [["part", "company"]]


class WheelProsBrand(django_db_models.Model):
    external_id = django_db_models.CharField(max_length=255)
    name = django_db_models.CharField(max_length=255)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "wheelpros_brands"
        unique_together = [["external_id"]]


class BrandWheelProsBrandMapping(django_db_models.Model):
    brand = django_db_models.ForeignKey(
        Brands,
        on_delete=django_db_models.CASCADE,
        related_name="wheelpros_brand_mappings",
    )
    wheelpros_brand = django_db_models.ForeignKey(
        WheelProsBrand,
        on_delete=django_db_models.CASCADE,
        related_name="brand_mappings",
    )

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "brand_wheelpros_brand_mapping"
        unique_together = [["brand", "wheelpros_brand"]]


class WheelProsPart(django_db_models.Model):
    brand = django_db_models.ForeignKey(
        WheelProsBrand,
        on_delete=django_db_models.CASCADE,
        related_name="parts",
    )
    feed_type = django_db_models.CharField(
        max_length=32,
        null=True,
        blank=True,
        help_text="Which SFTP feed last wrote this row: wheel, tire, or accessories.",
    )
    part_number = django_db_models.CharField(max_length=255)
    part_description = django_db_models.TextField(null=True, blank=True)
    display_style_no = django_db_models.CharField(max_length=255, null=True, blank=True)
    finish = django_db_models.CharField(max_length=255, null=True, blank=True)
    size = django_db_models.CharField(max_length=255, null=True, blank=True)
    bolt_pattern = django_db_models.CharField(max_length=255, null=True, blank=True)
    offset = django_db_models.CharField(max_length=255, null=True, blank=True)
    center_bore = django_db_models.CharField(max_length=255, null=True, blank=True)
    load_rating = django_db_models.CharField(max_length=255, null=True, blank=True)
    shipping_weight = django_db_models.DecimalField(max_digits=12, decimal_places=5, null=True, blank=True)
    image_url = django_db_models.TextField(null=True, blank=True)
    inv_order_type = django_db_models.CharField(max_length=255, null=True, blank=True)
    style = django_db_models.CharField(max_length=255, null=True, blank=True)
    total_qoh = django_db_models.IntegerField(null=True, blank=True)
    msrp_usd = django_db_models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    map_usd = django_db_models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    run_date = django_db_models.DateTimeField(null=True, blank=True)
    warehouse_availability = django_db_models.JSONField(null=True, blank=True)
    raw_data = django_db_models.JSONField(null=True, blank=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "wheelpros_parts"
        unique_together = [["brand", "part_number"]]


class WheelProsCompanyPricing(django_db_models.Model):
    """
    Per-company Wheel Pros pricing for a catalog row (WheelProsPart).
    MSRP/MAP come from each company's SFTP feed; ``cost_usd`` is derived from MSRP and optional
    credential fields ``wheel_markup`` / ``tire_markup`` / ``accessories_markup`` (percent off list).
    """

    part = django_db_models.ForeignKey(
        WheelProsPart,
        on_delete=django_db_models.CASCADE,
        related_name="company_pricing",
    )
    company = django_db_models.ForeignKey(
        Company,
        on_delete=django_db_models.CASCADE,
        related_name="wheelpros_company_pricing",
    )
    msrp_usd = django_db_models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    map_usd = django_db_models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    cost_usd = django_db_models.DecimalField(
        max_digits=12,
        decimal_places=2,
        null=True,
        blank=True,
        help_text="Dealer cost derived from MSRP and company wheel/tire/accessories discount % in credentials.",
    )

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "wheelpros_company_pricing"
        unique_together = [["part", "company"]]


class RoughCountryBrand(django_db_models.Model):
    """Single brand from Rough Country feed (e.g. manufacturer 'Rough Country')."""

    external_id = django_db_models.CharField(max_length=255)
    name = django_db_models.CharField(max_length=255)
    aaia_code = django_db_models.CharField(max_length=255, null=True, blank=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "rough_country_brands"
        unique_together = [["external_id"]]


class RoughCountryPart(django_db_models.Model):
    """Part from Rough Country feed (General tab)."""

    brand = django_db_models.ForeignKey(
        RoughCountryBrand,
        on_delete=django_db_models.CASCADE,
        related_name="parts",
    )
    sku = django_db_models.CharField(max_length=255)
    title = django_db_models.CharField(max_length=512, null=True, blank=True)
    description = django_db_models.TextField(null=True, blank=True)
    price = django_db_models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    sale_price = django_db_models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    cost = django_db_models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    cnd_map = django_db_models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    cnd_price = django_db_models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    availability = django_db_models.CharField(max_length=255, null=True, blank=True)
    nv_stock = django_db_models.IntegerField(null=True, blank=True)
    tn_stock = django_db_models.IntegerField(null=True, blank=True)
    link = django_db_models.TextField(null=True, blank=True)
    image_1 = django_db_models.TextField(null=True, blank=True)
    image_2 = django_db_models.TextField(null=True, blank=True)
    image_3 = django_db_models.TextField(null=True, blank=True)
    image_4 = django_db_models.TextField(null=True, blank=True)
    image_5 = django_db_models.TextField(null=True, blank=True)
    image_6 = django_db_models.TextField(null=True, blank=True)
    video = django_db_models.TextField(null=True, blank=True)
    features = django_db_models.TextField(null=True, blank=True)
    notes = django_db_models.TextField(null=True, blank=True)
    category = django_db_models.CharField(max_length=255, null=True, blank=True)
    manufacturer = django_db_models.CharField(max_length=255, null=True, blank=True)
    upc = django_db_models.CharField(max_length=255, null=True, blank=True)
    weight = django_db_models.CharField(max_length=64, null=True, blank=True)
    height = django_db_models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    width = django_db_models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    length = django_db_models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    added_date = django_db_models.DateField(null=True, blank=True)
    is_discontinued = django_db_models.BooleanField(default=False)
    discontinued_date = django_db_models.DateTimeField(null=True, blank=True)
    replacement_sku = django_db_models.CharField(max_length=255, null=True, blank=True)
    raw_data = django_db_models.JSONField(null=True, blank=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "rough_country_parts"
        unique_together = [["brand", "sku"]]


class RoughCountryCompanyPricing(django_db_models.Model):
    """
    Per-company Rough Country pricing for a catalog row (RoughCountryPart).
    Catalog/non-price fields live on RoughCountryPart; feed prices are stored per company
    so ProviderPartCompanyPricing sync keys off (part, company) like other providers.
    """

    part = django_db_models.ForeignKey(
        RoughCountryPart,
        on_delete=django_db_models.CASCADE,
        related_name="company_pricing",
    )
    company = django_db_models.ForeignKey(
        Company,
        on_delete=django_db_models.CASCADE,
        related_name="rough_country_company_pricing",
    )
    price = django_db_models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    sale_price = django_db_models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    cost = django_db_models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    cnd_map = django_db_models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    cnd_price = django_db_models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "rough_country_company_pricing"
        unique_together = [["part", "company"]]


class RoughCountryFitment(django_db_models.Model):
    """Vehicle fitment from Rough Country feed (Vehicle Fitment tab)."""

    part = django_db_models.ForeignKey(
        RoughCountryPart,
        on_delete=django_db_models.CASCADE,
        related_name="fitments",
    )
    start_year = django_db_models.IntegerField(null=True, blank=True)
    end_year = django_db_models.IntegerField(null=True, blank=True)
    make = django_db_models.CharField(max_length=128, null=True, blank=True)
    model = django_db_models.CharField(max_length=128, null=True, blank=True)
    submodel = django_db_models.CharField(max_length=255, null=True, blank=True)
    drive = django_db_models.CharField(max_length=64, null=True, blank=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "rough_country_fitment"
        unique_together = [["part", "start_year", "end_year", "make", "model", "submodel", "drive"]]


class BrandRoughCountryBrandMapping(django_db_models.Model):
    """Maps our Brands to RoughCountryBrand (for master parts sync)."""

    brand = django_db_models.ForeignKey(
        Brands,
        on_delete=django_db_models.CASCADE,
        related_name="rough_country_brand_mappings",
    )
    rough_country_brand = django_db_models.ForeignKey(
        RoughCountryBrand,
        on_delete=django_db_models.CASCADE,
        related_name="brand_mappings",
    )

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "brand_rough_country_brand_mapping"
        unique_together = ["brand", "rough_country_brand"]


class QuadratecBrand(django_db_models.Model):
    """Single brand from the Quadratec feed (feed 'Brand' column, stored uppercase)."""

    external_id = django_db_models.CharField(max_length=255)
    name = django_db_models.CharField(max_length=255)
    aaia_code = django_db_models.CharField(max_length=255, null=True, blank=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "quadratec_brands"
        unique_together = [["external_id"]]


class QuadratecPart(django_db_models.Model):
    """
    Part from the Quadratec feeds (catalog + distributor-wide inventory; not per-company --
    see QuadratecCompanyPricing for per-company price). ``sku`` is Quadratec's own part number
    (Quadratec PN); ``mpn`` is the manufacturer part number (MPN / Part No). Warehouse stock
    columns come from the wholesale feed and are Quadratec-wide, like RoughCountryPart's
    nv_stock/tn_stock. Prices live per company on QuadratecCompanyPricing.
    """

    brand = django_db_models.ForeignKey(
        QuadratecBrand,
        on_delete=django_db_models.CASCADE,
        related_name="parts",
    )
    sku = django_db_models.CharField(max_length=255)
    mpn = django_db_models.CharField(max_length=255, null=True, blank=True)
    title = django_db_models.CharField(max_length=512, null=True, blank=True)
    description = django_db_models.TextField(null=True, blank=True)
    upc = django_db_models.CharField(max_length=255, null=True, blank=True)
    inv_pa1 = django_db_models.IntegerField(null=True, blank=True)
    inv_pa2 = django_db_models.IntegerField(null=True, blank=True)
    inv_nv1 = django_db_models.IntegerField(null=True, blank=True)
    inv_total = django_db_models.IntegerField(null=True, blank=True)
    shipping_surcharge = django_db_models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    raw_data = django_db_models.JSONField(null=True, blank=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "quadratec_parts"
        unique_together = [["brand", "sku"]]


class QuadratecCompanyPricing(django_db_models.Model):
    """
    Per-company Quadratec pricing for a catalog row (QuadratecPart). Catalog/non-price fields
    live on QuadratecPart; feed prices are stored per company so ProviderPartCompanyPricing sync
    keys off (part, company) like other providers. ``cost`` is the dealer cost from the wholesale
    feed; ``retail_price``/``map`` are MSRP/MAP; ``wholesale_price`` is the pricing-sheet wholesale.
    """

    part = django_db_models.ForeignKey(
        QuadratecPart,
        on_delete=django_db_models.CASCADE,
        related_name="company_pricing",
    )
    company = django_db_models.ForeignKey(
        Company,
        on_delete=django_db_models.CASCADE,
        related_name="quadratec_company_pricing",
    )
    retail_price = django_db_models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    wholesale_price = django_db_models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    cost = django_db_models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    map = django_db_models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "quadratec_company_pricing"
        unique_together = [["part", "company"]]


class BrandQuadratecBrandMapping(django_db_models.Model):
    """Maps our Brands to QuadratecBrand (for master parts sync)."""

    brand = django_db_models.ForeignKey(
        Brands,
        on_delete=django_db_models.CASCADE,
        related_name="quadratec_brand_mappings",
    )
    quadratec_brand = django_db_models.ForeignKey(
        QuadratecBrand,
        on_delete=django_db_models.CASCADE,
        related_name="brand_mappings",
    )

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "brand_quadratec_brand_mapping"
        unique_together = ["brand", "quadratec_brand"]


class VossenBrand(django_db_models.Model):
    """Single brand from the Vossen feed — always one row, "VOSSEN" (Vossen is brand=distributor)."""

    external_id = django_db_models.CharField(max_length=255)
    name = django_db_models.CharField(max_length=255)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "vossen_brands"
        unique_together = [["external_id"]]


class VossenPart(django_db_models.Model):
    """
    Part from Vossen's AfterMarket.aspx CSV feed (catalog + manufacturer-wide stock; not
    per-company — see VossenCompanyPricing for per-company price). Wheel-spec columns
    (diameter/width/offset/bolt_pattern/center_bore) are stored as CharField like
    WheelProsPart's equivalents since raw feed formats vary and non-wheel rows (caps, lug nuts)
    carry "0"/blank placeholders rather than real dimensions.
    """

    brand = django_db_models.ForeignKey(
        VossenBrand,
        on_delete=django_db_models.CASCADE,
        related_name="parts",
    )
    sku = django_db_models.CharField(max_length=255)
    description = django_db_models.TextField(null=True, blank=True)
    available = django_db_models.IntegerField(null=True, blank=True)
    diameter = django_db_models.CharField(max_length=64, null=True, blank=True)
    width = django_db_models.CharField(max_length=64, null=True, blank=True)
    offset = django_db_models.CharField(max_length=64, null=True, blank=True)
    bolt_pattern = django_db_models.CharField(max_length=64, null=True, blank=True)
    center_bore = django_db_models.CharField(max_length=64, null=True, blank=True)
    raw_data = django_db_models.JSONField(null=True, blank=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "vossen_parts"
        unique_together = [["brand", "sku"]]


class VossenCompanyPricing(django_db_models.Model):
    """
    Per-company Vossen pricing for a catalog row (VossenPart), read from that company's own
    feed_url — mirrors RoughCountryCompanyPricing's split (catalog/stock on the Part, price
    per company) so ProviderPartCompanyPricing sync keys off (part, company) like other providers.
    """

    part = django_db_models.ForeignKey(
        VossenPart,
        on_delete=django_db_models.CASCADE,
        related_name="company_pricing",
    )
    company = django_db_models.ForeignKey(
        Company,
        on_delete=django_db_models.CASCADE,
        related_name="vossen_company_pricing",
    )
    price = django_db_models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "vossen_company_pricing"
        unique_together = [["part", "company"]]


class BrandVossenBrandMapping(django_db_models.Model):
    """Maps our Brands to VossenBrand (for master parts sync). Always a single row in practice."""

    brand = django_db_models.ForeignKey(
        Brands,
        on_delete=django_db_models.CASCADE,
        related_name="vossen_brand_mappings",
    )
    vossen_brand = django_db_models.ForeignKey(
        VossenBrand,
        on_delete=django_db_models.CASCADE,
        related_name="brand_mappings",
    )

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "brand_vossen_brand_mapping"
        unique_together = ["brand", "vossen_brand"]


class TireRackBrand(django_db_models.Model):
    """
    Manufacturer label from TireRack's feed (``Manufacturer Name`` column, e.g. "Bridgestone") --
    unlike VossenBrand (always one row, since Vossen itself is the brand), TireRack's single feed
    covers many tire manufacturers, one row per distinct name. The feed has no numeric/external
    id for a brand, so ``name`` is the natural key.
    """

    name = django_db_models.CharField(max_length=255)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "tirerack_brands"
        unique_together = [["name"]]


class BrandTireRackBrandMapping(django_db_models.Model):
    """Maps our Brands to TireRackBrand (for master parts sync)."""

    brand = django_db_models.ForeignKey(
        Brands,
        on_delete=django_db_models.CASCADE,
        related_name="tirerack_brand_mappings",
    )
    tirerack_brand = django_db_models.ForeignKey(
        TireRackBrand,
        on_delete=django_db_models.CASCADE,
        related_name="brand_mappings",
    )

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "brand_tirerack_brand_mapping"
        unique_together = [["brand", "tirerack_brand"]]


class TireRackParts(django_db_models.Model):
    """
    Row from TireRack's SFTP CSV feed. Each company has its own TireRack dealer account/feed
    (same pattern as DlgParts/DlgCompanyPricing) -- catalog/inventory fields here come from the
    PRIMARY connection's own feed only (shared catalog, mirrors DLG); company-specific cost
    lives on TireRackCompanyPricing, sourced from each company's own SFTP pull (see
    tirerack.sync_tirerack_company_pricing_for_company_provider).
    """

    brand = django_db_models.ForeignKey(
        TireRackBrand,
        on_delete=django_db_models.CASCADE,
        related_name="parts",
    )
    part_number = django_db_models.CharField(max_length=255)
    description = django_db_models.TextField(null=True, blank=True)
    quantity = django_db_models.IntegerField(null=True, blank=True)
    country_of_origin = django_db_models.CharField(max_length=255, null=True, blank=True)
    fet = django_db_models.DecimalField(max_digits=14, decimal_places=5, null=True, blank=True)
    base_price = django_db_models.DecimalField(max_digits=14, decimal_places=5, null=True, blank=True)
    total_price = django_db_models.DecimalField(max_digits=14, decimal_places=5, null=True, blank=True)
    road_hazard_warranty = django_db_models.CharField(max_length=255, null=True, blank=True)
    treadlife_warranty_1 = django_db_models.CharField(max_length=255, null=True, blank=True)
    treadlife_warranty_2 = django_db_models.CharField(max_length=255, null=True, blank=True)
    treadlife_warranty_3 = django_db_models.CharField(max_length=255, null=True, blank=True)
    raw_data = django_db_models.JSONField(null=True, blank=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "tirerack_parts"
        unique_together = [["part_number", "brand"]]


class TireRackCompanyPricing(django_db_models.Model):
    """
    Per-company TireRack pricing for a TireRackParts row (from that company's own SFTP feed --
    each company has its own TireRack dealer account/credentials, same as DlgCompanyPricing).
    Catalog/inventory fields stay on TireRackParts (shared, from the primary connection);
    total_price here is the company-specific amount.
    """

    part = django_db_models.ForeignKey(
        TireRackParts,
        on_delete=django_db_models.CASCADE,
        related_name="company_pricing",
    )
    company = django_db_models.ForeignKey(
        Company,
        on_delete=django_db_models.CASCADE,
        related_name="tirerack_company_pricing",
    )
    total_price = django_db_models.DecimalField(max_digits=14, decimal_places=5, null=True, blank=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "tirerack_company_pricing"
        unique_together = [["part", "company"]]


class MasterPart(django_db_models.Model):
    brand = django_db_models.ForeignKey(Brands, on_delete=django_db_models.CASCADE, related_name="master_parts")
    part_number = django_db_models.CharField(max_length=255)
    sku = django_db_models.CharField(max_length=255, null=True)
    description = django_db_models.TextField(null=True)
    aaia_code = django_db_models.CharField(max_length=255, null=True)
    image_url = django_db_models.TextField(null=True)
    gtin = django_db_models.CharField(max_length=255, null=True, blank=True)
    overview_category = django_db_models.CharField(max_length=255, null=True, blank=True)
    category = django_db_models.CharField(max_length=255, null=True, blank=True)
    # wheel / tire / part -- see src.enums.ProductType and the rules in
    # src.integrations.utils.product_type. NULL means "not classified yet", never "part":
    # 1.65M master parts reach us only through distributors that ship no type signal at all
    # (A-Tech, Keystone, Motor State, Quadratec, DLG), and guessing on those would bury the gap.
    product_type = django_db_models.CharField(max_length=16, null=True, blank=True, db_index=True)
    # AutoCare PCdb PartTerminologyID -- see PcdbTerminologyFlat for the terminology table and
    # ProductLineTerminologyMap for the group-level classification this is propagated from.
    # Not a Django FK to PcdbTerminologyFlat, matching how the Pcdb* tables avoid FKs to each
    # other (see src/models.py's Pcdb* section) -- validated by application code instead, so a
    # PCdb reload/migration never has to touch every classified MasterPart row's constraint.
    # NULL means "not classified yet" -- same never-guess convention as product_type above.
    part_terminology_id = django_db_models.IntegerField(null=True, blank=True, db_index=True)
    # Which rule produced product_type, e.g. "wheelpros:feed_type", "turn14:category",
    # "pcdb:7644", "keyword:tirerack-tire-size". Provenance is what makes a
    # 3.2M-row classification auditable and selectively re-runnable -- the inferential tiers
    # (brand:, keyword:) can be reverted on their own with a single DELETE-shaped UPDATE without
    # touching anything a distributor actually told us.
    product_type_source = django_db_models.CharField(max_length=64, null=True, blank=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "master_parts"
        unique_together = [["brand", "part_number"]]
        indexes = [
            django_db_models.Index(fields=["brand", "sku"], name="master_parts_brand_sku_idx"),
        ]


class ProviderPart(django_db_models.Model):
    master_part = django_db_models.ForeignKey(
        MasterPart, on_delete=django_db_models.CASCADE, related_name="provider_parts"
    )
    provider = django_db_models.ForeignKey(Providers, on_delete=django_db_models.CASCADE, related_name="provider_parts")
    provider_external_id = django_db_models.CharField(max_length=255)
    distributor_refreshed_at = django_db_models.DateTimeField(
        null=True,
        blank=True,
        help_text="Last refresh time from the distributor part row (source updated_at) when master parts sync ran.",
    )
    overview_category = django_db_models.CharField(max_length=255, null=True, blank=True)
    category = django_db_models.CharField(max_length=255, null=True, blank=True)
    subcategory = django_db_models.CharField(max_length=255, null=True, blank=True)
    product_details = django_db_models.JSONField(null=True)
    is_discontinued = django_db_models.BooleanField(default=False)
    # True when the distributor's own feed flags this as a kit (e.g. Keystone's IsKit) --
    # denormalized here so cart/quote code can check kit-ness without joining
    # kit_components. See ProviderPartKitComponent for the actual component breakdown; this
    # flag can be True with zero linked components if the feed's component VCPNs haven't
    # resolved to a ProviderPart yet (not yet synced, or a raw-data quality issue).
    is_kit = django_db_models.BooleanField(default=False)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "provider_parts"
        unique_together = [["master_part", "provider"]]
        indexes = [
            django_db_models.Index(fields=["category"], name="pp_category_idx"),
            django_db_models.Index(fields=["overview_category"], name="pp_overview_category_idx"),
        ]


class ProviderPartKitComponent(django_db_models.Model):
    """
    One component line of a kit ProviderPart (kit_part.is_kit=True) -- decoded from the
    distributor's own kit-components feed field into real FKs against the same provider's
    normal catalog rows, not just raw part-number strings. Lives on ProviderPart generically
    (not a Keystone-only table): Premier's PremierParts has the same is_kit/kit_component_list
    *concept*, populated into this same table by sync_premier_kit_components -- but NOT the
    identical raw format an earlier version of this docstring assumed. Confirmed live against
    real rows of each:
      - Keystone's KitComponents: hyphen-joined pairs, pipe-separated ("B94GNRC823-1|
        B94GNRM1123-1|") -- safe to split on each token's trailing hyphen since Keystone's own
        VCPN format never contains one (see master_parts._parse_keystone_kit_components).
      - Premier's Kit Component List: alternating pipe-delimited tokens, not hyphen-joined
        ("SYN8863-10|1|SYN8855-02|1|...") -- Premier part numbers routinely contain hyphens
        themselves (e.g. "SYN8863-10"), which is exactly why Keystone's hyphen-split approach
        would be ambiguous here (see master_parts._parse_premier_kit_components).
    Each distributor's own sync function owns decoding its own raw format into this shared
    table; don't assume a new distributor's kit-components field matches either shape without
    checking real rows first.

    The whole point of resolving real ProviderPart rows here (not just storing part-number
    text) is that "add a kit to cart" can add its components directly -- see
    purchase_orders_services.add_cart_item, which is already provider-agnostic and needs no
    per-distributor changes -- instead of ever sending the kit's own item number to a
    distributor's order API. Keystone's own API cannot place a kit order at all: confirmed live
    that GetShippingOptionsMultiplePartsPerWarehouse rejects a kit VCPN outright at quote time,
    and ShipOrderDropShipMultipleParts silently explodes one into components server-side at
    submit time with no way to reconcile that back to our own line items (see
    src.integrations.orders.keystone's module docstring). Premier's own order API's kit
    behavior is untested/undocumented (no confirmed failure, unlike Keystone) -- treated as
    unverified and risky by default rather than assumed safe. Expanding into components
    ourselves, before either distributor's order API is ever called, sidesteps both cases.
    """

    kit_part = django_db_models.ForeignKey(
        ProviderPart, on_delete=django_db_models.CASCADE, related_name="kit_components"
    )
    component_part = django_db_models.ForeignKey(
        ProviderPart, on_delete=django_db_models.PROTECT, related_name="kit_of"
    )
    quantity = django_db_models.PositiveIntegerField()

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "provider_part_kit_components"
        unique_together = [["kit_part", "component_part"]]


class ProviderPartInventory(django_db_models.Model):
    provider_part = django_db_models.OneToOneField(
        ProviderPart, on_delete=django_db_models.CASCADE, related_name="inventory"
    )
    warehouse_total_qty = django_db_models.IntegerField(default=0)
    manufacturer_inventory = django_db_models.IntegerField(null=True)
    manufacturer_esd = django_db_models.DateField(null=True)
    warehouse_availability = django_db_models.JSONField(null=True)
    last_synced_at = django_db_models.DateTimeField(null=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "provider_part_inventory"


class ProviderPartCompanyPricing(django_db_models.Model):
    provider_part = django_db_models.ForeignKey(
        ProviderPart, on_delete=django_db_models.CASCADE, related_name="company_pricing"
    )
    company = django_db_models.ForeignKey(
        Company, on_delete=django_db_models.CASCADE, related_name="provider_part_pricing"
    )
    cost = django_db_models.DecimalField(max_digits=10, decimal_places=2, null=True)
    jobber_price = django_db_models.DecimalField(max_digits=10, decimal_places=2, null=True)
    map_price = django_db_models.DecimalField(max_digits=10, decimal_places=2, null=True)
    msrp = django_db_models.DecimalField(max_digits=10, decimal_places=2, null=True)
    retail_price = django_db_models.DecimalField(max_digits=10, decimal_places=2, null=True)
    last_synced_at = django_db_models.DateTimeField(null=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "provider_part_company_pricing"
        unique_together = [["provider_part", "company"]]


class AsapBrand(django_db_models.Model):
    """
    Raw ASAP Network brand catalog (GET /webapi/brands). ``brand`` is resolved directly on this
    row (no separate ``Brand<X>BrandMapping`` join table) since ASAP is an enrichment-only data
    catalog, not a distributor a company connects to.
    """

    external_id = django_db_models.CharField(max_length=64, unique=True)
    term_name = django_db_models.CharField(max_length=255)
    name = django_db_models.CharField(max_length=255)
    brand = django_db_models.ForeignKey(
        Brands, on_delete=django_db_models.SET_NULL, null=True, blank=True, related_name="asap_brands"
    )
    last_synced_at = django_db_models.DateTimeField(
        null=True,
        blank=True,
        help_text="Set once a full product sync for this brand completes; skipped on future runs unless --force (ASAP is a paid API).",
    )

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "asap_brands"


class MasterPartData(django_db_models.Model):
    """
    Source-agnostic enrichment data for a MasterPart (images, description, specs, etc.),
    populated by catalog sources such as ASAP Network. Fields are filled-in only when currently
    blank, so different sources can enrich different brands (or different fields of the same
    part) without clobbering each other.
    """

    master_part = django_db_models.OneToOneField(MasterPart, on_delete=django_db_models.CASCADE, related_name="data")
    images = django_db_models.JSONField(null=True, blank=True)
    description = django_db_models.TextField(null=True, blank=True)
    color = django_db_models.CharField(max_length=255, null=True, blank=True)
    material = django_db_models.CharField(max_length=255, null=True, blank=True)
    series = django_db_models.CharField(max_length=255, null=True, blank=True)
    warranty = django_db_models.CharField(max_length=255, null=True, blank=True)
    vehicle_type = django_db_models.JSONField(null=True, blank=True)
    field_specs = django_db_models.JSONField(null=True, blank=True)
    youtube_video = django_db_models.CharField(max_length=500, null=True, blank=True)
    installation_instructions = django_db_models.JSONField(null=True, blank=True)
    source_provider = django_db_models.ForeignKey(
        Providers, on_delete=django_db_models.SET_NULL, null=True, blank=True, related_name="master_part_data"
    )
    source_external_id = django_db_models.CharField(max_length=255, null=True, blank=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "master_part_data"


class MasterPartFitment(django_db_models.Model):
    """
    Canonical vehicle fitment for a MasterPart. Year ranges are stored as-is (not exploded into
    per-year rows) since Postgres isn't the query layer for YMM search; per-year expansion only
    happens when building Meilisearch documents.
    """

    master_part = django_db_models.ForeignKey(MasterPart, on_delete=django_db_models.CASCADE, related_name="fitments")
    year_start = django_db_models.IntegerField()
    year_end = django_db_models.IntegerField()
    make = django_db_models.CharField(max_length=128)
    model = django_db_models.CharField(max_length=128)
    # blank=True, default="" (not null=True): Postgres treats each NULL as distinct for
    # uniqueness, which would break pgbulk.upsert dedup on unique_together below.
    submodel = django_db_models.CharField(max_length=255, blank=True, default="")
    engine = django_db_models.CharField(max_length=255, blank=True, default="")
    drive_type = django_db_models.CharField(max_length=64, blank=True, default="")
    source_provider = django_db_models.ForeignKey(
        Providers, on_delete=django_db_models.SET_NULL, null=True, blank=True, related_name="fitments"
    )

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "master_part_fitments"
        unique_together = [
            ["master_part", "year_start", "year_end", "make", "model", "submodel", "engine", "drive_type"]
        ]


class CategoryMapping(django_db_models.Model):
    """
    Map a distributor or feed ``source_category`` string to normalized ``category`` and ``overview_category``.
    """

    source_category = django_db_models.CharField(max_length=255, db_index=True)
    category = django_db_models.CharField(max_length=255, null=True, blank=True)
    overview_category = django_db_models.CharField(max_length=255, null=True, blank=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "category_mappings"


class IntegrationPricingSyncJob(django_db_models.Model):
    """
    Queue row processed by a cron management command: after integration credentials
    are saved, enqueue one job per CompanyProviders to pull distributor company
    pricing and fan out ProviderPartCompanyPricing for that company.
    """

    company_provider = django_db_models.ForeignKey(
        CompanyProviders,
        on_delete=django_db_models.CASCADE,
        related_name="pricing_sync_jobs",
    )
    status = django_db_models.PositiveSmallIntegerField()
    status_name = django_db_models.CharField(max_length=64)
    message = django_db_models.TextField(null=True, blank=True)
    error_message = django_db_models.TextField(null=True, blank=True)

    # When True the job skips the raw distributor data fetch (API / SFTP / CSV download)
    # and only runs the master-parts pricing sync layer.  Leave False (default) for
    # on-demand jobs triggered by new-company onboarding/reconnect so the full fetch + sync
    # cycle runs. NOTE: the nightly ingest_all_providers pipeline does NOT actually set this
    # True today — per-company pricing is never fetched in Phase 1, so there's nothing to
    # skip; see use_delta_fetch below for how the recurring cycle avoids a full re-fetch.
    skip_raw_fetch = django_db_models.BooleanField(default=False)

    # When True (currently only meaningful for Turn 14), the raw fetch uses the distributor's
    # pricing-changes endpoint scoped to the brands with recent changes, instead of paging
    # through every mapped brand's full pricing. Set by the recurring ingest_all_providers
    # cycle; left False for the initial connect/reconnect sync, which still wants a full fetch.
    use_delta_fetch = django_db_models.BooleanField(default=False)

    # Earliest time this job may be claimed. Set when a run is cut short by a distributor's
    # hard rate budget (see src.integrations.rate_limit.RateBudgetExhausted): the job goes back
    # to OPEN rather than FAILED, deferred until the budget window rolls over. Without this the
    # processing loop would immediately re-claim the same job and burn the retry against a
    # budget that is, by definition, already spent. Null means "claimable now".
    not_before = django_db_models.DateTimeField(null=True, blank=True, db_index=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)
    started_at = django_db_models.DateTimeField(null=True, blank=True)
    completed_at = django_db_models.DateTimeField(null=True, blank=True)

    class Meta:
        db_table = "integration_pricing_sync_job"
        ordering = ["id"]


class ScheduledTaskExecution(django_db_models.Model):
    """
    Audit table for scheduled task / cron executions (e.g. Turn 14 items updates,
    inventory updates). Reusable for any named task run on a schedule.
    """

    name = django_db_models.CharField(max_length=255)
    status = django_db_models.PositiveSmallIntegerField()
    status_name = django_db_models.CharField(max_length=255)
    message = django_db_models.TextField(null=True, blank=True)
    error_message = django_db_models.TextField(null=True, blank=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)
    completed_at = django_db_models.DateTimeField(null=True, blank=True)

    class Meta:
        db_table = "scheduled_task_execution"
        ordering = ["-created_at"]


class PartRequestAudit(django_db_models.Model):
    """
    Audit log for part search and part detail API requests.
    Used to track company/user request volume (e.g. how many searches or detail views per company/user).
    """

    company = django_db_models.ForeignKey(
        Company,
        on_delete=django_db_models.CASCADE,
        related_name="part_request_audits",
    )
    user = django_db_models.ForeignKey(
        auth_models.User,
        on_delete=django_db_models.SET_NULL,
        null=True,
        blank=True,
        related_name="part_request_audits",
    )
    action = django_db_models.CharField(max_length=32)  # 'search' | 'detail'
    search_query = django_db_models.CharField(max_length=512, null=True, blank=True)
    master_part_id = django_db_models.PositiveIntegerField(null=True, blank=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "part_request_audit"
        ordering = ["-created_at"]
        indexes = [
            django_db_models.Index(
                fields=["company", "action", "user", "created_at"],
                name="pra_co_act_usr_crt_idx",
            ),
            django_db_models.Index(
                fields=["company", "action", "created_at"],
                name="pra_co_act_crt_idx",
            ),
        ]


class SupportTicket(django_db_models.Model):
    STATUS_OPEN = "open"
    STATUS_IN_PROGRESS = "in_progress"
    STATUS_RESOLVED = "resolved"
    STATUS_CLOSED = "closed"

    company = django_db_models.ForeignKey(
        Company,
        on_delete=django_db_models.CASCADE,
        related_name="support_tickets",
    )
    user = django_db_models.ForeignKey(
        auth_models.User,
        on_delete=django_db_models.CASCADE,
        related_name="support_tickets",
    )
    subject = django_db_models.CharField(max_length=100)
    message = django_db_models.TextField()
    status = django_db_models.CharField(max_length=20, default=STATUS_OPEN)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "support_tickets"
        ordering = ["-created_at"]
        indexes = [
            django_db_models.Index(fields=["company"], name="st_company_idx"),
            django_db_models.Index(fields=["user"], name="st_user_idx"),
        ]


class USZipCode(django_db_models.Model):
    zip_code = django_db_models.CharField(max_length=10, unique=True)
    city = django_db_models.CharField(max_length=128)
    state = django_db_models.CharField(max_length=2)
    county = django_db_models.CharField(max_length=128, null=True, blank=True)
    latitude = django_db_models.DecimalField(max_digits=9, decimal_places=6, null=True, blank=True)
    longitude = django_db_models.DecimalField(max_digits=9, decimal_places=6, null=True, blank=True)
    population = django_db_models.IntegerField(null=True, blank=True)
    is_major_city = django_db_models.BooleanField(default=False)

    created_at = django_db_models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "us_zip_code"
        indexes = [
            django_db_models.Index(fields=["state"], name="uszip_state_idx"),
            django_db_models.Index(fields=["state", "city"], name="uszip_state_city_idx"),
            django_db_models.Index(fields=["is_major_city"], name="uszip_major_idx"),
        ]

    def __str__(self):
        return f"{self.zip_code} - {self.city}, {self.state}"


class Lead(django_db_models.Model):
    class Status(django_db_models.IntegerChoices):
        PENDING = 0, "Pending"
        CONTACTED = 1, "Contacted"
        QUALIFIED = 2, "Qualified"
        DISQUALIFIED = 3, "Disqualified"
        CONVERTED = 4, "Converted"

    # Identity
    place_id = django_db_models.CharField(max_length=255, unique=True)
    name = django_db_models.CharField(max_length=512)

    # Location
    address = django_db_models.TextField(null=True, blank=True)
    city = django_db_models.CharField(max_length=128, null=True, blank=True)
    state = django_db_models.CharField(max_length=64, null=True, blank=True)
    zip_code = django_db_models.CharField(max_length=10, null=True, blank=True)
    latitude = django_db_models.DecimalField(max_digits=9, decimal_places=6, null=True, blank=True)
    longitude = django_db_models.DecimalField(max_digits=9, decimal_places=6, null=True, blank=True)

    # Contact
    phone = django_db_models.CharField(max_length=64, null=True, blank=True)
    website = django_db_models.URLField(max_length=512, null=True, blank=True)
    website_not_found = django_db_models.BooleanField(
        default=False, blank=True
    )  # True = Tavily+Claude couldn't find one
    website_live = django_db_models.BooleanField(null=True, blank=True)  # None = not checked yet
    emails_not_found = django_db_models.BooleanField(
        default=False, blank=True
    )  # True = enrichment tried, nothing found
    email = django_db_models.EmailField(max_length=255, null=True, blank=True)
    emails = django_db_models.JSONField(default=list, blank=True)

    # Google Places data
    rating = django_db_models.DecimalField(max_digits=3, decimal_places=1, null=True, blank=True)
    review_count = django_db_models.IntegerField(null=True, blank=True)
    google_maps_url = django_db_models.URLField(max_length=512, null=True, blank=True)
    business_status = django_db_models.CharField(max_length=64, null=True, blank=True)

    # Search metadata
    search_query = django_db_models.CharField(max_length=255, null=True, blank=True)
    source_zip = django_db_models.CharField(max_length=10, null=True, blank=True)
    category = django_db_models.CharField(max_length=128, null=True, blank=True)

    # AI qualification
    is_qualified = django_db_models.BooleanField(null=True, blank=True)
    business_typology = django_db_models.CharField(max_length=64, null=True, blank=True)
    confidence_score = django_db_models.IntegerField(null=True, blank=True)
    brands_mentioned = django_db_models.JSONField(default=list, blank=True)
    ai_reasoning = django_db_models.TextField(null=True, blank=True)
    ai_skip_reason = django_db_models.CharField(max_length=255, null=True, blank=True)
    ai_qualified_at = django_db_models.DateTimeField(null=True, blank=True)

    # CRM status
    status = django_db_models.IntegerField(choices=Status.choices, default=Status.PENDING)
    importance = django_db_models.IntegerField(null=True, blank=True)  # 1–5 score
    notes = django_db_models.TextField(null=True, blank=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "lead"
        indexes = [
            django_db_models.Index(fields=["state", "city"], name="lead_state_city_idx"),
            django_db_models.Index(fields=["category"], name="lead_category_idx"),
            django_db_models.Index(fields=["status"], name="lead_status_idx"),
            django_db_models.Index(fields=["rating"], name="lead_rating_idx"),
        ]

    def __str__(self):
        return f"{self.name} ({self.city}, {self.state})"


class LeadEmail(django_db_models.Model):
    """One row per email address found on a qualified lead's website — with Reoon verification results."""

    lead = django_db_models.ForeignKey(Lead, on_delete=django_db_models.CASCADE, related_name="verified_emails")
    email = django_db_models.EmailField(max_length=255)

    # Claude AI pre-screening
    ai_valid = django_db_models.BooleanField(null=True, blank=True)  # None = not checked yet

    # Reoon verification results
    status = django_db_models.CharField(
        max_length=32, null=True, blank=True
    )  # valid, invalid, disposable, unknown, etc.
    is_valid = django_db_models.BooleanField(null=True, blank=True)
    is_disposable = django_db_models.BooleanField(null=True, blank=True)
    is_free_email = django_db_models.BooleanField(null=True, blank=True)
    is_role_based = django_db_models.BooleanField(null=True, blank=True)
    mx_found = django_db_models.BooleanField(null=True, blank=True)

    verified_at = django_db_models.DateTimeField(null=True, blank=True)
    created_at = django_db_models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "lead_email"
        unique_together = [("lead", "email")]
        indexes = [
            django_db_models.Index(fields=["email"], name="lead_email_email_idx"),
            django_db_models.Index(fields=["status"], name="lead_email_status_idx"),
            django_db_models.Index(fields=["is_valid"], name="lead_email_valid_idx"),
        ]

    def __str__(self):
        return f"{self.email} ({self.status})"


class RealTruckLead(django_db_models.Model):
    """
    Dealers scraped from RealTruck's dealer locator. The table is populated by the scraper
    outside of Django (hence the text primary key coming straight from RealTruck), so the
    columns below mirror what already exists in ``realtruck_leads``.
    """

    id = django_db_models.TextField(primary_key=True)  # RealTruck's own dealer id
    name = django_db_models.TextField()

    phone = django_db_models.TextField(null=True, blank=True)
    website = django_db_models.TextField(null=True, blank=True)
    # True = Tavily+Claude searched and genuinely found nothing. Only set on a search that ran --
    # a failed search leaves this alone so the lead can be retried.
    website_not_found = django_db_models.BooleanField(default=False, blank=True)
    website_live = django_db_models.BooleanField(null=True, blank=True)  # None = not checked yet
    website_checked_at = django_db_models.DateTimeField(null=True, blank=True)
    email = django_db_models.EmailField(max_length=255, null=True, blank=True)
    emails = django_db_models.JSONField(default=list, blank=True)
    emails_not_found = django_db_models.BooleanField(default=False, blank=True)  # scraped, nothing there

    address = django_db_models.TextField(null=True, blank=True)
    city = django_db_models.TextField(null=True, blank=True)
    state = django_db_models.TextField(null=True, blank=True)
    zipcode = django_db_models.TextField(null=True, blank=True)
    country = django_db_models.TextField(null=True, blank=True)
    full_address = django_db_models.TextField(null=True, blank=True)
    lat = django_db_models.FloatField(null=True, blank=True)
    lng = django_db_models.FloatField(null=True, blank=True)

    # RealTruck dealer badges
    is_preferred = django_db_models.BooleanField(default=False)
    is_double_warranty = django_db_models.BooleanField(default=False)
    is_next_gen = django_db_models.BooleanField(default=False)
    is_real_pro = django_db_models.BooleanField(default=False)
    is_international = django_db_models.BooleanField(default=False)

    sort_order = django_db_models.IntegerField(null=True, blank=True)
    brand_count = django_db_models.IntegerField(null=True, blank=True)
    preferred_brands = django_db_models.TextField(null=True, blank=True)
    all_brands = django_db_models.TextField(null=True, blank=True)
    brands = django_db_models.JSONField(null=True, blank=True)

    # Outreach prioritisation — which qualified shops to contact first.
    # outreach_priority is a 0-100 composite of the LLM's read of the site and hard signals
    # (locations, RealTruck dealer tier, brand count); tier is just its A/B/C bucketing.
    outreach_priority = django_db_models.IntegerField(null=True, blank=True)
    priority_tier = django_db_models.CharField(max_length=1, null=True, blank=True)
    website_quality = django_db_models.IntegerField(null=True, blank=True)  # LLM 0-100
    location_count = django_db_models.IntegerField(null=True, blank=True)  # sites sharing this domain
    priority_signals = django_db_models.JSONField(default=dict, blank=True)  # what drove the score
    priority_reasoning = django_db_models.TextField(null=True, blank=True)
    prioritized_at = django_db_models.DateTimeField(null=True, blank=True)

    # AI qualification — same shape as Lead, so both sources report identically
    is_qualified = django_db_models.BooleanField(null=True, blank=True)
    business_typology = django_db_models.CharField(max_length=64, null=True, blank=True)
    confidence_score = django_db_models.IntegerField(null=True, blank=True)
    brands_mentioned = django_db_models.JSONField(default=list, blank=True)
    ai_reasoning = django_db_models.TextField(null=True, blank=True)
    ai_skip_reason = django_db_models.CharField(max_length=255, null=True, blank=True)
    ai_qualified_at = django_db_models.DateTimeField(null=True, blank=True)

    # Scrape metadata
    found_via = django_db_models.TextField(null=True, blank=True)  # the search location that surfaced it
    distance_mi = django_db_models.FloatField(null=True, blank=True)
    first_seen_at = django_db_models.DateTimeField(default=timezone.now)
    last_seen_at = django_db_models.DateTimeField(default=timezone.now)

    class Meta:
        db_table = "realtruck_leads"
        indexes = [
            django_db_models.Index(fields=["state"], name="realtruck_leads_state_idx"),
            django_db_models.Index(fields=["is_preferred"], name="realtruck_leads_preferred_idx"),
        ]

    def __str__(self):
        return f"{self.name} ({self.city}, {self.state})"


class RealTruckLeadEmail(django_db_models.Model):
    """
    One row per email found on a qualified RealTruck dealer's site, with Reoon results.

    A separate table rather than a nullable FK on LeadEmail: that model's ``lead`` is non-null and
    existing code dereferences it freely, so rows with no ``lead`` would break those paths.
    """

    lead = django_db_models.ForeignKey(
        RealTruckLead, on_delete=django_db_models.CASCADE, related_name="verified_emails"
    )
    email = django_db_models.EmailField(max_length=255)

    # Reoon verification results
    status = django_db_models.CharField(max_length=32, null=True, blank=True)
    is_valid = django_db_models.BooleanField(null=True, blank=True)
    is_disposable = django_db_models.BooleanField(null=True, blank=True)
    is_free_email = django_db_models.BooleanField(null=True, blank=True)
    is_role_based = django_db_models.BooleanField(null=True, blank=True)
    mx_found = django_db_models.BooleanField(null=True, blank=True)

    verified_at = django_db_models.DateTimeField(null=True, blank=True)
    created_at = django_db_models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "realtruck_lead_email"
        unique_together = [("lead", "email")]
        indexes = [
            django_db_models.Index(fields=["email"], name="rt_lead_email_email_idx"),
            django_db_models.Index(fields=["status"], name="rt_lead_email_status_idx"),
            django_db_models.Index(fields=["is_valid"], name="rt_lead_email_valid_idx"),
        ]

    def __str__(self):
        return f"{self.email} ({self.status})"


class LeerLead(django_db_models.Model):
    """
    Dealers scraped from LEER's dealer locator. Like RealTruckLead the table is populated outside
    Django, so the first block mirrors what already exists in ``leer_leads``.

    Unlike RealTruck, LEER's locator publishes no website at all -- not in a column and not in the
    ``raw`` payload -- so ``website`` starts empty for every row and has to be discovered
    (``find_missing_websites --source leer``) before the liveness and qualification steps have
    anything to work with.
    """

    location_id = django_db_models.TextField(primary_key=True)  # LEER's own location id
    name = django_db_models.TextField()

    phone = django_db_models.TextField(null=True, blank=True)
    phone_digits = django_db_models.TextField(null=True, blank=True)
    address = django_db_models.TextField(null=True, blank=True)
    city = django_db_models.TextField(null=True, blank=True)
    state = django_db_models.TextField(null=True, blank=True)
    zipcode = django_db_models.TextField(null=True, blank=True)
    full_address = django_db_models.TextField(null=True, blank=True)
    lat = django_db_models.FloatField(null=True, blank=True)
    lng = django_db_models.FloatField(null=True, blank=True)
    company_id = django_db_models.TextField(null=True, blank=True)

    # Scrape metadata
    found_via = django_db_models.TextField(null=True, blank=True)
    distance_mi = django_db_models.FloatField(null=True, blank=True)
    raw = django_db_models.JSONField(null=True, blank=True)
    first_seen_at = django_db_models.DateTimeField(default=timezone.now)
    last_seen_at = django_db_models.DateTimeField(default=timezone.now)

    # Discovered contact details — none of this ships with the locator data
    website = django_db_models.TextField(null=True, blank=True)
    website_not_found = django_db_models.BooleanField(default=False, blank=True)
    website_live = django_db_models.BooleanField(null=True, blank=True)
    website_checked_at = django_db_models.DateTimeField(null=True, blank=True)
    email = django_db_models.EmailField(max_length=255, null=True, blank=True)
    emails = django_db_models.JSONField(default=list, blank=True)

    # AI qualification — same shape as Lead and RealTruckLead
    is_qualified = django_db_models.BooleanField(null=True, blank=True)
    business_typology = django_db_models.CharField(max_length=64, null=True, blank=True)
    confidence_score = django_db_models.IntegerField(null=True, blank=True)
    brands_mentioned = django_db_models.JSONField(default=list, blank=True)
    ai_reasoning = django_db_models.TextField(null=True, blank=True)
    ai_skip_reason = django_db_models.CharField(max_length=255, null=True, blank=True)
    ai_qualified_at = django_db_models.DateTimeField(null=True, blank=True)

    class Meta:
        db_table = "leer_leads"
        indexes = [
            django_db_models.Index(fields=["state"], name="leer_leads_state_idx"),
            django_db_models.Index(fields=["zipcode"], name="leer_leads_zipcode_idx"),
        ]

    def __str__(self):
        return f"{self.name} ({self.city}, {self.state})"


class NotificationEmailLog(django_db_models.Model):
    """
    Audit log of transactional notification emails sent via Resend (e.g. the
    "first sync completed" email). One row per send attempt, success or failure.
    """

    email_type = django_db_models.PositiveSmallIntegerField()
    email_type_name = django_db_models.CharField(max_length=64)

    to_email = django_db_models.EmailField(max_length=255)
    from_email = django_db_models.EmailField(max_length=255)
    subject = django_db_models.CharField(max_length=255)

    company = django_db_models.ForeignKey(
        Company,
        on_delete=django_db_models.SET_NULL,
        related_name="notification_email_logs",
        null=True,
        blank=True,
    )
    company_provider = django_db_models.ForeignKey(
        CompanyProviders,
        on_delete=django_db_models.SET_NULL,
        related_name="notification_email_logs",
        null=True,
        blank=True,
    )

    status = django_db_models.PositiveSmallIntegerField()
    status_name = django_db_models.CharField(max_length=32)
    provider_message_id = django_db_models.CharField(max_length=255, null=True, blank=True)
    error_message = django_db_models.TextField(null=True, blank=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "notification_email_log"


class PurchaseOrderGroup(django_db_models.Model):
    """
    Groups sibling PurchaseOrders created from one cross-distributor checkout (a shop's
    cart can span several distributors, each becoming its own PurchaseOrder). Purely
    organisational — distributors never see this, only the internal "review & quote"
    and PO-history screens do.
    """

    company = django_db_models.ForeignKey(
        Company, on_delete=django_db_models.CASCADE, related_name="purchase_order_groups"
    )
    created_by = django_db_models.ForeignKey(
        UserProfile,
        on_delete=django_db_models.SET_NULL,
        related_name="po_groups_created",
        null=True,
        blank=True,
    )
    reference = django_db_models.CharField(max_length=64, null=True, blank=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "purchase_order_groups"


class PurchaseOrder(django_db_models.Model):
    """
    A distributor-agnostic internal purchase order. See src.enums.PurchaseOrderStatus.

    A DRAFT/QUOTED row doubles as the per-distributor "Add to PO" cart: requesting a quote
    does not turn a cart into a real order by itself, it only attaches quote data to the same
    row (see src.api.services.purchase_orders._cart_queryset) — it still shows up in the cart,
    is still fully editable (editing reverts it back to DRAFT since the quote no longer
    matches), and is still invisible to order history. Only submit_order() moves a PO out of
    cart territory for good. At most one open cart (DRAFT or QUOTED) may exist per
    (company, company_provider) at a time (enforced below); "Add to PO" always finds-or-creates
    this row rather than ever risking a second concurrent draft.
    """

    company = django_db_models.ForeignKey(Company, on_delete=django_db_models.CASCADE, related_name="purchase_orders")
    company_provider = django_db_models.ForeignKey(
        CompanyProviders, on_delete=django_db_models.PROTECT, related_name="purchase_orders"
    )
    # Which of company_provider's order accounts this PO is placed through — see
    # CompanyProviderOrderAccount's docstring. Null resolves to that connection's is_default
    # account at order/quote time (see src.integrations.credentials.get_order_credentials),
    # which is what every PO means today for the overwhelming majority of connections (a single
    # account, always the default); only set explicitly when a company has configured more than
    # one named account and chose a non-default one at add-to-cart time (see
    # purchase_orders_services.add_cart_item). Decided once, at cart-creation time, and never
    # re-resolved implicitly afterward — requote/submit must keep using whatever account the
    # cart was actually built against.
    order_account = django_db_models.ForeignKey(
        CompanyProviderOrderAccount,
        on_delete=django_db_models.PROTECT,
        related_name="purchase_orders",
        null=True,
        blank=True,
    )
    group = django_db_models.ForeignKey(
        PurchaseOrderGroup,
        on_delete=django_db_models.SET_NULL,
        related_name="purchase_orders",
        null=True,
        blank=True,
    )

    # Our own PO number, sent to distributors that accept one (Meyer's CustPO,
    # Keystone's PONumber, Turn 14's po_number). Assigned when the cart is reviewed/quoted,
    # not when the draft is first created.
    po_number = django_db_models.CharField(max_length=64, unique=True, null=True, blank=True)

    status = django_db_models.PositiveSmallIntegerField()
    status_name = django_db_models.CharField(max_length=32)

    source = django_db_models.PositiveSmallIntegerField()
    source_name = django_db_models.CharField(max_length=32)
    # Free-text external reference for non-staff sources (e.g. a future SMS repair-order id).
    source_reference = django_db_models.CharField(max_length=255, null=True, blank=True)

    created_by = django_db_models.ForeignKey(
        UserProfile,
        on_delete=django_db_models.SET_NULL,
        related_name="purchase_orders_created",
        null=True,
        blank=True,
    )

    # Ship-to snapshot. Company has no address/ship-to model today, so this is captured
    # directly on the PO rather than inherited. Null while still a DRAFT cart.
    ship_to_name = django_db_models.CharField(max_length=255, null=True, blank=True)
    ship_to_attention = django_db_models.CharField(max_length=255, null=True, blank=True)
    ship_to_address1 = django_db_models.CharField(max_length=255, null=True, blank=True)
    ship_to_address2 = django_db_models.CharField(max_length=255, null=True, blank=True)
    ship_to_city = django_db_models.CharField(max_length=128, null=True, blank=True)
    ship_to_state = django_db_models.CharField(max_length=64, null=True, blank=True)
    ship_to_postal_code = django_db_models.CharField(max_length=32, null=True, blank=True)
    ship_to_country = django_db_models.CharField(max_length=64, null=True, blank=True)
    ship_to_phone = django_db_models.CharField(max_length=32, null=True, blank=True)
    # Set from the FE's review-cart request ({"ship_to": {..., "ship_to_my_shop": true}}).
    # Distinguishes "ship to the shop's own address" from "drop-ship to an end customer" —
    # passed straight through to Turn14 as recipient.is_shop_address (see turn_14.py's
    # _build_recipient). Defaults False (drop-ship) to match the field's prior hardcoded value
    # before this flag existed, so an FE that doesn't send it yet sees no behavior change.
    ship_to_is_shop_address = django_db_models.BooleanField(default=False)
    ship_method = django_db_models.CharField(max_length=64, null=True, blank=True)

    # Quote snapshot from the distributor adapter's get_shipping_quote(), before submit.
    quote_raw_response = django_db_models.JSONField(null=True, blank=True, encoder=DjangoJSONEncoder)
    quoted_at = django_db_models.DateTimeField(null=True, blank=True)

    # Normalized (distributor-agnostic), PO-level breakdown of the last quote's shipments —
    # one entry per distinct (warehouse, status) group, built once here rather than duplicated
    # inside every line item's own shipments (see PurchaseOrderLineItem.shipments, which now
    # only holds a lightweight {shipment_id, quantity_confirmed, quantity_backordered}
    # reference into this list):
    # [{id, warehouse_code, warehouse_name, status,
    #   items: [{line_item_id, provider_external_id, quantity, unit_price, line_total}],
    #   ship_options: [{id, code, name, verbose_eta, days_in_transit, cost, estimated_delivery_date}],
    #   selected_ship_option_id}].
    # `status` is one of "in_stock"/"backordered"/"not_orderable"/"transfer" — see
    # purchase_order_jobs._shipment_status. Distinguishes Keystone's four ShipFlag outcomes
    # (only two of which — in_stock/backordered — Turn14 can ever report) instead of collapsing
    # "not orderable" and "transfer" into a bare in-stock-or-not boolean.
    # `ship_options[].id` is the distributor's own per-option identifier (base.ShipOption.
    # quote_option_id) — what submit_order actually sends to select that option, not
    # service_level_code (which can recur across shipments/quotes). `selected_ship_option_id`
    # defaults at quote time (match po.ship_method's code if set, else cheapest) and can be
    # overridden per shipment via POST .../shipments/select/ before submitting.
    shipments = django_db_models.JSONField(null=True, blank=True, encoder=DjangoJSONEncoder)

    subtotal = django_db_models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    estimated_shipping = django_db_models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    total = django_db_models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)

    # Distributor's own quoted grand total (gross, before any shipping method is selected) —
    # display-only, informational comparison against our own `total` above, which stays
    # authoritative for billing (see base.ShippingQuoteResult.distributor_total).
    distributor_quoted_total = django_db_models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    # Order-level fees from the last quote that aren't tied to any specific line item (e.g.
    # Turn14's dropship fee): [{fee_type, description, amount}]. Display-only, same reasoning
    # as distributor_quoted_total — not folded into subtotal/estimated_shipping/total.
    fees = django_db_models.JSONField(null=True, blank=True, encoder=DjangoJSONEncoder)

    error_message = django_db_models.TextField(null=True, blank=True)
    notes = django_db_models.TextField(null=True, blank=True)
    # Customer-supplied PO name/reference, optionally set at submit time (POST .../submit/
    # body: {po_name}) — sent to the distributor as ITS po_number field instead of our own
    # po_number below, when set. po_number itself is never overridden: it's unique and is what
    # every adapter uses as the lookup key for post-submit status-check/cancel (Premier and
    # Keystone especially, which have no other order identifier) — swapping it out post-hoc
    # would break those lookups. See base.resolve_po_number — every adapter reads this.
    po_name = django_db_models.TextField(null=True, blank=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)
    submitted_at = django_db_models.DateTimeField(null=True, blank=True)
    # Last time refresh_confirmed_purchase_orders (a distinct, lighter-weight cadence than the
    # general STATUS_CHECK job) actually polled the distributor for this PO's raw order data.
    # Drives that command's "check often right after submission, then once a day" rule — see
    # its own module docstring for the exact policy.
    distributor_status_checked_at = django_db_models.DateTimeField(null=True, blank=True)

    class Meta:
        db_table = "purchase_orders"
        indexes = [
            django_db_models.Index(fields=["company", "status"], name="po_company_status_idx"),
            django_db_models.Index(fields=["company_provider", "status"], name="po_company_provider_status_idx"),
        ]
        constraints = [
            # At most one open cart (DRAFT=1 or QUOTED=2, see src.enums.PurchaseOrderStatus)
            # per distributor connection *and order account* at a time — "Add to PO" always
            # finds-or-creates this row. A quote-failed cart (status=FAILED) isn't covered here
            # since that requires a subquery a partial index can't express; the application layer
            # (_get_or_create_draft) is responsible for finding and reusing that row too.
            #
            # Split into two constraints (rather than one over
            # ["company", "company_provider", "order_account"]) because Postgres treats every
            # NULL as distinct from every other NULL in a unique index — a single constraint
            # including the nullable order_account column would let two open drafts for the
            # *default* account (order_account=None, still the overwhelming majority of
            # connections) coexist silently, which is exactly the bug this constraint exists to
            # prevent. The two conditions partition every PO into exactly one of them.
            django_db_models.UniqueConstraint(
                fields=["company", "company_provider"],
                condition=django_db_models.Q(status__in=[1, 2], order_account__isnull=True),
                name="po_one_open_draft_per_company_provider",
            ),
            django_db_models.UniqueConstraint(
                fields=["company", "company_provider", "order_account"],
                condition=django_db_models.Q(status__in=[1, 2], order_account__isnull=False),
                name="po_one_open_draft_per_company_provider_order_account",
            ),
        ]


class PurchaseOrderLineItem(django_db_models.Model):
    purchase_order = django_db_models.ForeignKey(
        PurchaseOrder, on_delete=django_db_models.CASCADE, related_name="line_items"
    )
    provider_part = django_db_models.ForeignKey(
        ProviderPart, on_delete=django_db_models.PROTECT, related_name="po_line_items"
    )
    # Set only when this line was created by expanding a kit ProviderPart into its components
    # (see purchase_orders_services.add_cart_item / ProviderPartKitComponent) -- the kit itself
    # is never added as its own line item, so this is purely traceability: lets the cart UI
    # group/label "these N lines came from kit X" instead of showing unrelated-looking parts.
    kit_source_provider_part = django_db_models.ForeignKey(
        ProviderPart,
        on_delete=django_db_models.SET_NULL,
        null=True,
        blank=True,
        related_name="expanded_kit_line_items",
    )

    quantity = django_db_models.PositiveIntegerField()

    # Frozen at add-to-cart time from ProviderPartCompanyPricing, since that changes over time.
    unit_cost = django_db_models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    line_total = django_db_models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)

    status = django_db_models.PositiveSmallIntegerField()
    status_name = django_db_models.CharField(max_length=32)

    # Distributor-side per-line detail, filled in after quote/submit. A single line item can be
    # fulfilled from more than one distributor shipment/warehouse at quote time (e.g. Turn14
    # splitting a qty=4 request into 1@warehouse-59 + 2@warehouse-02 + 1 backordered@warehouse-01)
    # — these fields are the AGGREGATE across every shipment (summed confirmed/backordered/
    # not_orderable, earliest ESD, warehouse_code only when there's exactly one shipment). The
    # per-shipment breakdown itself lives in ``shipments`` below; these aggregates exist so
    # callers that don't care about the split (e.g. a simple "x of y available" badge) don't
    # have to compute it.
    distributor_line_status_code = django_db_models.CharField(max_length=64, null=True, blank=True)
    distributor_line_status_message = django_db_models.TextField(null=True, blank=True)
    quantity_confirmed = django_db_models.PositiveIntegerField(null=True, blank=True)
    # Genuinely backordered — will still ship (and be billed) once the distributor restocks;
    # distinct from quantity_not_orderable below. Only ever set from shipment-splits whose
    # purchase_order_jobs._shipment_status() is "backordered" (Keystone ShipFlag B, Turn14
    # out_of_stock) — "not_orderable"/"transfer" splits are excluded and counted separately, so
    # this field can't silently mix "will ship later" with "will never ship" the way it used to.
    quantity_backordered = django_db_models.PositiveIntegerField(null=True, blank=True)
    # Cancelled outright — Keystone ShipFlag X ("not orderable"), never ships and is never
    # billed (see keystone.py/_billable_quantity). Previously lumped into quantity_backordered,
    # which made a fully-cancelled quantity look like it would eventually arrive.
    quantity_not_orderable = django_db_models.PositiveIntegerField(null=True, blank=True)
    manufacturer_esd = django_db_models.DateField(null=True, blank=True)
    warehouse_code = django_db_models.CharField(max_length=64, null=True, blank=True)

    # Lightweight references into PurchaseOrder.shipments (the full, deduplicated shipment
    # records — items + priced ship_options — now live there once, not copied per line item):
    # [{shipment_id, status, quantity_confirmed, quantity_backordered, manufacturer_esd}].
    # ``status`` is copied in from that same PurchaseOrder.shipments entry (rather than making
    # the FE cross-reference by shipment_id) so a given split's quantity_backordered can be read
    # correctly on its own — it means "cancelled, will never ship" when status is
    # "not_orderable", vs. "will ship once restocked" when status is "backordered". Almost
    # always a single-entry list; more than one entry means the distributor is fulfilling this
    # line from multiple shipments — see the aggregate fields above for the common case.
    shipments = django_db_models.JSONField(null=True, blank=True, encoder=DjangoJSONEncoder)

    # Distributor-applied discounts on this line from the last quote (e.g. Turn14's per-item
    # pricing promos): [{description, amount}]. Already netted into the distributor's own price
    # in quote_raw_response; subtracted from distributor_line_total below to produce
    # distributor_net_line_total, which IS what feeds po.subtotal (see
    # purchase_order_jobs.compute_totals) — never fed back into unit_cost/line_total
    # themselves, which stay our frozen catalog price regardless. Empty/null for distributors
    # that don't have this concept.
    promotions = django_db_models.JSONField(null=True, blank=True, encoder=DjangoJSONEncoder)

    # Distributor's own quoted pricing for this item from the last quote (gross, before
    # promotions). distributor_line_total is the sum of every shipment-split's line total for
    # this item; distributor_net_line_total is that total minus the promotions above — THIS is
    # the authoritative price fed into po.subtotal once a quote has returned one (see
    # purchase_order_jobs.compute_totals/effective_line_total), since a quote is exactly the
    # distributor telling us what they will actually charge. Falling back to unit_cost/line_total
    # (our frozen catalog price, left untouched by these fields) only happens for distributors
    # whose adapter doesn't return per-item pricing at quote time yet.
    distributor_unit_price = django_db_models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    distributor_line_total = django_db_models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    distributor_net_line_total = django_db_models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)

    # Whether the distributor flagged this item as subject to a California Prop 65 warning on
    # the last quote (e.g. Turn14's top-level "prop_65" array) — display-only.
    is_prop_65 = django_db_models.BooleanField(default=False)

    # Which distributor-side order slice this line ended up on. Nullable because a PO can
    # fan out across several distributor orders (Meyer's Orders array, Keystone/Turn14
    # multi-warehouse) — set once submit_order() resolves it.
    distributor_order = django_db_models.ForeignKey(
        "PurchaseOrderDistributorOrder",
        on_delete=django_db_models.SET_NULL,
        related_name="line_items",
        null=True,
        blank=True,
    )

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "purchase_order_line_items"
        unique_together = [["purchase_order", "provider_part"]]
        indexes = [
            django_db_models.Index(fields=["purchase_order"], name="po_line_items_po_idx"),
        ]


class PurchaseOrderDistributorOrder(django_db_models.Model):
    """
    One distributor-side order/shipment slice for a PurchaseOrder. A single PurchaseOrder
    can map to several of these (Meyer's Orders array of genuinely separate order numbers;
    Keystone/Turn14's multi-warehouse fan-out within one order).
    """

    purchase_order = django_db_models.ForeignKey(
        PurchaseOrder, on_delete=django_db_models.CASCADE, related_name="distributor_orders"
    )

    distributor_order_number = django_db_models.CharField(max_length=128)
    warehouse_code = django_db_models.CharField(max_length=64, null=True, blank=True)

    # Captured up front at submit time from the distributor's raw response (see
    # turn_14.extract_po_reference) so later refreshes (confirmed_purchase_order_sync) don't
    # need to re-derive it from a raw_response shape that can change between distributor
    # endpoints.
    po_number = django_db_models.CharField(max_length=128, null=True, blank=True)

    # The distributor's own Turn14-internal order id (attributes.order_number from the
    # orders/po/{ref} lookup, e.g. "16958747") -- a different, unrelated numbering from both
    # distributor_order_number (our website_order_number match key) and po_number. Only
    # populated once a refresh (confirmed_purchase_order_sync) has actually looked this order up.
    distributor_internal_order_number = django_db_models.CharField(max_length=128, null=True, blank=True)

    status = django_db_models.PositiveSmallIntegerField()
    status_name = django_db_models.CharField(max_length=32)

    # The distributor's own raw order status (see src.enums.DistributorOrderRawStatus) --
    # distinct from status/status_name above, which is our internal fulfillment lifecycle. Only
    # populated once a refresh has looked this order up; currently only Turn14 is translated.
    distributor_order_status = django_db_models.PositiveSmallIntegerField(null=True, blank=True)
    distributor_order_status_name = django_db_models.CharField(max_length=16, null=True, blank=True)

    tracking_numbers = django_db_models.JSONField(default=list, blank=True, encoder=DjangoJSONEncoder)
    carrier = django_db_models.CharField(max_length=64, null=True, blank=True)

    # Populated only where the distributor's status/tracking response actually says so (see
    # base.DistributorOrderStatus) — null for distributors/status-checks that don't carry this.
    ship_date = django_db_models.DateField(null=True, blank=True)
    estimated_delivery_date = django_db_models.DateField(null=True, blank=True)
    delivery_status = django_db_models.CharField(max_length=32, null=True, blank=True)

    raw_response = django_db_models.JSONField(null=True, blank=True, encoder=DjangoJSONEncoder)

    # A human-readable, one-row-per-line-item distillation of raw_response's full transaction
    # history (see keystone.decode_and_merge_order_history) -- e.g. Keystone's GetOrderHistory
    # returns one row per (line item, status transition) pair, so a 2-line PO that's reached
    # INVOICE has 10 raw rows; this collapses each line item's rows down to a single merged
    # entry carrying its current status/timestamp plus every other field's most recent non-null
    # value across all of that line item's stages (e.g. tracking_number, only ever populated on
    # the PACKAGE-stage row, still shows up here even though the final/INVOICE-stage row is what
    # supplies status/invoice_number). Only populated once a refresh has looked this order up;
    # currently only Keystone is decoded this way.
    processed_order = django_db_models.JSONField(null=True, blank=True, encoder=DjangoJSONEncoder)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "purchase_order_distributor_orders"
        unique_together = [["purchase_order", "distributor_order_number"]]


class PurchaseOrderInvoice(django_db_models.Model):
    """
    A distributor-issued invoice for (part of) a PurchaseOrder — created once items actually
    ship, not at order-placement time (see base.DistributorInvoice), so a single PO commonly
    accumulates more than one of these over its lifetime (e.g. an immediate shipment plus a
    later backorder release — confirmed against Turn14's own invoice dashboard, which lists
    multiple invoice numbers under the same P.O. #). Fetched during the same status-check job
    that already polls order status (see purchase_order_jobs._run_status_check), for
    distributors where supports_invoices() is True.
    """

    purchase_order = django_db_models.ForeignKey(
        PurchaseOrder, on_delete=django_db_models.CASCADE, related_name="invoices"
    )

    invoice_number = django_db_models.CharField(max_length=128)
    invoice_date = django_db_models.DateField(null=True, blank=True)
    # The distributor's own order id this invoice was billed against (Turn14's relationships[].
    # order.order_id) — informational only, not a FK to PurchaseOrderDistributorOrder: nothing
    # here depends on that row already existing, or on ids lining up cleanly across the two.
    distributor_order_number = django_db_models.CharField(max_length=128, null=True, blank=True)
    website_order_number = django_db_models.CharField(max_length=128, null=True, blank=True)

    total_price = django_db_models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    freight = django_db_models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    discount_amount = django_db_models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    paid_amount = django_db_models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    amount_due = django_db_models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)

    # [{ship_method, tracking_number}] — one entry per package; an invoice commonly ships as
    # more than one package/tracking number.
    tracking = django_db_models.JSONField(default=list, blank=True, encoder=DjangoJSONEncoder)
    # [{part_number, description, quantity, unit_price, total_price, warehouse_code}] — see
    # base.InvoiceLineItem. Empty for adapters whose invoice data is header-only (Keystone's
    # synthesized invoice) rather than itemized.
    line_items = django_db_models.JSONField(default=list, blank=True, encoder=DjangoJSONEncoder)
    comments = django_db_models.TextField(null=True, blank=True)

    raw_response = django_db_models.JSONField(null=True, blank=True, encoder=DjangoJSONEncoder)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "purchase_order_invoices"
        unique_together = [["purchase_order", "invoice_number"]]


class PurchaseOrderSubmissionAttempt(django_db_models.Model):
    """
    Audit log of every quote/submit/status-check/cancel call made to a distributor's order
    API for a PurchaseOrder — one row per attempt (success or failure), not a mutable field,
    since submission is retried and each attempt's raw payload matters for diagnosing
    distributor rejections. Mirrors NotificationEmailLog's one-row-per-event style.
    """

    purchase_order = django_db_models.ForeignKey(
        PurchaseOrder, on_delete=django_db_models.CASCADE, related_name="submission_attempts"
    )
    operation = django_db_models.PositiveSmallIntegerField()
    operation_name = django_db_models.CharField(max_length=32)
    success = django_db_models.BooleanField()

    # Credentials must be redacted before storing here.
    request_payload = django_db_models.JSONField(null=True, blank=True, encoder=DjangoJSONEncoder)
    response_payload = django_db_models.JSONField(null=True, blank=True, encoder=DjangoJSONEncoder)
    error_message = django_db_models.TextField(null=True, blank=True)
    duration_ms = django_db_models.IntegerField(null=True, blank=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "purchase_order_submission_attempts"


class PurchaseOrderJob(django_db_models.Model):
    """
    Queue row processed by a cron management command, same shape as
    IntegrationPricingSyncJob but scoped to a single PurchaseOrder + operation
    (see src.enums.PurchaseOrderOperation). Submission is bounded by attempt_count/
    max_attempts since distributor order APIs can rate-limit (e.g. Meyer's "try again in
    15 minutes" response) and must never be retried unboundedly.
    """

    purchase_order = django_db_models.ForeignKey(PurchaseOrder, on_delete=django_db_models.CASCADE, related_name="jobs")
    operation = django_db_models.PositiveSmallIntegerField()
    operation_name = django_db_models.CharField(max_length=32)

    status = django_db_models.PositiveSmallIntegerField()
    status_name = django_db_models.CharField(max_length=32)
    message = django_db_models.TextField(null=True, blank=True)
    error_message = django_db_models.TextField(null=True, blank=True)

    attempt_count = django_db_models.PositiveSmallIntegerField(default=0)
    max_attempts = django_db_models.PositiveSmallIntegerField(default=3)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)
    started_at = django_db_models.DateTimeField(null=True, blank=True)
    completed_at = django_db_models.DateTimeField(null=True, blank=True)

    class Meta:
        db_table = "purchase_order_jobs"
        ordering = ["id"]


# ---------------------------------------------------------------------------
# Motor State Distributing (kind=MOTOR_STATE_DISTRIBUTING) raw feed tables.
#
# Motor State's API has no bulk product export. Raw ingest is built from three
# endpoints (see src.integrations.services.motorstate):
#   * GET /api/Brands                    -> MotorStateBrand   (global catalog)
#   * GET /api/ProductAvailabilityChange -> MotorStateAvailability (per-company
#       stock/status feed; also the part-number spine, since a per-brand pass
#       with fromDateTime=epoch is the only way to enumerate every part number)
#   * GET /api/Product (<=15 part numbers) -> MotorStateProduct (per-company
#       detail + account pricing)
# These are deliberately thin raw mirrors; nothing here is wired into the
# master-parts layer yet.
# ---------------------------------------------------------------------------
class MotorStateBrand(django_db_models.Model):
    """A Motor State brand from GET /api/Brands. Global — brand catalog is not
    account-specific. ``code`` is Motor State's brand code (e.g. "AAA"), used as
    the ``brand`` filter on the availability endpoint."""

    code = django_db_models.CharField(max_length=32)
    name = django_db_models.CharField(max_length=255, null=True)
    offered = django_db_models.BooleanField(default=False)
    is_inventory_available = django_db_models.BooleanField(default=False)
    # Full raw brand object (description, categories, resources, ...) kept verbatim
    # so downstream work can pull fields without another sync.
    data = django_db_models.JSONField(null=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "motorstate_brands"
        unique_together = ["code"]


class MotorStateAvailability(django_db_models.Model):
    """
    One row per Motor State part number from GET /api/ProductAvailabilityChange. Doubles as the
    catalog spine (initial per-brand epoch pass) and the live stock/status feed (periodic
    fromDateTime=watermark polling).

    Distributor-wide, not per company: stock and status are the same whichever dealer's API key
    asks, so this is maintained once from the primary connection (like every other provider's
    shared catalog/inventory tables) and read by every company. Only price is account-specific
    -- see MotorStateCompanyPricing.

    ``source_updated_on`` is Motor State's own UpdatedOn timestamp and is the high-water mark
    for incremental polling. ``brand_code`` is the brand the row was fetched under during the
    spine pass (null for rows first seen via an unfiltered incremental poll).
    """

    part_number = django_db_models.CharField(max_length=128, unique=True)
    brand_code = django_db_models.CharField(max_length=32, null=True)
    # Motor State StatusType: S=stocking, O=order-as-needed, X=discontinued (raw, as returned).
    status_type = django_db_models.CharField(max_length=8, null=True)
    quantity_available = django_db_models.IntegerField(null=True)
    # Motor State "UpdatedOn" for this part; the incremental-poll high-water mark.
    source_updated_on = django_db_models.DateTimeField(null=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "motorstate_availability"
        indexes = [
            django_db_models.Index(fields=["source_updated_on"], name="ms_avail_updated_on_idx"),
            django_db_models.Index(fields=["brand_code"], name="ms_avail_brand_code_idx"),
        ]


class MotorStateProduct(django_db_models.Model):
    """One row per (company, part number) from GET /api/Product — catalog detail
    plus account-specific pricing (Motor State returns both in one payload).
    Per-company because prices are tied to the account behind the API key.
    ``found`` mirrors the API's per-part Found flag; unfound part numbers are
    still recorded so a later pass need not re-query them blindly."""

    part_number = django_db_models.CharField(max_length=128, unique=True)
    # Motor State's /api/Product does not return a brand, so this FK is carried over from the
    # MotorStateAvailability spine (the brand the part was fetched under). Null when the part
    # has no matching availability row. brand_code mirrors brand.code for join-free filtering.
    brand = django_db_models.ForeignKey(
        MotorStateBrand, on_delete=django_db_models.SET_NULL, null=True, related_name="products"
    )
    brand_code = django_db_models.CharField(max_length=32, null=True)
    found = django_db_models.BooleanField(default=False)

    vendor_part_number = django_db_models.CharField(max_length=128, null=True)
    supersede_part_number = django_db_models.CharField(max_length=128, null=True)
    short_description = django_db_models.TextField(null=True)
    # Motor State numeric Status code (raw).
    status = django_db_models.IntegerField(null=True)
    is_stocking = django_db_models.BooleanField(default=False)
    quantity = django_db_models.IntegerField(null=True)

    # Ordering capabilities describe the part itself, so they stay on the catalog row; the
    # per-account fees they imply live on MotorStateCompanyPricing.
    can_special_order = django_db_models.BooleanField(default=False)
    can_drop_ship = django_db_models.BooleanField(default=False)
    can_regular_back_order = django_db_models.BooleanField(default=False)

    # Full raw Product object kept verbatim (notes, duty/tariff, any fields not columned above).
    data = django_db_models.JSONField(null=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "motorstate_products"
        indexes = [
            django_db_models.Index(fields=["brand"], name="ms_products_brand_idx"),
        ]


class MotorStateCompanyPricing(django_db_models.Model):
    """
    Per-company Motor State pricing for a catalog row (MotorStateProduct). Catalog/non-price
    fields live on MotorStateProduct; prices are stored per company so
    ProviderPartCompanyPricing sync keys off (part, company) like every other provider.

    Motor State returns catalog and account pricing in the same /api/Product payload (there is
    no price-only endpoint), so both tables are written by the same hydrate pass —
    unlike distributors whose catalog and pricing arrive on separate feeds.

    ``customer_price`` is this account's actual buy price; ``base_price`` is Motor State's
    undiscounted wholesale; ``list_price`` is MSRP.
    """

    product = django_db_models.ForeignKey(
        MotorStateProduct, on_delete=django_db_models.CASCADE, related_name="company_pricing"
    )
    company = django_db_models.ForeignKey(
        Company, on_delete=django_db_models.CASCADE, related_name="motorstate_company_pricing"
    )

    customer_price = django_db_models.DecimalField(max_digits=12, decimal_places=2, null=True)
    customer_price_non_promotional = django_db_models.DecimalField(max_digits=12, decimal_places=2, null=True)
    base_price = django_db_models.DecimalField(max_digits=12, decimal_places=2, null=True)
    list_price = django_db_models.DecimalField(max_digits=12, decimal_places=2, null=True)
    map_price = django_db_models.DecimalField(max_digits=12, decimal_places=2, null=True)
    is_map_restricted = django_db_models.BooleanField(default=False)

    special_order_charge = django_db_models.DecimalField(max_digits=12, decimal_places=2, null=True)
    drop_ship_charge = django_db_models.DecimalField(max_digits=12, decimal_places=2, null=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "motorstate_company_pricing"
        unique_together = ["product", "company"]


# ---------------------------------------------------------------------------
# Western Power Sports (kind=WESTERN_POWER_SPORTS) raw feed tables.
#
# WPS exposes a proper cursor-paginated JSON:API (page[size] + page[cursor], sparse
# fieldsets, filters, includes), so the whole catalog comes down in ~13 calls -- no
# per-part hydrate like Motor State. Catalog, brands and inventory are distributor-wide
# and maintained once from the primary connection; only price is per company (each
# connection reads /items with its own bearer token).
# ---------------------------------------------------------------------------
class WpsBrand(django_db_models.Model):
    """A brand from GET /brands. Global -- the brand catalog is not account-specific."""

    external_id = django_db_models.CharField(max_length=32)
    name = django_db_models.CharField(max_length=255, null=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "wps_brands"
        unique_together = ["external_id"]


class WpsWarehouse(django_db_models.Model):
    """
    WPS warehouse from GET /warehouses. ``db2_key`` (ID/CA/PA/IN/TX/GA/PA2) is the prefix of the
    matching per-warehouse column on WpsInventory, so this table is what turns a bare
    ``pa2_warehouse`` figure into "Jessup" for display.
    """

    external_id = django_db_models.CharField(max_length=32)
    db2_key = django_db_models.CharField(max_length=16, null=True)
    name = django_db_models.CharField(max_length=255, null=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "wps_warehouses"
        unique_together = ["external_id"]


class WpsProduct(django_db_models.Model):
    """
    Parent product from GET /products -- the marketing record a group of item variants (SKUs)
    hangs off. This is where the long ``description`` lives; individual items carry only a short
    ``name``, so MasterPart descriptions come from here.
    """

    external_id = django_db_models.CharField(max_length=32)
    name = django_db_models.CharField(max_length=255, null=True)
    description = django_db_models.TextField(null=True)
    data = django_db_models.JSONField(null=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "wps_products"
        unique_together = ["external_id"]


class WpsItem(django_db_models.Model):
    """
    One SKU from GET /items -- WPS's own words: "the simplest form of a Product ... one
    individual variant (specific configuration) or SKU". Distributor-wide catalog; prices
    live per company on WpsCompanyPricing.

    ``sku`` is WPS's own part number (e.g. "020-00010"); ``supplier_product_id`` is the
    manufacturer's part number (e.g. "TC-M6M8") and is what MasterPart keys on, since the
    WPS SKU would never dedupe against another distributor's catalog.
    """

    external_id = django_db_models.CharField(max_length=32)  # WPS item id
    brand = django_db_models.ForeignKey(WpsBrand, on_delete=django_db_models.SET_NULL, null=True, related_name="items")
    sku = django_db_models.CharField(max_length=128, null=True)
    name = django_db_models.CharField(max_length=255, null=True)
    supplier_product_id = django_db_models.CharField(max_length=128, null=True)
    upc = django_db_models.CharField(max_length=64, null=True)
    superseded_sku = django_db_models.CharField(max_length=128, null=True)

    # WPS status: STK stocking, NLA no longer available, NEW, DIR direct-ship, CLO closeout,
    # NA not available, PRE pre-release, SPEC special order, DSC discontinued.
    status = django_db_models.CharField(max_length=16, null=True)
    product_type = django_db_models.CharField(max_length=255, null=True)

    length = django_db_models.FloatField(null=True)
    width = django_db_models.FloatField(null=True)
    height = django_db_models.FloatField(null=True)
    weight = django_db_models.FloatField(null=True)

    has_map_policy = django_db_models.BooleanField(default=False)
    drop_ship_eligible = django_db_models.BooleanField(default=False)
    drop_ship_fee = django_db_models.CharField(max_length=32, null=True)

    # Parent product (GET /products) -- carries the long marketing description.
    product = django_db_models.ForeignKey(
        WpsProduct, on_delete=django_db_models.SET_NULL, null=True, related_name="items"
    )
    # Enrichment pulled from ?include= on /items, since neither joins back to an item id
    # from its own collection endpoint.
    #   image_url  -- first image, assembled as https://{domain}{path}{filename}
    #   images     -- every image for the item, same assembled shape
    #   taxonomy   -- taxonomyterm names (WPS's category tree) for this item
    image_url = django_db_models.TextField(null=True)
    images = django_db_models.JSONField(null=True)
    taxonomy = django_db_models.JSONField(null=True)

    # WPS's own updated_at for the row; high-water mark for filter[updated_at][gt] polling.
    source_updated_at = django_db_models.DateTimeField(null=True)
    # Full raw item object (prop 65, carb, country/product/uom ids, anything not columned).
    data = django_db_models.JSONField(null=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "wps_items"
        unique_together = ["external_id"]
        indexes = [
            django_db_models.Index(fields=["brand"], name="wps_items_brand_idx"),
            django_db_models.Index(fields=["source_updated_at"], name="wps_items_updated_idx"),
            django_db_models.Index(fields=["sku"], name="wps_items_sku_idx"),
        ]


class WpsInventory(django_db_models.Model):
    """
    Per-warehouse stock from GET /inventory, distributor-wide.

    WPS caps each warehouse figure at 25 ("If we have 25 or more of a particular item, we will
    just show 25 ... so as to avoid disclosing our actual inventory levels"), so ``total`` is a
    floor, not a true count -- fine for availability, wrong for anything that needs real depth.
    """

    item = django_db_models.OneToOneField(WpsItem, on_delete=django_db_models.CASCADE, related_name="inventory")
    external_id = django_db_models.CharField(max_length=32, null=True)  # WPS inventory row id
    sku = django_db_models.CharField(max_length=128, null=True)

    ca_warehouse = django_db_models.IntegerField(default=0)  # Fresno, CA
    ga_warehouse = django_db_models.IntegerField(default=0)  # Midway, GA
    id_warehouse = django_db_models.IntegerField(default=0)  # Boise, ID
    in_warehouse = django_db_models.IntegerField(default=0)  # Ashley, IN
    pa_warehouse = django_db_models.IntegerField(default=0)  # Elizabethtown, PA
    pa2_warehouse = django_db_models.IntegerField(default=0)  # Jessup, PA
    tx_warehouse = django_db_models.IntegerField(default=0)  # Midlothian, TX
    total = django_db_models.IntegerField(default=0)

    source_updated_at = django_db_models.DateTimeField(null=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "wps_inventory"


class WpsCompanyPricing(django_db_models.Model):
    """
    Per-company WPS pricing for a catalog row (WpsItem). WPS has no pricing endpoint -- prices
    come back on /items -- so each connection re-reads /items with its own bearer token and only
    the price columns land here; catalog and inventory stay shared. That re-read is ~13 calls,
    so unlike Motor State this needs no throttling.

    ``dealer_price`` is WPS's standard_dealer_price (cost), ``list_price`` is MSRP, and
    ``map_price`` is mapp_price (meaningful only when has_map_policy is set; WPS returns "0.00"
    for plenty of rows that do carry a policy).
    """

    item = django_db_models.ForeignKey(WpsItem, on_delete=django_db_models.CASCADE, related_name="company_pricing")
    company = django_db_models.ForeignKey(
        Company, on_delete=django_db_models.CASCADE, related_name="wps_company_pricing"
    )

    list_price = django_db_models.DecimalField(max_digits=12, decimal_places=2, null=True)
    dealer_price = django_db_models.DecimalField(max_digits=12, decimal_places=2, null=True)
    map_price = django_db_models.DecimalField(max_digits=12, decimal_places=2, null=True)
    has_map_policy = django_db_models.BooleanField(default=False)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "wps_company_pricing"
        unique_together = ["item", "company"]


class BrandWpsBrandMapping(django_db_models.Model):
    """Maps our Brands to WpsBrand (for master parts sync). WPS's /brands carries no AAIA code,
    so resolution is name-based only (see wps.sync_unmapped_wps_brands_to_brands)."""

    brand = django_db_models.ForeignKey(Brands, on_delete=django_db_models.CASCADE, related_name="wps_brand_mappings")
    wps_brand = django_db_models.ForeignKey(WpsBrand, on_delete=django_db_models.CASCADE, related_name="brand_mappings")

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "brand_wps_brand_mapping"
        unique_together = ["brand", "wps_brand"]


class BrandMotorStateBrandMapping(django_db_models.Model):
    """Maps our Brands to MotorStateBrand (for master parts sync). Motor State's /api/Brands
    carries no AAIA code, so resolution is name-based only (see
    motorstate.sync_unmapped_motorstate_brands_to_brands)."""

    brand = django_db_models.ForeignKey(
        Brands, on_delete=django_db_models.CASCADE, related_name="motorstate_brand_mappings"
    )
    motorstate_brand = django_db_models.ForeignKey(
        MotorStateBrand, on_delete=django_db_models.CASCADE, related_name="brand_mappings"
    )

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "brand_motorstate_brand_mapping"
        unique_together = ["brand", "motorstate_brand"]


# ---------------------------------------------------------------------------
# Helmet House (kind=HELMHOUSE) raw feed tables.
#
# Helmet House publishes their whole catalog as flat files on plain FTP, rewritten once a
# day. One file (masterv.csv, ~41k rows) carries everything: brand, both part numbers,
# prices, per-warehouse stock, dimensions, UPC and status -- so unlike WPS or Motor State
# there is no second call to make and no incremental cursor to keep. See
# src.integrations.clients.helmet_house for the file layout.
#
# The login is shared rather than issued per dealer, so in practice every connected company
# reads the same file and sees the same dealer cost. Price is still stored per company
# (HelmetHouseCompanyPricing) so the master pricing layer keys the same way as every other
# provider, and so per-dealer logins would need no schema change.
# ---------------------------------------------------------------------------
class HelmetHouseBrand(django_db_models.Model):
    """
    One brand from the feed's ``Brand`` column. ``external_id``/``name`` are the normalised,
    uppercase name we resolve against Brands; ``source_name`` keeps Helmet House's own spelling
    so a feed value can always be traced back.

    Helmet House spells a few brands in ways that would never match on their own -- "T/M" is
    Tourmaster, "100 %" is 100% -- and files shields, decals and luggage under the non-brands
    "MISC" and "BAGS". Both are handled by src.constants.HELMET_HOUSE_BRAND_ALIASES, which
    folds the two buckets into a "HELMET HOUSE" house brand rather than creating literal
    MISC/BAGS brands in the catalog.
    """

    external_id = django_db_models.CharField(max_length=255)
    name = django_db_models.CharField(max_length=255)
    source_name = django_db_models.CharField(max_length=255, null=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "helmet_house_brands"
        unique_together = [["external_id"]]


class HelmetHousePart(django_db_models.Model):
    """
    One row of the Helmet House catalog: distributor-wide catalog data plus their two-warehouse
    stock. Prices live per company on HelmetHouseCompanyPricing.

    ``sku`` is Helmet House's own part number ("0810-1234-00"), unique across the whole feed.
    ``vendor_part_number`` is the manufacturer's number ("315012710-48" for an Alpinestars item)
    and is what MasterPart keys on where it is usable -- see
    master_parts._helmet_house_master_part_number for when it is not.

    ``photo_filename`` is deliberately just the filename, not a URL: the feed's own image host
    (ftp.helmethouse.com/Images11/) answers 404 for every file it names, so building a URL from
    it would put a dead link on every master part. Kept so images can be wired up the day a
    working host is available.
    """

    brand = django_db_models.ForeignKey(HelmetHouseBrand, on_delete=django_db_models.CASCADE, related_name="parts")
    sku = django_db_models.CharField(max_length=255)
    # Helmet House's own part number with the dashes removed ("01-003" -> "01003"). Their
    # "Alt Part#" column; not a separate part, and not a manufacturer number.
    alt_part_number = django_db_models.CharField(max_length=255, null=True)
    vendor_part_number = django_db_models.CharField(max_length=255, null=True)

    description = django_db_models.CharField(max_length=512, null=True)
    long_description = django_db_models.TextField(null=True)
    upc = django_db_models.CharField(max_length=64, null=True)

    # Feed values: OK, On Sale, Out of Stock, Discontinued.
    status = django_db_models.CharField(max_length=32, null=True)
    category = django_db_models.CharField(max_length=255, null=True)
    product_class = django_db_models.CharField(max_length=255, null=True)
    size = django_db_models.CharField(max_length=64, null=True)
    color = django_db_models.CharField(max_length=128, null=True)
    model = django_db_models.CharField(max_length=255, null=True)
    country_of_origin = django_db_models.CharField(max_length=16, null=True)

    weight = django_db_models.FloatField(null=True)
    length = django_db_models.FloatField(null=True)
    width = django_db_models.FloatField(null=True)
    depth = django_db_models.FloatField(null=True)

    photo_filename = django_db_models.CharField(max_length=255, null=True)
    alt_photo_filenames = django_db_models.JSONField(null=True)

    # Helmet House runs two distribution centres and reports each separately; total_qty is their
    # own TTL Qty column, not a sum we compute, so it stays authoritative if they ever add a third.
    west_qty = django_db_models.IntegerField(default=0)
    east_qty = django_db_models.IntegerField(default=0)
    total_qty = django_db_models.IntegerField(default=0)

    has_map_policy = django_db_models.BooleanField(default=False)

    # Which file this row was parsed from (masterv.csv normally, master.csv on a fallback), so a
    # row missing its vendor part number can be explained without re-reading the feed.
    source_filename = django_db_models.CharField(max_length=64, null=True)
    raw_data = django_db_models.JSONField(null=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "helmet_house_parts"
        unique_together = [["brand", "sku"]]
        indexes = [
            django_db_models.Index(fields=["sku"], name="hh_parts_sku_idx"),
            django_db_models.Index(fields=["status"], name="hh_parts_status_idx"),
        ]


class HelmetHouseCompanyPricing(django_db_models.Model):
    """
    Per-company Helmet House pricing for a catalog row (HelmetHousePart).

    ``dealer_price`` is the feed's Dealer column (cost), ``retail_price`` its Retail (MSRP) and
    ``map_price`` its MAPP Price. MAP is only meaningful where ``has_map_policy`` is set: roughly
    a third of rows carry no policy and leave the price blank, and storing that as 0.00 would
    floor the price downstream.
    """

    part = django_db_models.ForeignKey(
        HelmetHousePart, on_delete=django_db_models.CASCADE, related_name="company_pricing"
    )
    company = django_db_models.ForeignKey(
        Company, on_delete=django_db_models.CASCADE, related_name="helmet_house_company_pricing"
    )

    dealer_price = django_db_models.DecimalField(max_digits=12, decimal_places=2, null=True)
    retail_price = django_db_models.DecimalField(max_digits=12, decimal_places=2, null=True)
    map_price = django_db_models.DecimalField(max_digits=12, decimal_places=2, null=True)
    has_map_policy = django_db_models.BooleanField(default=False)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "helmet_house_company_pricing"
        unique_together = ["part", "company"]


class BrandHelmetHouseBrandMapping(django_db_models.Model):
    """Maps our Brands to HelmetHouseBrand (for master parts sync). The feed carries no AAIA
    code, so resolution is name-based only (see
    helmet_house.sync_unmapped_helmet_house_brands_to_brands)."""

    brand = django_db_models.ForeignKey(
        Brands, on_delete=django_db_models.CASCADE, related_name="helmet_house_brand_mappings"
    )
    helmet_house_brand = django_db_models.ForeignKey(
        HelmetHouseBrand, on_delete=django_db_models.CASCADE, related_name="brand_mappings"
    )

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "brand_helmet_house_brand_mapping"
        unique_together = ["brand", "helmet_house_brand"]


class EliteWheelBrand(django_db_models.Model):
    """
    One manufacturer from Elite Wheel & Tire's inventory workbook. Elite's feed is split in two --
    a worksheet per wheel manufacturer plus a single ``Tires`` sheet covering every tire
    manufacturer -- and the two halves land in separate part tables (EliteWheelPartWheel /
    EliteWheelPartTire), so ``product_type`` records which half a brand row belongs to.
    ``external_id`` is ``"<product_type>:<NAME>"`` (e.g. ``"WHEEL:AZARA"``) rather than the bare
    name: the same manufacturer can legitimately appear on both sides of the catalog, and each
    side needs its own brand row to key its own parts.
    """

    PRODUCT_TYPE_WHEEL = "WHEEL"
    PRODUCT_TYPE_TIRE = "TIRE"

    external_id = django_db_models.CharField(max_length=255)
    name = django_db_models.CharField(max_length=255)
    product_type = django_db_models.CharField(max_length=16)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "elitewheels_brands"
        unique_together = [["external_id"]]


class EliteWheelPartWheel(django_db_models.Model):
    """
    A wheel from Elite's workbook (catalog + distributor-wide per-location stock; not per-company
    -- see EliteWheelWheelCompanyPricing for price). Wheel-spec columns are CharField like
    VossenPart's and WheelProsPart's equivalents because the sheet's own formats vary: a staggered
    fitment ships as ``"22x9 / 22x10"`` with offset ``"32 / 38"``, and the bolt-pattern columns
    carry placeholders such as ``"Blank 5x/6x"`` for undrilled blanks.

    ``qty_*`` are Elite's four warehouses (see ``constants.ELITE_WHEEL_QTY_FIELD_TO_LOCATION_LABEL``);
    ``location_availability`` keeps the raw per-location dict verbatim so a warehouse Elite adds
    later is still captured (and counted in ``total_available``) before it gets a column here.
    """

    brand = django_db_models.ForeignKey(
        EliteWheelBrand,
        on_delete=django_db_models.CASCADE,
        related_name="wheel_parts",
    )
    part_number = django_db_models.CharField(max_length=255)
    # Model/finish block title the row sat under, e.g. "XF-211 Gloss Black & Milled".
    group_label = django_db_models.CharField(max_length=512, null=True, blank=True)
    size = django_db_models.CharField(max_length=128, null=True, blank=True)
    bolt_pattern_1 = django_db_models.CharField(max_length=128, null=True, blank=True)
    bolt_pattern_2 = django_db_models.CharField(max_length=128, null=True, blank=True)
    offset = django_db_models.CharField(max_length=64, null=True, blank=True)
    center_bore = django_db_models.CharField(max_length=64, null=True, blank=True)
    finish = django_db_models.CharField(max_length=255, null=True, blank=True)

    qty_tampa = django_db_models.IntegerField(null=True, blank=True)
    qty_atlanta = django_db_models.IntegerField(null=True, blank=True)
    qty_miami = django_db_models.IntegerField(null=True, blank=True)
    qty_decatur = django_db_models.IntegerField(null=True, blank=True)
    total_available = django_db_models.IntegerField(null=True, blank=True)
    location_availability = django_db_models.JSONField(null=True, blank=True)

    # Date from the workbook's "As of MM/DD/YYYY" banner, and the drop it was read from.
    as_of_date = django_db_models.DateField(null=True, blank=True)
    source_filename = django_db_models.CharField(max_length=255, null=True, blank=True)
    raw_data = django_db_models.JSONField(null=True, blank=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "elitewheels_part_wheels"
        unique_together = [["brand", "part_number"]]


class EliteWheelPartTire(django_db_models.Model):
    """
    A tire from the workbook's ``Tires`` sheet (catalog + distributor-wide per-location stock; not
    per-company -- see EliteWheelTireCompanyPricing for price). ``raw_size`` is the sheet's own
    packed size code (``"1856514"`` for 185/65R14) kept verbatim rather than expanded, and
    ``tire_diameter`` comes from the ``Inventory for Tire Diameter: <n>`` section the row sat under.
    """

    brand = django_db_models.ForeignKey(
        EliteWheelBrand,
        on_delete=django_db_models.CASCADE,
        related_name="tire_parts",
    )
    part_number = django_db_models.CharField(max_length=255)
    model = django_db_models.CharField(max_length=255, null=True, blank=True)
    raw_size = django_db_models.CharField(max_length=64, null=True, blank=True)
    tire_diameter = django_db_models.CharField(max_length=16, null=True, blank=True)

    qty_tampa = django_db_models.IntegerField(null=True, blank=True)
    qty_atlanta = django_db_models.IntegerField(null=True, blank=True)
    qty_miami = django_db_models.IntegerField(null=True, blank=True)
    qty_decatur = django_db_models.IntegerField(null=True, blank=True)
    total_available = django_db_models.IntegerField(null=True, blank=True)
    location_availability = django_db_models.JSONField(null=True, blank=True)

    as_of_date = django_db_models.DateField(null=True, blank=True)
    source_filename = django_db_models.CharField(max_length=255, null=True, blank=True)
    raw_data = django_db_models.JSONField(null=True, blank=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "elitewheels_part_tires"
        unique_together = [["brand", "part_number"]]


class EliteWheelWheelCompanyPricing(django_db_models.Model):
    """
    Per-company Elite pricing for a wheel (EliteWheelPartWheel). Catalog and stock live on the part
    and are shared; prices are per company so ProviderPartCompanyPricing sync keys off
    (part, company) like every other provider. Two pricing tables rather than one with two nullable
    FKs, mirroring the wheel/tire split of the part tables so each keeps a real FK and a
    (part, company) unique constraint.

    Elite's public inventory share carries no price columns at all, so these rows only appear once
    a dealer's own feed is connected -- see
    ``src.integrations.services.elite_wheel.sync_elite_wheel_company_pricing_for_company_provider``.
    """

    part = django_db_models.ForeignKey(
        EliteWheelPartWheel,
        on_delete=django_db_models.CASCADE,
        related_name="company_pricing",
    )
    company = django_db_models.ForeignKey(
        Company,
        on_delete=django_db_models.CASCADE,
        related_name="elite_wheel_wheel_company_pricing",
    )
    cost = django_db_models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    map = django_db_models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    retail_price = django_db_models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "elitewheels_wheel_company_pricing"
        unique_together = [["part", "company"]]


class EliteWheelTireCompanyPricing(django_db_models.Model):
    """Per-company Elite pricing for a tire (EliteWheelPartTire) -- see EliteWheelWheelCompanyPricing."""

    part = django_db_models.ForeignKey(
        EliteWheelPartTire,
        on_delete=django_db_models.CASCADE,
        related_name="company_pricing",
    )
    company = django_db_models.ForeignKey(
        Company,
        on_delete=django_db_models.CASCADE,
        related_name="elite_wheel_tire_company_pricing",
    )
    cost = django_db_models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    map = django_db_models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    retail_price = django_db_models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "elitewheels_tire_company_pricing"
        unique_together = [["part", "company"]]


class BrandEliteWheelBrandMapping(django_db_models.Model):
    """Maps our Brands to EliteWheelBrand (for master parts sync)."""

    brand = django_db_models.ForeignKey(
        Brands,
        on_delete=django_db_models.CASCADE,
        related_name="elite_wheel_brand_mappings",
    )
    elite_wheel_brand = django_db_models.ForeignKey(
        EliteWheelBrand,
        on_delete=django_db_models.CASCADE,
        related_name="brand_mappings",
    )

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "brand_elite_wheel_brand_mapping"
        unique_together = ["brand", "elite_wheel_brand"]


class TheWheelGroupBrand(django_db_models.Model):
    """
    One house brand from The Wheel Group's US mastersheet (Touren, Mayhem, ION Alloy, Cali
    Off-Road, Ridler, Dirty Life, Kraze, American Truxx, Mazzi, TuffStuff, ION Trailer).
    ``external_id`` is the uppercased brand name -- unlike EliteWheelBrand there is no wheel/tire
    split to disambiguate, TWG sells wheels only. ``aaia_code`` is the brand's dominant AAIA code
    from the sheet's per-row ``AAIA CODE`` column, used when a matching Brands row has to be
    created (same role as TurnFourteenBrand.aaia_code).
    """

    external_id = django_db_models.CharField(max_length=255)
    name = django_db_models.CharField(max_length=255)
    aaia_code = django_db_models.CharField(max_length=255, null=True, blank=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "thewheelgroup_brands"
        unique_together = [["external_id"]]


class TheWheelGroupPart(django_db_models.Model):
    """
    A wheel from TWG's ``US Data Mastersheet`` worksheet (catalog + list prices; per-company cost
    lives on TheWheelGroupCompanyPricing).

    Wheel-spec and dimension columns are CharField like VossenPart's and EliteWheelPartWheel's
    equivalents: the sheet's own formats vary (bolt patterns as ``"5-114.3"``, backspace as a full
    float expansion ``"4.751968503937007"``, blanks and Excel error cells), and coercing them would
    silently drop values rather than surface them. Only ``msrp``/``map_price`` are numeric, because
    they are the two fields the pricing layer actually computes on.

    There is no quantity column here: the mastersheet is a catalog and list-price sheet with no
    stock at all, so TWG parts get no ProviderPartInventory row until a real feed is delivered
    (see ``master_parts.sync_provider_details_from_the_wheel_group``).
    """

    brand = django_db_models.ForeignKey(
        TheWheelGroupBrand,
        on_delete=django_db_models.CASCADE,
        related_name="parts",
    )
    sku = django_db_models.CharField(max_length=255)
    aaia_code = django_db_models.CharField(max_length=255, null=True, blank=True)

    name = django_db_models.CharField(max_length=255, null=True, blank=True)
    style_number = django_db_models.CharField(max_length=64, null=True, blank=True)
    description = django_db_models.TextField(null=True, blank=True)
    short_description = django_db_models.TextField(null=True, blank=True)

    diameter = django_db_models.CharField(max_length=32, null=True, blank=True)
    wheel_width = django_db_models.CharField(max_length=32, null=True, blank=True)
    hub_bore = django_db_models.CharField(max_length=32, null=True, blank=True)
    bolt_pattern_1 = django_db_models.CharField(max_length=64, null=True, blank=True)
    bolt_pattern_2 = django_db_models.CharField(max_length=64, null=True, blank=True)
    # OFFSETNUM (millimetres) and OFFSET (TWG's LOW/MID/HIGH/DUALLY class) respectively.
    offset = django_db_models.CharField(max_length=32, null=True, blank=True)
    offset_class = django_db_models.CharField(max_length=32, null=True, blank=True)
    backspace = django_db_models.CharField(max_length=32, null=True, blank=True)
    wheel_lip_size = django_db_models.CharField(max_length=32, null=True, blank=True)
    load_rating = django_db_models.CharField(max_length=32, null=True, blank=True)
    color = django_db_models.CharField(max_length=64, null=True, blank=True)
    finish = django_db_models.CharField(max_length=128, null=True, blank=True)

    upc = django_db_models.CharField(max_length=64, null=True, blank=True)
    country_of_origin = django_db_models.CharField(max_length=64, null=True, blank=True)
    division = django_db_models.CharField(max_length=64, null=True, blank=True)
    group_code = django_db_models.CharField(max_length=64, null=True, blank=True)
    # Companion SKUs, not attributes: the center cap and the hardware that ship with this wheel.
    wheel_cap = django_db_models.CharField(max_length=64, null=True, blank=True)
    screw = django_db_models.CharField(max_length=64, null=True, blank=True)

    dually_wheel = django_db_models.BooleanField(null=True, blank=True)
    winter_approved = django_db_models.BooleanField(null=True, blank=True)
    tpms_compatible = django_db_models.BooleanField(null=True, blank=True)

    lugnut_open_closed = django_db_models.CharField(max_length=32, null=True, blank=True)
    lugnut_type_1 = django_db_models.CharField(max_length=32, null=True, blank=True)
    lugnut_type_2 = django_db_models.CharField(max_length=32, null=True, blank=True)
    lugseat_type = django_db_models.CharField(max_length=32, null=True, blank=True)

    structure_warranty = django_db_models.CharField(max_length=128, null=True, blank=True)
    finish_warranty = django_db_models.CharField(max_length=128, null=True, blank=True)
    beadlock_instructions_url = django_db_models.CharField(max_length=512, null=True, blank=True)

    # Shipping carton, not the wheel: WIDTH / HEIGHT / DEPTH and the two weight columns.
    box_width = django_db_models.CharField(max_length=32, null=True, blank=True)
    box_height = django_db_models.CharField(max_length=32, null=True, blank=True)
    box_depth = django_db_models.CharField(max_length=32, null=True, blank=True)
    product_weight = django_db_models.CharField(max_length=32, null=True, blank=True)
    ship_weight = django_db_models.CharField(max_length=32, null=True, blank=True)

    image_1 = django_db_models.CharField(max_length=1024, null=True, blank=True)
    image_2 = django_db_models.CharField(max_length=1024, null=True, blank=True)
    image_3 = django_db_models.CharField(max_length=1024, null=True, blank=True)
    image_4 = django_db_models.CharField(max_length=1024, null=True, blank=True)

    note = django_db_models.TextField(null=True, blank=True)
    comment = django_db_models.TextField(null=True, blank=True)
    bullet_points = django_db_models.TextField(null=True, blank=True)
    sales_description = django_db_models.TextField(null=True, blank=True)

    # Distributor-wide list prices from the sheet. ``map_price`` is null whenever MAP isn't
    # enforced for the SKU -- TWG writes 0 in that case, which is not a price.
    msrp = django_db_models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    map_price = django_db_models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    map_enforced = django_db_models.BooleanField(null=True, blank=True)

    source_filename = django_db_models.CharField(max_length=255, null=True, blank=True)
    raw_data = django_db_models.JSONField(null=True, blank=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "thewheelgroup_parts"
        unique_together = [["brand", "sku"]]


class TheWheelGroupCompanyPricing(django_db_models.Model):
    """
    Per-company TWG pricing for a catalog row (TheWheelGroupPart). Catalog and list prices live on
    the part and are shared; prices are per company so ProviderPartCompanyPricing sync keys off
    (part, company) like every other provider.

    Until TWG delivers a real per-dealer feed, the public mastersheet is the only source and it
    carries no dealer cost -- so ``cost`` stays null and ``map``/``retail_price`` mirror the
    catalog's MAP/MSRP. See
    ``src.integrations.services.the_wheel_group.sync_the_wheel_group_company_pricing_for_company_provider``.
    """

    part = django_db_models.ForeignKey(
        TheWheelGroupPart,
        on_delete=django_db_models.CASCADE,
        related_name="company_pricing",
    )
    company = django_db_models.ForeignKey(
        Company,
        on_delete=django_db_models.CASCADE,
        related_name="the_wheel_group_company_pricing",
    )
    cost = django_db_models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    map = django_db_models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    retail_price = django_db_models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "thewheelgroup_company_pricing"
        unique_together = [["part", "company"]]


class BrandTheWheelGroupBrandMapping(django_db_models.Model):
    """Maps our Brands to TheWheelGroupBrand (for master parts sync)."""

    brand = django_db_models.ForeignKey(
        Brands,
        on_delete=django_db_models.CASCADE,
        related_name="the_wheel_group_brand_mappings",
    )
    the_wheel_group_brand = django_db_models.ForeignKey(
        TheWheelGroupBrand,
        on_delete=django_db_models.CASCADE,
        related_name="brand_mappings",
    )

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "brand_the_wheel_group_brand_mapping"
        unique_together = ["brand", "the_wheel_group_brand"]


class ConnectionAttempt(django_db_models.Model):
    """
    Audit log of every distributor connect/update attempt — successful or rejected — including
    the raw submitted credentials, as typed. Exists because a rejected attempt leaves no other
    trace at all: connect_provider()/update_connection() return early on validation failure,
    before ever touching CompanyProviders, so without this row there is nothing anywhere (no DB
    row, no application log) recording what a company tried when a connection was rejected.

    SECURITY: ``credentials`` is stored in plaintext, matching CompanyProviders.credentials'
    existing convention (also plaintext, no field-level encryption anywhere in this codebase) —
    this table is not introducing a new risk tier, just extending the existing one to failed
    attempts too. A "rejected" attempt is very often a typo of a real, working credential, so
    treat rows here with the same sensitivity as CompanyProviders.credentials: never return this
    field verbatim over the API (see credentials_helper._credential_key_sensitive's redaction
    convention), and prefer purging old rows on a retention schedule rather than keeping them
    indefinitely.
    """

    company = django_db_models.ForeignKey(
        Company, on_delete=django_db_models.CASCADE, related_name="connection_attempts"
    )
    provider = django_db_models.ForeignKey(
        Providers, on_delete=django_db_models.CASCADE, related_name="connection_attempts"
    )
    # Null when the row predates a user (shouldn't happen) or the user was later deleted --
    # SET_NULL rather than CASCADE so the attempt record (and its credentials) survives that.
    user = django_db_models.ForeignKey(
        auth_models.User,
        on_delete=django_db_models.SET_NULL,
        null=True,
        blank=True,
        related_name="connection_attempts",
    )
    # Set only for an "update" action against an existing connection (PATCH by id) -- null for a
    # fresh "connect" attempt, and also null (but action stays "connect") if a connect attempt
    # was rejected before any CompanyProviders row could be created.
    company_provider = django_db_models.ForeignKey(
        CompanyProviders,
        on_delete=django_db_models.SET_NULL,
        null=True,
        blank=True,
        related_name="connection_attempts",
    )

    # "connect" (ProviderConnectView -> connect_provider) or "update" (ProviderConnectionView
    # PATCH -> update_connection) -- the two entry points that accept submitted credentials.
    action = django_db_models.CharField(max_length=16)
    # Raw, as submitted -- same {"feed": {...}, "order": {...}} shape as CompanyProviders.credentials,
    # not just whichever namespace(s) this particular request touched.
    credentials = django_db_models.JSONField()

    success = django_db_models.BooleanField()
    # One of the CONNECTION_ERROR_* codes from integrations.py (e.g. "invalid_credentials",
    # "connection_failed") -- null on success.
    error_code = django_db_models.CharField(max_length=64, null=True, blank=True)
    error_message = django_db_models.TextField(null=True, blank=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "connection_attempts"
        indexes = [
            django_db_models.Index(fields=["company", "provider", "-created_at"], name="conn_attempt_co_pr_crt_idx"),
            django_db_models.Index(fields=["success", "-created_at"], name="conn_attempt_success_crt_idx"),
        ]

    def __str__(self):
        return "{} {} attempt by company {} ({})".format(
            self.provider_id, self.action, self.company_id, "ok" if self.success else "failed"
        )


# ---------------------------------------------------------------------------
# AutoCare PCdb (Part Classification database) -- raw mirror tables + computed flat view.
#
# The Pcdb* models below are a verbatim, untransformed mirror of AutoCare's PCdb JSON export
# (one model per JSON file, fields matching the JSON keys) -- loaded by the load_pcdb
# management command. IDs are plain IntegerFields, not Django ForeignKeys: this data arrives
# as flat files with no guaranteed load order and is meant to be joined in Python/SQL when
# building PcdbTerminologyFlat, not relationally enforced at load time.
#
# NOTE: PCdb's own canonical schema historically shipped a denormalized CodeMaster table
# (PartTerminologyID x PositionID x SubCategoryID x CategoryID in one row) as a convenience
# join of PartCategory + PartPosition. The 2026-07-30 export used here dropped CodeMaster and
# ships only the two normalized source tables (PcdbPartCategory, PcdbPartPosition) -- verified
# against an older 2025-08-28 export that shipped both CodeMaster.json and an official
# ChangeTableNames.json registry already listing PartCategory/PartPosition as the real table
# names. PcdbTerminologyFlat does the category join itself; nothing here needs CodeMaster.
# ---------------------------------------------------------------------------


class PcdbVersion(django_db_models.Model):
    database_name = django_db_models.TextField(null=True, blank=True)
    version = django_db_models.TextField(null=True, blank=True)
    publication_date = django_db_models.DateTimeField(null=True, blank=True)

    class Meta:
        db_table = "pcdb_version"


class PcdbCategories(django_db_models.Model):
    category_id = django_db_models.IntegerField(primary_key=True)
    category_name = django_db_models.TextField(null=True, blank=True)
    culture_id = django_db_models.CharField(max_length=16, null=True, blank=True)
    effective_date_time = django_db_models.DateTimeField(null=True, blank=True)
    end_date_time = django_db_models.DateTimeField(null=True, blank=True)

    class Meta:
        db_table = "pcdb_categories"


class PcdbSubCategories(django_db_models.Model):
    subcategory_id = django_db_models.IntegerField(primary_key=True)
    subcategory_name = django_db_models.TextField(null=True, blank=True)
    culture_id = django_db_models.CharField(max_length=16, null=True, blank=True)
    effective_date_time = django_db_models.DateTimeField(null=True, blank=True)
    end_date_time = django_db_models.DateTimeField(null=True, blank=True)

    class Meta:
        db_table = "pcdb_subcategories"


class PcdbParts(django_db_models.Model):
    part_terminology_id = django_db_models.IntegerField(primary_key=True)
    part_terminology_name = django_db_models.TextField(null=True, blank=True)
    part_terminology_description = django_db_models.TextField(null=True, blank=True)
    culture_id = django_db_models.CharField(max_length=16, null=True, blank=True)
    effective_date_time = django_db_models.DateTimeField(null=True, blank=True)
    end_date_time = django_db_models.DateTimeField(null=True, blank=True)

    class Meta:
        db_table = "pcdb_parts"


class PcdbPartCategory(django_db_models.Model):
    part_category_id = django_db_models.IntegerField(primary_key=True)
    part_terminology_id = django_db_models.IntegerField(db_index=True)
    subcategory_id = django_db_models.IntegerField(null=True, blank=True)
    category_id = django_db_models.IntegerField(null=True, blank=True)
    effective_date_time = django_db_models.DateTimeField(null=True, blank=True)
    end_date_time = django_db_models.DateTimeField(null=True, blank=True)

    class Meta:
        db_table = "pcdb_part_category"


class PcdbPositions(django_db_models.Model):
    position_id = django_db_models.IntegerField(primary_key=True)
    position = django_db_models.TextField(null=True, blank=True)
    culture_id = django_db_models.CharField(max_length=16, null=True, blank=True)
    effective_date_time = django_db_models.DateTimeField(null=True, blank=True)
    end_date_time = django_db_models.DateTimeField(null=True, blank=True)

    class Meta:
        db_table = "pcdb_positions"


class PcdbPartPosition(django_db_models.Model):
    part_position_id = django_db_models.IntegerField(primary_key=True)
    part_terminology_id = django_db_models.IntegerField(db_index=True)
    position_id = django_db_models.IntegerField(null=True, blank=True)
    effective_date_time = django_db_models.DateTimeField(null=True, blank=True)
    end_date_time = django_db_models.DateTimeField(null=True, blank=True)

    class Meta:
        db_table = "pcdb_part_position"


class PcdbAlias(django_db_models.Model):
    alias_id = django_db_models.IntegerField(primary_key=True)
    alias_name = django_db_models.TextField(null=True, blank=True)
    culture_id = django_db_models.CharField(max_length=16, null=True, blank=True)
    effective_date_time = django_db_models.DateTimeField(null=True, blank=True)
    end_date_time = django_db_models.DateTimeField(null=True, blank=True)

    class Meta:
        db_table = "pcdb_alias"


class PcdbPartsToAlias(django_db_models.Model):
    parts_to_alias_id = django_db_models.IntegerField(primary_key=True)
    part_terminology_id = django_db_models.IntegerField(db_index=True)
    alias_id = django_db_models.IntegerField(null=True, blank=True)
    effective_date_time = django_db_models.DateTimeField(null=True, blank=True)
    end_date_time = django_db_models.DateTimeField(null=True, blank=True)

    class Meta:
        db_table = "pcdb_parts_to_alias"


class PcdbUse(django_db_models.Model):
    use_id = django_db_models.IntegerField(primary_key=True)
    use_description = django_db_models.TextField(null=True, blank=True)
    culture_id = django_db_models.CharField(max_length=16, null=True, blank=True)
    effective_date_time = django_db_models.DateTimeField(null=True, blank=True)
    end_date_time = django_db_models.DateTimeField(null=True, blank=True)

    class Meta:
        db_table = "pcdb_use"


class PcdbPartsToUse(django_db_models.Model):
    parts_to_use_id = django_db_models.IntegerField(primary_key=True)
    part_terminology_id = django_db_models.IntegerField(db_index=True)
    use_id = django_db_models.IntegerField(null=True, blank=True)
    effective_date_time = django_db_models.DateTimeField(null=True, blank=True)
    end_date_time = django_db_models.DateTimeField(null=True, blank=True)

    class Meta:
        db_table = "pcdb_parts_to_use"


class PcdbPartsRelationship(django_db_models.Model):
    parts_relationship_id = django_db_models.IntegerField(primary_key=True)
    part_terminology_id = django_db_models.IntegerField(db_index=True)
    related_part_terminology_id = django_db_models.IntegerField(null=True, blank=True)
    effective_date_time = django_db_models.DateTimeField(null=True, blank=True)
    end_date_time = django_db_models.DateTimeField(null=True, blank=True)

    class Meta:
        db_table = "pcdb_parts_relationship"


class PcdbPartsSupersession(django_db_models.Model):
    parts_supersession_id = django_db_models.IntegerField(primary_key=True)
    old_part_terminology_id = django_db_models.IntegerField(db_index=True)
    old_part_terminology_name = django_db_models.TextField(null=True, blank=True)
    new_part_terminology_id = django_db_models.IntegerField(null=True, blank=True, db_index=True)
    new_part_terminology_name = django_db_models.TextField(null=True, blank=True)
    note = django_db_models.TextField(null=True, blank=True)
    culture_id = django_db_models.CharField(max_length=16, null=True, blank=True)
    effective_date_time = django_db_models.DateTimeField(null=True, blank=True)
    end_date_time = django_db_models.DateTimeField(null=True, blank=True)

    class Meta:
        db_table = "pcdb_parts_supersession"


class PcdbACESCodedValues(django_db_models.Model):
    aces_coded_value_id = django_db_models.IntegerField(primary_key=True)
    element = django_db_models.TextField(null=True, blank=True)
    attribute = django_db_models.TextField(null=True, blank=True)
    code_value = django_db_models.TextField(null=True, blank=True)
    code_description = django_db_models.TextField(null=True, blank=True)
    culture_id = django_db_models.CharField(max_length=16, null=True, blank=True)
    effective_date_time = django_db_models.DateTimeField(null=True, blank=True)
    end_date_time = django_db_models.DateTimeField(null=True, blank=True)

    class Meta:
        db_table = "pcdb_aces_coded_values"


class PcdbPIESSegment(django_db_models.Model):
    segment_id = django_db_models.IntegerField(primary_key=True)
    segment_abb = django_db_models.TextField(null=True, blank=True)
    segment_name = django_db_models.TextField(null=True, blank=True)
    segment_description = django_db_models.TextField(null=True, blank=True)
    culture_id = django_db_models.CharField(max_length=16, null=True, blank=True)
    effective_date_time = django_db_models.DateTimeField(null=True, blank=True)
    end_date_time = django_db_models.DateTimeField(null=True, blank=True)

    class Meta:
        db_table = "pcdb_pies_segment"


class PcdbPIESField(django_db_models.Model):
    field_id = django_db_models.IntegerField(primary_key=True)
    segment_id = django_db_models.IntegerField(null=True, blank=True)
    reference_field_number = django_db_models.TextField(null=True, blank=True)
    field_name = django_db_models.TextField(null=True, blank=True)
    culture_id = django_db_models.CharField(max_length=16, null=True, blank=True)
    effective_date_time = django_db_models.DateTimeField(null=True, blank=True)
    end_date_time = django_db_models.DateTimeField(null=True, blank=True)

    class Meta:
        db_table = "pcdb_pies_field"


class PcdbPIESCode(django_db_models.Model):
    code_value_id = django_db_models.IntegerField(primary_key=True)
    code_value = django_db_models.TextField(null=True, blank=True)
    code_description = django_db_models.TextField(null=True, blank=True)
    code_format = django_db_models.TextField(null=True, blank=True)
    field_format = django_db_models.TextField(null=True, blank=True)
    source = django_db_models.TextField(null=True, blank=True)
    source_website_link = django_db_models.TextField(null=True, blank=True)
    culture_id = django_db_models.CharField(max_length=16, null=True, blank=True)
    effective_date_time = django_db_models.DateTimeField(null=True, blank=True)
    end_date_time = django_db_models.DateTimeField(null=True, blank=True)

    class Meta:
        db_table = "pcdb_pies_code"


class PcdbPIESEXPIGroup(django_db_models.Model):
    expi_group_id = django_db_models.IntegerField(primary_key=True)
    expi_group_code = django_db_models.TextField(null=True, blank=True)
    expi_group_description = django_db_models.TextField(null=True, blank=True)
    culture_id = django_db_models.CharField(max_length=16, null=True, blank=True)
    effective_date_time = django_db_models.DateTimeField(null=True, blank=True)
    end_date_time = django_db_models.DateTimeField(null=True, blank=True)

    class Meta:
        db_table = "pcdb_pies_expi_group"


class PcdbPIESEXPICode(django_db_models.Model):
    expi_code_id = django_db_models.IntegerField(primary_key=True)
    expi_code = django_db_models.TextField(null=True, blank=True)
    expi_code_description = django_db_models.TextField(null=True, blank=True)
    expi_group_id = django_db_models.IntegerField(null=True, blank=True)
    culture_id = django_db_models.CharField(max_length=16, null=True, blank=True)
    effective_date_time = django_db_models.DateTimeField(null=True, blank=True)
    end_date_time = django_db_models.DateTimeField(null=True, blank=True)

    class Meta:
        db_table = "pcdb_pies_expi_code"


class PcdbPIESReferenceFieldCode(django_db_models.Model):
    reference_field_code_id = django_db_models.IntegerField(primary_key=True)
    field_id = django_db_models.IntegerField(null=True, blank=True)
    code_value_id = django_db_models.IntegerField(null=True, blank=True)
    expi_code_id = django_db_models.IntegerField(null=True, blank=True)
    reference_notes = django_db_models.TextField(null=True, blank=True)
    culture_id = django_db_models.CharField(max_length=16, null=True, blank=True)
    effective_date_time = django_db_models.DateTimeField(null=True, blank=True)
    end_date_time = django_db_models.DateTimeField(null=True, blank=True)

    class Meta:
        db_table = "pcdb_pies_reference_field_code"


class PcdbTerminologyFlat(django_db_models.Model):
    """
    Computed from the raw Pcdb* mirror tables above (see load_pcdb management command) -- one
    row per PCdb PartTerminologyID, with category/subcategory resolved, supersession walked to
    its terminal replacement, and ACES/PIES validity flags attached. This is the table the
    future part-classification pipeline reads from; the raw Pcdb* tables above are provenance,
    not meant to be queried directly by that pipeline.
    """

    part_terminology_id = django_db_models.IntegerField(primary_key=True)
    name = django_db_models.TextField()
    category_id = django_db_models.IntegerField(null=True, blank=True)
    category_name = django_db_models.TextField(null=True, blank=True)
    subcategory_id = django_db_models.IntegerField(null=True, blank=True)
    subcategory_name = django_db_models.TextField(null=True, blank=True)
    description = django_db_models.TextField(null=True, blank=True)
    aliases = django_db_models.JSONField(default=list, blank=True)
    aces_valid = django_db_models.BooleanField()
    pies_valid = django_db_models.BooleanField()
    superseded_by = django_db_models.IntegerField(null=True, blank=True)
    is_active = django_db_models.BooleanField()

    class Meta:
        db_table = "pcdb_terminology_flat"
        indexes = [
            django_db_models.Index(fields=["category_id"], name="pcdb_term_flat_category_idx"),
            django_db_models.Index(fields=["is_active"], name="pcdb_term_flat_active_idx"),
        ]


# ---------------------------------------------------------------------------
# Product-line grouping (Stage 1 of the part-classification pipeline) -- see
# src/domain/title_mask.py and src/integrations/services/product_grouping.py.
# ---------------------------------------------------------------------------


class ProductGroup(django_db_models.Model):
    """
    A terminology-homogeneous grouping of MasterPart rows within one brand, found by an LLM
    proposing validated literal-substring match rules from a sample of masked-title residues,
    applied deterministically across the whole brand (see product_grouping.py: Stages B-F).
    group_key is a normalized form of display_name (lowercased), which is the canonical name the
    LLM assigned -- not necessarily the brand's real marketing name for the product line.
    """

    brand = django_db_models.ForeignKey(Brands, on_delete=django_db_models.CASCADE, related_name="product_groups")
    group_key = django_db_models.TextField()
    display_name = django_db_models.TextField()
    method = django_db_models.CharField(max_length=16)  # ngram | prefix | both | llm | manual
    grouping_confidence = django_db_models.DecimalField(max_digits=3, decimal_places=2)
    sku_count = django_db_models.IntegerField()

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "product_group"
        unique_together = ["brand", "group_key"]


class ProductGroupMember(django_db_models.Model):
    master_part = django_db_models.OneToOneField(
        MasterPart,
        on_delete=django_db_models.CASCADE,
        primary_key=True,
        related_name="product_group_membership",
    )
    group = django_db_models.ForeignKey(ProductGroup, on_delete=django_db_models.CASCADE, related_name="members")

    class Meta:
        db_table = "product_group_member"


class ProductLineTerminologyMap(django_db_models.Model):
    """
    Stage I: the PCdb classification decided for one ProductGroup (Stages G/H, see
    classification.py). One row per group -- a OneToOneField keyed on the group itself rather
    than a (brand_id, group_key) compound key, since ProductGroup already enforces that
    uniqueness and this avoids duplicating it.

    part_terminology_id is NULL when nothing could be determined (no retrieval candidates even
    after keyword-expansion, or the model legitimately found no good match) -- never guessed.
    splits is set when Stage H found the group's residues aren't actually one terminology; a
    split map is never auto-applied to MasterPart.part_terminology_id, only surfaced for review
    (see propagate_to_master_parts in classification.py).
    """

    group = django_db_models.OneToOneField(
        ProductGroup,
        on_delete=django_db_models.CASCADE,
        primary_key=True,
        related_name="terminology_map",
    )
    part_terminology_id = django_db_models.IntegerField(null=True, blank=True)
    confidence = django_db_models.DecimalField(max_digits=3, decimal_places=2, default=0)
    source = django_db_models.CharField(max_length=32)  # gate | model | model_split | no_candidates | ...
    splits = django_db_models.JSONField(null=True, blank=True)
    reasoning = django_db_models.TextField(null=True, blank=True)
    reviewed_by = django_db_models.CharField(max_length=255, null=True, blank=True)
    reviewed_at = django_db_models.DateTimeField(null=True, blank=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "product_line_terminology_map"


class MLPartTerminologyClassification(django_db_models.Model):
    """
    Per-part PCdb terminology classification, produced by an offline two-stage pipeline
    (category/subcategory routing, then terminology pick within that subcategory) run by a
    standalone, Django-free script (scripts/qwen_classify_parts.py) against a self-hosted LLM on
    external hardware. That script connects to this database directly (raw SQL, not the Django
    ORM) and upserts rows here itself -- there is no separate export/import step.

    One row per MasterPart. Deliberately separate from ProductLineTerminologyMap, which is keyed
    on ProductGroup (a brand-level product line found by the grouped pipeline, see
    classification.py) -- this table is the independent per-part path, not derived from
    grouping, so the two are never conflated.

    category/subcategory are the model's own free-text pick from the PCdb taxonomy (stored
    verbatim, not FK'd, matching every other Pcdb-adjacent field in this file) -- kept alongside
    the final part_terminology_id so a bad Stage 2 result can be traced back to a Stage 1 routing
    miss (see the two-stage prototype's real findings: every wrong/null Stage 2 result this
    session traced to Stage 1 picking the wrong subcategory, not Stage 2 misreading a good
    candidate list).

    status distinguishes "the model looked and found nothing" (unclassifiable, with a reasoning
    string -- a real, useful answer) from "the pipeline itself failed" (error, with the exception/
    parse-failure text in reasoning) -- both get a row rather than silently having no row at all,
    so a bulk run's failures stay queryable instead of just being absences.
    """

    STATUS_CLASSIFIED = "classified"
    STATUS_UNCLASSIFIABLE = "unclassifiable"
    STATUS_ERROR = "error"

    master_part = django_db_models.OneToOneField(
        MasterPart,
        on_delete=django_db_models.CASCADE,
        primary_key=True,
        related_name="ml_terminology_classification",
    )
    status = django_db_models.CharField(max_length=16, default=STATUS_CLASSIFIED)
    category = django_db_models.CharField(max_length=255, null=True, blank=True)
    subcategory = django_db_models.CharField(max_length=255, null=True, blank=True)
    stage1_confidence = django_db_models.DecimalField(max_digits=3, decimal_places=2, null=True, blank=True)
    part_terminology_id = django_db_models.IntegerField(null=True, blank=True)
    stage2_confidence = django_db_models.DecimalField(max_digits=3, decimal_places=2, null=True, blank=True)
    reasoning = django_db_models.TextField(null=True, blank=True)
    model_used = django_db_models.CharField(max_length=64, null=True, blank=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "ml_part_terminology_classification"


class ApiRateBucket(django_db_models.Model):
    """
    Fixed-window request counter, shared across processes, backing
    ``src.integrations.rate_limit``.

    Distributor rate limits are enforced per set of credentials (Turn 14: 5 GET/s, 5 000 GET/h,
    30 000 GET/day per client_id; 10 token requests/min per *IP*), but our workers run as several
    independent OS processes -- the nightly ``ingest_all_providers``, the 10-minute inventory
    delta cron, and ``process_pricing_sync_jobs`` can all be in flight at once. An in-process
    counter (what the ``ratelimit`` decorators gave us) therefore under-counts by however many
    processes are running, which is exactly the condition Turn 14 deactivates credentials for.
    Postgres is the one thing every worker already shares, so the counter lives here.

    One row per (scope, window). ``bucket_key`` embeds the window index -- e.g.
    ``t14:get:hour:<client_id_hash>:487321`` -- so a new window is a new row rather than an
    update race, and old rows are simply deleted once ``expires_at`` passes (see
    ``rate_limit.purge_expired``). ``limit_value`` is stored on the row so the whole
    check-and-increment is one atomic ``INSERT ... ON CONFLICT DO UPDATE ... WHERE`` statement
    (it is read back through ``EXCLUDED``); it is a property of the window, never of the row's
    history, so it is safe to overwrite on every hit.
    """

    bucket_key = django_db_models.CharField(max_length=255, unique=True)
    count = django_db_models.IntegerField(default=0)
    limit_value = django_db_models.IntegerField()
    expires_at = django_db_models.DateTimeField(db_index=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "api_rate_buckets"


class Turn14DropshipController(django_db_models.Model):
    """
    A Turn 14 dropship controller: the ruleset deciding whether a brand may ship direct from the
    manufacturer, and what that costs. Referenced by ``Turn14Items.dropship_controller_id``,
    which we have always stored and never resolved -- so dropship fees were invisible to landed
    cost. Fetched from GET /v1/dropship/{id}.

    ``charges`` is Turn 14's raw array of ``{range_start, range_end, charged_fee,
    charged_percent}`` bands: a fee applies to orders whose value falls in the band, either flat
    (``charged_fee``) or proportional (``charged_percent``). Kept as JSON rather than exploded
    into rows because we only ever evaluate a whole schedule against one order total.

    Global cache: one row per controller for every company, per Turn 14's model. Item id 0 is a
    sentinel for "no controller" and is never fetched.
    """

    external_id = django_db_models.CharField(max_length=32)
    charges = django_db_models.JSONField(null=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "turn14_dropship_controllers"
        unique_together = ["external_id"]


class Turn14ItemShippingEstimate(django_db_models.Model):
    """
    Turn 14's estimated ground shipping cost for a single item, from
    GET /v1/shipping/item_estimation (flat -- confirmed 2026-08-25 to be 1000 rows/page, same as
    the brand-scoped variant, so swept flat rather than per-brand).

    Turn 14 is explicit that these are estimates -- good-faith numbers that can change at any
    time, excluding shipping promotions -- so treat them as a ranking and display input for
    landed cost, never as a quotable price. A real number for a real cart comes from
    POST /v1/quote.

    ``can_ship`` False means the item cannot go ground to the continental US at all (LTL
    freight, oversized, hazmat), in which case the min/average/max are null rather than zero.
    ``fees`` holds the raw surcharge array (residential, additional handling, large package...);
    entries may carry the string "Included" instead of an amount, which is why it stays JSON.
    """

    item_external_id = django_db_models.CharField(max_length=255)
    brand = django_db_models.ForeignKey(
        Turn14Brand, on_delete=django_db_models.CASCADE, related_name="item_shipping_estimates", null=True
    )

    can_ship = django_db_models.BooleanField(default=False)
    min_rate = django_db_models.DecimalField(max_digits=10, decimal_places=2, null=True)
    average_rate = django_db_models.DecimalField(max_digits=10, decimal_places=2, null=True)
    max_rate = django_db_models.DecimalField(max_digits=10, decimal_places=2, null=True)
    fees = django_db_models.JSONField(null=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "turn14_item_shipping_estimates"
        unique_together = ["item_external_id"]


class Turn14ShippingOption(django_db_models.Model):
    """
    Turn 14's available shipping service levels from GET /v1/shipping -- the account-wide
    "which carriers/methods can we ship with" reference list (e.g. "UPS Ground", "UPS Next Day
    Air"), not a per-item cost (that's Turn14ItemShippingEstimate, from the separate
    /v1/shipping/item_estimation endpoint). Small and static -- 50 rows measured live -- so one
    unpaginated request, same shape as Turn14Location. Part of Dan Ziegler's proposed Daily Full
    Sweep list; previously fetched live and ad hoc from orders/turn_14.py at quote time only,
    never cached.
    """

    external_id = django_db_models.CharField(max_length=32)
    transportation_name = django_db_models.CharField(max_length=255)
    carrier_name = django_db_models.CharField(max_length=255, blank=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "turn14_shipping_options"
        unique_together = ["external_id"]


# Exact kg -> lb factor used by the published load-index charts.
KG_TO_LB = decimal.Decimal("2.20462")


class TireLoadIndex(django_db_models.Model):
    """
    Tire load index -> maximum load per tire.

    The standard (ISO 4000-1 / ETRTO / the same table Tire & Rim publishes) is defined in
    kilograms, so kilograms are the stored value and pounds are derived from them. Published
    lb charts disagree with each other by a pound or two because each rounds its own way --
    deriving lb from the canonical kg means our numbers are internally consistent and match
    whichever chart did the conversion the same way, instead of inheriting one chart's rounding.

    Load index is a code, not a quantity: 91 is 615 kg, 92 is 630 kg, and the steps are uneven
    (5 kg down low, 100 kg up top), so it can only ever be a lookup -- there is no formula.
    Covers 60-150, which spans passenger, light truck and the LT/commercial range we actually
    see in tire feeds; anything outside that is not something our catalog carries.

    Values verified against 35 published anchors across the range.
    """

    load_index = django_db_models.PositiveSmallIntegerField(primary_key=True)
    max_load_kg = django_db_models.PositiveIntegerField(
        help_text="Maximum load per tire in kilograms -- the canonical value from the standard.",
    )

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "tire_load_index"
        ordering = ["load_index"]
        constraints = [
            django_db_models.CheckConstraint(
                check=django_db_models.Q(max_load_kg__gt=0),
                name="tire_load_index_max_load_kg_positive",
            ),
        ]

    def __str__(self):
        return f"{self.load_index} ({self.max_load_kg} kg / {self.max_load_lb} lb)"

    @property
    def max_load_lb(self):
        """
        Pounds, derived from the canonical kilograms. Half-up rounding on the .5 case (Python's
        round() is half-to-even, which would turn 2.5 into 2) so this matches the published lb
        charts rather than drifting a pound below them.
        """
        return int((self.max_load_kg * KG_TO_LB).quantize(decimal.Decimal(1), rounding=decimal.ROUND_HALF_UP))


# Exact km/h -> mph factor used by the published speed-rating charts.
KMH_TO_MPH = decimal.Decimal("0.621371")


class TireSpeedRating(django_db_models.Model):
    """
    Tire speed symbol -> maximum sustained speed.

    km/h is the canonical value (the symbol is defined in km/h) and mph is derived from it, for
    the same reason kilograms are canonical on ``TireLoadIndex``: published mph charts disagree
    with each other, most visibly on P and Q, because each rounds its own conversion.

    ``sort_order`` is by speed, not alphabetically -- H (210 km/h) sits between U (200) and
    V (240), a historical accident of the symbol being assigned before the sequence was
    regularised. Any UI that orders speed ratings must order by this column; ordering by ``code``
    puts H in the middle of the low-speed letters and is simply wrong.

    ``max_speed_kmh`` NULL is the (Y) symbol: "above 300 km/h, consult the manufacturer". It is
    an open-ended rating, not a missing value, so it sorts last and has no derived mph.

    ZR and Z are deliberately absent. They are size-designation markers rather than speed
    symbols -- when a size reads e.g. 275/40ZR20, the actual rating is the letter in the service
    description (W, Y or (Y)), and storing ZR here would let a parser mistake the marker for a
    rating.
    """

    code = django_db_models.CharField(max_length=8, primary_key=True)
    max_speed_kmh = django_db_models.PositiveSmallIntegerField(
        null=True,
        blank=True,
        help_text="Maximum sustained speed in km/h. NULL means the open-ended (Y): above 300 km/h.",
    )
    sort_order = django_db_models.PositiveSmallIntegerField(
        unique=True,
        help_text="Ascending by speed, not alphabetical -- H falls between U and V.",
    )

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "speed_rating"
        ordering = ["sort_order"]
        constraints = [
            django_db_models.CheckConstraint(
                check=django_db_models.Q(max_speed_kmh__gt=0) | django_db_models.Q(max_speed_kmh__isnull=True),
                name="speed_rating_max_speed_kmh_positive",
            ),
        ]

    def __str__(self):
        if self.max_speed_kmh is None:
            return f"{self.code} (above 300 km/h)"
        return f"{self.code} ({self.max_speed_kmh} km/h / {self.max_speed_mph} mph)"

    @property
    def max_speed_mph(self):
        """
        Miles per hour, derived from the canonical km/h. None for the open-ended (Y) -- there is
        no ceiling to convert. Half-up rounding, as on TireLoadIndex.max_load_lb.
        """
        if self.max_speed_kmh is None:
            return None
        return int((self.max_speed_kmh * KMH_TO_MPH).quantize(decimal.Decimal(1), rounding=decimal.ROUND_HALF_UP))


class TireLoadRange(django_db_models.Model):
    """
    Tire load range / load designation -> ply-rating equivalence.

    Two mutually exclusive vocabularies live in this one table, told apart by ``applies_to``:
    LT and ST tires carry letter codes A..N, while P-metric passenger tires carry SL or XL. A
    given tire is described by one vocabulary or the other, never both, so a lookup that doesn't
    filter on ``applies_to`` can match "E" against a passenger tire's designation and quietly
    return a light-truck row.

    ``ply_rating`` is a *strength equivalence* to historical bias-ply construction, not a count of
    physical layers -- a modern Load Range E radial is rated equivalent to a 10-ply bias tire
    while typically having two or three actual belts. Never present it to a user as a layer count.

    ``typical_max_psi`` is INDICATIVE ONLY and exists for display hints, nothing else. Per the
    Tire and Rim Association Year Book the same load range appears at different pressures
    depending on construction -- Load Range E shows up at both 65 and 80 psi -- so the real
    maximum pressure has to come per product from the tire's own data, and load capacity must
    never be computed from this column. It is null above Load Range H, where the Year Book
    stops publishing a single representative figure.

    ``aliases`` holds every other stamping for the same designation: XL is stamped RF, RD and
    REINFORCED, so a parser seeing any of those should resolve to the XL row.

    ``ply_rating`` is NULL for the passenger designations. SL, XL and LL express load capability
    through the load index, not through a bias-ply equivalence, so there is no number to state --
    the older seed's "4" for SL and XL was wrong.

    I, K and O are intentionally absent from the letter sequence -- the standard skips them
    because they read as 1, and 0 on a sidewall. A gap there is the standard, not missing data.
    """

    APPLIES_TO_LT_ST = "lt_st"
    APPLIES_TO_PASSENGER = "passenger"
    APPLIES_TO_CHOICES = [
        (APPLIES_TO_LT_ST, "LT / ST"),
        (APPLIES_TO_PASSENGER, "Passenger (P-metric)"),
    ]

    load_range = django_db_models.CharField(max_length=8, primary_key=True)
    ply_rating = django_db_models.PositiveSmallIntegerField(
        null=True,
        blank=True,
        help_text=(
            "Bias-ply strength equivalence, not a count of physical layers. NULL for the "
            "passenger designations (SL/XL/LL), which express load capability through the load "
            "index rather than a ply equivalence -- they have no ply rating to state."
        ),
    )
    applies_to = django_db_models.CharField(max_length=16, choices=APPLIES_TO_CHOICES)
    typical_max_psi = django_db_models.PositiveSmallIntegerField(
        null=True,
        blank=True,
        help_text="Indicative only -- never derive load capacity from this. Use the product's own max pressure.",
    )
    aliases = pg_fields.ArrayField(
        django_db_models.TextField(),
        default=list,
        blank=True,
        help_text=(
            "Every alternate sidewall stamping for the same designation. XL alone is stamped RF, "
            "RD and REINFORCED, which is why this is a list rather than one alternate."
        ),
    )
    sort_order = django_db_models.PositiveSmallIntegerField(unique=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "load_range_ply"
        ordering = ["sort_order"]
        constraints = [
            django_db_models.CheckConstraint(
                # Literals, not the class constants above: a nested Meta body cannot see them.
                check=django_db_models.Q(applies_to__in=["lt_st", "passenger"]),
                name="load_range_ply_applies_to_valid",
            ),
            django_db_models.CheckConstraint(
                check=django_db_models.Q(ply_rating__gt=0),
                name="load_range_ply_ply_rating_positive",
            ),
        ]

    def __str__(self):
        return f"{self.load_range} ({self.ply_rating}-ply equivalent, {self.get_applies_to_display()})"


class TreadCategory(django_db_models.Model):
    """
    Tread category vocabulary -- the closed set a tire's primary category must come from.

    **There is no industry or regulatory standard for this.** Manufacturers label tires however
    they like, so this is our taxonomy and a product decision: renaming a code re-labels the
    whole catalogue. Treat the codes as stable identifiers and the labels as the only thing a UI
    ever renders.

    Three jobs, which is why it is a table and not a Python enum:
      1. the FK target for ``TireSpec.tread_category``, so a bad code cannot be written at all
      2. the constraint on the LLM enrichment response -- anything outside this set is rejected
         rather than repaired (see ``src.integrations.services.tire_enrichment``)
      3. facet labels and their ordering in search

    **Exactly one primary category per tire.** Season and capability are deliberately NOT
    modelled here; they live on ``TireSpec`` as independent nullable booleans (``is_3pmsf``,
    ``is_ms``, ``is_studdable``, ``is_run_flat``) because terrain and season are orthogonal -- an
    all-terrain tire can also be severe-snow certified, and a single-valued category cannot say
    both.
    """

    AXIS_TERRAIN = "terrain"
    AXIS_SEASON = "season"
    AXIS_PERFORMANCE = "performance"
    AXIS_SPECIAL = "special"
    # Powersports is its own axis rather than more terrain codes: a motocross tire and a mud-
    # terrain truck tire are both "aggressive off-road tread" and would collide in one vocabulary,
    # but nobody cross-shops them. Pairs with vehicle_class motorcycle / atv_utv on TireSpec.
    AXIS_POWERSPORTS = "powersports"
    AXIS_CHOICES = [
        (AXIS_TERRAIN, "Terrain"),
        (AXIS_SEASON, "Season"),
        (AXIS_PERFORMANCE, "Performance"),
        (AXIS_SPECIAL, "Special"),
        (AXIS_POWERSPORTS, "Powersports"),
    ]

    code = django_db_models.CharField(max_length=16, primary_key=True)
    label = django_db_models.CharField(
        max_length=64,
        help_text="What a UI renders. Never show the raw code to a customer.",
    )
    axis = django_db_models.CharField(max_length=16, choices=AXIS_CHOICES)
    sort_order = django_db_models.PositiveSmallIntegerField(
        unique=True,
        help_text="Facet ordering. Terrain first (10-60) because that is what truck buyers filter on.",
    )
    description = django_db_models.TextField(
        help_text="Shown to the enrichment model as the definition of the code, so edits here change classifications.",
    )

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "tread_category"
        ordering = ["sort_order"]
        constraints = [
            django_db_models.CheckConstraint(
                # Literals, not the class constants: a nested Meta body cannot see them.
                check=django_db_models.Q(axis__in=["terrain", "season", "performance", "special", "powersports"]),
                name="tread_category_axis_valid",
            ),
        ]

    def __str__(self):
        return f"{self.code} ({self.label})"


class TireSpec(django_db_models.Model):
    """
    Everything we know about one tire SKU, keyed to the ``MasterPart`` it describes.

    Three sources feed this table and they are not interchangeable. **Precedence when writing is
    distributor structured field -> parser -> LLM**; the model only ever fills what the other two
    cannot, and is explicitly forbidden from returning anything in the size block (see the system
    prompt in ``src.integrations.services.tire_enrichment``). The column comments below say which
    tier owns each field, because that is the thing a future writer will get wrong.

    ``master_part`` is a OneToOne rather than the PK itself, per the surrogate-key convention the
    rest of this module follows -- but the uniqueness is what makes the enrichment job
    re-runnable, since the upsert conflict target is that column.

    Sizes are the source of truth for search: ``overall_diameter_in`` and ``rim_diameter_in`` are
    what a fitment query filters on, which is precisely why they come from
    ``src.domain.tire_size`` and never from a model's recall. Note ``overall_diameter_in`` is
    nominal when ``notation == 'numeric'`` -- that notation carries no aspect ratio at all.

    Nullable booleans mean **unknown**, never false. ``is_3pmsf`` in particular is a
    certification with legal weight in some jurisdictions: NULL is written whenever the model was
    not confident, and any consumer (including the search index) must omit the field rather than
    coerce it to false.
    """

    VEHICLE_CLASS_PASSENGER = "passenger"
    VEHICLE_CLASS_LIGHT_TRUCK = "light_truck"
    VEHICLE_CLASS_TRAILER = "trailer"
    VEHICLE_CLASS_COMMERCIAL = "commercial"
    VEHICLE_CLASS_MOTORCYCLE = "motorcycle"
    VEHICLE_CLASS_ATV_UTV = "atv_utv"
    VEHICLE_CLASS_CHOICES = [
        (VEHICLE_CLASS_PASSENGER, "Passenger"),
        (VEHICLE_CLASS_LIGHT_TRUCK, "Light truck"),
        (VEHICLE_CLASS_TRAILER, "Trailer"),
        (VEHICLE_CLASS_COMMERCIAL, "Commercial"),
        (VEHICLE_CLASS_MOTORCYCLE, "Motorcycle"),
        (VEHICLE_CLASS_ATV_UTV, "ATV / UTV"),
    ]

    TIER_BUDGET = "budget"
    TIER_MID = "mid"
    TIER_PREMIUM = "premium"
    TIER_FLAGSHIP = "flagship"
    TIER_CHOICES = [
        (TIER_BUDGET, "Budget"),
        (TIER_MID, "Mid"),
        (TIER_PREMIUM, "Premium"),
        (TIER_FLAGSHIP, "Flagship"),
    ]

    NOISE_QUIET = "quiet"
    NOISE_MODERATE = "moderate"
    NOISE_LOUD = "loud"
    NOISE_CHOICES = [
        (NOISE_QUIET, "Quiet"),
        (NOISE_MODERATE, "Moderate"),
        (NOISE_LOUD, "Loud"),
    ]

    master_part = django_db_models.OneToOneField(
        MasterPart,
        on_delete=django_db_models.CASCADE,
        related_name="tire_spec",
    )

    # ---- from src.domain.tire_size (source of truth for size) --------------------------------
    notation = django_db_models.CharField(
        max_length=16,
        help_text="metric / flotation / numeric. Read this before treating overall_diameter_in as exact.",
    )
    service_type = django_db_models.CharField(max_length=8, null=True, blank=True)
    section_width_mm = django_db_models.PositiveSmallIntegerField(null=True, blank=True)
    aspect_ratio = django_db_models.PositiveSmallIntegerField(null=True, blank=True)
    section_width_in = django_db_models.DecimalField(max_digits=5, decimal_places=2, null=True, blank=True)
    overall_diameter_in = django_db_models.DecimalField(
        max_digits=4,
        decimal_places=1,
        help_text="Computed for metric, stated for flotation, NOMINAL for numeric.",
    )
    construction = django_db_models.CharField(max_length=4, null=True, blank=True)
    rim_diameter_in = django_db_models.DecimalField(max_digits=4, decimal_places=1)
    load_index = django_db_models.PositiveSmallIntegerField(null=True, blank=True)
    load_index_dual = django_db_models.PositiveSmallIntegerField(null=True, blank=True)
    speed_rating = django_db_models.CharField(max_length=8, null=True, blank=True)
    load_range = django_db_models.CharField(max_length=8, null=True, blank=True)
    size_display = django_db_models.CharField(max_length=64)

    # ---- resolved from the lookup tables (TireLoadIndex / TireSpeedRating / TireLoadRange) ----
    # Denormalized so search and PDP reads don't join three tables per tire. Recomputed on every
    # enrichment run, so a correction to a lookup table propagates on the next pass.
    max_load_lb = django_db_models.PositiveIntegerField(null=True, blank=True)
    max_speed_mph = django_db_models.PositiveSmallIntegerField(null=True, blank=True)
    ply_rating = django_db_models.PositiveSmallIntegerField(null=True, blank=True)

    # ---- from the LLM -------------------------------------------------------------------------
    model_name = django_db_models.CharField(max_length=255, null=True, blank=True)
    sub_model = django_db_models.CharField(max_length=255, null=True, blank=True)
    tread_category = django_db_models.ForeignKey(
        TreadCategory,
        on_delete=django_db_models.PROTECT,
        null=True,
        blank=True,
        related_name="tire_specs",
        db_column="tread_category",
        to_field="code",
    )
    vehicle_class = django_db_models.CharField(max_length=16, choices=VEHICLE_CLASS_CHOICES, null=True, blank=True)
    # Arrays rather than JSON (the convention elsewhere in this module) because both are searched
    # by containment and fed straight into the search index as multi-value facets; a JSON blob
    # would need casting at every read and cannot take a GIN index usefully.
    search_aliases = pg_fields.ArrayField(
        django_db_models.TextField(),
        default=list,
        blank=True,
        help_text="What a customer would type: short forms, misspellings, distributor abbreviations.",
    )
    use_case_tags = pg_fields.ArrayField(django_db_models.TextField(), default=list, blank=True)
    tier = django_db_models.CharField(max_length=16, choices=TIER_CHOICES, null=True, blank=True)
    noise_level = django_db_models.CharField(max_length=16, choices=NOISE_CHOICES, null=True, blank=True)
    # NULL means unknown on every flag below -- see the class docstring. is_3pmsf especially.
    is_3pmsf = django_db_models.BooleanField(null=True, blank=True)
    is_ms = django_db_models.BooleanField(null=True, blank=True)
    is_run_flat = django_db_models.BooleanField(null=True, blank=True)
    is_studdable = django_db_models.BooleanField(null=True, blank=True)
    has_reinforced_sidewall = django_db_models.BooleanField(null=True, blank=True)

    # ---- from distributor structured fields where available ----------------------------------
    tread_depth_32nds = django_db_models.PositiveSmallIntegerField(null=True, blank=True)
    max_psi = django_db_models.PositiveSmallIntegerField(
        null=True,
        blank=True,
        help_text="Per tire, from the product's own data. NEVER derived from load range.",
    )
    rim_width_min_in = django_db_models.DecimalField(max_digits=4, decimal_places=1, null=True, blank=True)
    rim_width_max_in = django_db_models.DecimalField(max_digits=4, decimal_places=1, null=True, blank=True)
    utqg_treadwear = django_db_models.PositiveSmallIntegerField(null=True, blank=True)
    utqg_traction = django_db_models.CharField(max_length=4, null=True, blank=True)
    utqg_temperature = django_db_models.CharField(max_length=4, null=True, blank=True)

    # ---- provenance ---------------------------------------------------------------------------
    llm_confidence = django_db_models.DecimalField(max_digits=3, decimal_places=2, null=True, blank=True)
    llm_reason = django_db_models.TextField(null=True, blank=True)
    llm_model_used = django_db_models.CharField(max_length=64, null=True, blank=True)
    size_disputed = django_db_models.BooleanField(
        default=False,
        help_text="Parser and model disagree, or two providers describe different sizes. Specs are written anyway; review them.",
    )
    category_reconciled = django_db_models.BooleanField(
        default=False,
        help_text="tread_category was overwritten by the per-model majority vote rather than this SKU's own answer.",
    )
    enriched_at = django_db_models.DateTimeField(default=timezone.now)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "tire_specs"
        indexes = [
            # The fitment query: "what fits a 18" rim at 33" tall".
            django_db_models.Index(fields=["rim_diameter_in", "overall_diameter_in"], name="tire_specs_diameter_idx"),
            django_db_models.Index(fields=["size_display"], name="tire_specs_size_display_idx"),
            # Drives the reconciliation pass's per-model vote and the review queue.
            django_db_models.Index(fields=["model_name"], name="tire_specs_model_name_idx"),
            django_db_models.Index(fields=["tread_category"], name="tire_specs_tread_category_idx"),
        ]
        constraints = [
            django_db_models.CheckConstraint(
                check=django_db_models.Q(overall_diameter_in__gt=0),
                name="tire_specs_overall_diameter_positive",
            ),
            django_db_models.CheckConstraint(
                check=django_db_models.Q(rim_diameter_in__gt=0),
                name="tire_specs_rim_diameter_positive",
            ),
            django_db_models.CheckConstraint(
                # A tire is always taller than the wheel it mounts on. Cheap tripwire against a
                # transposed pair reaching the table.
                check=django_db_models.Q(overall_diameter_in__gt=django_db_models.F("rim_diameter_in")),
                name="tire_specs_taller_than_rim",
            ),
        ]

    def __str__(self):
        return f"{self.size_display} {self.model_name or '(unidentified)'}"


class FacetConfig(django_db_models.Model):
    """
    Which facets a search mode exposes, in what order, with what labels and widgets.

    The server owns this so the FE's facet rail changes without a client deploy: reordering
    ``tread_category`` above ``rim_diameter_in``, relabelling ``MT`` from "MT" to "Mud terrain",
    or collapsing a long list after 8 entries are all data edits here, not releases.

    ``field`` **must be a filterable attribute of that mode's index** -- a facet the index cannot
    facet on returns nothing and looks like empty inventory. For tire mode that means
    ``src.search.tires_index.FILTERABLE_ATTRIBUTES``; the search service validates it on load
    rather than trusting the row.

    ``value_labels`` maps raw index values to display text (``{"MT": "Mud terrain"}``). It is
    deliberately not a FK to ``tread_category``: most facet fields are not categories at all, and
    a single JSON column beats a per-field lookup table for every one of them.
    """

    MODE_TIRE = "tire"
    MODE_WHEEL = "wheel"
    MODE_PART = "part"
    MODE_CHOICES = [(MODE_TIRE, "Tire"), (MODE_WHEEL, "Wheel"), (MODE_PART, "Part")]

    WIDGET_MULTISELECT = "multiselect"
    WIDGET_RANGE = "range"
    WIDGET_TOGGLE = "toggle"
    WIDGET_CHOICES = [
        (WIDGET_MULTISELECT, "Multi-select"),
        (WIDGET_RANGE, "Range"),
        (WIDGET_TOGGLE, "Toggle"),
    ]

    mode = django_db_models.CharField(max_length=16, choices=MODE_CHOICES)
    field = django_db_models.CharField(max_length=64)
    label = django_db_models.CharField(max_length=64)
    widget = django_db_models.CharField(max_length=16, choices=WIDGET_CHOICES)
    sort_order = django_db_models.PositiveSmallIntegerField()
    collapse_after = django_db_models.PositiveSmallIntegerField(
        default=8,
        help_text="Show this many values before a 'show more' control.",
    )
    unit = django_db_models.CharField(max_length=16, null=True, blank=True)
    value_labels = django_db_models.JSONField(
        null=True,
        blank=True,
        help_text='Raw index value -> display text, e.g. {"MT": "Mud terrain"}.',
    )

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "facet_config"
        unique_together = [["mode", "field"]]
        ordering = ["mode", "sort_order"]
        constraints = [
            django_db_models.CheckConstraint(
                # Literals, not the class constants: a nested Meta body cannot see them.
                check=django_db_models.Q(widget__in=["multiselect", "range", "toggle"]),
                name="facet_config_widget_valid",
            ),
        ]

    def __str__(self):
        return f"{self.mode}.{self.field} ({self.widget})"


class SimpleTireSku(django_db_models.Model):
    """
    One purchasable tire SKU exactly as simpletire.com publishes it -- a competitor-catalog
    snapshot, deliberately **not** joined to ``MasterPart`` or ``TireSpec``.

    Flat on purpose. Brand and product-line columns repeat on every SKU row because this table is
    a scrape landing zone, not a catalog: nothing in the app reads it yet, and the one question it
    has to answer cheaply ("what does SimpleTire list, at what price, in what size") is a single
    filter over one table. Normalize it later, from this data, if a consumer ever needs to.

    Where the values come from
    --------------------------
    SimpleTire renders its PDP from ``GET /api/product-detail``, which returns JSON -- the HTML
    page merely embeds the same payload in the React flight stream. So this table is populated
    from the JSON, and "parsing the page" means parsing that object, not the markup. Three blocks
    of it matter, and ``src.integrations.services.simpletire`` maps them here:

    ``siteProductLine``             brand + model, identical across every SKU of a line
    ``siteProductLineSizeDetail``   the *selected* SKU: price, stock, scores, fees
    ``siteProductSpecs``            the *selected* SKU's spec sheet -- load index, UTQG, weight...

    The spec sheet is per-SKU, not per-model: 265/70R18 and 225/65R16 of the same tire report
    different load indexes, weights and overall diameters. That is why the crawler issues one
    request per size rather than one per model, and why every ``spec_*`` column below is a
    property of this row alone.

    Typed columns vs. raw blobs
    ---------------------------
    Every typed column is a parse of a display string SimpleTire chose for humans
    (``'2756 lbs (116)'``, ``'460AA'``, ``'32.6"'``). Those strings are not a contract and will
    drift, so the untouched source objects are kept in ``raw_specs`` / ``raw_size`` /
    ``raw_size_detail`` / ``raw_product_line``. **A parser fix must never require a re-crawl** --
    re-derive the columns from the blobs instead.

    NULL means "SimpleTire did not publish it". Never 0, never False. ``spec_is_3pmsf`` in
    particular is a certification claim: a missing spec line is unknown, not "not certified".

    Prices are integer cents, as the API sends them. Do not introduce a float here.

    ``item_id`` is SimpleTire's own SKU id and the natural key -- the upsert conflict target,
    which is what makes a resumed or repeated crawl idempotent.
    """

    # ---- identity -----------------------------------------------------------------------------
    item_id = django_db_models.BigIntegerField(
        unique=True,
        help_text="SimpleTire's SKU id (siteProductLineSizeDetail.id). Natural key; upsert target.",
    )
    part_number = django_db_models.CharField(
        max_length=255,
        null=True,
        blank=True,
        db_index=True,
        help_text="Manufacturer part number / MPN as SimpleTire lists it. Not unique: two brands can collide.",
    )
    product_line_id = django_db_models.IntegerField(
        null=True,
        blank=True,
        db_index=True,
        help_text="SimpleTire's model id. Shared by every SKU of the same tire model.",
    )

    # ---- provenance: exactly what was requested to produce this row ----------------------------
    brand_slug = django_db_models.CharField(max_length=128, db_index=True)
    product_line_slug = django_db_models.CharField(max_length=255, db_index=True)
    page_url = django_db_models.TextField(help_text="Human-facing PDP the row came from.")

    # ---- brand / model (from siteProductLine; identical across the line) ------------------------
    brand_name = django_db_models.CharField(max_length=255, null=True, blank=True, db_index=True)
    brand_tier = django_db_models.PositiveSmallIntegerField(
        null=True,
        blank=True,
        help_text="SimpleTire's own 1-3 brand ranking. Their editorial opinion, not a fact about the brand.",
    )
    brand_logo_url = django_db_models.TextField(null=True, blank=True)
    product_line_name = django_db_models.CharField(max_length=255, null=True, blank=True)
    product_line_overview = django_db_models.TextField(
        null=True,
        blank=True,
        help_text="Marketing copy. Contains HTML (<ul>/<b>) -- escape before rendering.",
    )
    product_line_image_url = django_db_models.TextField(null=True, blank=True)
    starting_price_cents = django_db_models.IntegerField(
        null=True,
        blank=True,
        help_text="Cheapest SKU in the line at scrape time -- a line-level figure, repeated on each row.",
    )

    # ---- size ----------------------------------------------------------------------------------
    size_display = django_db_models.CharField(
        max_length=64,
        null=True,
        blank=True,
        db_index=True,
        help_text="As shown: '265/70R18', 'LT285/75R16', '11R22.5', '18x9.50-8'. Not normalized.",
    )
    tire_size_slug = django_db_models.CharField(
        max_length=64,
        null=True,
        blank=True,
        help_text="SimpleTire's URL form ('265-70rr18'). Required, with item_id, to re-fetch this SKU.",
    )
    load_speed_rating = django_db_models.CharField(
        max_length=16,
        null=True,
        blank=True,
        help_text="Combined as printed: '116S', '106/104T'.",
    )
    load_range = django_db_models.CharField(max_length=16, null=True, blank=True)
    rim_diameter_in = django_db_models.DecimalField(max_digits=5, decimal_places=2, null=True, blank=True)
    product_type_id = django_db_models.PositiveSmallIntegerField(null=True, blank=True)
    product_sub_type = django_db_models.CharField(
        max_length=64,
        null=True,
        blank=True,
        db_index=True,
        help_text="Passenger / Light Truck / Commercial / Trailer / ATV-UTV / Farm / OTR / ...",
    )

    # ---- availability & price (cents, as sent) --------------------------------------------------
    product_status = django_db_models.CharField(
        max_length=64,
        null=True,
        blank=True,
        db_index=True,
        help_text="ProductStatusAvailable / ProductStatusOutOfStock. Out-of-stock sizes are largely "
        "absent from the size list, so this is mostly 'Available' plus the fallback SKU of a dead line.",
    )
    quantity = django_db_models.IntegerField(null=True, blank=True, help_text="Units SimpleTire showed as on hand.")
    delivery_days = django_db_models.PositiveSmallIntegerField(null=True, blank=True)
    estimated_retail_price_cents = django_db_models.IntegerField(null=True, blank=True)
    sale_price_cents = django_db_models.IntegerField(null=True, blank=True, db_index=True)
    web_price_cents = django_db_models.IntegerField(null=True, blank=True)
    price_label = django_db_models.CharField(max_length=64, null=True, blank=True, help_text="e.g. '36% off'.")
    road_hazard_price_cents = django_db_models.IntegerField(null=True, blank=True)
    road_hazard_duration_label = django_db_models.CharField(max_length=64, null=True, blank=True)
    oversize_fee_cents = django_db_models.IntegerField(null=True, blank=True)
    fet_fee_cents = django_db_models.IntegerField(
        null=True,
        blank=True,
        help_text="Federal Excise Tax. Sent as a number whose unit the API does not state; stored verbatim.",
    )

    # ---- flags (NULL = not published) -----------------------------------------------------------
    is_run_flat = django_db_models.BooleanField(null=True, blank=True)
    is_electric_optimized = django_db_models.BooleanField(null=True, blank=True)
    is_oversized = django_db_models.BooleanField(null=True, blank=True)
    is_installable = django_db_models.BooleanField(null=True, blank=True)

    # ---- SimpleTire's scores (0-10, their proprietary blend) -------------------------------------
    simple_score = django_db_models.DecimalField(max_digits=4, decimal_places=2, null=True, blank=True)
    handling_durability_score = django_db_models.DecimalField(max_digits=4, decimal_places=2, null=True, blank=True)
    longevity_score = django_db_models.DecimalField(max_digits=4, decimal_places=2, null=True, blank=True)
    traction_score = django_db_models.DecimalField(max_digits=4, decimal_places=2, null=True, blank=True)

    # ---- spec sheet, parsed (siteProductSpecs) ---------------------------------------------------
    # Every column here is derived from a display string; the string itself survives in raw_specs.
    spec_category = django_db_models.CharField(
        max_length=64,
        null=True,
        blank=True,
        db_index=True,
        help_text="SimpleTire's tread category: All Season / All Terrain / Winter / Mud Terrain / UHP / ...",
    )
    spec_vehicle = django_db_models.CharField(max_length=64, null=True, blank=True)
    spec_sidewall = django_db_models.CharField(
        max_length=64,
        null=True,
        blank=True,
        help_text="Blackwall / Outlined White Lettering / Tubeless / Tube-Type / Blue Stripe / ...",
    )
    spec_tread_design = django_db_models.CharField(
        max_length=32, null=True, blank=True, help_text="Symmetrical / Asymmetrical / Directional."
    )
    spec_load_range = django_db_models.CharField(
        max_length=32, null=True, blank=True, help_text="Printed form: 'Standard (SL)', 'E (10 Ply)'."
    )
    spec_ply_rating = django_db_models.PositiveSmallIntegerField(
        null=True, blank=True, help_text="Parsed out of 'E (10 Ply)'. NULL for SL/XL, which state no ply count."
    )
    spec_load_index = django_db_models.PositiveSmallIntegerField(
        null=True, blank=True, help_text="Single tire. Dual, when given, goes to spec_load_index_dual."
    )
    spec_load_index_dual = django_db_models.PositiveSmallIntegerField(null=True, blank=True)
    spec_max_load_lb = django_db_models.PositiveIntegerField(null=True, blank=True)
    spec_max_load_dual_lb = django_db_models.PositiveIntegerField(null=True, blank=True)
    spec_speed_rating = django_db_models.CharField(
        max_length=8, null=True, blank=True, help_text="Letter symbol from 'Max Speed', e.g. S, H, W, A8."
    )
    spec_max_speed_mph = django_db_models.PositiveSmallIntegerField(null=True, blank=True)
    spec_tread_depth_32nds = django_db_models.DecimalField(
        max_digits=5, decimal_places=2, null=True, blank=True, help_text="In 32nds, as printed ('11/32nds')."
    )
    spec_overall_diameter_in = django_db_models.DecimalField(max_digits=6, decimal_places=2, null=True, blank=True)
    spec_section_width_in = django_db_models.DecimalField(max_digits=6, decimal_places=2, null=True, blank=True)
    spec_max_psi = django_db_models.PositiveSmallIntegerField(
        null=True, blank=True, help_text="From the 'Inflation Pressure' spec. Never derived from load range."
    )
    spec_rim_width_min_in = django_db_models.DecimalField(max_digits=5, decimal_places=2, null=True, blank=True)
    spec_rim_width_max_in = django_db_models.DecimalField(
        max_digits=5,
        decimal_places=2,
        null=True,
        blank=True,
        help_text="Equal to the min when 'Rim Range' names a single width ('8.25\"').",
    )
    spec_tire_weight_lb = django_db_models.DecimalField(max_digits=7, decimal_places=2, null=True, blank=True)
    spec_utqg = django_db_models.CharField(max_length=16, null=True, blank=True, help_text="Verbatim, e.g. '460AA'.")
    spec_utqg_treadwear = django_db_models.PositiveSmallIntegerField(null=True, blank=True)
    spec_utqg_traction = django_db_models.CharField(max_length=4, null=True, blank=True)
    spec_utqg_temperature = django_db_models.CharField(max_length=4, null=True, blank=True)
    spec_wet_traction = django_db_models.CharField(max_length=8, null=True, blank=True)
    spec_mileage_warranty = django_db_models.CharField(
        max_length=32, null=True, blank=True, help_text="As printed: 'N/A', '65k'."
    )
    spec_mileage_warranty_miles = django_db_models.PositiveIntegerField(
        null=True, blank=True, help_text="'65k' -> 65000. NULL when the line reads N/A."
    )
    spec_is_3pmsf = django_db_models.BooleanField(
        null=True, blank=True, help_text="Three-Peak Mountain Snowflake. NULL = unpublished, not 'uncertified'."
    )
    spec_is_studdable = django_db_models.BooleanField(null=True, blank=True)
    spec_commercial_position = django_db_models.CharField(
        max_length=32, null=True, blank=True, help_text="Steer / Drive / Trailer / All Position."
    )
    spec_commercial_application = django_db_models.CharField(
        max_length=32, null=True, blank=True, help_text="Urban / Regional / Long Haul / Mixed Service."
    )
    spec_smartway_verified = django_db_models.CharField(max_length=32, null=True, blank=True)

    # ---- untouched source objects ----------------------------------------------------------------
    raw_specs = django_db_models.JSONField(
        null=True,
        blank=True,
        encoder=DjangoJSONEncoder,
        help_text="siteProductSpecs verbatim: [{name, values, description, cta, flair}, ...].",
    )
    specs_map = django_db_models.JSONField(
        null=True,
        blank=True,
        encoder=DjangoJSONEncoder,
        help_text="raw_specs flattened to {spec name: joined value} -- the shape to query when a spec has no column.",
    )
    raw_size = django_db_models.JSONField(
        null=True,
        blank=True,
        encoder=DjangoJSONEncoder,
        help_text="This SKU's siteProductLineAvailableSizeList entry, incl. its own thin specList.",
    )
    raw_size_detail = django_db_models.JSONField(
        null=True, blank=True, encoder=DjangoJSONEncoder, help_text="siteProductLineSizeDetail verbatim."
    )
    raw_product_line = django_db_models.JSONField(
        null=True,
        blank=True,
        encoder=DjangoJSONEncoder,
        help_text="siteProductLine verbatim, minus the hero/CMS image fields nothing will ever read.",
    )

    scraped_at = django_db_models.DateTimeField(
        default=timezone.now, db_index=True, help_text="When this row was last fetched. Drives --resume."
    )
    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "simpletire_skus"
        indexes = [
            django_db_models.Index(fields=["brand_slug", "product_line_slug"], name="simpletire_brand_line_idx"),
            django_db_models.Index(fields=["size_display", "brand_name"], name="simpletire_size_brand_idx"),
        ]

    def __str__(self):
        return f"{self.brand_name} {self.product_line_name} {self.size_display} ({self.part_number})"

