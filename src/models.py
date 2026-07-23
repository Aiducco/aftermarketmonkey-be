import enum
from django.contrib.auth import models as auth_models
from django.core.serializers.json import DjangoJSONEncoder
from django.db import models as django_db_models

class Company(django_db_models.Model):
    name = django_db_models.CharField(max_length=255)
    slug = django_db_models.CharField(max_length=255)
    status = django_db_models.PositiveSmallIntegerField()
    status_name = django_db_models.CharField(max_length=255)

    # Onboarding / B2B fields (Step 2)
    business_type = django_db_models.JSONField(default=list, blank=True)  # list[str], e.g. ["retail_store", "dealership"]
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
    provider = django_db_models.ForeignKey(Providers, on_delete=django_db_models.CASCADE, related_name="brand_providers")

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

    class Meta:
        db_table = "company_providers"
        unique_together = ["company", "provider"]


class IntegrationRequest(django_db_models.Model):
    company = django_db_models.ForeignKey(Company, on_delete=django_db_models.CASCADE, related_name="integration_requests")
    provider = django_db_models.ForeignKey(Providers, on_delete=django_db_models.CASCADE, related_name="requests")
    created_at = django_db_models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "integration_requests"
        unique_together = ["company", "provider"]


class CustomIntegrationRequest(django_db_models.Model):
    company = django_db_models.ForeignKey(Company, on_delete=django_db_models.CASCADE, related_name="custom_integration_requests")
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
    company_destination = django_db_models.ForeignKey(CompanyDestinations, on_delete=django_db_models.CASCADE, related_name="parts_preferences")
    preferences = django_db_models.JSONField()

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "company_destination_parts_preferences"
        unique_together = ["company_destination"]


class CompanyDestinationParts(django_db_models.Model):
    company_destination = django_db_models.ForeignKey(CompanyDestinations, on_delete=django_db_models.CASCADE, related_name="parts")
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
    user = django_db_models.OneToOneField(
        auth_models.User, on_delete=django_db_models.CASCADE, related_name="profile"
    )
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
    company_brand = django_db_models.ForeignKey(CompanyBrands, on_delete=django_db_models.CASCADE, related_name="destinations")
    destination = django_db_models.ForeignKey(CompanyDestinations, on_delete=django_db_models.CASCADE, related_name="company_brands")

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "company_brand_destination"
        unique_together = ["company_brand", "destination"]


class CompanyDestinationExecutionRun(django_db_models.Model):
    company_brand_destination = django_db_models.ForeignKey(CompanyBrandDestination, on_delete=django_db_models.CASCADE, related_name="execution_runs")
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
    destination_part = django_db_models.ForeignKey(CompanyDestinationParts, on_delete=django_db_models.CASCADE, related_name="history")
    execution_run = django_db_models.ForeignKey(CompanyDestinationExecutionRun, on_delete=django_db_models.CASCADE, related_name="history_records", null=True)
    data = django_db_models.JSONField()
    changes = django_db_models.JSONField(null=True)
    synced = django_db_models.BooleanField(default=False)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "company_destination_parts_history"
        # unique_together = ["company_destination"]

class BrandTurn14BrandMapping(django_db_models.Model):
    brand = django_db_models.ForeignKey(Brands, on_delete=django_db_models.CASCADE, related_name="turn14_brand_mappings")
    turn14_brand = django_db_models.ForeignKey(Turn14Brand, on_delete=django_db_models.CASCADE, related_name="brand_mappings")

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
    brand = django_db_models.ForeignKey(Turn14Brand, on_delete=django_db_models.CASCADE, related_name="brand_inventory", null=True)
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
    company_destination = django_db_models.ForeignKey(CompanyDestinations, on_delete=django_db_models.CASCADE, related_name="bigcommerce_parts")

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "bigcommerce_parts"
        unique_together = ["external_id", "sku", "company_destination"]


class BigCommerceBrands(django_db_models.Model):
    external_id = django_db_models.CharField(max_length=255)
    name = django_db_models.TextField(max_length=255)
    brand = django_db_models.ForeignKey(Brands, on_delete=django_db_models.CASCADE, related_name="bigcommerce_brands")
    company_destination = django_db_models.ForeignKey(CompanyDestinations, on_delete=django_db_models.CASCADE, related_name="bigcommerce_brands")

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
    company_destination = django_db_models.ForeignKey(CompanyDestinations, on_delete=django_db_models.CASCADE, related_name="bigcommerce_categories")

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
    sdc_brand = django_db_models.ForeignKey(SDCBrands, on_delete=django_db_models.CASCADE, related_name="brand_mappings")

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
    brand = django_db_models.ForeignKey(Brands, on_delete=django_db_models.CASCADE, related_name="keystone_brand_mappings")
    keystone_brand = django_db_models.ForeignKey(KeystoneBrand, on_delete=django_db_models.CASCADE, related_name="brand_mappings")

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
    brand = django_db_models.ForeignKey(
        PremierBrand, on_delete=django_db_models.CASCADE, related_name="parts"
    )
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
    part = django_db_models.ForeignKey(
        PremierParts, on_delete=django_db_models.CASCADE, related_name="company_pricing"
    )
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
    brand = django_db_models.ForeignKey(
        Brands, on_delete=django_db_models.CASCADE, related_name="meyer_brand_mappings"
    )
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
    brand = django_db_models.ForeignKey(
        MeyerBrand, on_delete=django_db_models.CASCADE, related_name="parts"
    )
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

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "master_parts"
        unique_together = [["brand", "part_number"]]
        indexes = [
            django_db_models.Index(fields=["brand", "sku"], name="master_parts_brand_sku_idx"),
        ]


class ProviderPart(django_db_models.Model):
    master_part = django_db_models.ForeignKey(MasterPart, on_delete=django_db_models.CASCADE, related_name="provider_parts")
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

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "provider_parts"
        unique_together = [["master_part", "provider"]]
        indexes = [
            django_db_models.Index(fields=["category"], name="pp_category_idx"),
            django_db_models.Index(fields=["overview_category"], name="pp_overview_category_idx"),
        ]


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
    company = django_db_models.ForeignKey(Company, on_delete=django_db_models.CASCADE, related_name="provider_part_pricing")
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
        unique_together = [["master_part", "year_start", "year_end", "make", "model", "submodel", "engine", "drive_type"]]


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
    website_not_found = django_db_models.BooleanField(default=False, blank=True)  # True = Tavily+Claude couldn't find one
    website_live = django_db_models.BooleanField(null=True, blank=True)  # None = not checked yet
    emails_not_found = django_db_models.BooleanField(default=False, blank=True)  # True = enrichment tried, nothing found
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
    status = django_db_models.CharField(max_length=32, null=True, blank=True)   # valid, invalid, disposable, unknown, etc.
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


class BrandFilterCache(django_db_models.Model):
    """
    Materialised list of brands that have at least one MasterPart.
    Rebuilt at the end of every master-parts sync so the /parts/search/brands/
    endpoint can do a simple table scan instead of an expensive DISTINCT subquery.
    """

    brand = django_db_models.OneToOneField(
        Brands,
        on_delete=django_db_models.CASCADE,
        related_name="filter_cache",
        primary_key=True,
    )
    name = django_db_models.CharField(max_length=255, db_index=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "brand_filter_cache"
        ordering = ["name"]


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
    company = django_db_models.ForeignKey(
        Company, on_delete=django_db_models.CASCADE, related_name="purchase_orders"
    )
    company_provider = django_db_models.ForeignKey(
        CompanyProviders, on_delete=django_db_models.PROTECT, related_name="purchase_orders"
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
    # would break those lookups. Currently only Turn14OrderAdapter reads this (see
    # _turn14_po_number); Keystone/Meyer/Premier still always send po_number as-is.
    po_name = django_db_models.TextField(null=True, blank=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)
    submitted_at = django_db_models.DateTimeField(null=True, blank=True)

    class Meta:
        db_table = "purchase_orders"
        indexes = [
            django_db_models.Index(fields=["company", "status"], name="po_company_status_idx"),
            django_db_models.Index(fields=["company_provider", "status"], name="po_company_provider_status_idx"),
        ]
        constraints = [
            # At most one open cart (DRAFT=1 or QUOTED=2, see src.enums.PurchaseOrderStatus)
            # per distributor connection at a time — "Add to PO" always finds-or-creates this
            # row. A quote-failed cart (status=FAILED) isn't covered here since that requires
            # a subquery a partial index can't express; the application layer
            # (_get_or_create_draft) is responsible for finding and reusing that row too.
            django_db_models.UniqueConstraint(
                fields=["company", "company_provider"],
                condition=django_db_models.Q(status__in=[1, 2]),
                name="po_one_open_draft_per_company_provider",
            ),
        ]


class PurchaseOrderLineItem(django_db_models.Model):
    purchase_order = django_db_models.ForeignKey(
        PurchaseOrder, on_delete=django_db_models.CASCADE, related_name="line_items"
    )
    provider_part = django_db_models.ForeignKey(
        ProviderPart, on_delete=django_db_models.PROTECT, related_name="po_line_items"
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
    distributor_net_line_total = django_db_models.DecimalField(
        max_digits=12, decimal_places=2, null=True, blank=True
    )

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

    status = django_db_models.PositiveSmallIntegerField()
    status_name = django_db_models.CharField(max_length=32)

    tracking_numbers = django_db_models.JSONField(default=list, blank=True, encoder=DjangoJSONEncoder)
    carrier = django_db_models.CharField(max_length=64, null=True, blank=True)

    raw_response = django_db_models.JSONField(null=True, blank=True, encoder=DjangoJSONEncoder)

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
    purchase_order = django_db_models.ForeignKey(
        PurchaseOrder, on_delete=django_db_models.CASCADE, related_name="jobs"
    )
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