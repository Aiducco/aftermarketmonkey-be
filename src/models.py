import enum
from django.contrib.auth import models as auth_models
from django.db import models as django_db_models

class Company(django_db_models.Model):
    name = django_db_models.CharField(max_length=255)
    slug = django_db_models.CharField(max_length=255)
    status = django_db_models.PositiveSmallIntegerField()
    status_name = django_db_models.CharField(max_length=255)

    # Onboarding / B2B fields (Step 2)
    business_type = django_db_models.CharField(max_length=64, null=True, blank=True)
    country = django_db_models.CharField(max_length=64, null=True, blank=True)
    state_province = django_db_models.CharField(max_length=128, null=True, blank=True)
    tax_id = django_db_models.CharField(max_length=64, null=True, blank=True)

    # Onboarding progress: 0=not_started, 1=account_created, 2=company_details, 3=personalization, 4=complete
    onboarding_step = django_db_models.PositiveSmallIntegerField(default=0, null=True, blank=True)

    # Stripe billing
    stripe_customer_id = django_db_models.CharField(max_length=255, null=True, blank=True)

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

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "company_providers"
        unique_together = ["company", "provider"]


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
    Catalog/inventory fields live on WheelProsPart; msrp/map come from each company's SFTP feed.
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