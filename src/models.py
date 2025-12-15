import enum
from django.contrib.auth import models as auth_models
from django.db import models as django_db_models

class Company(django_db_models.Model):
    name = django_db_models.CharField(max_length=255)
    slug = django_db_models.CharField(max_length=255)
    status = django_db_models.PositiveSmallIntegerField()
    status_name = django_db_models.CharField(max_length=255)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "company"
        unique_together = ["slug"]


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
    source_data = django_db_models.JSONField()
    source_external_id = django_db_models.CharField(max_length=255)
    brand_id = django_db_models.ForeignKey(Brands, on_delete=django_db_models.CASCADE, related_name="parts")

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "company_destination_parts"
        # unique_together = ["company_destination"]

class CompanyDestinationPartsHistory(django_db_models.Model):
    destination_part = django_db_models.ForeignKey(CompanyDestinationParts, on_delete=django_db_models.CASCADE, related_name="history")
    data = django_db_models.JSONField()

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "company_destination_parts_history"
        # unique_together = ["company_destination"]

class UserProfile(django_db_models.Model):
    user = django_db_models.OneToOneField(
        auth_models.User, on_delete=django_db_models.CASCADE, related_name="profile"
    )
    company = django_db_models.ForeignKey(
        Company,
        on_delete=django_db_models.CASCADE,
        related_name="user_profiles"
    )

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
    type = django_db_models.CharField(max_length=255, null=True)
    purchase_cost = django_db_models.DecimalField(max_digits=10, decimal_places=2, null=True)
    has_map = django_db_models.BooleanField(default=False)
    can_purchase = django_db_models.BooleanField(default=False)
    pricelists = django_db_models.JSONField(null=True)

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "turn14_brand_pricing"
        unique_together = ["external_id"]


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