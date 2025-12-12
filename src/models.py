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

    created_at = django_db_models.DateTimeField(auto_now_add=True)
    updated_at = django_db_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "providers"
        unique_together = ["name", "type"]


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