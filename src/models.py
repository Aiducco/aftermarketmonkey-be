import enum

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

    class Meta:
        db_table = "company_destinations"

