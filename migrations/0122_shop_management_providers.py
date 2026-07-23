import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0121_po_customer_po_name"),
    ]

    operations = [
        migrations.CreateModel(
            name="ShopManagementProviders",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("name", models.CharField(max_length=255)),
                ("status", models.PositiveSmallIntegerField()),
                ("status_name", models.CharField(max_length=255)),
                ("kind", models.PositiveSmallIntegerField()),
                ("kind_name", models.CharField(max_length=255)),
                ("coming_soon", models.BooleanField(default=False)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "db_table": "shop_management_providers",
                "unique_together": {("kind",)},
            },
        ),
        migrations.CreateModel(
            name="CompanyShopManagementProviders",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("credentials", models.JSONField()),
                ("active", models.BooleanField(default=True)),
                ("status", models.PositiveSmallIntegerField(blank=True, null=True)),
                ("status_name", models.CharField(blank=True, max_length=32, null=True)),
                ("status_reason", models.TextField(blank=True, null=True)),
                ("status_checked_at", models.DateTimeField(blank=True, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "company",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="shop_management_providers",
                        to="src.company",
                    ),
                ),
                (
                    "provider",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="company_connections",
                        to="src.shopmanagementproviders",
                    ),
                ),
            ],
            options={
                "db_table": "company_shop_management_providers",
                "unique_together": {("company", "provider")},
            },
        ),
    ]
