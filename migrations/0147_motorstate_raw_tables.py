# Motor State Distributing raw-feed tables (brands, availability, product detail/pricing).

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):
    dependencies = [
        ("src", "0146_asap_brand_manual_mappings"),
    ]

    operations = [
        migrations.CreateModel(
            name="MotorStateBrand",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("code", models.CharField(max_length=32)),
                ("name", models.CharField(max_length=255, null=True)),
                ("offered", models.BooleanField(default=False)),
                ("is_inventory_available", models.BooleanField(default=False)),
                ("data", models.JSONField(null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "db_table": "motorstate_brands",
                "unique_together": {("code",)},
            },
        ),
        migrations.CreateModel(
            name="MotorStateAvailability",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("part_number", models.CharField(max_length=128)),
                ("brand_code", models.CharField(max_length=32, null=True)),
                ("status_type", models.CharField(max_length=8, null=True)),
                ("quantity_available", models.IntegerField(null=True)),
                ("source_updated_on", models.DateTimeField(null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "company",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="motorstate_availability",
                        to="src.company",
                    ),
                ),
            ],
            options={
                "db_table": "motorstate_availability",
            },
        ),
        migrations.CreateModel(
            name="MotorStateProduct",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("part_number", models.CharField(max_length=128)),
                ("found", models.BooleanField(default=False)),
                ("vendor_part_number", models.CharField(max_length=128, null=True)),
                ("supersede_part_number", models.CharField(max_length=128, null=True)),
                ("short_description", models.TextField(null=True)),
                ("status", models.IntegerField(null=True)),
                ("is_stocking", models.BooleanField(default=False)),
                ("quantity", models.IntegerField(null=True)),
                ("customer_price", models.DecimalField(decimal_places=2, max_digits=12, null=True)),
                (
                    "customer_price_non_promotional",
                    models.DecimalField(decimal_places=2, max_digits=12, null=True),
                ),
                ("base_price", models.DecimalField(decimal_places=2, max_digits=12, null=True)),
                ("list_price", models.DecimalField(decimal_places=2, max_digits=12, null=True)),
                ("map_price", models.DecimalField(decimal_places=2, max_digits=12, null=True)),
                ("is_map_restricted", models.BooleanField(default=False)),
                ("can_special_order", models.BooleanField(default=False)),
                ("can_drop_ship", models.BooleanField(default=False)),
                ("can_regular_back_order", models.BooleanField(default=False)),
                ("special_order_charge", models.DecimalField(decimal_places=2, max_digits=12, null=True)),
                ("drop_ship_charge", models.DecimalField(decimal_places=2, max_digits=12, null=True)),
                ("data", models.JSONField(null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "company",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="motorstate_products",
                        to="src.company",
                    ),
                ),
            ],
            options={
                "db_table": "motorstate_products",
                "unique_together": {("company", "part_number")},
            },
        ),
        migrations.AddIndex(
            model_name="motorstateavailability",
            index=models.Index(
                fields=["company", "source_updated_on"],
                name="motorstate__company_a2bf3a_idx",
            ),
        ),
        migrations.AddIndex(
            model_name="motorstateavailability",
            index=models.Index(
                fields=["company", "brand_code"],
                name="motorstate__company_839dcf_idx",
            ),
        ),
        migrations.AlterUniqueTogether(
            name="motorstateavailability",
            unique_together={("company", "part_number")},
        ),
    ]
