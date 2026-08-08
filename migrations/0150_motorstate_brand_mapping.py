# Brands <-> MotorStateBrand mapping (master parts sync) + product brand index.

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):
    dependencies = [
        ("src", "0149_elite_wheel_provider"),
    ]

    operations = [
        migrations.CreateModel(
            name="BrandMotorStateBrandMapping",
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
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "brand",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="motorstate_brand_mappings",
                        to="src.brands",
                    ),
                ),
                (
                    "motorstate_brand",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="brand_mappings",
                        to="src.motorstatebrand",
                    ),
                ),
            ],
            options={
                "db_table": "brand_motorstate_brand_mapping",
                "unique_together": {("brand", "motorstate_brand")},
            },
        ),
        migrations.AddIndex(
            model_name="motorstateproduct",
            index=models.Index(
                fields=["company", "brand"], name="ms_products_company_brand_idx"
            ),
        ),
    ]
