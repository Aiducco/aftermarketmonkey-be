import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0136_tirerack_provider_and_catalog"),
    ]

    operations = [
        migrations.CreateModel(
            name="TireRackCompanyPricing",
            fields=[
                ("id", models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("total_price", models.DecimalField(blank=True, decimal_places=5, max_digits=14, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "company",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="tirerack_company_pricing",
                        to="src.company",
                    ),
                ),
                (
                    "part",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="company_pricing",
                        to="src.tirerackparts",
                    ),
                ),
            ],
            options={
                "db_table": "tirerack_company_pricing",
                "unique_together": {("part", "company")},
            },
        ),
    ]
