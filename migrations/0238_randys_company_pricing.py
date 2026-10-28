from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):
    dependencies = [
        ("src", "0237_randys_models"),
    ]

    operations = [
        migrations.CreateModel(
            name="RandysCompanyPricing",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True, primary_key=True, serialize=False, verbose_name="ID"
                    ),
                ),
                (
                    "cost",
                    models.DecimalField(blank=True, decimal_places=2, max_digits=12, null=True),
                ),
                (
                    "jobber_price",
                    models.DecimalField(blank=True, decimal_places=2, max_digits=12, null=True),
                ),
                (
                    "map_price",
                    models.DecimalField(blank=True, decimal_places=2, max_digits=12, null=True),
                ),
                (
                    "retail_price",
                    models.DecimalField(blank=True, decimal_places=2, max_digits=12, null=True),
                ),
                ("cost_source", models.CharField(blank=True, max_length=16, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "company",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="randys_company_pricing",
                        to="src.company",
                    ),
                ),
                (
                    "part",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="company_pricing",
                        to="src.randyspart",
                    ),
                ),
            ],
            options={
                "db_table": "randys_company_pricing",
                "unique_together": {("part", "company")},
            },
        ),
    ]
