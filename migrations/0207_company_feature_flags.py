# Per-company feature gate for staged rollouts -- see CompanyFeatureFlags in src/models.py.
# First use: gating the ShopMonkey shop-management integration to Trident Motorsports only
# (company_id=16, our own internal account) -- see src/api/services/feature_flags.py.
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0206_company_userprofile_crm_mirror"),
    ]

    operations = [
        migrations.CreateModel(
            name="CompanyFeatureFlags",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("feature", models.CharField(max_length=64)),
                ("enabled", models.BooleanField(default=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "company",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="feature_flags",
                        to="src.company",
                    ),
                ),
            ],
            options={
                "db_table": "company_feature_flags",
                "unique_together": {("company", "feature")},
            },
        ),
    ]
