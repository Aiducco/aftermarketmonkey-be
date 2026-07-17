from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):
    dependencies = [
        ("src", "0111_po_cart_includes_quoted"),
    ]

    operations = [
        migrations.CreateModel(
            name="CompanyLocation",
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
                ("label", models.CharField(max_length=100)),
                ("name", models.CharField(max_length=255)),
                ("attention", models.CharField(blank=True, max_length=255, null=True)),
                ("address1", models.CharField(max_length=255)),
                ("address2", models.CharField(blank=True, max_length=255, null=True)),
                ("city", models.CharField(max_length=128)),
                ("state", models.CharField(max_length=64)),
                ("postal_code", models.CharField(max_length=32)),
                ("country", models.CharField(max_length=64)),
                ("phone", models.CharField(blank=True, max_length=32, null=True)),
                ("is_primary", models.BooleanField(default=False)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "company",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="locations",
                        to="src.company",
                    ),
                ),
            ],
            options={
                "db_table": "company_locations",
            },
        ),
    ]
