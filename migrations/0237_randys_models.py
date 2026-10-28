from django.db import migrations, models
import django.db.models.deletion


def create_randys_provider(apps, schema_editor):
    Providers = apps.get_model("src", "Providers")
    Providers.objects.get_or_create(
        kind=42,
        defaults={
            "name": "RANDYS Worldwide",
            "status": 1,
            "status_name": "ACTIVE",
            "type": 2,
            "type_name": "DISTRIBUTOR",
            "kind_name": "RANDYS_WORLDWIDE",
        },
    )


def remove_randys_provider(apps, schema_editor):
    """
    Reversing drops the Providers row only when nothing points at it. A CompanyProviders row
    surviving a rollback would be orphaned by the delete, so the row is left in place instead --
    same stance as the XDP migration, which adopted a pre-existing row for exactly this reason.
    """
    Providers = apps.get_model("src", "Providers")
    CompanyProviders = apps.get_model("src", "CompanyProviders")
    provider = Providers.objects.filter(kind=42).first()
    if provider is None:
        return
    if CompanyProviders.objects.filter(provider_id=provider.id).exists():
        return
    provider.delete()


class Migration(migrations.Migration):
    dependencies = [
        ("src", "0236_xdp_models"),
    ]

    operations = [
        migrations.CreateModel(
            name="RandysBrand",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True, primary_key=True, serialize=False, verbose_name="ID"
                    ),
                ),
                ("name", models.CharField(max_length=255, unique=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "db_table": "randys_brands",
            },
        ),
        migrations.CreateModel(
            name="BrandRandysBrandMapping",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True, primary_key=True, serialize=False, verbose_name="ID"
                    ),
                ),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "brand",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="randys_brand_mappings",
                        to="src.brands",
                    ),
                ),
                (
                    "randys_brand",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="brand_mappings",
                        to="src.randysbrand",
                    ),
                ),
            ],
            options={
                "db_table": "brand_randys_brand_mapping",
                "unique_together": {("brand", "randys_brand")},
            },
        ),
        migrations.CreateModel(
            name="RandysPart",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True, primary_key=True, serialize=False, verbose_name="ID"
                    ),
                ),
                ("code", models.CharField(max_length=64, unique=True)),
                ("sku", models.CharField(blank=True, db_index=True, max_length=255, null=True)),
                ("title", models.CharField(blank=True, max_length=512, null=True)),
                ("handle", models.CharField(blank=True, max_length=512, null=True)),
                ("url", models.TextField(blank=True, null=True)),
                ("description", models.TextField(blank=True, null=True)),
                ("image_url", models.TextField(blank=True, null=True)),
                ("image_urls", models.JSONField(blank=True, null=True)),
                (
                    "price",
                    models.DecimalField(blank=True, decimal_places=2, max_digits=12, null=True),
                ),
                (
                    "compare_at_price",
                    models.DecimalField(blank=True, decimal_places=2, max_digits=12, null=True),
                ),
                ("in_stock", models.BooleanField(blank=True, null=True)),
                ("quantity", models.IntegerField(blank=True, null=True)),
                ("inventory_synced_at", models.DateTimeField(blank=True, null=True)),
                ("weight_grams", models.IntegerField(blank=True, null=True)),
                ("category", models.CharField(blank=True, max_length=255, null=True)),
                ("sub_category", models.CharField(blank=True, max_length=255, null=True)),
                ("fitment_type", models.CharField(blank=True, max_length=64, null=True)),
                ("differential_codes", models.JSONField(blank=True, null=True)),
                ("differential_brands", models.JSONField(blank=True, null=True)),
                ("wheel_diameter", models.CharField(blank=True, max_length=32, null=True)),
                ("wheel_width", models.CharField(blank=True, max_length=32, null=True)),
                ("wheel_offset", models.CharField(blank=True, max_length=32, null=True)),
                ("wheel_bolt_pattern", models.CharField(blank=True, max_length=64, null=True)),
                ("tags", models.JSONField(blank=True, null=True)),
                ("fitment_rows", models.JSONField(blank=True, null=True)),
                ("fitment_row_count", models.IntegerField(blank=True, null=True)),
                ("fitment_truncated", models.BooleanField(default=False)),
                ("fitment_synced_at", models.DateTimeField(blank=True, null=True)),
                ("raw_data", models.JSONField(blank=True, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "brand",
                    models.ForeignKey(
                        null=True,
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="parts",
                        to="src.randysbrand",
                    ),
                ),
            ],
            options={
                "db_table": "randys_parts",
            },
        ),
        migrations.AddIndex(
            model_name="randyspart",
            index=models.Index(fields=["brand", "sku"], name="randys_brand_sku_idx"),
        ),
        migrations.RunPython(create_randys_provider, remove_randys_provider),
    ]
