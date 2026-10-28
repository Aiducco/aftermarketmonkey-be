from django.db import migrations, models
import django.db.models.deletion


def create_xdp_provider(apps, schema_editor):
    """
    XDP (BrandProviderKind.XDP == 33) already has a Providers row in production -- it was offered
    in the connectable catalog until 2026-10-13 with no backend behind it, and two companies
    connected to it (see the note in constants.py). get_or_create therefore adopts the existing
    row rather than creating a second one; the defaults only apply on a fresh database.
    """
    Providers = apps.get_model("src", "Providers")
    Providers.objects.get_or_create(
        kind=33,
        defaults={
            "name": "XDP",
            "status": 1,
            "status_name": "ACTIVE",
            "type": 2,
            "type_name": "DISTRIBUTOR",
            "kind_name": "XDP",
        },
    )


def remove_xdp_provider(apps, schema_editor):
    """Deliberately a no-op: the kind=33 row predates this migration and two existing
    CompanyProviders rows point at it. Reversing the schema must not delete it out from under
    them."""
    return None


class Migration(migrations.Migration):
    dependencies = [
        ("src", "0235_drop_motorstate_availability"),
    ]

    operations = [
        migrations.CreateModel(
            name="XdpBrand",
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
                "db_table": "xdp_brands",
            },
        ),
        migrations.CreateModel(
            name="BrandXdpBrandMapping",
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
                        related_name="xdp_brand_mappings",
                        to="src.brands",
                    ),
                ),
                (
                    "xdp_brand",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="brand_mappings",
                        to="src.xdpbrand",
                    ),
                ),
            ],
            options={
                "db_table": "brand_xdp_brand_mapping",
                "unique_together": {("brand", "xdp_brand")},
            },
        ),
        migrations.CreateModel(
            name="XdpPart",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True, primary_key=True, serialize=False, verbose_name="ID"
                    ),
                ),
                ("code", models.CharField(max_length=64, unique=True)),
                ("product_id", models.CharField(blank=True, db_index=True, max_length=64, null=True)),
                ("sku", models.CharField(blank=True, db_index=True, max_length=255, null=True)),
                ("mpn", models.CharField(blank=True, db_index=True, max_length=255, null=True)),
                ("name", models.CharField(blank=True, max_length=512, null=True)),
                ("short_desc", models.TextField(blank=True, null=True)),
                ("url", models.TextField(blank=True, null=True)),
                ("image_url", models.TextField(blank=True, null=True)),
                (
                    "price",
                    models.DecimalField(blank=True, decimal_places=2, max_digits=12, null=True),
                ),
                (
                    "map_price",
                    models.DecimalField(blank=True, decimal_places=2, max_digits=12, null=True),
                ),
                ("in_stock", models.BooleanField(blank=True, null=True)),
                ("limited_stock", models.BooleanField(blank=True, null=True)),
                ("quantity", models.IntegerField(blank=True, null=True)),
                ("quantity_raw", models.IntegerField(blank=True, null=True)),
                (
                    "weight",
                    models.DecimalField(blank=True, decimal_places=2, max_digits=10, null=True),
                ),
                (
                    "width",
                    models.DecimalField(blank=True, decimal_places=2, max_digits=10, null=True),
                ),
                (
                    "length",
                    models.DecimalField(blank=True, decimal_places=2, max_digits=10, null=True),
                ),
                (
                    "height",
                    models.DecimalField(blank=True, decimal_places=2, max_digits=10, null=True),
                ),
                ("application", models.TextField(blank=True, null=True)),
                ("product_type", models.CharField(blank=True, max_length=255, null=True)),
                ("is_variant_parent", models.BooleanField(default=False)),
                ("fitment_paths", models.JSONField(blank=True, null=True)),
                ("raw_data", models.JSONField(blank=True, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "brand",
                    models.ForeignKey(
                        null=True,
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="parts",
                        to="src.xdpbrand",
                    ),
                ),
            ],
            options={
                "db_table": "xdp_parts",
            },
        ),
        migrations.AddIndex(
            model_name="xdppart",
            index=models.Index(fields=["brand", "mpn"], name="xdp_brand_mpn_idx"),
        ),
        migrations.CreateModel(
            name="XdpCompanyPricing",
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
                    "map_price",
                    models.DecimalField(blank=True, decimal_places=2, max_digits=12, null=True),
                ),
                (
                    "retail_price",
                    models.DecimalField(blank=True, decimal_places=2, max_digits=12, null=True),
                ),
                ("quantity", models.IntegerField(blank=True, null=True)),
                (
                    "markup_percent_applied",
                    models.DecimalField(blank=True, decimal_places=2, max_digits=5, null=True),
                ),
                ("cost_source", models.CharField(blank=True, max_length=16, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "part",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="company_pricing",
                        to="src.xdppart",
                    ),
                ),
                (
                    "company",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="xdp_company_pricing",
                        to="src.company",
                    ),
                ),
            ],
            options={
                "db_table": "xdp_company_pricing",
                "unique_together": {("part", "company")},
            },
        ),
        migrations.RunPython(create_xdp_provider, remove_xdp_provider),
    ]
