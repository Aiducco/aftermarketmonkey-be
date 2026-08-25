import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0175_api_rate_buckets"),
    ]

    operations = [
        migrations.CreateModel(
            name="Turn14DropshipController",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("external_id", models.CharField(max_length=32)),
                ("charges", models.JSONField(null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "db_table": "turn14_dropship_controllers",
                "unique_together": {("external_id",)},
            },
        ),
        migrations.CreateModel(
            name="Turn14ItemShippingEstimate",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("item_external_id", models.CharField(max_length=255)),
                ("can_ship", models.BooleanField(default=False)),
                ("min_rate", models.DecimalField(decimal_places=2, max_digits=10, null=True)),
                ("average_rate", models.DecimalField(decimal_places=2, max_digits=10, null=True)),
                ("max_rate", models.DecimalField(decimal_places=2, max_digits=10, null=True)),
                ("fees", models.JSONField(null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "brand",
                    models.ForeignKey(
                        null=True,
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="item_shipping_estimates",
                        to="src.turn14brand",
                    ),
                ),
            ],
            options={
                "db_table": "turn14_item_shipping_estimates",
                "unique_together": {("item_external_id",)},
            },
        ),
    ]
