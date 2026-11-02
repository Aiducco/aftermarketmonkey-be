import django.db.models.deletion
from django.core.serializers.json import DjangoJSONEncoder
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("src", "0239_randys_markup_percent_applied"),
    ]

    operations = [
        migrations.CreateModel(
            name="ExternalPurchaseOrder",
            fields=[
                (
                    "id",
                    models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID"),
                ),
                ("distributor_order_number", models.CharField(max_length=128)),
                ("customer_po_number", models.CharField(blank=True, max_length=128, null=True)),
                ("origin_signal", models.PositiveSmallIntegerField()),
                ("origin_signal_name", models.CharField(max_length=32)),
                ("distributor_status", models.CharField(blank=True, max_length=64, null=True)),
                ("order_date", models.DateField(blank=True, null=True)),
                ("ship_to_name", models.CharField(blank=True, max_length=255, null=True)),
                ("ship_to_address1", models.CharField(blank=True, max_length=255, null=True)),
                ("ship_to_address2", models.CharField(blank=True, max_length=255, null=True)),
                ("ship_to_city", models.CharField(blank=True, max_length=128, null=True)),
                ("ship_to_state", models.CharField(blank=True, max_length=64, null=True)),
                ("ship_to_postal_code", models.CharField(blank=True, max_length=32, null=True)),
                ("ship_to_country", models.CharField(blank=True, max_length=64, null=True)),
                ("ship_to_phone", models.CharField(blank=True, max_length=32, null=True)),
                ("subtotal", models.DecimalField(blank=True, decimal_places=2, max_digits=12, null=True)),
                ("freight", models.DecimalField(blank=True, decimal_places=2, max_digits=12, null=True)),
                ("total", models.DecimalField(blank=True, decimal_places=2, max_digits=12, null=True)),
                ("line_items", models.JSONField(blank=True, default=list, encoder=DjangoJSONEncoder)),
                ("raw_response", models.JSONField(blank=True, encoder=DjangoJSONEncoder, null=True)),
                ("discovered_at", models.DateTimeField(auto_now_add=True)),
                ("last_synced_at", models.DateTimeField(auto_now=True)),
                (
                    "company",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="external_purchase_orders",
                        to="src.company",
                    ),
                ),
                (
                    "company_provider",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="external_purchase_orders",
                        to="src.companyproviders",
                    ),
                ),
                (
                    "order_account",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="external_purchase_orders",
                        to="src.companyproviderorderaccount",
                    ),
                ),
            ],
            options={
                "db_table": "external_purchase_orders",
                "unique_together": {("order_account", "distributor_order_number")},
            },
        ),
        migrations.CreateModel(
            name="ExternalOrderSyncWatermark",
            fields=[
                (
                    "id",
                    models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID"),
                ),
                ("last_synced_through", models.DateTimeField(blank=True, null=True)),
                ("last_run_at", models.DateTimeField(blank=True, null=True)),
                ("last_run_ok", models.BooleanField(default=True)),
                ("last_error", models.TextField(blank=True, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "order_account",
                    models.OneToOneField(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="external_order_sync_watermark",
                        to="src.companyproviderorderaccount",
                    ),
                ),
            ],
            options={"db_table": "external_order_sync_watermarks"},
        ),
        migrations.AddIndex(
            model_name="externalpurchaseorder",
            index=models.Index(fields=["company", "order_date"], name="ext_po_company_date_idx"),
        ),
    ]
