import django.db.models.deletion
from django.core.serializers.json import DjangoJSONEncoder
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0121_po_customer_po_name"),
    ]

    operations = [
        migrations.CreateModel(
            name="PurchaseOrderInvoice",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("invoice_number", models.CharField(max_length=128)),
                ("invoice_date", models.DateField(blank=True, null=True)),
                ("distributor_order_number", models.CharField(blank=True, max_length=128, null=True)),
                ("website_order_number", models.CharField(blank=True, max_length=128, null=True)),
                ("total_price", models.DecimalField(blank=True, decimal_places=2, max_digits=12, null=True)),
                ("freight", models.DecimalField(blank=True, decimal_places=2, max_digits=12, null=True)),
                ("discount_amount", models.DecimalField(blank=True, decimal_places=2, max_digits=12, null=True)),
                ("paid_amount", models.DecimalField(blank=True, decimal_places=2, max_digits=12, null=True)),
                ("amount_due", models.DecimalField(blank=True, decimal_places=2, max_digits=12, null=True)),
                ("tracking", models.JSONField(blank=True, default=list, encoder=DjangoJSONEncoder)),
                ("comments", models.TextField(blank=True, null=True)),
                ("raw_response", models.JSONField(blank=True, encoder=DjangoJSONEncoder, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "purchase_order",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="invoices",
                        to="src.purchaseorder",
                    ),
                ),
            ],
            options={
                "db_table": "purchase_order_invoices",
                "unique_together": {("purchase_order", "invoice_number")},
            },
        ),
    ]
