from django.core.serializers.json import DjangoJSONEncoder
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0117_company_providers_order_status"),
    ]

    operations = [
        migrations.RemoveField(
            model_name="purchaseorderlineitem",
            name="ship_options",
        ),
        migrations.AddField(
            model_name="purchaseorderlineitem",
            name="shipments",
            field=models.JSONField(
                null=True,
                blank=True,
                encoder=DjangoJSONEncoder,
                help_text=(
                    "Per-shipment breakdown for this line's last quote: [{warehouse_code, "
                    "quantity_confirmed, quantity_backordered, manufacturer_esd, ship_options: "
                    "[{code, name, cost, estimated_delivery_date}]}]. More than one entry means "
                    "the distributor is fulfilling this line from multiple warehouses/shipments."
                ),
            ),
        ),
        migrations.AddField(
            model_name="purchaseorderlineitem",
            name="promotions",
            field=models.JSONField(
                null=True,
                blank=True,
                encoder=DjangoJSONEncoder,
                help_text="Distributor-applied discounts on this line from the last quote: [{description, amount}].",
            ),
        ),
    ]
