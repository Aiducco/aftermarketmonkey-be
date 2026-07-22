from django.core.serializers.json import DjangoJSONEncoder
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0118_po_line_item_shipments"),
    ]

    operations = [
        migrations.AddField(
            model_name="purchaseorder",
            name="distributor_quoted_total",
            field=models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True),
        ),
        migrations.AddField(
            model_name="purchaseorder",
            name="fees",
            field=models.JSONField(null=True, blank=True, encoder=DjangoJSONEncoder),
        ),
        migrations.AddField(
            model_name="purchaseorderlineitem",
            name="distributor_unit_price",
            field=models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True),
        ),
        migrations.AddField(
            model_name="purchaseorderlineitem",
            name="distributor_line_total",
            field=models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True),
        ),
        migrations.AddField(
            model_name="purchaseorderlineitem",
            name="distributor_net_line_total",
            field=models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True),
        ),
        migrations.AddField(
            model_name="purchaseorderlineitem",
            name="is_prop_65",
            field=models.BooleanField(default=False),
        ),
    ]
