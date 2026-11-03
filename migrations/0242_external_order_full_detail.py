# Brings a discovered external order up to parity with what the confirmed-order screen already
# shows for an order we placed ourselves -- invoice numbers, tracking, ship method, and the dated
# status progression. Purely additive: five new columns on external_purchase_orders, all with
# defaults, no backfill. The data itself arrives on the next sync_external_orders run, which
# re-reads every order in its window and upserts in place.
from django.core.serializers.json import DjangoJSONEncoder
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("src", "0241_shopmonkey_sync_multivendor"),
    ]

    operations = [
        migrations.AddField(
            model_name="externalpurchaseorder",
            name="distributor_invoice_numbers",
            field=models.JSONField(blank=True, default=list, encoder=DjangoJSONEncoder),
        ),
        migrations.AddField(
            model_name="externalpurchaseorder",
            name="tracking",
            field=models.JSONField(blank=True, default=list, encoder=DjangoJSONEncoder),
        ),
        migrations.AddField(
            model_name="externalpurchaseorder",
            name="ship_method_code",
            field=models.CharField(blank=True, max_length=16, null=True),
        ),
        migrations.AddField(
            model_name="externalpurchaseorder",
            name="ship_method",
            field=models.CharField(blank=True, max_length=64, null=True),
        ),
        migrations.AddField(
            model_name="externalpurchaseorder",
            name="status_history",
            field=models.JSONField(blank=True, default=list, encoder=DjangoJSONEncoder),
        ),
    ]
