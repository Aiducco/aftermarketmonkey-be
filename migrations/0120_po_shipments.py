from django.core.serializers.json import DjangoJSONEncoder
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0119_po_distributor_pricing_fees"),
    ]

    operations = [
        migrations.AddField(
            model_name="purchaseorder",
            name="shipments",
            field=models.JSONField(null=True, blank=True, encoder=DjangoJSONEncoder),
        ),
    ]
