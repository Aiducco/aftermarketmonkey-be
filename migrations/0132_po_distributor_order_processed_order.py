from django.core.serializers.json import DjangoJSONEncoder
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0131_po_distributor_order_raw_status"),
    ]

    operations = [
        migrations.AddField(
            model_name="purchaseorderdistributororder",
            name="processed_order",
            field=models.JSONField(null=True, blank=True, encoder=DjangoJSONEncoder),
        ),
    ]
