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
            field=models.JSONField(null=True, blank=True, encoder=DjangoJSONEncoder),
        ),
        migrations.AddField(
            model_name="purchaseorderlineitem",
            name="promotions",
            field=models.JSONField(null=True, blank=True, encoder=DjangoJSONEncoder),
        ),
    ]
