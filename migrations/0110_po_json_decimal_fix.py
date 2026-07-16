# Hand-trimmed from an auto-generated migration: the autodetector also picked up unrelated
# pre-existing model/migration drift (id field types, constraint renames on other models)
# that predates this fix and isn't part of it — only the JSONField encoder fix and the two
# new PurchaseOrderLineItem fields are included here. See 0108_purchase_orders.py for the
# same pattern used the first time this drift showed up.
#
# Root cause of the encoder fix: Turn14's client parses JSON with parse_float=decimal.Decimal
# to preserve money precision, but none of these JSONFields declared an encoder capable of
# serializing Decimal, so Django's default (plain) JSON encoder raised
# "Object of type Decimal is not JSON serializable" on every successful quote save.

import django.core.serializers.json
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("src", "0109_asap_network_models"),
    ]

    operations = [
        migrations.AddField(
            model_name="purchaseorderlineitem",
            name="ship_options",
            field=models.JSONField(
                blank=True,
                encoder=django.core.serializers.json.DjangoJSONEncoder,
                null=True,
            ),
        ),
        migrations.AddField(
            model_name="purchaseorderlineitem",
            name="warehouse_code",
            field=models.CharField(blank=True, max_length=64, null=True),
        ),
        migrations.AlterField(
            model_name="purchaseorder",
            name="quote_raw_response",
            field=models.JSONField(
                blank=True,
                encoder=django.core.serializers.json.DjangoJSONEncoder,
                null=True,
            ),
        ),
        migrations.AlterField(
            model_name="purchaseorderdistributororder",
            name="raw_response",
            field=models.JSONField(
                blank=True,
                encoder=django.core.serializers.json.DjangoJSONEncoder,
                null=True,
            ),
        ),
        migrations.AlterField(
            model_name="purchaseorderdistributororder",
            name="tracking_numbers",
            field=models.JSONField(
                blank=True,
                default=list,
                encoder=django.core.serializers.json.DjangoJSONEncoder,
            ),
        ),
        migrations.AlterField(
            model_name="purchaseordersubmissionattempt",
            name="request_payload",
            field=models.JSONField(
                blank=True,
                encoder=django.core.serializers.json.DjangoJSONEncoder,
                null=True,
            ),
        ),
        migrations.AlterField(
            model_name="purchaseordersubmissionattempt",
            name="response_payload",
            field=models.JSONField(
                blank=True,
                encoder=django.core.serializers.json.DjangoJSONEncoder,
                null=True,
            ),
        ),
    ]
