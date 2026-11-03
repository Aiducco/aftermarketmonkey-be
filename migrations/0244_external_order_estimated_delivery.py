# When the distributor says the order should arrive. Purely additive: one nullable date column,
# no backfill -- the value arrives on the next sync, which re-reads every order in its window.
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("src", "0243_external_order_unique_per_connection"),
    ]

    operations = [
        migrations.AddField(
            model_name="externalpurchaseorder",
            name="estimated_delivery_date",
            field=models.DateField(blank=True, null=True),
        ),
    ]
