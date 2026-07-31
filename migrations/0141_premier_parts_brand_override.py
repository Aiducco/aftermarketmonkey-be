import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0140_company_provider_order_account_status"),
    ]

    operations = [
        migrations.AddField(
            model_name="premierparts",
            name="brand_override",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name="premier_brand_overrides",
                to="src.brands",
            ),
        ),
    ]
