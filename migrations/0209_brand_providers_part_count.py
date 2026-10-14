# Precomputed part count on brand_providers -- see that field's own docstring in src/models.py.
# Fixes a real production timeout: the ShopMonkey inventory-sync brand picker was GROUP BY-ing
# the multi-million-row provider_parts table live on every request.
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0208_shopmonkey_sync"),
    ]

    operations = [
        migrations.AddField(
            model_name="brandproviders",
            name="part_count",
            field=models.IntegerField(blank=True, null=True),
        ),
    ]
