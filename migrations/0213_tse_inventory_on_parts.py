# Move quantity/availability from TSECompanyPricing to TSEParts -- Tri-State's real physical
# warehouse stock is the same regardless of which account's credentials ask (confirmed live),
# unlike account_price/list_price which genuinely are account-specific. Matches the
# KeystoneParts/MeyerParts precedent of carrying quantity on the shared catalog table rather
# than the per-company pricing table. See TSEParts/TSECompanyPricing docstrings in src/models.py.
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0212_tse_models"),
    ]

    operations = [
        migrations.AddField(
            model_name="tseparts",
            name="quantity_available",
            field=models.IntegerField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="tseparts",
            name="orderable",
            field=models.BooleanField(default=True),
        ),
        migrations.AddField(
            model_name="tseparts",
            name="warehouse_availability",
            field=models.JSONField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="tseparts",
            name="inventory_synced_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.RemoveField(
            model_name="tsecompanypricing",
            name="quantity_available",
        ),
        migrations.RemoveField(
            model_name="tsecompanypricing",
            name="orderable",
        ),
    ]
