# Space allowance (see src.api.services.billing.space_allowance). Both columns are nullable and
# need no backfill: null means "no paid add-on" and "no staff base", so every company keeps its
# plan's base allowance.
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("src", "0249_plan_expiry_downgrade"),
    ]

    operations = [
        migrations.AddField(
            model_name="company",
            name="additional_spaces",
            field=models.PositiveSmallIntegerField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="company",
            name="space_base_override",
            field=models.PositiveSmallIntegerField(blank=True, null=True),
        ),
    ]
