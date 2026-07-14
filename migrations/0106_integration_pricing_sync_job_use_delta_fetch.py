from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0105_company_providers_connection_status"),
    ]

    operations = [
        migrations.AddField(
            model_name="integrationpricingsyncjob",
            name="use_delta_fetch",
            field=models.BooleanField(
                default=False,
                help_text=(
                    "When True (currently only meaningful for Turn 14), the raw fetch uses "
                    "the pricing-changes endpoint scoped to recently changed brands instead "
                    "of a full re-fetch of every mapped brand."
                ),
            ),
        ),
    ]
