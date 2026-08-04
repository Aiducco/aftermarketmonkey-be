from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0143_master_parts_normalized_part_number_index"),
    ]

    operations = [
        migrations.AddField(
            model_name="companyproviders",
            name="pricing_propagation_watermark",
            field=models.DateTimeField(blank=True, null=True),
        ),
    ]
