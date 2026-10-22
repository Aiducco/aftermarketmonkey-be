# MasterPart.part_aliases -- extra searchable spellings of a part number, populated from the PACE
# catalog's search-key expansion (see the field's docstring in src/models.py).
#
# Hand-written for the same reason as 0222-0228: makemigrations also wants ~25 unrelated
# AlterField operations from pre-existing model/migration drift elsewhere in src.
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0228_tristate_relay_feed_reshape"),
    ]

    operations = [
        migrations.AddField(
            model_name="masterpart",
            name="part_aliases",
            field=models.JSONField(blank=True, default=list),
        ),
    ]
