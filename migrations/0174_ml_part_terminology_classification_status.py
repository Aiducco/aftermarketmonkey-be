# Hand-written: 0173's on-disk file was edited (status field added) after it had already been
# applied against the real database, so Django's migration-state bookkeeping believes status was
# always part of 0173's CreateModel and makemigrations generates no AddField for it. The real
# table is missing the column, so this adds it directly. See models.py's
# MLPartTerminologyClassification.status for the field definition this must match.

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("src", "0173_ml_part_terminology_classification"),
    ]

    operations = [
        migrations.AddField(
            model_name="mlpartterminologyclassification",
            name="status",
            field=models.CharField(default="classified", max_length=16),
        ),
    ]
