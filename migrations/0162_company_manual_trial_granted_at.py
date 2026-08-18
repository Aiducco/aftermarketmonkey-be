# Hand-trimmed from the auto-generated migration: makemigrations picked up a large amount of
# pre-existing drift unrelated to this change (id-field alterations, constraint removals, Meta
# option changes across many unrelated models, plus a spurious LeerLead/RealTruckLead delete
# caused by unrelated concurrent WIP in models.py at the time this was generated). Only the
# Company.manual_trial_granted_at field is kept here.
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("src", "0160_connection_attempt"),
    ]

    operations = [
        migrations.AddField(
            model_name="company",
            name="manual_trial_granted_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
    ]
