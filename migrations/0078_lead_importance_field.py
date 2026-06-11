from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0077_lead_emails_field"),
    ]

    operations = [
        migrations.AddField(
            model_name="lead",
            name="importance",
            field=models.IntegerField(blank=True, null=True),
        ),
    ]
