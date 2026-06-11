from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0076_texas_zip_codes_and_leads"),
    ]

    operations = [
        migrations.AddField(
            model_name="lead",
            name="emails",
            field=models.JSONField(blank=True, default=list),
        ),
    ]
