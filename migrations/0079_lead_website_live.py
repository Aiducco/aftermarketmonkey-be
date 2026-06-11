from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0078_lead_importance_field"),
    ]

    operations = [
        migrations.AddField(
            model_name="lead",
            name="website_live",
            field=models.BooleanField(blank=True, null=True),
        ),
    ]
