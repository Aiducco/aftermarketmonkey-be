from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0080_lead_ai_qualification"),
    ]

    operations = [
        migrations.AddField(
            model_name="lead",
            name="ai_skip_reason",
            field=models.CharField(blank=True, max_length=255, null=True),
        ),
    ]
