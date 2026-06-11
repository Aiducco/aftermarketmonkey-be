from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0079_lead_website_live"),
    ]

    operations = [
        migrations.AddField(
            model_name="lead",
            name="is_qualified",
            field=models.BooleanField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="lead",
            name="business_typology",
            field=models.CharField(blank=True, max_length=64, null=True),
        ),
        migrations.AddField(
            model_name="lead",
            name="confidence_score",
            field=models.IntegerField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="lead",
            name="brands_mentioned",
            field=models.JSONField(blank=True, default=list),
        ),
        migrations.AddField(
            model_name="lead",
            name="ai_reasoning",
            field=models.TextField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="lead",
            name="ai_qualified_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
    ]
