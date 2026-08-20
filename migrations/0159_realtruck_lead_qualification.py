from django.db import migrations, models


class Migration(migrations.Migration):
    """
    AI qualification columns on ``realtruck_leads``, mirroring the ones ``Lead`` already carries so
    ``qualify_leads --source realtruck`` writes the same shape and both lead sources report
    identically. All nullable -- ``is_qualified IS NULL AND ai_skip_reason IS NULL`` is what the
    command uses to mean "not looked at yet".
    """

    dependencies = [
        ("src", "0158_realtruck_lead_website_live"),
    ]

    operations = [
        migrations.AddField(
            model_name="realtrucklead",
            name="is_qualified",
            field=models.BooleanField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="realtrucklead",
            name="business_typology",
            field=models.CharField(blank=True, max_length=64, null=True),
        ),
        migrations.AddField(
            model_name="realtrucklead",
            name="confidence_score",
            field=models.IntegerField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="realtrucklead",
            name="brands_mentioned",
            field=models.JSONField(blank=True, default=list),
        ),
        migrations.AddField(
            model_name="realtrucklead",
            name="ai_reasoning",
            field=models.TextField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="realtrucklead",
            name="ai_skip_reason",
            field=models.CharField(blank=True, max_length=255, null=True),
        ),
        migrations.AddField(
            model_name="realtrucklead",
            name="ai_qualified_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
    ]
