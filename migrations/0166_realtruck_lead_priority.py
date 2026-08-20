from django.db import migrations, models


class Migration(migrations.Migration):
    """
    Outreach prioritisation columns on ``realtruck_leads``.

    Qualified tells you a lead is worth contacting; it does not tell you who to contact first.
    These carry a 0-100 composite score built from an LLM read of the dealer's website plus hard
    signals already in the table (how many locations share the domain, RealTruck's own preferred /
    double-warranty / next-gen flags, brand_count).

    Hand-written -- makemigrations on this repo bundles unrelated drift (see 0163).
    """

    dependencies = [("src", "0165_realtruck_lead_email")]

    operations = [
        migrations.AddField(model_name="realtrucklead", name="outreach_priority",
                            field=models.IntegerField(blank=True, null=True)),
        migrations.AddField(model_name="realtrucklead", name="priority_tier",
                            field=models.CharField(blank=True, max_length=1, null=True)),
        migrations.AddField(model_name="realtrucklead", name="website_quality",
                            field=models.IntegerField(blank=True, null=True)),
        migrations.AddField(model_name="realtrucklead", name="location_count",
                            field=models.IntegerField(blank=True, null=True)),
        migrations.AddField(model_name="realtrucklead", name="priority_signals",
                            field=models.JSONField(blank=True, default=dict)),
        migrations.AddField(model_name="realtrucklead", name="priority_reasoning",
                            field=models.TextField(blank=True, null=True)),
        migrations.AddField(model_name="realtrucklead", name="prioritized_at",
                            field=models.DateTimeField(blank=True, null=True)),
    ]
