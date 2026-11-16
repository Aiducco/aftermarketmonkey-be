# Plan expiry / downgrade (see src.api.services.plan_lifecycle). Every column is additive and
# either nullable or carries a default, so it applies to the existing rows with no backfill:
# nothing is paused, suspended or downgraded until the demote_expired_trials command next runs
# against a trial that has actually ended.
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("src", "0248_live_inventory_house_account_audit"),
    ]

    operations = [
        migrations.AddField(
            model_name="company",
            name="keep_integration_ids",
            field=models.JSONField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="company",
            name="keep_integration_ids_source",
            field=models.CharField(blank=True, max_length=8, null=True),
        ),
        migrations.AddField(
            model_name="company",
            name="connections_locked_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="company",
            name="downgraded_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="company",
            name="downgraded_from_plan",
            field=models.CharField(blank=True, max_length=32, null=True),
        ),
        migrations.AddField(
            model_name="company",
            name="downgrade_acknowledged_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="company",
            name="warning_emails_sent",
            field=models.JSONField(blank=True, default=dict),
        ),
        migrations.AddField(
            model_name="companyproviders",
            name="paused_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="companyproviders",
            name="paused_reason",
            field=models.CharField(blank=True, max_length=32, null=True),
        ),
        migrations.AddField(
            model_name="userprofile",
            name="suspended_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="userprofile",
            name="suspended_reason",
            field=models.CharField(blank=True, max_length=32, null=True),
        ),
    ]
