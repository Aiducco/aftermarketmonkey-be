from django.db import migrations, models


class Migration(migrations.Migration):
    """
    Three columns ``find_missing_websites --source realtruck`` needs for the 2,111 RealTruck
    dealers whose locator entry carries no website.

    Hand-written on purpose: ``makemigrations`` wanted to bundle 31 operations of pre-existing
    repo drift alongside these three -- including RemoveConstraint on ``customintegrationrequest``
    and ``premierbrand``, which would drop unique constraints on unrelated production tables.
    Only the three AddFields below belong to this change.
    """

    dependencies = [
        ("src", "0162_company_manual_trial_granted_at"),
        # RealTruckLead is created in 0158 and extended in 0159; without this the model is
        # absent from migration state here and the AddFields below raise KeyError.
        ("src", "0159_realtruck_lead_qualification"),
    ]

    operations = [
        migrations.AddField(
            model_name="realtrucklead",
            name="website_not_found",
            field=models.BooleanField(blank=True, default=False),
        ),
        migrations.AddField(
            model_name="realtrucklead",
            name="email",
            field=models.EmailField(blank=True, max_length=255, null=True),
        ),
        migrations.AddField(
            model_name="realtrucklead",
            name="emails",
            field=models.JSONField(blank=True, default=list),
        ),
    ]
