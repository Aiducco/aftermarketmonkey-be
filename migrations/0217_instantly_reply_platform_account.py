# Tracks the AftermarketScout account auto-provisioned for a cold-reply deal's company (Day 0 of
# the rescue process, automated rather than done by hand) -- see InstantlyReply's own docstring
# and instantly_freshsales_sync.provision_platform_account_for_reply.
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0216_atd_models"),
    ]

    operations = [
        migrations.AddField(
            model_name="instantlyreply",
            name="platform_company_id",
            field=models.IntegerField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="instantlyreply",
            name="platform_user_id",
            field=models.IntegerField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="instantlyreply",
            name="platform_account_created_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
    ]
