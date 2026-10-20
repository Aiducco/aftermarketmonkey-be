# Backoff state for the feed connectivity check (check_company_provider_connections), so a
# connection whose credentials the distributor keeps rejecting stops re-authenticating against
# that distributor every 5 minutes forever -- see CompanyProviders.status_consecutive_failures.
#
# Hand-written rather than generated: `makemigrations` on this project also picks up
# unrelated BigAutoField/unique_together drift across a dozen older models, which does not
# belong here.
#
# Defaults match "never failed, check on the normal cadence", so every existing row -- including
# ones currently sitting in FAILING -- starts un-deferred and gets one more immediate check
# before any backoff can build up.
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0219_deal_notes_and_signup_deals"),
    ]

    operations = [
        migrations.AddField(
            model_name="companyproviders",
            name="status_consecutive_failures",
            field=models.PositiveSmallIntegerField(default=0),
        ),
        migrations.AddField(
            model_name="companyproviders",
            name="status_next_check_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
    ]
