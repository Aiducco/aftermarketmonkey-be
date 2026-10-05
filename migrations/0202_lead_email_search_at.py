# Records that the web-search email hunt (search_lead_emails) has already run for a lead.
#
# Needed because a search that finds nothing leaves the row looking exactly like a row that was
# never searched -- emails=[] and emails_not_found=True, both already set by the earlier scrape.
# Without this column a re-run pays Tavily again for the same 700-odd businesses it already
# failed on, and Tavily is metered per search on a monthly allowance.
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('src', '0201_motorstate_feed_catalog'),
    ]

    operations = [
        migrations.AddField(
            model_name='lead',
            name='email_search_at',
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='realtrucklead',
            name='email_search_at',
            field=models.DateTimeField(blank=True, null=True),
        ),
    ]
