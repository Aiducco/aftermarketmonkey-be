# freshsales_deal_note_id: lets push_contacts (every reply) and create_deals (positive replies
# only) share one open-deal note path without double-posting the same reply twice.
# Company.freshsales_deal_id: the deal FRESHSALES_SIGNUP_CREATE_DEALS opens for a platform signup
# -- see platform_crm_sync._push_company.
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0217_instantly_reply_platform_account"),
    ]

    operations = [
        migrations.AddField(
            model_name="instantlyreply",
            name="freshsales_deal_note_id",
            field=models.TextField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="company",
            name="freshsales_deal_id",
            field=models.TextField(blank=True, null=True),
        ),
    ]
