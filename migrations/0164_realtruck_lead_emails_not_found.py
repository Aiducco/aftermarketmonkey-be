from django.db import migrations, models


class Migration(migrations.Migration):
    """
    ``emails_not_found`` on ``realtruck_leads`` -- the flag ``enrich_lead_emails`` uses to avoid
    re-scraping a site it has already read and found no address on.

    Hand-written rather than generated: ``makemigrations`` on this repo bundles ~31 operations of
    pre-existing drift, including RemoveConstraint on unrelated production tables (see 0163).
    """

    # Converges the three leaf nodes the graph ended up with after 0160_connection_attempt was
    # rebased onto 0156, orphaning the 0157->0159 branch. All three are already applied, so
    # naming them here just re-links the graph -- no merge migration needed.
    dependencies = [
        ("src", "0163_realtruck_lead_website_discovery"),
        ("src", "0161_leer_lead"),
    ]

    operations = [
        migrations.AddField(
            model_name="realtrucklead",
            name="emails_not_found",
            field=models.BooleanField(blank=True, default=False),
        ),
    ]
