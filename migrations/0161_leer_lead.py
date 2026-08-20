import django.utils.timezone
from django.db import migrations, models


class Migration(migrations.Migration):
    """
    Brings ``leer_leads`` under Django and adds the enrichment columns.

    Same shape as 0158/0159 did for ``realtruck_leads``: the table was created by the LEER
    dealer-locator scraper outside Django, so ``CreateModel`` is state-only and the real schema
    change is the AddFields below.

    LEER's locator publishes no website (not in a column, not in the ``raw`` payload), so unlike
    RealTruck this table needs ``website``/``website_not_found`` as well -- they get populated by
    ``find_missing_websites --source leer`` before anything else can run.
    """

    dependencies = [
        ("src", "0160_connection_attempt"),
    ]

    operations = [
        migrations.SeparateDatabaseAndState(
            database_operations=[],
            state_operations=[
                migrations.CreateModel(
                    name="LeerLead",
                    fields=[
                        ("location_id", models.TextField(primary_key=True, serialize=False)),
                        ("name", models.TextField()),
                        ("phone", models.TextField(blank=True, null=True)),
                        ("phone_digits", models.TextField(blank=True, null=True)),
                        ("address", models.TextField(blank=True, null=True)),
                        ("city", models.TextField(blank=True, null=True)),
                        ("state", models.TextField(blank=True, null=True)),
                        ("zipcode", models.TextField(blank=True, null=True)),
                        ("full_address", models.TextField(blank=True, null=True)),
                        ("lat", models.FloatField(blank=True, null=True)),
                        ("lng", models.FloatField(blank=True, null=True)),
                        ("company_id", models.TextField(blank=True, null=True)),
                        ("found_via", models.TextField(blank=True, null=True)),
                        ("distance_mi", models.FloatField(blank=True, null=True)),
                        ("raw", models.JSONField(blank=True, null=True)),
                        ("first_seen_at", models.DateTimeField(default=django.utils.timezone.now)),
                        ("last_seen_at", models.DateTimeField(default=django.utils.timezone.now)),
                    ],
                    options={
                        "db_table": "leer_leads",
                    },
                ),
                migrations.AddIndex(
                    model_name="leerlead",
                    index=models.Index(fields=["state"], name="leer_leads_state_idx"),
                ),
                migrations.AddIndex(
                    model_name="leerlead",
                    index=models.Index(fields=["zipcode"], name="leer_leads_zipcode_idx"),
                ),
            ],
        ),
        migrations.AddField(
            model_name="leerlead", name="website",
            field=models.TextField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="leerlead", name="website_not_found",
            field=models.BooleanField(blank=True, default=False),
        ),
        migrations.AddField(
            model_name="leerlead", name="website_live",
            field=models.BooleanField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="leerlead", name="website_checked_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="leerlead", name="email",
            field=models.EmailField(blank=True, max_length=255, null=True),
        ),
        migrations.AddField(
            model_name="leerlead", name="emails",
            field=models.JSONField(blank=True, default=list),
        ),
        migrations.AddField(
            model_name="leerlead", name="is_qualified",
            field=models.BooleanField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="leerlead", name="business_typology",
            field=models.CharField(blank=True, max_length=64, null=True),
        ),
        migrations.AddField(
            model_name="leerlead", name="confidence_score",
            field=models.IntegerField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="leerlead", name="brands_mentioned",
            field=models.JSONField(blank=True, default=list),
        ),
        migrations.AddField(
            model_name="leerlead", name="ai_reasoning",
            field=models.TextField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="leerlead", name="ai_skip_reason",
            field=models.CharField(blank=True, max_length=255, null=True),
        ),
        migrations.AddField(
            model_name="leerlead", name="ai_qualified_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
    ]
