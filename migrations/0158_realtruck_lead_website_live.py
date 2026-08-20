import django.utils.timezone
from django.db import migrations, models


class Migration(migrations.Migration):
    """
    Brings ``realtruck_leads`` under Django and adds the website-liveness columns.

    The table itself was created by the RealTruck dealer-locator scraper outside of Django, so
    ``CreateModel`` here is state-only -- it teaches the migration graph what already exists in
    the database without re-issuing the CREATE TABLE. Only ``website_live`` and
    ``website_checked_at`` are real schema changes (two nullable columns), which is what
    ``validate_lead_websites --source realtruck`` fills in.
    """

    dependencies = [
        ("src", "0157_master_part_enrichment_indexes"),
    ]

    operations = [
        migrations.SeparateDatabaseAndState(
            database_operations=[],
            state_operations=[
                migrations.CreateModel(
                    name="RealTruckLead",
                    fields=[
                        ("id", models.TextField(primary_key=True, serialize=False)),
                        ("name", models.TextField()),
                        ("phone", models.TextField(blank=True, null=True)),
                        ("website", models.TextField(blank=True, null=True)),
                        ("address", models.TextField(blank=True, null=True)),
                        ("city", models.TextField(blank=True, null=True)),
                        ("state", models.TextField(blank=True, null=True)),
                        ("zipcode", models.TextField(blank=True, null=True)),
                        ("country", models.TextField(blank=True, null=True)),
                        ("full_address", models.TextField(blank=True, null=True)),
                        ("lat", models.FloatField(blank=True, null=True)),
                        ("lng", models.FloatField(blank=True, null=True)),
                        ("is_preferred", models.BooleanField(default=False)),
                        ("is_double_warranty", models.BooleanField(default=False)),
                        ("is_next_gen", models.BooleanField(default=False)),
                        ("is_real_pro", models.BooleanField(default=False)),
                        ("is_international", models.BooleanField(default=False)),
                        ("sort_order", models.IntegerField(blank=True, null=True)),
                        ("brand_count", models.IntegerField(blank=True, null=True)),
                        ("preferred_brands", models.TextField(blank=True, null=True)),
                        ("all_brands", models.TextField(blank=True, null=True)),
                        ("brands", models.JSONField(blank=True, null=True)),
                        ("found_via", models.TextField(blank=True, null=True)),
                        ("distance_mi", models.FloatField(blank=True, null=True)),
                        ("first_seen_at", models.DateTimeField(default=django.utils.timezone.now)),
                        ("last_seen_at", models.DateTimeField(default=django.utils.timezone.now)),
                    ],
                    options={
                        "db_table": "realtruck_leads",
                    },
                ),
                migrations.AddIndex(
                    model_name="realtrucklead",
                    index=models.Index(fields=["state"], name="realtruck_leads_state_idx"),
                ),
                migrations.AddIndex(
                    model_name="realtrucklead",
                    index=models.Index(fields=["is_preferred"], name="realtruck_leads_preferred_idx"),
                ),
            ],
        ),
        migrations.AddField(
            model_name="realtrucklead",
            name="website_live",
            field=models.BooleanField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="realtrucklead",
            name="website_checked_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
    ]
