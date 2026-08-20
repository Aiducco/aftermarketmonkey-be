import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):
    """
    ``realtruck_lead_email`` -- Reoon verification results for RealTruck dealer emails, mirroring
    what ``lead_email`` does for Google Maps leads.

    A new table rather than a nullable ``lead`` FK on ``lead_email``: that column is non-null today
    and existing code dereferences ``.lead`` without guarding, so rows belonging to a different
    lead source would break those paths.

    Hand-written -- makemigrations on this repo bundles unrelated drift (see 0163).
    """

    dependencies = [
        ("src", "0164_realtruck_lead_emails_not_found"),
    ]

    operations = [
        migrations.CreateModel(
            name="RealTruckLeadEmail",
            fields=[
                ("id", models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("email", models.EmailField(max_length=255)),
                ("status", models.CharField(blank=True, max_length=32, null=True)),
                ("is_valid", models.BooleanField(blank=True, null=True)),
                ("is_disposable", models.BooleanField(blank=True, null=True)),
                ("is_free_email", models.BooleanField(blank=True, null=True)),
                ("is_role_based", models.BooleanField(blank=True, null=True)),
                ("mx_found", models.BooleanField(blank=True, null=True)),
                ("verified_at", models.DateTimeField(blank=True, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("lead", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE,
                                           related_name="verified_emails", to="src.realtrucklead")),
            ],
            options={
                "db_table": "realtruck_lead_email",
                "unique_together": {("lead", "email")},
            },
        ),
        migrations.AddIndex(
            model_name="realtruckleademail",
            index=models.Index(fields=["email"], name="rt_lead_email_email_idx"),
        ),
        migrations.AddIndex(
            model_name="realtruckleademail",
            index=models.Index(fields=["status"], name="rt_lead_email_status_idx"),
        ),
        migrations.AddIndex(
            model_name="realtruckleademail",
            index=models.Index(fields=["is_valid"], name="rt_lead_email_valid_idx"),
        ),
    ]
