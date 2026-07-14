from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0106_integration_pricing_sync_job_use_delta_fetch"),
    ]

    operations = [
        migrations.CreateModel(
            name="NotificationEmailLog",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("email_type", models.PositiveSmallIntegerField()),
                ("email_type_name", models.CharField(max_length=64)),
                ("to_email", models.EmailField(max_length=255)),
                ("from_email", models.EmailField(max_length=255)),
                ("subject", models.CharField(max_length=255)),
                ("status", models.PositiveSmallIntegerField()),
                ("status_name", models.CharField(max_length=32)),
                (
                    "provider_message_id",
                    models.CharField(blank=True, max_length=255, null=True),
                ),
                ("error_message", models.TextField(blank=True, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                (
                    "company",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="notification_email_logs",
                        to="src.company",
                    ),
                ),
                (
                    "company_provider",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="notification_email_logs",
                        to="src.companyproviders",
                    ),
                ),
            ],
            options={
                "db_table": "notification_email_log",
                "ordering": ["-id"],
            },
        ),
    ]
