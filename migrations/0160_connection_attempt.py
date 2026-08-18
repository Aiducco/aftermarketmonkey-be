# Hand-trimmed from the auto-generated migration: makemigrations picked up a large amount of
# pre-existing drift unrelated to this change (id-field alterations, constraint removals, Meta
# option changes across many unrelated models) because migration state had drifted from models.py
# before this change. Only the ConnectionAttempt operations are kept here — the unrelated drift
# is a separate, pre-existing issue to reconcile on its own.
from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):
    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ("src", "0156_helmet_house_raw_tables"),
    ]

    operations = [
        migrations.CreateModel(
            name="ConnectionAttempt",
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
                ("action", models.CharField(max_length=16)),
                ("credentials", models.JSONField()),
                ("success", models.BooleanField()),
                ("error_code", models.CharField(blank=True, max_length=64, null=True)),
                ("error_message", models.TextField(blank=True, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                (
                    "company",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="connection_attempts",
                        to="src.company",
                    ),
                ),
                (
                    "company_provider",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="connection_attempts",
                        to="src.companyproviders",
                    ),
                ),
                (
                    "provider",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="connection_attempts",
                        to="src.providers",
                    ),
                ),
                (
                    "user",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="connection_attempts",
                        to=settings.AUTH_USER_MODEL,
                    ),
                ),
            ],
            options={
                "db_table": "connection_attempts",
            },
        ),
        migrations.AddIndex(
            model_name="connectionattempt",
            index=models.Index(
                fields=["company", "provider", "-created_at"],
                name="conn_attempt_co_pr_crt_idx",
            ),
        ),
        migrations.AddIndex(
            model_name="connectionattempt",
            index=models.Index(
                fields=["success", "-created_at"], name="conn_attempt_success_crt_idx"
            ),
        ),
    ]
