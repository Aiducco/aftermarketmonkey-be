from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0081_lead_ai_skip_reason"),
    ]

    operations = [
        migrations.CreateModel(
            name="LeadEmail",
            fields=[
                ("id", models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("lead", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="verified_emails", to="src.lead")),
                ("email", models.EmailField(max_length=255)),
                ("status", models.CharField(blank=True, max_length=32, null=True)),
                ("is_valid", models.BooleanField(blank=True, null=True)),
                ("is_disposable", models.BooleanField(blank=True, null=True)),
                ("is_free_email", models.BooleanField(blank=True, null=True)),
                ("is_role_based", models.BooleanField(blank=True, null=True)),
                ("mx_found", models.BooleanField(blank=True, null=True)),
                ("verified_at", models.DateTimeField(blank=True, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
            ],
            options={"db_table": "lead_email"},
        ),
        migrations.AddConstraint(
            model_name="leademail",
            constraint=models.UniqueConstraint(fields=["lead", "email"], name="lead_email_unique"),
        ),
        migrations.AddIndex(
            model_name="leademail",
            index=models.Index(fields=["email"], name="lead_email_email_idx"),
        ),
        migrations.AddIndex(
            model_name="leademail",
            index=models.Index(fields=["status"], name="lead_email_status_idx"),
        ),
        migrations.AddIndex(
            model_name="leademail",
            index=models.Index(fields=["is_valid"], name="lead_email_valid_idx"),
        ),
    ]
