from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0087_providers_coming_soon"),
    ]

    operations = [
        migrations.CreateModel(
            name="IntegrationRequest",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("company", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="integration_requests", to="src.company")),
                ("provider", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="requests", to="src.providers")),
            ],
            options={"db_table": "integration_requests"},
        ),
        migrations.AlterUniqueTogether(
            name="integrationrequest",
            unique_together={("company", "provider")},
        ),
    ]
