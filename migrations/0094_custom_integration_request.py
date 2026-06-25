from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0093_support_tickets"),
    ]

    operations = [
        migrations.CreateModel(
            name="CustomIntegrationRequest",
            fields=[
                ("id", models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                (
                    "company",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="custom_integration_requests",
                        to="src.company",
                    ),
                ),
                ("distributor_name", models.CharField(max_length=255)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
            ],
            options={"db_table": "custom_integration_requests"},
        ),
        migrations.AddConstraint(
            model_name="customintegrationrequest",
            constraint=models.UniqueConstraint(
                fields=["company", "distributor_name"],
                name="unique_company_distributor_name",
            ),
        ),
    ]
