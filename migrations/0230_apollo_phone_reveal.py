# ApolloPhoneReveal -- landing table for Apollo's asynchronous direct-dial callbacks.
# See the model docstring in src/models.py for why a webhook is the only delivery channel.
#
# Hand-written for the same reason as 0222-0229: makemigrations also wants ~25 unrelated
# AlterField operations from pre-existing model/migration drift elsewhere in src.
import django.core.serializers.json
import django.utils.timezone
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0229_master_part_aliases"),
    ]

    operations = [
        migrations.CreateModel(
            name="ApolloPhoneReveal",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("apollo_person_id", models.CharField(max_length=64, unique=True)),
                ("name", models.CharField(blank=True, default="", max_length=255)),
                ("title", models.CharField(blank=True, default="", max_length=255)),
                ("email", models.CharField(blank=True, default="", max_length=255)),
                ("organization_domain", models.CharField(blank=True, default="", max_length=255)),
                ("status", models.CharField(default="requested", max_length=16)),
                ("phone_numbers", models.JSONField(blank=True, default=list)),
                (
                    "raw_payload",
                    models.JSONField(
                        blank=True, default=dict, encoder=django.core.serializers.json.DjangoJSONEncoder
                    ),
                ),
                ("requested_at", models.DateTimeField(default=django.utils.timezone.now)),
                ("received_at", models.DateTimeField(blank=True, null=True)),
            ],
            options={"db_table": "apollo_phone_reveal"},
        ),
        migrations.AddIndex(
            model_name="apollophonereveal",
            index=models.Index(fields=["organization_domain"], name="apr_org_domain_idx"),
        ),
        migrations.AddIndex(
            model_name="apollophonereveal",
            index=models.Index(fields=["status"], name="apr_status_idx"),
        ),
    ]
