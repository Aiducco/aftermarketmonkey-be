# EdiDocument / EdiControlNumber -- the audit trail and the control-number counters behind the
# Rough Country EDI order channel. See the model docstrings in src/models.py for why both are
# prerequisites rather than conveniences: control numbers cannot come from a clock (a duplicate
# ISA13 is a silent rejection, since Rough Country publishes no 997 for our 850s), and inbound
# FTP delivery is at-least-once, so the unique constraint on edi_documents is what stops a
# replayed 810 creating a second invoice.
#
# Hand-written for the same reason as 0222-0230: makemigrations also wants ~25 unrelated
# AlterField operations from pre-existing model/migration drift elsewhere in src.
import django.core.serializers.json
import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0230_apollo_phone_reveal"),
    ]

    operations = [
        migrations.CreateModel(
            name="EdiControlNumber",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("partner_id", models.CharField(max_length=15)),
                ("kind", models.CharField(max_length=16)),
                ("value", models.BigIntegerField(default=0)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={"db_table": "edi_control_numbers", "unique_together": {("partner_id", "kind")}},
        ),
        migrations.CreateModel(
            name="EdiDocument",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("direction", models.PositiveSmallIntegerField()),
                ("direction_name", models.CharField(max_length=16)),
                ("partner_id", models.CharField(max_length=15)),
                ("doc_type", models.CharField(max_length=8)),
                ("interchange_control_number", models.CharField(max_length=32)),
                ("group_control_number", models.CharField(blank=True, max_length=32, null=True)),
                ("transaction_control_number", models.CharField(blank=True, max_length=32, null=True)),
                ("filename", models.CharField(blank=True, max_length=255, null=True)),
                ("raw_payload", models.TextField()),
                (
                    "parsed_payload",
                    models.JSONField(
                        blank=True, encoder=django.core.serializers.json.DjangoJSONEncoder, null=True
                    ),
                ),
                (
                    "company_provider",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="edi_documents",
                        to="src.companyproviders",
                    ),
                ),
                ("status", models.PositiveSmallIntegerField()),
                ("status_name", models.CharField(max_length=32)),
                ("error_message", models.TextField(blank=True, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "purchase_order",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="edi_documents",
                        to="src.purchaseorder",
                    ),
                ),
            ],
            options={
                "db_table": "edi_documents",
                "unique_together": {("company_provider", "direction", "interchange_control_number")},
            },
        ),
        migrations.AddIndex(
            model_name="edicontrolnumber",
            index=models.Index(fields=["partner_id", "kind"], name="edi_ctl_partner_kind_idx"),
        ),
        migrations.AddIndex(
            model_name="edidocument",
            index=models.Index(fields=["company_provider"], name="edi_doc_cp_idx"),
        ),
        migrations.AddIndex(
            model_name="edidocument",
            index=models.Index(fields=["purchase_order"], name="edi_doc_po_idx"),
        ),
        migrations.AddIndex(
            model_name="edidocument",
            index=models.Index(fields=["doc_type", "status"], name="edi_doc_type_status_idx"),
        ),
        migrations.AddIndex(
            model_name="edidocument",
            index=models.Index(fields=["created_at"], name="edi_doc_created_idx"),
        ),
    ]
