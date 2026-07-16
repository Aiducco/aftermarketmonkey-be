# Hand-trimmed from an auto-generated migration: the autodetector also picked up unrelated
# pre-existing model/migration drift (id field types, constraint renames on other models)
# that predates this feature and isn't part of it — only the new Purchase Orders models are
# included here.

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):
    dependencies = [
        ("src", "0107_notification_email_log"),
    ]

    operations = [
        migrations.CreateModel(
            name="PurchaseOrder",
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
                (
                    "po_number",
                    models.CharField(blank=True, max_length=64, null=True, unique=True),
                ),
                ("status", models.PositiveSmallIntegerField()),
                ("status_name", models.CharField(max_length=32)),
                ("source", models.PositiveSmallIntegerField()),
                ("source_name", models.CharField(max_length=32)),
                (
                    "source_reference",
                    models.CharField(blank=True, max_length=255, null=True),
                ),
                (
                    "ship_to_name",
                    models.CharField(blank=True, max_length=255, null=True),
                ),
                (
                    "ship_to_attention",
                    models.CharField(blank=True, max_length=255, null=True),
                ),
                (
                    "ship_to_address1",
                    models.CharField(blank=True, max_length=255, null=True),
                ),
                (
                    "ship_to_address2",
                    models.CharField(blank=True, max_length=255, null=True),
                ),
                (
                    "ship_to_city",
                    models.CharField(blank=True, max_length=128, null=True),
                ),
                (
                    "ship_to_state",
                    models.CharField(blank=True, max_length=64, null=True),
                ),
                (
                    "ship_to_postal_code",
                    models.CharField(blank=True, max_length=32, null=True),
                ),
                (
                    "ship_to_country",
                    models.CharField(blank=True, max_length=64, null=True),
                ),
                (
                    "ship_to_phone",
                    models.CharField(blank=True, max_length=32, null=True),
                ),
                ("ship_method", models.CharField(blank=True, max_length=64, null=True)),
                ("quote_raw_response", models.JSONField(blank=True, null=True)),
                ("quoted_at", models.DateTimeField(blank=True, null=True)),
                (
                    "subtotal",
                    models.DecimalField(
                        blank=True, decimal_places=2, max_digits=12, null=True
                    ),
                ),
                (
                    "estimated_shipping",
                    models.DecimalField(
                        blank=True, decimal_places=2, max_digits=12, null=True
                    ),
                ),
                (
                    "total",
                    models.DecimalField(
                        blank=True, decimal_places=2, max_digits=12, null=True
                    ),
                ),
                ("error_message", models.TextField(blank=True, null=True)),
                ("notes", models.TextField(blank=True, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("submitted_at", models.DateTimeField(blank=True, null=True)),
            ],
            options={
                "db_table": "purchase_orders",
            },
        ),
        migrations.CreateModel(
            name="PurchaseOrderDistributorOrder",
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
                ("distributor_order_number", models.CharField(max_length=128)),
                (
                    "warehouse_code",
                    models.CharField(blank=True, max_length=64, null=True),
                ),
                ("status", models.PositiveSmallIntegerField()),
                ("status_name", models.CharField(max_length=32)),
                ("tracking_numbers", models.JSONField(blank=True, default=list)),
                ("carrier", models.CharField(blank=True, max_length=64, null=True)),
                ("raw_response", models.JSONField(blank=True, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "db_table": "purchase_order_distributor_orders",
            },
        ),
        migrations.CreateModel(
            name="PurchaseOrderGroup",
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
                ("reference", models.CharField(blank=True, max_length=64, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
            ],
            options={
                "db_table": "purchase_order_groups",
            },
        ),
        migrations.CreateModel(
            name="PurchaseOrderJob",
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
                ("operation", models.PositiveSmallIntegerField()),
                ("operation_name", models.CharField(max_length=32)),
                ("status", models.PositiveSmallIntegerField()),
                ("status_name", models.CharField(max_length=32)),
                ("message", models.TextField(blank=True, null=True)),
                ("error_message", models.TextField(blank=True, null=True)),
                ("attempt_count", models.PositiveSmallIntegerField(default=0)),
                ("max_attempts", models.PositiveSmallIntegerField(default=3)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("started_at", models.DateTimeField(blank=True, null=True)),
                ("completed_at", models.DateTimeField(blank=True, null=True)),
            ],
            options={
                "db_table": "purchase_order_jobs",
                "ordering": ["id"],
            },
        ),
        migrations.CreateModel(
            name="PurchaseOrderLineItem",
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
                ("quantity", models.PositiveIntegerField()),
                (
                    "unit_cost",
                    models.DecimalField(
                        blank=True, decimal_places=2, max_digits=10, null=True
                    ),
                ),
                (
                    "line_total",
                    models.DecimalField(
                        blank=True, decimal_places=2, max_digits=12, null=True
                    ),
                ),
                ("status", models.PositiveSmallIntegerField()),
                ("status_name", models.CharField(max_length=32)),
                (
                    "distributor_line_status_code",
                    models.CharField(blank=True, max_length=64, null=True),
                ),
                (
                    "distributor_line_status_message",
                    models.TextField(blank=True, null=True),
                ),
                (
                    "quantity_confirmed",
                    models.PositiveIntegerField(blank=True, null=True),
                ),
                (
                    "quantity_backordered",
                    models.PositiveIntegerField(blank=True, null=True),
                ),
                ("manufacturer_esd", models.DateField(blank=True, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "db_table": "purchase_order_line_items",
            },
        ),
        migrations.CreateModel(
            name="PurchaseOrderSubmissionAttempt",
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
                ("operation", models.PositiveSmallIntegerField()),
                ("operation_name", models.CharField(max_length=32)),
                ("success", models.BooleanField()),
                ("request_payload", models.JSONField(blank=True, null=True)),
                ("response_payload", models.JSONField(blank=True, null=True)),
                ("error_message", models.TextField(blank=True, null=True)),
                ("duration_ms", models.IntegerField(blank=True, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
            ],
            options={
                "db_table": "purchase_order_submission_attempts",
            },
        ),
        migrations.AddField(
            model_name="purchaseordersubmissionattempt",
            name="purchase_order",
            field=models.ForeignKey(
                on_delete=django.db.models.deletion.CASCADE,
                related_name="submission_attempts",
                to="src.purchaseorder",
            ),
        ),
        migrations.AddField(
            model_name="purchaseorderlineitem",
            name="distributor_order",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name="line_items",
                to="src.purchaseorderdistributororder",
            ),
        ),
        migrations.AddField(
            model_name="purchaseorderlineitem",
            name="provider_part",
            field=models.ForeignKey(
                on_delete=django.db.models.deletion.PROTECT,
                related_name="po_line_items",
                to="src.providerpart",
            ),
        ),
        migrations.AddField(
            model_name="purchaseorderlineitem",
            name="purchase_order",
            field=models.ForeignKey(
                on_delete=django.db.models.deletion.CASCADE,
                related_name="line_items",
                to="src.purchaseorder",
            ),
        ),
        migrations.AddField(
            model_name="purchaseorderjob",
            name="purchase_order",
            field=models.ForeignKey(
                on_delete=django.db.models.deletion.CASCADE,
                related_name="jobs",
                to="src.purchaseorder",
            ),
        ),
        migrations.AddField(
            model_name="purchaseordergroup",
            name="company",
            field=models.ForeignKey(
                on_delete=django.db.models.deletion.CASCADE,
                related_name="purchase_order_groups",
                to="src.company",
            ),
        ),
        migrations.AddField(
            model_name="purchaseordergroup",
            name="created_by",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name="po_groups_created",
                to="src.userprofile",
            ),
        ),
        migrations.AddField(
            model_name="purchaseorderdistributororder",
            name="purchase_order",
            field=models.ForeignKey(
                on_delete=django.db.models.deletion.CASCADE,
                related_name="distributor_orders",
                to="src.purchaseorder",
            ),
        ),
        migrations.AddField(
            model_name="purchaseorder",
            name="company",
            field=models.ForeignKey(
                on_delete=django.db.models.deletion.CASCADE,
                related_name="purchase_orders",
                to="src.company",
            ),
        ),
        migrations.AddField(
            model_name="purchaseorder",
            name="company_provider",
            field=models.ForeignKey(
                on_delete=django.db.models.deletion.PROTECT,
                related_name="purchase_orders",
                to="src.companyproviders",
            ),
        ),
        migrations.AddField(
            model_name="purchaseorder",
            name="created_by",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name="purchase_orders_created",
                to="src.userprofile",
            ),
        ),
        migrations.AddField(
            model_name="purchaseorder",
            name="group",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name="purchase_orders",
                to="src.purchaseordergroup",
            ),
        ),
        migrations.AddIndex(
            model_name="purchaseorderlineitem",
            index=models.Index(fields=["purchase_order"], name="po_line_items_po_idx"),
        ),
        migrations.AlterUniqueTogether(
            name="purchaseorderlineitem",
            unique_together={("purchase_order", "provider_part")},
        ),
        migrations.AlterUniqueTogether(
            name="purchaseorderdistributororder",
            unique_together={("purchase_order", "distributor_order_number")},
        ),
        migrations.AddIndex(
            model_name="purchaseorder",
            index=models.Index(
                fields=["company", "status"], name="po_company_status_idx"
            ),
        ),
        migrations.AddIndex(
            model_name="purchaseorder",
            index=models.Index(
                fields=["company_provider", "status"],
                name="po_company_provider_status_idx",
            ),
        ),
        migrations.AddConstraint(
            model_name="purchaseorder",
            constraint=models.UniqueConstraint(
                condition=models.Q(("status", 1)),
                fields=("company", "company_provider"),
                name="po_one_open_draft_per_company_provider",
            ),
        ),
    ]
