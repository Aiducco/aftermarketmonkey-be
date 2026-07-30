import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0133_provider_part_kit_components"),
    ]

    operations = [
        migrations.CreateModel(
            name="CompanyProviderOrderAccount",
            fields=[
                ("id", models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("label", models.CharField(max_length=100)),
                ("credentials", models.JSONField()),
                ("active", models.BooleanField(default=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "company_provider",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="order_accounts",
                        to="src.companyproviders",
                    ),
                ),
            ],
            options={
                "db_table": "company_provider_order_accounts",
                "unique_together": {("company_provider", "label")},
            },
        ),
        migrations.AddField(
            model_name="purchaseorder",
            name="order_account",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.PROTECT,
                related_name="purchase_orders",
                to="src.companyproviderorderaccount",
            ),
        ),
        migrations.RemoveConstraint(
            model_name="purchaseorder",
            name="po_one_open_draft_per_company_provider",
        ),
        migrations.AddConstraint(
            model_name="purchaseorder",
            constraint=models.UniqueConstraint(
                condition=models.Q(("order_account__isnull", True), ("status__in", [1, 2])),
                fields=("company", "company_provider"),
                name="po_one_open_draft_per_company_provider",
            ),
        ),
        migrations.AddConstraint(
            model_name="purchaseorder",
            constraint=models.UniqueConstraint(
                condition=models.Q(("order_account__isnull", False), ("status__in", [1, 2])),
                fields=("company", "company_provider", "order_account"),
                name="po_one_open_draft_per_company_provider_order_account",
            ),
        ),
    ]
