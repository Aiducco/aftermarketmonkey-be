# Spaces, step 1 of 3 (schema only): the Space table, its member / order-account links, and a NULLABLE
# `space` on company_locations, purchase_orders and external_purchase_orders, plus the new
# purchase_orders.po_prefix and external_purchase_orders.space_* columns. Nothing is backfilled here:
# 0252 fills the data and 0253 makes `space` NOT NULL. They are separate migrations because Postgres
# foreign keys are DEFERRABLE INITIALLY DEFERRED, and an UPDATE followed by ALTER TABLE in one
# transaction fails with "pending trigger events".
import django.db.models.deletion
import django.db.models.functions.text
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("src", "0250_company_space_allowance"),
    ]

    operations = [
        migrations.CreateModel(
            name="Space",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("kind", models.PositiveSmallIntegerField()),
                ("kind_name", models.CharField(max_length=32)),
                ("name", models.CharField(max_length=255)),
                ("prefix", models.CharField(blank=True, max_length=16, null=True)),
                ("is_main", models.BooleanField(default=False)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "company",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE, related_name="spaces", to="src.company"
                    ),
                ),
                (
                    "location",
                    models.OneToOneField(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="address_of_space",
                        to="src.companylocation",
                    ),
                ),
                (
                    "members",
                    models.ManyToManyField(
                        blank=True, db_table="space_members", related_name="spaces", to="src.userprofile"
                    ),
                ),
                (
                    "order_accounts",
                    models.ManyToManyField(
                        blank=True,
                        db_table="space_order_accounts",
                        related_name="spaces",
                        to="src.companyproviderorderaccount",
                    ),
                ),
            ],
            options={
                "db_table": "spaces",
            },
        ),
        migrations.AddConstraint(
            model_name="space",
            constraint=models.UniqueConstraint(
                condition=models.Q(("is_main", True)), fields=("company",), name="space_one_main_per_company"
            ),
        ),
        migrations.AddConstraint(
            model_name="space",
            constraint=models.UniqueConstraint(
                django.db.models.functions.text.Lower("name"),
                models.F("company"),
                name="space_name_unique_per_company",
            ),
        ),
        migrations.AddField(
            model_name="companylocation",
            name="space",
            field=models.ForeignKey(
                null=True,
                on_delete=django.db.models.deletion.CASCADE,
                related_name="locations",
                to="src.space",
            ),
        ),
        migrations.AddField(
            model_name="purchaseorder",
            name="space",
            field=models.ForeignKey(
                null=True,
                on_delete=django.db.models.deletion.PROTECT,
                related_name="purchase_orders",
                to="src.space",
            ),
        ),
        migrations.AddField(
            model_name="purchaseorder",
            name="po_prefix",
            field=models.CharField(blank=True, max_length=16, null=True),
        ),
        migrations.AddField(
            model_name="externalpurchaseorder",
            name="space",
            field=models.ForeignKey(
                null=True,
                on_delete=django.db.models.deletion.PROTECT,
                related_name="external_purchase_orders",
                to="src.space",
            ),
        ),
        migrations.AddField(
            model_name="externalpurchaseorder",
            name="space_reason",
            field=models.PositiveSmallIntegerField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="externalpurchaseorder",
            name="space_reason_name",
            field=models.CharField(blank=True, max_length=16, null=True),
        ),
        migrations.AddField(
            model_name="externalpurchaseorder",
            name="space_reason_prefix",
            field=models.CharField(blank=True, max_length=16, null=True),
        ),
        migrations.AddField(
            model_name="externalpurchaseorder",
            name="space_moved_by",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name="+",
                to="src.userprofile",
            ),
        ),
        migrations.AddField(
            model_name="externalpurchaseorder",
            name="space_moved_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
    ]
