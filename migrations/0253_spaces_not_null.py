# Spaces, step 3 of 3: `space` becomes NOT NULL on company_locations, purchase_orders and
# external_purchase_orders (0252 filled every row; Model.save() fills it for new ones), and the two
# open-cart unique constraints gain `space`, so two spaces of one company can each have an open cart
# on the same connection and account. A company with one space is unchanged.
import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("src", "0252_spaces_backfill"),
    ]

    operations = [
        migrations.AlterField(
            model_name="companylocation",
            name="space",
            field=models.ForeignKey(
                on_delete=django.db.models.deletion.CASCADE, related_name="locations", to="src.space"
            ),
        ),
        migrations.AlterField(
            model_name="purchaseorder",
            name="space",
            field=models.ForeignKey(
                on_delete=django.db.models.deletion.PROTECT, related_name="purchase_orders", to="src.space"
            ),
        ),
        migrations.AlterField(
            model_name="externalpurchaseorder",
            name="space",
            field=models.ForeignKey(
                on_delete=django.db.models.deletion.PROTECT,
                related_name="external_purchase_orders",
                to="src.space",
            ),
        ),
        migrations.RemoveConstraint(
            model_name="purchaseorder",
            name="po_one_open_draft_per_company_provider",
        ),
        migrations.RemoveConstraint(
            model_name="purchaseorder",
            name="po_one_open_draft_per_company_provider_order_account",
        ),
        migrations.AddConstraint(
            model_name="purchaseorder",
            constraint=models.UniqueConstraint(
                condition=models.Q(("order_account__isnull", True), ("status__in", [1, 2])),
                fields=("company", "space", "company_provider"),
                name="po_one_open_draft_per_space_company_provider",
            ),
        ),
        migrations.AddConstraint(
            model_name="purchaseorder",
            constraint=models.UniqueConstraint(
                condition=models.Q(("order_account__isnull", False), ("status__in", [1, 2])),
                fields=("company", "space", "company_provider", "order_account"),
                name="po_one_open_draft_per_space_company_provider_order_account",
            ),
        ),
    ]
