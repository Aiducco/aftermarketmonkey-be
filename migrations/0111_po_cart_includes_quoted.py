# Hand-trimmed from an auto-generated migration: the autodetector also picked up unrelated
# pre-existing model/migration drift (id field types, unique_together renames, index/options
# changes on other models) that predates this change and isn't part of it — only the
# PurchaseOrder constraint widening below is included here. See 0108_purchase_orders.py and
# 0110_po_json_decimal_fix.py for the same pattern used previously.
#
# Widens the "one open cart per distributor connection" partial unique constraint from
# DRAFT-only (status=1) to DRAFT-or-QUOTED (status in (1, 2)): a quote no longer promotes a
# cart into something a second concurrent "Add to PO" could duplicate — see
# src.models.PurchaseOrder and src.api.services.purchase_orders._cart_queryset.

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("src", "0110_po_json_decimal_fix"),
    ]

    operations = [
        migrations.RemoveConstraint(
            model_name="purchaseorder",
            name="po_one_open_draft_per_company_provider",
        ),
        migrations.AddConstraint(
            model_name="purchaseorder",
            constraint=models.UniqueConstraint(
                condition=models.Q(("status__in", [1, 2])),
                fields=("company", "company_provider"),
                name="po_one_open_draft_per_company_provider",
            ),
        ),
    ]
