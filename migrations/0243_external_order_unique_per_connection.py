# An external order belongs to the CONNECTION, not to whichever of its credential sets happened
# to find it. The old key was (order_account, distributor_order_number), which stored one order
# once per account -- and Keystone hands both of Trident's accounts the same customer history, so
# all 32 of its orders were duplicated and the purchase-orders screen showed every Keystone order
# as a pair.
#
# The duplicates have to go before the new constraint can be added, so this deletes them first,
# keeping the oldest row of each (company_provider, distributor_order_number) group. Oldest, not
# newest, so discovered_at stays truthful about when we first saw the order. Nothing is lost: the
# copies are byte-identical apart from which account found them, and both were refreshed by the
# same poll.
from django.db import migrations
from django.db.models import Count, Min


def _drop_duplicate_orders(apps, schema_editor):
    ExternalPurchaseOrder = apps.get_model("src", "ExternalPurchaseOrder")
    duplicated = (
        ExternalPurchaseOrder.objects.values("company_provider_id", "distributor_order_number")
        .annotate(n=Count("id"), keep=Min("id"))
        .filter(n__gt=1)
    )
    for group in duplicated:
        ExternalPurchaseOrder.objects.filter(
            company_provider_id=group["company_provider_id"],
            distributor_order_number=group["distributor_order_number"],
        ).exclude(id=group["keep"]).delete()


def _noop_reverse(apps, schema_editor):
    """Nothing to undo: re-widening the key cannot recreate rows this deleted, and the next poll
    repopulates them anyway."""


class Migration(migrations.Migration):
    dependencies = [
        ("src", "0242_external_order_full_detail"),
    ]

    operations = [
        migrations.RunPython(_drop_duplicate_orders, _noop_reverse),
        migrations.AlterUniqueTogether(
            name="externalpurchaseorder",
            unique_together={("company_provider", "distributor_order_number")},
        ),
    ]
