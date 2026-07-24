from django.db import migrations


class Migration(migrations.Migration):
    """Merge migration: 0122_shop_management_providers and
    0127_po_invoice_tracking_wheelpros_warehouse were both authored against
    0121_po_customer_po_name/0126_meyerlocation respectively as independent branches, leaving
    two leaf nodes in the graph. No schema changes here -- this only resolves the ambiguity so
    a plain ``migrate``/``makemigrations`` (no explicit target) can run again; every deploy's
    ``manage.py migrate --no-input`` was failing outright on this ("Conflicting migrations
    detected; multiple leaf nodes") even though every underlying migration was already applied.
    """

    dependencies = [
        ("src", "0122_shop_management_providers"),
        ("src", "0127_po_invoice_tracking_wheelpros_warehouse"),
    ]

    operations = []
