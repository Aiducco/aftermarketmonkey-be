from django.db import migrations


class Migration(migrations.Migration):
    """
    Rejoin the two migration branches into a single head. No schema change of its own.

    The graph had split: the realtruck/lead branch ran 0157-0166 while the master-parts branch
    ran 0167 (pcdb) -> 0168 (gtin index) -> 0169 (product groups) -> 0170 (product_type), leaving
    0166 and 0170 as parallel tips. Django refuses to migrate with multiple leaf nodes.

    This could not be written earlier: 0157-0166 existed only as uncommitted local files, so any
    migration depending on 0166 broke deploys with NodeNotFoundError (which is why 0168 was
    reverted to depending on 0167 alone). Now that the realtruck branch is committed, depending
    on both tips is safe.
    """

    dependencies = [
        ("src", "0166_realtruck_lead_priority"),
        ("src", "0170_master_part_product_type"),
    ]

    operations = []
