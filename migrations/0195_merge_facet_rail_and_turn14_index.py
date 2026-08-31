"""
Rejoin the two migration leaves 0193 and 0194 left behind.

Not a mistake either side could have avoided alone: 0194 was written against origin/main while
0191-0193 were still uncommitted in a shared working directory, and deliberately depended on 0190
rather than on migrations that did not yet exist -- depending on an unpushed migration is the
worse failure (``NodeNotFoundError`` on every deploy, not just the one). The two touch unrelated
tables (``turn14_brand_pricing`` and ``facet_config``), so there is no ordering to preserve
between them and this node carries no operations.

Django refuses to run with two leaf nodes at all, so the cost of the split was the whole
``migrate`` step of one deploy -- caught there rather than by anything subtler.
"""
from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0193_facet_rail_v2"),
        ("src", "0194_turn14_brand_pricing_company_brand_id_index"),
    ]

    operations = []
