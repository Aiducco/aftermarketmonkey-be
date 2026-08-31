"""
Index ``wheelpros_parts.part_number`` on its own.

The table already has ``UNIQUE (brand_id, part_number)``, but a composite index cannot serve a
lookup that does not know the leading column, and every join we make into this table is by part
number alone -- the link from ``provider_parts`` is
``split_part(provider_external_id, '_', 2) = wp.part_number``, with no brand in hand.

Without it the wheels index projection does a sequential scan of 82,441 rows for every document it
builds: 3,000 documents took 34 seconds, which extrapolates to eight minutes for a full rebuild of
43,272. With it the same work is an index probe.

Cheap and safe: 82k rows, no lock worth worrying about, and purely additive -- nothing changes
behaviour, only the plan.
"""
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0198_brand_tire_data"),
    ]

    operations = [
        migrations.AddIndex(
            model_name="wheelprospart",
            index=models.Index(fields=["part_number"], name="wheelpros_parts_part_no_idx"),
        ),
    ]
