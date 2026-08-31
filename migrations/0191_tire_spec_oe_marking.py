"""
Add ``oe_marking`` and admit TDG as a second catalog source.

``oe_marking`` records original-equipment homologation: 'N0 - Porsche', 'MO - Mercedes-Benz',
'AO - Audi', '* - BMW'. A tire carrying one was approved by that manufacturer for that car, which
is a different and stronger claim than merely fitting it, and buyers of those cars search for it
by name. Nothing else we ingest carries it -- not the distributor feeds, not the sidewall string,
not SimpleTire. It arrives only from TDG, on 3,988 of the tires we match there.

``spec_source`` gains 'tdg'. The choices are ordered by precedence, and the order is measured
rather than assumed: across 10,740 tires described by both catalogs, SimpleTire has materially
better coverage on every shared field (tread depth present where TDG is silent on 1,703 rows
against 3 the other way; rim widths 3,508 against 6) and finer precision where both answer -- TDG
rounds tread depth to whole 32nds while 26% of SimpleTire's are fractional. So TDG never overwrites
a SimpleTire value; it fills gaps, reaches 8,656 tires SimpleTire never had, and supplies fields
SimpleTire does not carry at all.

Not taken from TDG, and worth recording why: ``warranty_mileage_miles`` is kilometres despite its
name. Its modal values are 80,000 / 105,000 / 120,000 where SimpleTire's are 50,000 / 60,000 /
45,000, and 80,000 km is 49,710 miles. US tire warranties do not run to 120,000 miles. ``max_load_lb``
is NULL on all 34,996 TDG tires.

Hand-written for the same reason as 0181-0190: ``makemigrations`` wants to bundle two dozen
unrelated ``AlterField`` operations converting ``id`` to BigAutoField across the catalog tables.
"""
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0190_tire_spec_catalog_precision"),
    ]

    operations = [
        migrations.AddField(
            model_name="tirespec",
            name="oe_marking",
            field=models.CharField(
                blank=True,
                max_length=128,
                null=True,
                help_text=(
                    "Original-equipment homologation codes, as published: 'N0 - Porsche', 'MO - "
                    "Mercedes-Benz', 'AO - Audi'. A tire can carry more than one, comma separated. "
                    "Nothing else in the catalog records this, and it is the difference between a "
                    "tire that fits a car and one the manufacturer approved for it."
                ),
            ),
        ),
        migrations.AlterField(
            model_name="tirespec",
            name="spec_source",
            field=models.CharField(
                choices=[("parser", "Parser"), ("simpletire", "SimpleTire catalog"), ("tdg", "TDG catalog")],
                db_index=True,
                default="parser",
                max_length=16,
                help_text=(
                    "Who owns the size block on this row. 'simpletire' means a matched catalog row "
                    "supplied it and reparse_tire_sizes must leave it alone -- without this flag the "
                    "next parser fix would silently overwrite authoritative data with a derived guess."
                ),
            ),
        ),
    ]
