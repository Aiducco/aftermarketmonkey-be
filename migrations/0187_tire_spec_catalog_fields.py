"""
Seven ``tire_specs`` columns for facts only a manufacturer-grade catalog can supply, plus the
provenance flag that stops them being overwritten.

Six of them describe things no source we already have can tell us: no distributor feed we ingest
carries a tread depth, a UTQG grade, a rim-width range, a tread design or a sidewall style
(verified by surveying every distinct ``product_details`` key in the catalog), and none of them
are encoded in the sidewall string, so ``src.domain.tire_size`` cannot derive them either. They
arrive only from a matched SimpleTire row -- 1,972 of 1,974 matched tires carry a tread depth,
1,965 a max PSI, 1,966 a rim-width range.

``sidewall_style`` and ``is_tubeless`` are two columns because the source packs two unrelated
facts into one field: 'Blackwall' and 'Whitewall' describe appearance while 'Tubeless' and
'Tube-Type' describe construction, and a tire has both.

``spec_source`` is the important one. ``reparse_tire_sizes`` recomputes the whole size block from
the parser on every run -- that is what makes a parser fix cost 21 seconds instead of $190 -- so
without a flag saying "a catalog owns this row", the next parser fix would silently replace
authoritative measured values with derived ones. Existing rows default to 'parser', which is
true of all 47,312 of them.

Deliberately NOT included: farm / industrial / otr / lawn_garden / golf on ``vehicle_class``.
SimpleTire carries 357 such rows, but we hold 13 BKT master parts and 7 Carlstar tires against
their 672 and 704 -- five enum values for ten rows. The mapping is recorded in the sync service
and is a one-line change if agricultural stock is ever carried.

Hand-written for the same reason as 0181-0184: ``makemigrations`` wants to bundle two dozen
unrelated ``AlterField`` operations converting ``id`` to BigAutoField across the catalog tables,
each a full table rewrite on millions of rows.
"""
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0186_wheelpros_vehicles"),
    ]

    operations = [
        migrations.AddField(
            model_name="tirespec",
            name="sidewall_style",
            field=models.CharField(
                blank=True,
                max_length=32,
                null=True,
                help_text="Blackwall, Whitewall, Outlined White Lettering, Raised White Lettering.",
            ),
        ),
        migrations.AddField(
            model_name="tirespec",
            name="is_tubeless",
            field=models.BooleanField(
                blank=True,
                null=True,
                help_text=(
                    "Split out of the same source field as sidewall_style, which conflates "
                    "appearance with construction -- 'Blackwall' and 'Tube-Type' arrive in one "
                    "column and are two different facts about a tire."
                ),
            ),
        ),
        migrations.AddField(
            model_name="tirespec",
            name="tread_design",
            field=models.CharField(
                blank=True,
                max_length=16,
                null=True,
                help_text=(
                    "Asymmetrical, Symmetrical or Directional -- affects whether a tire can be "
                    "rotated freely."
                ),
            ),
        ),
        migrations.AddField(
            model_name="tirespec",
            name="mileage_warranty_miles",
            field=models.PositiveIntegerField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="tirespec",
            name="commercial_position",
            field=models.CharField(
                blank=True,
                max_length=16,
                null=True,
                help_text="Steer, Drive, Trailer or All Position. Commercial tires only.",
            ),
        ),
        migrations.AddField(
            model_name="tirespec",
            name="tire_weight_lb",
            field=models.DecimalField(blank=True, decimal_places=2, max_digits=6, null=True),
        ),
        migrations.AddField(
            model_name="tirespec",
            name="spec_source",
            field=models.CharField(
                choices=[("parser", "Parser"), ("simpletire", "SimpleTire catalog")],
                db_index=True,
                default="parser",
                max_length=16,
                help_text=(
                    "Who owns the size block on this row. 'simpletire' means a matched catalog "
                    "row supplied it and reparse_tire_sizes must leave it alone -- without this "
                    "flag the next parser fix would silently overwrite authoritative data with a "
                    "derived guess."
                ),
            ),
        ),
    ]
