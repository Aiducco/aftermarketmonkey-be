"""
Widen six catalog columns to hold what the catalog actually publishes.

All six were added empty in 0187 and sized by guess. Matching 22,155 tires against
``simpletire_skus`` showed three of those guesses lose data:

``tread_depth_32nds`` was a small integer on the assumption that tread depth is a whole number of
32nds. It is not: 13,324 of 50,602 published depths -- 26% -- are fractional (7.2, 6.3, 10.5), and
an integer column silently rounds every one of them. Tread depth is a number customers compare
directly, so the rounding would be visible.

``rim_width_min_in`` / ``rim_width_max_in`` were one decimal place. 1,357 and 1,624 published
values need two (10.25, 8.75 -- quarter-inch rim widths are ordinary).

The three ``CharField`` widths are raised to match the source columns exactly. Today's longest
values fit comfortably inside the old limits, so nothing is truncated right now; the point is that
a re-crawl bringing a longer string should not be able to lose characters, and widening a varchar
in Postgres does not rewrite the table. This also lets ``split_sidewall`` stop truncating.

Safe to run: every one of these columns is 100% NULL, so the type changes convert no data.

Hand-written for the same reason as 0181-0188: ``makemigrations`` wants to bundle two dozen
unrelated ``AlterField`` operations converting ``id`` to BigAutoField across the catalog tables.
"""
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0189_tdg_products"),
    ]

    operations = [
        migrations.AlterField(
            model_name="tirespec",
            name="tread_depth_32nds",
            field=models.DecimalField(
                blank=True,
                decimal_places=2,
                max_digits=5,
                null=True,
                help_text="Decimal, not whole 32nds: 26% of published tread depths are fractional (7.2, 6.3).",
            ),
        ),
        migrations.AlterField(
            model_name="tirespec",
            name="rim_width_min_in",
            field=models.DecimalField(blank=True, decimal_places=2, max_digits=5, null=True),
        ),
        migrations.AlterField(
            model_name="tirespec",
            name="rim_width_max_in",
            field=models.DecimalField(blank=True, decimal_places=2, max_digits=5, null=True),
        ),
        migrations.AlterField(
            model_name="tirespec",
            name="sidewall_style",
            field=models.CharField(
                blank=True,
                max_length=64,
                null=True,
                help_text="Blackwall, Whitewall, Outlined White Lettering, Raised White Lettering.",
            ),
        ),
        migrations.AlterField(
            model_name="tirespec",
            name="tread_design",
            field=models.CharField(
                blank=True,
                max_length=32,
                null=True,
                help_text=(
                    "Asymmetrical, Symmetrical or Directional -- affects whether a tire can be "
                    "rotated freely."
                ),
            ),
        ),
        migrations.AlterField(
            model_name="tirespec",
            name="commercial_position",
            field=models.CharField(
                blank=True,
                max_length=32,
                null=True,
                help_text="Steer, Drive, Trailer or All Position. Commercial tires only.",
            ),
        ),
    ]
