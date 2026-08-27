"""
Bring ``TireLoadRange`` into line with the table as it now stands.

**The database was changed outside the migration system.** ``load_range_ply`` already has
``aliases`` (a text array) in place of ``alias``, a nullable ``ply_rating``, and a new ``LL``
designation -- while ``models.py`` still described the old shape and ``showmigrations`` reported
everything applied. Django could not see the difference, so ``manage.py check`` passed and every
ORM read of the table failed at runtime instead: ``enrich_tire_specs`` and
``reconcile_tire_specs`` both died on ``column load_range_ply.alias does not exist``.

This migration exists to make Django's recorded state match reality. Apply it with ``--fake`` on
any database that already has the new columns (production does); it runs for real only on one
built from scratch.

Two substantive changes are captured here, both correct:

  ``aliases`` is a list because XL is stamped RF, RD *and* REINFORCED. One alternate could not
  hold that, so a parser seeing "RD" had no way to resolve it to XL.

  ``ply_rating`` is nullable, and the passenger designations are NULL. SL, XL and LL express
  load capability through the load index, not a bias-ply equivalence, so there is no number to
  state -- migration 0179's "4" for SL and XL was simply wrong. Nothing downstream is affected:
  every persisted spec with those designations already carries a NULL ply_rating.

Hand-written rather than generated, for the same reason as 0181-0183: ``makemigrations`` wants to
bundle two dozen unrelated ``AlterField`` operations converting ``id`` to BigAutoField across the
catalog tables, each a full table rewrite on millions of rows.
"""
import django.contrib.postgres.fields
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("src", "0183_powersports_tread_categories"),
    ]

    operations = [
        migrations.RemoveField(model_name="tireloadrange", name="alias"),
        migrations.AddField(
            model_name="tireloadrange",
            name="aliases",
            field=django.contrib.postgres.fields.ArrayField(
                base_field=models.TextField(),
                blank=True,
                default=list,
                help_text=(
                    "Every alternate sidewall stamping for the same designation. XL alone is "
                    "stamped RF, RD and REINFORCED, which is why this is a list rather than one "
                    "alternate."
                ),
                size=None,
            ),
        ),
        migrations.AlterField(
            model_name="tireloadrange",
            name="ply_rating",
            field=models.PositiveSmallIntegerField(
                blank=True,
                null=True,
                help_text=(
                    "Bias-ply strength equivalence, not a count of physical layers. NULL for the "
                    "passenger designations (SL/XL/LL), which express load capability through the "
                    "load index rather than a ply equivalence -- they have no ply rating to state."
                ),
            ),
        ),
    ]
