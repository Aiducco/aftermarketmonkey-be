"""
Link a tire spec to the TDG row merged into it.

Parallel to the ``simpletire_*`` columns in 0188, and separate from them on purpose: 11,719 tires
are described by *both* catalogs, and collapsing the two pointers into one would lose the ability
to say which source supplied which field, or to re-run one merge without the other.

``SET_NULL``, like the SimpleTire link: ``tdg_products`` is a scrape landing zone that may be
re-crawled or truncated, and losing the pointer must not take the specs with it.

Hand-written for the same reason as 0181-0191.
"""
import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0191_tire_spec_oe_marking"),
    ]

    operations = [
        migrations.AddField(
            model_name="tirespec",
            name="tdg_product",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name="tire_specs",
                to="src.tdgproduct",
                help_text="The TDG row merged into this spec. A tire can be described by both catalogs.",
            ),
        ),
        migrations.AddField(
            model_name="tirespec",
            name="tdg_match_tier",
            field=models.PositiveSmallIntegerField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="tirespec",
            name="tdg_synced_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
    ]
