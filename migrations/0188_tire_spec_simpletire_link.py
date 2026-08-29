"""
Link a tire spec to the catalog row it was merged from, and give the taxonomy a second axis.

``season_category`` exists because one FK cannot hold what our own taxonomy says. ``tread_category``
rows carry an ``axis`` -- season, terrain, performance, special -- and a real tire has a value on
more than one: a summer UHP tire is UHP on the performance axis and SUMMER on the season axis.
Measured against 22,155 SimpleTire-matched rows, 4,860 of the apparent category disagreements were
exactly that shape -- both sides right, different axes -- against 982 genuine contradictions. Until
now the loser was simply discarded.

The check constraint repeats the four season codes literally because a CHECK cannot read
``tread_category.axis`` from another table. The FK guarantees the code exists; the constraint
guarantees it is a season.

The three ``simpletire_*`` columns record the merge itself rather than its results: which row we
took values from, which of the three match tiers found it, and when. The tier is stored so that a
tier later shown to be unsafe can be retracted by query instead of by re-deriving every match, and
``simpletire_synced_at`` compared against the SKU's own ``updated_at`` is what makes a re-crawl
pick up only what actually moved.

``SET_NULL`` on the FK, not ``PROTECT``: ``simpletire_skus`` is a scrape landing zone that may be
re-crawled or truncated, and losing the pointer must not cascade into losing the specs.

Hand-written for the same reason as 0181-0187: ``makemigrations`` wants to bundle two dozen
unrelated ``AlterField`` operations converting ``id`` to BigAutoField across the catalog tables,
each a full table rewrite on millions of rows.
"""
import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("src", "0187_tire_spec_catalog_fields"),
    ]

    operations = [
        migrations.AddField(
            model_name="tirespec",
            name="season_category",
            field=models.ForeignKey(
                blank=True,
                null=True,
                db_column="season_category",
                on_delete=django.db.models.deletion.PROTECT,
                related_name="tire_specs_by_season",
                to="src.treadcategory",
                help_text="ALL_SEASON / ALL_WEATHER / SUMMER / WINTER. Independent of tread_category.",
            ),
        ),
        migrations.AddField(
            model_name="tirespec",
            name="simpletire_sku",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name="tire_specs",
                to="src.simpletiresku",
                help_text=(
                    "The catalog row the spec block came from. SET_NULL rather than PROTECT: the "
                    "scrape table is a landing zone that may be re-crawled or truncated, and "
                    "losing the pointer must not take the specs with it."
                ),
            ),
        ),
        migrations.AddField(
            model_name="tirespec",
            name="simpletire_match_tier",
            field=models.PositiveSmallIntegerField(
                blank=True,
                null=True,
                help_text=(
                    "How the row was matched: 1 = brand + part number, 2 = part number + agreeing "
                    "size, 3 = brand + model + size. Stored so a tier that later proves unsafe can "
                    "be retracted without re-deriving every match."
                ),
            ),
        ),
        migrations.AddField(
            model_name="tirespec",
            name="simpletire_synced_at",
            field=models.DateTimeField(
                blank=True,
                null=True,
                help_text=(
                    "When the catalog values were last copied in. Compare against the SKU's "
                    "updated_at to find rows a re-crawl has moved on from."
                ),
            ),
        ),
        migrations.AddConstraint(
            model_name="tirespec",
            constraint=models.CheckConstraint(
                check=models.Q(season_category__isnull=True)
                | models.Q(season_category__in=["ALL_SEASON", "ALL_WEATHER", "SUMMER", "WINTER"]),
                name="tire_specs_season_category_valid",
            ),
        ),
    ]
