"""
Tire speed rating lookup table, plus its seed data.

Like the load index in 0177, this table is the standard itself rather than anything a feed
supplies, so the rows ship with the schema. km/h is stored and mph is derived in
``TireSpeedRating.max_speed_mph``; see the model docstring for why, and for why ZR/Z are not
rows here.

The seed upserts, so re-running only corrects drift.
"""
from django.db import migrations, models
from django.utils import timezone


# code -> (max speed in km/h, sort order). Sort order is ascending by speed, which is why H
# (210) lands between U (200) and V (240) rather than up with the letters near it. None km/h is
# the open-ended (Y): above 300 km/h, consult the manufacturer.
SPEED_RATINGS = [
    ("A1", 5, 0),
    ("A2", 10, 1),
    ("A3", 15, 2),
    ("A4", 20, 3),
    ("A5", 25, 4),
    ("A6", 30, 5),
    ("A7", 35, 6),
    ("A8", 40, 7),
    ("B", 50, 8),
    ("C", 60, 9),
    ("D", 65, 10),
    ("E", 70, 11),
    ("F", 80, 12),
    ("G", 90, 13),
    ("J", 100, 14),
    ("K", 110, 15),
    ("L", 120, 16),
    ("M", 130, 17),
    ("N", 140, 18),
    ("P", 150, 19),
    ("Q", 160, 20),
    ("R", 170, 21),
    ("S", 180, 22),
    ("T", 190, 23),
    ("U", 200, 24),
    ("H", 210, 25),
    ("V", 240, 26),
    ("W", 270, 27),
    ("Y", 300, 28),
    ("(Y)", None, 29),
]


def seed_speed_ratings(apps, schema_editor):
    TireSpeedRating = apps.get_model("src", "TireSpeedRating")

    existing = {row.code: row for row in TireSpeedRating.objects.all()}
    to_create = []
    to_update = []
    for code, max_speed_kmh, sort_order in SPEED_RATINGS:
        row = existing.get(code)
        if row is None:
            to_create.append(
                TireSpeedRating(code=code, max_speed_kmh=max_speed_kmh, sort_order=sort_order)
            )
        elif (row.max_speed_kmh, row.sort_order) != (max_speed_kmh, sort_order):
            row.max_speed_kmh = max_speed_kmh
            row.sort_order = sort_order
            # bulk_update() doesn't fire auto_now, so stamp it ourselves.
            row.updated_at = timezone.now()
            to_update.append(row)

    if to_create:
        TireSpeedRating.objects.bulk_create(to_create)
    if to_update:
        # sort_order is unique and NOT NULL, so a reshuffle has to move the old values out of
        # the way before writing the new ones, or the batch collides with itself mid-statement.
        # SPEED_RATINGS only ever uses 0-29, so +1000 is guaranteed free.
        TireSpeedRating.objects.filter(pk__in=[row.pk for row in to_update]).update(
            sort_order=models.F("sort_order") + 1000
        )
        TireSpeedRating.objects.bulk_update(to_update, ["max_speed_kmh", "sort_order", "updated_at"])


def unseed_speed_ratings(apps, schema_editor):
    """Only the codes this migration seeds, so a hand-added rating survives."""
    TireSpeedRating = apps.get_model("src", "TireSpeedRating")
    TireSpeedRating.objects.filter(code__in=[code for code, _, _ in SPEED_RATINGS]).delete()


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0177_tire_load_index"),
    ]

    operations = [
        migrations.CreateModel(
            name="TireSpeedRating",
            fields=[
                ("code", models.CharField(max_length=8, primary_key=True, serialize=False)),
                (
                    "max_speed_kmh",
                    models.PositiveSmallIntegerField(
                        blank=True,
                        null=True,
                        help_text="Maximum sustained speed in km/h. NULL means the open-ended (Y): above 300 km/h.",
                    ),
                ),
                (
                    "sort_order",
                    models.PositiveSmallIntegerField(
                        unique=True,
                        help_text="Ascending by speed, not alphabetical -- H falls between U and V.",
                    ),
                ),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "db_table": "speed_rating",
                "ordering": ["sort_order"],
            },
        ),
        migrations.AddConstraint(
            model_name="tirespeedrating",
            constraint=models.CheckConstraint(
                check=models.Q(("max_speed_kmh__gt", 0), ("max_speed_kmh__isnull", True), _connector="OR"),
                name="speed_rating_max_speed_kmh_positive",
            ),
        ),
        migrations.RunPython(seed_speed_ratings, unseed_speed_ratings),
    ]
