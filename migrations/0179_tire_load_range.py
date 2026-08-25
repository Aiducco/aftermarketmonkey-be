"""
Tire load range / load designation lookup table, plus its seed data.

Third of the tire reference tables (see 0177 load index, 0178 speed rating) -- the standard
itself, so the rows ship with the schema and the seed upserts.

Note that ``typical_max_psi`` is a display hint only; the model docstring spells out why load
capacity must never be derived from it.
"""
from django.db import migrations, models
from django.utils import timezone


LT_ST = "lt_st"
PASSENGER = "passenger"

# (load_range, ply_rating, applies_to, typical_max_psi, alias, sort_order).
# Passenger designations sort first, then the LT/ST letters in ascending strength. I, K and O
# are absent by design -- the standard skips them because they read as 1 and 0 on a sidewall.
LOAD_RANGES = [
    ("SL", 4, PASSENGER, 35, None, 0),
    ("XL", 4, PASSENGER, 41, "RF", 1),
    ("A", 2, LT_ST, 35, None, 2),
    ("B", 4, LT_ST, 35, None, 3),
    ("C", 6, LT_ST, 50, None, 4),
    ("D", 8, LT_ST, 65, None, 5),
    ("E", 10, LT_ST, 80, None, 6),
    ("F", 12, LT_ST, 95, None, 7),
    ("G", 14, LT_ST, 110, None, 8),
    ("H", 16, LT_ST, 120, None, 9),
    ("J", 18, LT_ST, None, None, 10),
    ("L", 20, LT_ST, None, None, 11),
    ("M", 22, LT_ST, None, None, 12),
    ("N", 24, LT_ST, None, None, 13),
]

_FIELDS = ["ply_rating", "applies_to", "typical_max_psi", "alias", "sort_order"]


def seed_load_ranges(apps, schema_editor):
    TireLoadRange = apps.get_model("src", "TireLoadRange")

    existing = {row.load_range: row for row in TireLoadRange.objects.all()}
    to_create = []
    to_update = []
    for load_range, ply_rating, applies_to, typical_max_psi, alias, sort_order in LOAD_RANGES:
        values = {
            "ply_rating": ply_rating,
            "applies_to": applies_to,
            "typical_max_psi": typical_max_psi,
            "alias": alias,
            "sort_order": sort_order,
        }
        row = existing.get(load_range)
        if row is None:
            to_create.append(TireLoadRange(load_range=load_range, **values))
        elif any(getattr(row, field) != value for field, value in values.items()):
            for field, value in values.items():
                setattr(row, field, value)
            # bulk_update() doesn't fire auto_now, so stamp it ourselves.
            row.updated_at = timezone.now()
            to_update.append(row)

    if to_create:
        TireLoadRange.objects.bulk_create(to_create)
    if to_update:
        # sort_order is unique and NOT NULL, so a reshuffle has to move the old values out of the
        # way before writing the new ones, or the batch collides with itself mid-statement.
        # LOAD_RANGES only ever uses 0-13, so +1000 is guaranteed free.
        TireLoadRange.objects.filter(pk__in=[row.pk for row in to_update]).update(
            sort_order=models.F("sort_order") + 1000
        )
        TireLoadRange.objects.bulk_update(to_update, _FIELDS + ["updated_at"])


def unseed_load_ranges(apps, schema_editor):
    """Only the designations this migration seeds, so a hand-added row survives."""
    TireLoadRange = apps.get_model("src", "TireLoadRange")
    TireLoadRange.objects.filter(load_range__in=[row[0] for row in LOAD_RANGES]).delete()


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0178_tire_speed_rating"),
    ]

    operations = [
        migrations.CreateModel(
            name="TireLoadRange",
            fields=[
                ("load_range", models.CharField(max_length=8, primary_key=True, serialize=False)),
                (
                    "ply_rating",
                    models.PositiveSmallIntegerField(
                        help_text="Bias-ply strength equivalence, not a count of physical layers."
                    ),
                ),
                (
                    "applies_to",
                    models.CharField(
                        choices=[("lt_st", "LT / ST"), ("passenger", "Passenger (P-metric)")], max_length=16
                    ),
                ),
                (
                    "typical_max_psi",
                    models.PositiveSmallIntegerField(
                        blank=True,
                        null=True,
                        help_text=(
                            "Indicative only -- never derive load capacity from this. "
                            "Use the product's own max pressure."
                        ),
                    ),
                ),
                (
                    "alias",
                    models.CharField(
                        blank=True,
                        max_length=8,
                        null=True,
                        help_text="Alternate sidewall stamping for the same designation (XL is also stamped RF).",
                    ),
                ),
                ("sort_order", models.PositiveSmallIntegerField(unique=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "db_table": "load_range_ply",
                "ordering": ["sort_order"],
            },
        ),
        migrations.AddConstraint(
            model_name="tireloadrange",
            constraint=models.CheckConstraint(
                check=models.Q(("applies_to__in", ["lt_st", "passenger"])),
                name="load_range_ply_applies_to_valid",
            ),
        ),
        migrations.AddConstraint(
            model_name="tireloadrange",
            constraint=models.CheckConstraint(
                check=models.Q(("ply_rating__gt", 0)), name="load_range_ply_ply_rating_positive"
            ),
        ),
        migrations.RunPython(seed_load_ranges, unseed_load_ranges),
    ]
