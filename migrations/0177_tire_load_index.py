"""
Tire load index lookup table, plus its seed data.

The table is the standard itself -- it has no rows that come from a distributor feed or a user,
so the data ships with the schema rather than through an ingest command. Kilograms are the
stored value because the standard is defined in kg; pounds are derived in
``TireLoadIndex.max_load_lb`` (see the model docstring for why).

The seed upserts, so re-running against a database that already has the table only corrects
drift, and adding indices later is a matter of extending LOAD_INDEX_KG in a new migration.
"""
from django.db import migrations, models
from django.utils import timezone


# load_index -> max load per tire in kilograms. Range 60-150.
LOAD_INDEX_KG = {
    60: 250, 61: 257, 62: 265, 63: 272, 64: 280, 65: 290, 66: 300, 67: 307, 68: 315, 69: 325,
    70: 335, 71: 345, 72: 355, 73: 365, 74: 375, 75: 387, 76: 400, 77: 412, 78: 425, 79: 437,
    80: 450, 81: 462, 82: 475, 83: 487, 84: 500, 85: 515, 86: 530, 87: 545, 88: 560, 89: 580,
    90: 600, 91: 615, 92: 630, 93: 650, 94: 670, 95: 690, 96: 710, 97: 730, 98: 750, 99: 775,
    100: 800, 101: 825, 102: 850, 103: 875, 104: 900, 105: 925, 106: 950, 107: 975, 108: 1000,
    109: 1030, 110: 1060, 111: 1090, 112: 1120, 113: 1150, 114: 1180, 115: 1215, 116: 1250,
    117: 1285, 118: 1320, 119: 1360, 120: 1400, 121: 1450, 122: 1500, 123: 1550, 124: 1600,
    125: 1650, 126: 1700, 127: 1750, 128: 1800, 129: 1850, 130: 1900, 131: 1950, 132: 2000,
    133: 2060, 134: 2120, 135: 2180, 136: 2240, 137: 2300, 138: 2360, 139: 2430, 140: 2500,
    141: 2575, 142: 2650, 143: 2725, 144: 2800, 145: 2900, 146: 3000, 147: 3075, 148: 3150,
    149: 3250, 150: 3350,
}


def seed_tire_load_index(apps, schema_editor):
    TireLoadIndex = apps.get_model("src", "TireLoadIndex")

    existing = {row.load_index: row for row in TireLoadIndex.objects.all()}
    to_create = []
    to_update = []
    for load_index, max_load_kg in LOAD_INDEX_KG.items():
        row = existing.get(load_index)
        if row is None:
            to_create.append(TireLoadIndex(load_index=load_index, max_load_kg=max_load_kg))
        elif row.max_load_kg != max_load_kg:
            row.max_load_kg = max_load_kg
            # bulk_update() doesn't fire auto_now, so stamp it ourselves.
            row.updated_at = timezone.now()
            to_update.append(row)

    if to_create:
        TireLoadIndex.objects.bulk_create(to_create)
    if to_update:
        TireLoadIndex.objects.bulk_update(to_update, ["max_load_kg", "updated_at"])


def unseed_tire_load_index(apps, schema_editor):
    """Only the indices this migration seeds, so a hand-added row outside 60-150 survives."""
    TireLoadIndex = apps.get_model("src", "TireLoadIndex")
    TireLoadIndex.objects.filter(load_index__in=list(LOAD_INDEX_KG)).delete()


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0176_turn14_dropship_and_shipping_estimates"),
    ]

    operations = [
        migrations.CreateModel(
            name="TireLoadIndex",
            fields=[
                ("load_index", models.PositiveSmallIntegerField(primary_key=True, serialize=False)),
                (
                    "max_load_kg",
                    models.PositiveIntegerField(
                        help_text="Maximum load per tire in kilograms -- the canonical value from the standard."
                    ),
                ),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "db_table": "tire_load_index",
                "ordering": ["load_index"],
            },
        ),
        migrations.AddConstraint(
            model_name="tireloadindex",
            constraint=models.CheckConstraint(
                check=models.Q(("max_load_kg__gt", 0)), name="tire_load_index_max_load_kg_positive"
            ),
        ),
        migrations.RunPython(seed_tire_load_index, unseed_tire_load_index),
    ]
