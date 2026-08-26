"""
Powersports tread categories, and the ``powersports`` axis they live on.

Found while costing a full-catalogue enrichment: Kenda (1,068 parseable tires) and Maxxis (448)
are largely motorcycle and ATV/UTV products, and the original 18-code vocabulary has nowhere to
put them. Without these codes the model shoehorns a motocross tire into ``MT`` and a scooter tire
into ``SUMMER`` -- confidently, and wrongly, because the prompt forces a choice from the list it
is given.

A separate axis rather than more ``terrain`` codes: a motocross knobby and a mud-terrain truck
tire are both "aggressive off-road tread", so one vocabulary would make them compete for the same
code, but nobody cross-shops them. Terrain stays a light-vehicle axis; powersports is its own.

Pairs with the two new ``TireSpec.vehicle_class`` values (``motorcycle``, ``atv_utv``), which is
what a UI would filter on to keep powersports out of a truck-tire search entirely.
"""
from django.db import migrations, models
from django.utils import timezone


# (code, label, sort_order, description). 400-block, after special (310-340).
POWERSPORTS_CATEGORIES = [
    ("MC_STREET", "Motorcycle Street / Sport", 410,
     "Street and sport motorcycle. Shallow tread, high grip, road use."),
    ("MC_TOURING", "Motorcycle Touring / Cruiser", 420,
     "Long-mileage motorcycle tread for cruisers and touring bikes. Comfort and wear over grip."),
    ("MC_ADVENTURE", "Dual-Sport / Adventure", 430,
     "Mixed on/off road motorcycle. Blocky but street legal; the ADV equivalent of an all terrain."),
    ("MC_OFFROAD", "Motocross / Enduro", 440,
     "Knobby off-road motorcycle tread. Motocross, enduro and trials. Usually not street legal."),
    ("MC_TRACK", "Motorcycle Track / Race", 450,
     "Racetrack motorcycle tyre, slick or minimally grooved. Very short life, often DOT-exempt."),
    ("ATV_AT", "ATV / UTV All-Terrain", 460,
     "General purpose ATV, UTV and side-by-side tread. Trail and utility use."),
    ("ATV_MT", "ATV / UTV Mud", 470,
     "Deep-lug ATV and UTV mud tread. Tall paddles-like lugs, poor on hardpack."),
    ("ATV_SPORT", "ATV / UTV Sport", 480,
     "Sport quad and racing ATV/UTV tread. Light construction, high grip, short life."),
    ("TURF", "Turf / Lawn", 490,
     "Low-pressure turf tread for mowers, garden tractors and utility vehicles. Minimises surface damage."),
]


def seed_powersports(apps, schema_editor):
    TreadCategory = apps.get_model("src", "TreadCategory")

    existing = {row.code: row for row in TreadCategory.objects.all()}
    to_create, to_update = [], []
    for code, label, sort_order, description in POWERSPORTS_CATEGORIES:
        values = dict(label=label, axis="powersports", sort_order=sort_order, description=description)
        row = existing.get(code)
        if row is None:
            to_create.append(TreadCategory(code=code, **values))
            continue
        if all(getattr(row, key) == value for key, value in values.items()):
            continue
        for key, value in values.items():
            setattr(row, key, value)
        # bulk_update() doesn't fire auto_now, so stamp it ourselves (as 0177/0181/0182 do).
        row.updated_at = timezone.now()
        to_update.append(row)

    if to_create:
        TreadCategory.objects.bulk_create(to_create)
    if to_update:
        TreadCategory.objects.bulk_update(to_update, ["label", "axis", "sort_order", "description", "updated_at"])


def unseed_powersports(apps, schema_editor):
    """
    Only the codes this migration adds, and only where nothing references them -- a tire already
    classified as MC_OFFROAD would otherwise violate the FK on the way back down.
    """
    TreadCategory = apps.get_model("src", "TreadCategory")
    TireSpec = apps.get_model("src", "TireSpec")
    codes = [row[0] for row in POWERSPORTS_CATEGORIES]
    in_use = set(
        TireSpec.objects.filter(tread_category__in=codes).values_list("tread_category", flat=True).distinct()
    )
    TreadCategory.objects.filter(code__in=[code for code in codes if code not in in_use]).delete()


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0182_facet_config"),
    ]

    operations = [
        # The axis CHECK has to accept "powersports" before any row can use it.
        migrations.RemoveConstraint(model_name="treadcategory", name="tread_category_axis_valid"),
        migrations.AddConstraint(
            model_name="treadcategory",
            constraint=models.CheckConstraint(
                check=models.Q(("axis__in", ["terrain", "season", "performance", "special", "powersports"])),
                name="tread_category_axis_valid",
            ),
        ),
        migrations.AlterField(
            model_name="treadcategory",
            name="axis",
            field=models.CharField(
                choices=[
                    ("terrain", "Terrain"),
                    ("season", "Season"),
                    ("performance", "Performance"),
                    ("special", "Special"),
                    ("powersports", "Powersports"),
                ],
                max_length=16,
            ),
        ),
        migrations.AlterField(
            model_name="tirespec",
            name="vehicle_class",
            field=models.CharField(
                blank=True,
                choices=[
                    ("passenger", "Passenger"),
                    ("light_truck", "Light truck"),
                    ("trailer", "Trailer"),
                    ("commercial", "Commercial"),
                    ("motorcycle", "Motorcycle"),
                    ("atv_utv", "ATV / UTV"),
                ],
                max_length=16,
                null=True,
            ),
        ),
        migrations.RunPython(seed_powersports, unseed_powersports),
    ]
