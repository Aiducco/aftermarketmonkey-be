"""
``facet_config`` plus the tire-mode rows.

Hand-written for the same reason as 0181: ``makemigrations`` still wants to bundle the 23
unrelated ``AlterField`` operations converting ``id`` to BigAutoField across the catalog tables
(pre-existing ``DEFAULT_AUTO_FIELD`` drift, each one a full table rewrite). Only this model is
here.

Field names are the tires index's own ``filterableAttributes``, not the shorter names in the
handoff document -- ``brand_name`` rather than ``brand``, ``distributor_names`` rather than
``distributor_ids``. A facet whose field is not filterable on the index returns nothing, so the
names have to match the index exactly; the display text is what ``label`` is for.
"""
from django.db import migrations, models
from django.utils import timezone


MULTISELECT = "multiselect"
RANGE = "range"
TOGGLE = "toggle"

# (field, label, widget, sort_order, collapse_after, unit, value_labels)
#
# Order is the handoff document's: terrain first, because it is what truck buyers filter on,
# then the size fields, then service description, then commerce.
TIRE_FACETS = [
    ("tread_category", "Tread type", MULTISELECT, 10, 8, None, None),
    ("rim_diameter_in", "Wheel size", MULTISELECT, 20, 10, "in", None),
    ("section_width_mm", "Section width", MULTISELECT, 30, 10, "mm", None),
    ("aspect_ratio", "Aspect ratio", MULTISELECT, 40, 10, "%", None),
    ("overall_diameter_in", "Overall diameter", RANGE, 50, 8, "in", None),
    ("load_range", "Load range", MULTISELECT, 60, 8, None, None),
    (
        "speed_rating",
        "Speed rating",
        MULTISELECT,
        70,
        8,
        None,
        # The FE cannot order these itself: H is 210 km/h and belongs between U and V, so
        # alphabetical order is simply wrong. Labels carry the speed so the order reads as
        # sensible rather than arbitrary.
        {
            "Q": "Q — 99 mph", "R": "R — 106 mph", "S": "S — 112 mph", "T": "T — 118 mph",
            "U": "U — 124 mph", "H": "H — 130 mph", "V": "V — 149 mph", "W": "W — 168 mph",
            "Y": "Y — 186 mph",
        },
    ),
    ("is_3pmsf", "Severe snow rated (3PMSF)", TOGGLE, 80, 8, None, None),
    (
        "service_type",
        "Service type",
        MULTISELECT,
        90,
        8,
        None,
        {"LT": "Light truck", "P": "Passenger", "ST": "Trailer", "T": "Temporary spare", "C": "Commercial"},
    ),
    ("brand_name", "Brand", MULTISELECT, 100, 8, None, None),
    ("use_case_tags", "Use case", MULTISELECT, 110, 8, None, None),
    ("in_stock", "In stock", TOGGLE, 120, 8, None, None),
    ("distributor_names", "Distributor", MULTISELECT, 130, 8, None, None),
]


def seed_facets(apps, schema_editor):
    FacetConfig = apps.get_model("src", "FacetConfig")

    existing = {row.field: row for row in FacetConfig.objects.filter(mode="tire")}
    to_create, to_update = [], []
    for field, label, widget, sort_order, collapse_after, unit, value_labels in TIRE_FACETS:
        row = existing.get(field)
        values = dict(
            label=label, widget=widget, sort_order=sort_order,
            collapse_after=collapse_after, unit=unit, value_labels=value_labels,
        )
        if row is None:
            to_create.append(FacetConfig(mode="tire", field=field, **values))
            continue
        if all(getattr(row, key) == value for key, value in values.items()):
            continue
        for key, value in values.items():
            setattr(row, key, value)
        # bulk_update() doesn't fire auto_now, so stamp it ourselves (as 0177 and 0181 do).
        row.updated_at = timezone.now()
        to_update.append(row)

    if to_create:
        FacetConfig.objects.bulk_create(to_create)
    if to_update:
        FacetConfig.objects.bulk_update(
            to_update, ["label", "widget", "sort_order", "collapse_after", "unit", "value_labels", "updated_at"]
        )


def unseed_facets(apps, schema_editor):
    """Only the fields this migration seeds, so a hand-added facet survives a rollback."""
    FacetConfig = apps.get_model("src", "FacetConfig")
    FacetConfig.objects.filter(mode="tire", field__in=[row[0] for row in TIRE_FACETS]).delete()


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0181_tire_specs"),
    ]

    operations = [
        migrations.CreateModel(
            name="FacetConfig",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("mode", models.CharField(choices=[("tire", "Tire"), ("wheel", "Wheel"), ("part", "Part")], max_length=16)),
                ("field", models.CharField(max_length=64)),
                ("label", models.CharField(max_length=64)),
                (
                    "widget",
                    models.CharField(
                        choices=[("multiselect", "Multi-select"), ("range", "Range"), ("toggle", "Toggle")],
                        max_length=16,
                    ),
                ),
                ("sort_order", models.PositiveSmallIntegerField()),
                (
                    "collapse_after",
                    models.PositiveSmallIntegerField(
                        default=8, help_text="Show this many values before a 'show more' control."
                    ),
                ),
                ("unit", models.CharField(blank=True, max_length=16, null=True)),
                (
                    "value_labels",
                    models.JSONField(
                        blank=True,
                        help_text='Raw index value -> display text, e.g. {"MT": "Mud terrain"}.',
                        null=True,
                    ),
                ),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "db_table": "facet_config",
                "ordering": ["mode", "sort_order"],
                "unique_together": {("mode", "field")},
            },
        ),
        migrations.AddConstraint(
            model_name="facetconfig",
            constraint=models.CheckConstraint(
                check=models.Q(("widget__in", ["multiselect", "range", "toggle"])),
                name="facet_config_widget_valid",
            ),
        ),
        migrations.RunPython(seed_facets, unseed_facets),
    ]
