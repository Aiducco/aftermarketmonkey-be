"""
The tire facet rail as the FE handoff specifies it: four visibility/order columns on
``facet_config``, plus the rows.

Hand-written for the same reason as 0181/0182: ``makemigrations`` still wants to bundle 23
unrelated ``AlterField`` operations converting ``id`` to BigAutoField across the catalog tables
(pre-existing ``DEFAULT_AUTO_FIELD`` drift, each one a full table rewrite).

What changes, beyond the three new facets:

  * ``vehicle_class`` goes to the top of the rail. It is the highest-value split we were not
    offering at all -- without it a motorcycle tire and a 37" mud terrain sit in the same result
    set, which reads as a broken catalog rather than a missing filter.
  * ``distributor_names`` becomes ``distributor_ids``. Both are filterable on the index; the ids
    are stable and the names are not (a provider rename would silently orphan every saved filter
    and every chip), and the search service labels the ids from ``providers`` so nothing is lost
    on screen.
  * ``overall_diameter_in``, ``service_type``, ``speed_rating``, ``is_3pmsf`` and ``oe_marking``
    become conditional -- see the column help text on ``FacetConfig``. They were not hidden
    before, they were simply always rendered, which is how "Service type" ends up offering a
    single value on a result set that has only one.

Field names are the tires index's own ``filterableAttributes``, not the shorter names in the
handoff document -- ``brand_name`` rather than ``brand``. A facet whose field is not filterable
on the index returns nothing, so the names have to match the index exactly; the display text is
what ``label`` is for. ``oe_marking`` is filterable only as of the same change that adds it to
the projection, so **this migration requires a tires reindex to take effect**:

    manage.py index_tires_meilisearch --swap
"""
from django.db import migrations, models
from django.utils import timezone


MULTISELECT = "multiselect"
RANGE = "range"
TOGGLE = "toggle"

COUNT = "count"
NUMERIC = "numeric"
VOCABULARY = "vocabulary"

# field, label, widget, sort_order, collapse_after, unit, value_labels, value_order,
# min_distinct_values, requires_filter_on, requires_true_value
#
# value_labels is left None wherever a table already owns the vocabulary (tread_category,
# load_range, speed_rating, vehicle_class, tier, providers) -- src.api.services.tire_search
# injects those at read time so the rail and the detail panel can never disagree.
TIRE_FACETS = [
    # ---- always shown ------------------------------------------------------------------------
    ("vehicle_class", "Vehicle type", MULTISELECT, 10, 8, None, None, COUNT, 1, None, False),
    ("tread_category", "Tread type", MULTISELECT, 20, 8, None, None, VOCABULARY, 1, None, False),
    ("rim_diameter_in", "Wheel size", MULTISELECT, 30, 10, "in", None, NUMERIC, 1, None, False),
    ("section_width_mm", "Section width", MULTISELECT, 40, 10, "mm", None, NUMERIC, 1, None, False),
    ("aspect_ratio", "Aspect ratio", MULTISELECT, 50, 10, "%", None, NUMERIC, 1, None, False),
    ("load_range", "Load range", MULTISELECT, 60, 8, None, None, VOCABULARY, 1, None, False),
    ("brand_name", "Brand", MULTISELECT, 70, 8, None, None, COUNT, 1, None, False),
    ("in_stock", "In stock", TOGGLE, 80, 8, None, None, COUNT, 1, None, False),
    ("distributor_ids", "Distributor", MULTISELECT, 90, 8, None, None, COUNT, 1, None, False),
    # ---- conditional -------------------------------------------------------------------------
    # Unscoped this spans 14.7" (lawn) to 37.4" (off-road) and is meaningless; scoped to a wheel
    # size it is the control that answers "how tall can I go".
    ("overall_diameter_in", "Overall diameter", RANGE, 100, 8, "in", None, NUMERIC, 1, "rim_diameter_in", False),
    ("service_type", "Service type", MULTISELECT, 110, 8, None,
     {"P": "Passenger", "LT": "Light truck", "ST": "Trailer", "T": "Temporary spare", "C": "Commercial"},
     COUNT, 2, None, False),
    ("speed_rating", "Speed rating", MULTISELECT, 120, 8, None, None, VOCABULARY, 2, None, False),
    ("is_3pmsf", "Severe snow rated (3PMSF)", TOGGLE, 130, 8, None, None, COUNT, 1, None, True),
    ("tier", "Tier", MULTISELECT, 140, 8, None, None, COUNT, 1, None, False),
    # 3,986 tires carry one and buyers search them by name ("Porsche N0"). Hidden on any result
    # set where nothing is homologated, which is most of them.
    ("oe_marking", "OE approval", MULTISELECT, 150, 8, None, None, COUNT, 1, None, False),
]

# Superseded by distributor_ids. Dropped rather than left in place: two Distributor facets on one
# rail is worse than either alone.
RETIRED_FIELDS = ["distributor_names"]

_COLUMNS = (
    "label", "widget", "sort_order", "collapse_after", "unit", "value_labels",
    "value_order", "min_distinct_values", "requires_filter_on", "requires_true_value",
)


def seed_facets(apps, schema_editor):
    FacetConfig = apps.get_model("src", "FacetConfig")

    FacetConfig.objects.filter(mode="tire", field__in=RETIRED_FIELDS).delete()

    existing = {row.field: row for row in FacetConfig.objects.filter(mode="tire")}
    to_create, to_update = [], []
    for row_spec in TIRE_FACETS:
        field = row_spec[0]
        values = dict(zip(_COLUMNS, row_spec[1:]))
        row = existing.get(field)
        if row is None:
            to_create.append(FacetConfig(mode="tire", field=field, **values))
            continue
        if all(getattr(row, key) == value for key, value in values.items()):
            continue
        for key, value in values.items():
            setattr(row, key, value)
        # bulk_update() doesn't fire auto_now, so stamp it ourselves (as 0177/0181/0182 do).
        row.updated_at = timezone.now()
        to_update.append(row)

    if to_create:
        FacetConfig.objects.bulk_create(to_create)
    if to_update:
        FacetConfig.objects.bulk_update(to_update, list(_COLUMNS) + ["updated_at"])


def unseed_facets(apps, schema_editor):
    """
    Back to the 0182 rail: drop the three facets that did not exist then and restore the
    distributor facet to names. The rest keep their new labels and order -- a rollback of the
    columns cannot restore per-row values the columns no longer hold, and the old rail's ordering
    is not worth a second data table to remember.
    """
    FacetConfig = apps.get_model("src", "FacetConfig")
    FacetConfig.objects.filter(mode="tire", field__in=["vehicle_class", "tier", "oe_marking", "distributor_ids"]).delete()
    if not FacetConfig.objects.filter(mode="tire", field="distributor_names").exists():
        FacetConfig.objects.create(
            mode="tire", field="distributor_names", label="Distributor",
            widget=MULTISELECT, sort_order=130, collapse_after=8,
        )


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0192_tire_spec_tdg_link"),
    ]

    operations = [
        migrations.AddField(
            model_name="facetconfig",
            name="value_order",
            field=models.CharField(
                choices=[
                    ("count", "Most results first"),
                    ("numeric", "Numeric, ascending"),
                    ("vocabulary", "The reference table's own order"),
                ],
                default="count",
                help_text="How to order the values inside this facet.",
                max_length=16,
            ),
        ),
        migrations.AddField(
            model_name="facetconfig",
            name="min_distinct_values",
            field=models.PositiveSmallIntegerField(
                default=1,
                help_text=(
                    "Hide the facet unless the result set has at least this many distinct values. 2 means "
                    "'only when it can actually split the results' -- a one-value filter is a dead control."
                ),
            ),
        ),
        migrations.AddField(
            model_name="facetconfig",
            name="requires_filter_on",
            field=models.CharField(
                blank=True,
                help_text=(
                    "Render only once this other field is filtered. Overall diameter is the case: "
                    "unscoped it spans lawn tires to 37s and means nothing; scoped to one wheel size it "
                    "is the most useful control on the rail."
                ),
                max_length=64,
                null=True,
            ),
        ),
        migrations.AddField(
            model_name="facetconfig",
            name="requires_true_value",
            field=models.BooleanField(
                default=False,
                help_text=(
                    "Toggle facets: render only when some row in the result set is actually true. Keeps "
                    "3PMSF hidden while that certification is unknown catalog-wide, instead of offering a "
                    "filter that empties the page."
                ),
            ),
        ),
        migrations.AddConstraint(
            model_name="facetconfig",
            constraint=models.CheckConstraint(
                check=models.Q(("value_order__in", ["count", "numeric", "vocabulary"])),
                name="facet_config_value_order_valid",
            ),
        ),
        migrations.RunPython(seed_facets, unseed_facets),
    ]
