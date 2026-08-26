"""
``tread_category`` (with its 18-code seed) and ``tire_specs``.

Hand-trimmed. ``makemigrations`` wanted to bundle 23 ``AlterField`` operations turning
``id`` from AutoField to BigAutoField on unrelated models -- pre-existing drift from the
``DEFAULT_AUTO_FIELD`` setting that nobody has migrated, and each one is a full table rewrite on
tables with millions of rows. That drift is a separate decision from adding tire specs, so only
the two new models are here. Re-running ``makemigrations`` will still offer it.

The tread-category seed upserts and lives with the schema rather than in an ingest command, for
the same reason as the tire lookup tables in 0177-0179: the vocabulary is a product decision, not
anything a distributor feed supplies. Editing a ``description`` here changes how the enrichment
model classifies, since the descriptions are what goes into its prompt.
"""
import django.contrib.postgres.fields
from django.db import migrations, models
import django.db.models.deletion
import django.utils.timezone
from django.utils import timezone


# code -> (label, axis, sort_order, description). Terrain first (10-60): it is what truck buyers
# filter on. Season 110-140, performance 210-240, special 310-340.
TREAD_CATEGORIES = [
    ("HT", "Highway Terrain", "terrain", 10,
     "Closed tread, quiet, long wearing. Common OE fitment on trucks and SUVs."),
    ("AT", "All Terrain", "terrain", 20,
     "Moderate blocks. On/off road compromise. The volume seller."),
    ("RT", "Rugged Terrain", "terrain", 30,
     "Hybrid between AT and MT. Aggressive look, less noise than MT. Fast growing."),
    ("MT", "Mud Terrain", "terrain", 40,
     "Large blocks, wide voids, reinforced sidewall. Loud on pavement, poor in wet."),
    ("XT", "Extreme Terrain", "terrain", 50,
     "Beyond MT. Competition crawling and extreme off-road. Rare in retail."),
    ("SAND", "Sand / Paddle", "terrain", 60,
     "Paddle or scoop tread for dunes. Not street legal."),
    ("ALL_SEASON", "All Season", "season", 110,
     "Year-round compromise. Not severe-snow rated unless flagged."),
    ("ALL_WEATHER", "All Weather", "season", 120,
     "All-season tread with severe-snow certification. Year-round in real winters."),
    ("SUMMER", "Summer", "season", 130,
     "Warm-weather compound. Loses grip below about 7C."),
    ("WINTER", "Winter", "season", 140,
     "Soft compound, heavily siped. Seasonal fitment only."),
    ("TOURING", "Touring", "performance", 210,
     "Comfort and tread life prioritised. Quiet, long warranty."),
    ("PERFORMANCE", "Performance", "performance", 220,
     "Responsive handling, higher wet/dry grip, shorter tread life."),
    ("UHP", "Ultra High Performance", "performance", 230,
     "Low profile, maximum grip and steering response. Summer or all-season."),
    ("TRACK", "Track / Competition", "performance", 240,
     "Racetrack, autocross, drag. Very short wear life. Some street legal."),
    ("TRAILER", "Trailer (ST)", "special", 310,
     "Special trailer service. Stiff sidewall, not steered or driven."),
    ("COMMERCIAL", "Commercial / Van", "special", 320,
     "Light commercial. High load, long life, scrub resistant."),
    ("SPARE", "Temporary Spare", "special", 330,
     "Compact spare. Speed and distance restricted."),
    ("VINTAGE", "Vintage / Classic", "special", 340,
     "Period-correct sizes and sidewalls for classic vehicles."),
]


def seed_tread_categories(apps, schema_editor):
    TreadCategory = apps.get_model("src", "TreadCategory")

    existing = {row.code: row for row in TreadCategory.objects.all()}
    to_create, to_update = [], []
    for code, label, axis, sort_order, description in TREAD_CATEGORIES:
        row = existing.get(code)
        if row is None:
            to_create.append(TreadCategory(
                code=code, label=label, axis=axis, sort_order=sort_order, description=description,
            ))
            continue
        if (row.label, row.axis, row.sort_order, row.description) == (label, axis, sort_order, description):
            continue
        row.label, row.axis, row.sort_order, row.description = label, axis, sort_order, description
        # bulk_update() doesn't fire auto_now, so stamp it ourselves (as migration 0177 does).
        row.updated_at = timezone.now()
        to_update.append(row)

    if to_create:
        TreadCategory.objects.bulk_create(to_create)
    if to_update:
        TreadCategory.objects.bulk_update(to_update, ["label", "axis", "sort_order", "description", "updated_at"])


def unseed_tread_categories(apps, schema_editor):
    """Only the codes this migration seeds, so a hand-added category survives a rollback."""
    TreadCategory = apps.get_model("src", "TreadCategory")
    TreadCategory.objects.filter(code__in=[row[0] for row in TREAD_CATEGORIES]).delete()


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0180_turn14_shipping_options"),
    ]

    operations = [
        migrations.CreateModel(
            name='TireSpec',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('notation', models.CharField(help_text='metric / flotation / numeric. Read this before treating overall_diameter_in as exact.', max_length=16)),
                ('service_type', models.CharField(blank=True, max_length=8, null=True)),
                ('section_width_mm', models.PositiveSmallIntegerField(blank=True, null=True)),
                ('aspect_ratio', models.PositiveSmallIntegerField(blank=True, null=True)),
                ('section_width_in', models.DecimalField(blank=True, decimal_places=2, max_digits=5, null=True)),
                ('overall_diameter_in', models.DecimalField(decimal_places=1, help_text='Computed for metric, stated for flotation, NOMINAL for numeric.', max_digits=4)),
                ('construction', models.CharField(blank=True, max_length=4, null=True)),
                ('rim_diameter_in', models.DecimalField(decimal_places=1, max_digits=4)),
                ('load_index', models.PositiveSmallIntegerField(blank=True, null=True)),
                ('load_index_dual', models.PositiveSmallIntegerField(blank=True, null=True)),
                ('speed_rating', models.CharField(blank=True, max_length=8, null=True)),
                ('load_range', models.CharField(blank=True, max_length=8, null=True)),
                ('size_display', models.CharField(max_length=64)),
                ('max_load_lb', models.PositiveIntegerField(blank=True, null=True)),
                ('max_speed_mph', models.PositiveSmallIntegerField(blank=True, null=True)),
                ('ply_rating', models.PositiveSmallIntegerField(blank=True, null=True)),
                ('model_name', models.CharField(blank=True, max_length=255, null=True)),
                ('sub_model', models.CharField(blank=True, max_length=255, null=True)),
                ('vehicle_class', models.CharField(blank=True, choices=[('passenger', 'Passenger'), ('light_truck', 'Light truck'), ('trailer', 'Trailer'), ('commercial', 'Commercial')], max_length=16, null=True)),
                ('search_aliases', django.contrib.postgres.fields.ArrayField(base_field=models.TextField(), blank=True, default=list, help_text='What a customer would type: short forms, misspellings, distributor abbreviations.', size=None)),
                ('use_case_tags', django.contrib.postgres.fields.ArrayField(base_field=models.TextField(), blank=True, default=list, size=None)),
                ('tier', models.CharField(blank=True, choices=[('budget', 'Budget'), ('mid', 'Mid'), ('premium', 'Premium'), ('flagship', 'Flagship')], max_length=16, null=True)),
                ('noise_level', models.CharField(blank=True, choices=[('quiet', 'Quiet'), ('moderate', 'Moderate'), ('loud', 'Loud')], max_length=16, null=True)),
                ('is_3pmsf', models.BooleanField(blank=True, null=True)),
                ('is_ms', models.BooleanField(blank=True, null=True)),
                ('is_run_flat', models.BooleanField(blank=True, null=True)),
                ('is_studdable', models.BooleanField(blank=True, null=True)),
                ('has_reinforced_sidewall', models.BooleanField(blank=True, null=True)),
                ('tread_depth_32nds', models.PositiveSmallIntegerField(blank=True, null=True)),
                ('max_psi', models.PositiveSmallIntegerField(blank=True, help_text="Per tire, from the product's own data. NEVER derived from load range.", null=True)),
                ('rim_width_min_in', models.DecimalField(blank=True, decimal_places=1, max_digits=4, null=True)),
                ('rim_width_max_in', models.DecimalField(blank=True, decimal_places=1, max_digits=4, null=True)),
                ('utqg_treadwear', models.PositiveSmallIntegerField(blank=True, null=True)),
                ('utqg_traction', models.CharField(blank=True, max_length=4, null=True)),
                ('utqg_temperature', models.CharField(blank=True, max_length=4, null=True)),
                ('llm_confidence', models.DecimalField(blank=True, decimal_places=2, max_digits=3, null=True)),
                ('llm_reason', models.TextField(blank=True, null=True)),
                ('llm_model_used', models.CharField(blank=True, max_length=64, null=True)),
                ('size_disputed', models.BooleanField(default=False, help_text='Parser and model disagree, or two providers describe different sizes. Specs are written anyway; review them.')),
                ('category_reconciled', models.BooleanField(default=False, help_text="tread_category was overwritten by the per-model majority vote rather than this SKU's own answer.")),
                ('enriched_at', models.DateTimeField(default=django.utils.timezone.now)),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('updated_at', models.DateTimeField(auto_now=True)),
            ],
            options={
                'db_table': 'tire_specs',
            },
        ),
        migrations.CreateModel(
            name='TreadCategory',
            fields=[
                ('code', models.CharField(max_length=16, primary_key=True, serialize=False)),
                ('label', models.CharField(help_text='What a UI renders. Never show the raw code to a customer.', max_length=64)),
                ('axis', models.CharField(choices=[('terrain', 'Terrain'), ('season', 'Season'), ('performance', 'Performance'), ('special', 'Special')], max_length=16)),
                ('sort_order', models.PositiveSmallIntegerField(help_text='Facet ordering. Terrain first (10-60) because that is what truck buyers filter on.', unique=True)),
                ('description', models.TextField(help_text='Shown to the enrichment model as the definition of the code, so edits here change classifications.')),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('updated_at', models.DateTimeField(auto_now=True)),
            ],
            options={
                'db_table': 'tread_category',
                'ordering': ['sort_order'],
            },
        ),
        migrations.AddConstraint(
            model_name='treadcategory',
            constraint=models.CheckConstraint(check=models.Q(('axis__in', ['terrain', 'season', 'performance', 'special'])), name='tread_category_axis_valid'),
        ),
        migrations.AddField(
            model_name='tirespec',
            name='master_part',
            field=models.OneToOneField(on_delete=django.db.models.deletion.CASCADE, related_name='tire_spec', to='src.masterpart'),
        ),
        migrations.AddField(
            model_name='tirespec',
            name='tread_category',
            field=models.ForeignKey(blank=True, db_column='tread_category', null=True, on_delete=django.db.models.deletion.PROTECT, related_name='tire_specs', to='src.treadcategory'),
        ),
        migrations.AddIndex(
            model_name='tirespec',
            index=models.Index(fields=['rim_diameter_in', 'overall_diameter_in'], name='tire_specs_diameter_idx'),
        ),
        migrations.AddIndex(
            model_name='tirespec',
            index=models.Index(fields=['size_display'], name='tire_specs_size_display_idx'),
        ),
        migrations.AddIndex(
            model_name='tirespec',
            index=models.Index(fields=['model_name'], name='tire_specs_model_name_idx'),
        ),
        migrations.AddIndex(
            model_name='tirespec',
            index=models.Index(fields=['tread_category'], name='tire_specs_tread_category_idx'),
        ),
        migrations.AddConstraint(
            model_name='tirespec',
            constraint=models.CheckConstraint(check=models.Q(('overall_diameter_in__gt', 0)), name='tire_specs_overall_diameter_positive'),
        ),
        migrations.AddConstraint(
            model_name='tirespec',
            constraint=models.CheckConstraint(check=models.Q(('rim_diameter_in__gt', 0)), name='tire_specs_rim_diameter_positive'),
        ),
        migrations.AddConstraint(
            model_name='tirespec',
            constraint=models.CheckConstraint(check=models.Q(('overall_diameter_in__gt', models.F('rim_diameter_in'))), name='tire_specs_taller_than_rim'),
        ),
        migrations.RunPython(seed_tread_categories, unseed_tread_categories),
    ]
