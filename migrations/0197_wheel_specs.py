"""
Create ``wheel_specs``: the wheel counterpart to ``tire_specs``.

Wheels reach us in a much better state than tires did, and the table reflects that. Nothing in the
catalog carried a tire's tread depth or load range as a field, so all of it had to be read out of
distributor titles. Wheels arrive structured: 51,097 master parts sit behind ``wheelpros_parts``,
``thewheelgroup_parts``, ``vossen_parts`` or ``elitewheels_part_wheels``, each publishing diameter,
width, bolt pattern, offset and bore in their own columns -- and the feed says outright that the
row is a wheel, which is identification we had to buy from an LLM for every single tire.

Three details in here are load-bearing rather than decorative:

``bolt_circle_mm`` is millimetres, always. The feeds publish both units in one column: Wheel Pros
writes ``6X5.5`` and ``6X135`` on adjacent rows, and those are a 139.7 mm circle and a 135 mm
circle -- two wheels that do not fit the same car. ``src.domain.wheel_size`` converts inch values
and snaps them to the standard they spell, which took title-versus-feed agreement from 84.5% to
99.7% across 38,501 Wheel Pros rows.

``is_blank_drilled`` with a NULL circle is an undrilled wheel, machined to order. 674 Wheel Pros
rows are these. A constraint enforces that a blank has no circle, because the alternative -- a row
claiming both -- would read as "pattern unknown" and show up as a universal fit.

The second bolt pattern is two nullable column pairs, not a child table. No wheel among ~60,000
SKUs across four feeds is drilled three ways, and a join table would cost every fitment query.

Provenance is a ``(source_feed, source_external_id)`` pair rather than one nullable FK per feed.
There are already four, each a landing zone that may be truncated and reloaded; four FKs would be
four columns NULL three-quarters of the time and four cascade paths to reason about.

Hand-assembled from the autodetector's output, keeping only the ``WheelSpec`` operations: left to
itself ``makemigrations`` sweeps in ~30 unrelated ops from pre-existing model drift (BigAutoField
id changes, Premier and CustomIntegrationRequest constraint churn), several of which are full
table rewrites on multi-million-row tables.
"""
import django.contrib.postgres.fields
import django.db.models.deletion
import django.utils.timezone
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0196_wheelpros_part_api_data"),
    ]

    operations = [
        migrations.CreateModel(
            name='WheelSpec',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('diameter_in', models.DecimalField(decimal_places=2, max_digits=4)),
                ('width_in', models.DecimalField(decimal_places=2, max_digits=4)),
                ('size_display', models.CharField(db_index=True, help_text='As published: "20x9".', max_length=32)),
                ('bolt_lug_count', models.PositiveSmallIntegerField(blank=True, null=True)),
                ('bolt_circle_mm', models.DecimalField(blank=True, decimal_places=2, help_text='Always millimetres. Inch patterns are converted and snapped to the standard they spell.', max_digits=6, null=True)),
                ('bolt_pattern_display', models.CharField(blank=True, help_text='The source spelling, kept for search: a Jeep owner types "6x5.5", not "6x139.7".', max_length=24, null=True)),
                ('bolt_lug_count_2', models.PositiveSmallIntegerField(blank=True, null=True)),
                ('bolt_circle_mm_2', models.DecimalField(blank=True, decimal_places=2, max_digits=6, null=True)),
                ('bolt_pattern_2_display', models.CharField(blank=True, max_length=24, null=True)),
                ('is_blank_drilled', models.BooleanField(default=False, help_text="Undrilled, machined to order. Fits nothing as shipped -- must not be treated as 'pattern unknown'.")),
                ('offset_mm', models.SmallIntegerField(blank=True, help_text='Signed. Zero is a real and common value, not an absence.', null=True)),
                ('backspacing_in', models.DecimalField(blank=True, decimal_places=2, help_text='The off-road way of stating the same thing. Converting needs the width, so both are kept.', max_digits=4, null=True)),
                ('center_bore_mm', models.DecimalField(blank=True, decimal_places=2, max_digits=5, null=True)),
                ('load_rating_lb', models.PositiveIntegerField(blank=True, null=True)),
                ('weight_lb', models.DecimalField(blank=True, decimal_places=2, max_digits=6, null=True)),
                ('model_name', models.CharField(blank=True, db_index=True, max_length=255, null=True)),
                ('sub_model', models.CharField(blank=True, max_length=255, null=True)),
                ('style_number', models.CharField(blank=True, db_index=True, help_text="The manufacturer's own style id (Wheel Pros 'display_style_no', TWG 'style_number').", max_length=64, null=True)),
                ('finish', models.CharField(blank=True, help_text='As published: "SATIN BLACK BRIGHT MACH FACE".', max_length=128, null=True)),
                ('finish_family', models.CharField(blank=True, db_index=True, help_text='The normalised bucket a customer filters on: black, machined, chrome, bronze...', max_length=32, null=True)),
                ('construction', models.CharField(blank=True, choices=[('cast', 'Cast'), ('flow_formed', 'Flow formed'), ('forged', 'Forged'), ('steel', 'Steel'), ('multi_piece', 'Multi piece')], max_length=16, null=True)),
                ('material', models.CharField(blank=True, max_length=32, null=True)),
                ('vehicle_class', models.CharField(blank=True, choices=[('passenger', 'Passenger'), ('light_truck', 'Light truck'), ('trailer', 'Trailer'), ('commercial', 'Commercial'), ('motorcycle', 'Motorcycle'), ('atv_utv', 'ATV / UTV')], max_length=16, null=True)),
                ('is_beadlock', models.BooleanField(blank=True, null=True)),
                ('is_dually', models.BooleanField(blank=True, null=True)),
                ('tpms_compatible', models.BooleanField(blank=True, null=True)),
                ('lug_seat', models.CharField(blank=True, help_text='Conical / ball / flat. Wrong seat means the lug will not hold.', max_length=24, null=True)),
                ('search_aliases', django.contrib.postgres.fields.ArrayField(base_field=models.TextField(), blank=True, default=list, size=None)),
                ('spec_source', models.CharField(choices=[('feed', 'Distributor feed (structured)'), ('parser', 'Parsed from titles'), ('catalog', 'External catalog')], db_index=True, default='parser', max_length=16)),
                ('source_feed', models.CharField(blank=True, db_index=True, help_text='Which feed supplied the dimensions: wheelpros, thewheelgroup, vossen, elitewheels, tdg.', max_length=32, null=True)),
                ('source_external_id', models.CharField(blank=True, max_length=128, null=True)),
                ('size_disputed', models.BooleanField(default=False, help_text='The feed and the title disagree on a dimension. Written anyway; review it.')),
                ('llm_confidence', models.DecimalField(blank=True, decimal_places=2, max_digits=3, null=True)),
                ('llm_reason', models.TextField(blank=True, null=True)),
                ('llm_model_used', models.CharField(blank=True, max_length=64, null=True)),
                ('enriched_at', models.DateTimeField(default=django.utils.timezone.now)),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('updated_at', models.DateTimeField(auto_now=True)),
                ('master_part', models.OneToOneField(on_delete=django.db.models.deletion.CASCADE, related_name='wheel_spec', to='src.masterpart')),
            ],
            options={
                'db_table': 'wheel_specs',
            },
        ),
        migrations.AddIndex(
            model_name='wheelspec',
            index=models.Index(fields=['bolt_lug_count', 'bolt_circle_mm'], name='wheel_specs_bolt_idx'),
        ),
        migrations.AddIndex(
            model_name='wheelspec',
            index=models.Index(fields=['diameter_in', 'width_in'], name='wheel_specs_size_idx'),
        ),
        migrations.AddIndex(
            model_name='wheelspec',
            index=models.Index(fields=['model_name'], name='wheel_specs_model_name_idx'),
        ),
        migrations.AddConstraint(
            model_name='wheelspec',
            constraint=models.CheckConstraint(check=models.Q(('diameter_in__gt', 0), ('width_in__gt', 0)), name='wheel_specs_dimensions_positive'),
        ),
        migrations.AddConstraint(
            model_name='wheelspec',
            constraint=models.CheckConstraint(check=models.Q(('width_in__lte', models.F('diameter_in'))), name='wheel_specs_width_not_over_diameter'),
        ),
        migrations.AddConstraint(
            model_name='wheelspec',
            constraint=models.CheckConstraint(
                check=models.Q(('is_blank_drilled', True), _negated=True) | models.Q(('bolt_circle_mm__isnull', True)),
                name='wheel_specs_blank_has_no_circle',
            ),
        ),
    ]
