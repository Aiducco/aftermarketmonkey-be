"""
Landing table for the simpletire.com catalog scrape -- see
``src/integrations/services/simpletire.py`` and the ``fetch_simpletire_catalog`` command.

Additive only: one new table, two indexes, nothing else touched. (``makemigrations`` also wanted
to emit ~30 unrelated AlterField/AlterModelOptions operations for pre-existing drift between
models.py and the migration history; those are somebody else's change and were removed here so
this migration stays reviewable and independently revertible.)
"""
import django.core.serializers.json
import django.utils.timezone
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0184_load_range_aliases"),
    ]

    operations = [
        migrations.CreateModel(
            name='SimpleTireSku',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('item_id', models.BigIntegerField(help_text="SimpleTire's SKU id (siteProductLineSizeDetail.id). Natural key; upsert target.", unique=True)),
                ('part_number', models.CharField(blank=True, db_index=True, help_text='Manufacturer part number / MPN as SimpleTire lists it. Not unique: two brands can collide.', max_length=255, null=True)),
                ('product_line_id', models.IntegerField(blank=True, db_index=True, help_text="SimpleTire's model id. Shared by every SKU of the same tire model.", null=True)),
                ('brand_slug', models.CharField(db_index=True, max_length=128)),
                ('product_line_slug', models.CharField(db_index=True, max_length=255)),
                ('page_url', models.TextField(help_text='Human-facing PDP the row came from.')),
                ('brand_name', models.CharField(blank=True, db_index=True, max_length=255, null=True)),
                ('brand_tier', models.PositiveSmallIntegerField(blank=True, help_text="SimpleTire's own 1-3 brand ranking. Their editorial opinion, not a fact about the brand.", null=True)),
                ('brand_logo_url', models.TextField(blank=True, null=True)),
                ('product_line_name', models.CharField(blank=True, max_length=255, null=True)),
                ('product_line_overview', models.TextField(blank=True, help_text='Marketing copy. Contains HTML (<ul>/<b>) -- escape before rendering.', null=True)),
                ('product_line_image_url', models.TextField(blank=True, null=True)),
                ('starting_price_cents', models.IntegerField(blank=True, help_text='Cheapest SKU in the line at scrape time -- a line-level figure, repeated on each row.', null=True)),
                ('size_display', models.CharField(blank=True, db_index=True, help_text="As shown: '265/70R18', 'LT285/75R16', '11R22.5', '18x9.50-8'. Not normalized.", max_length=64, null=True)),
                ('tire_size_slug', models.CharField(blank=True, help_text="SimpleTire's URL form ('265-70rr18'). Required, with item_id, to re-fetch this SKU.", max_length=64, null=True)),
                ('load_speed_rating', models.CharField(blank=True, help_text="Combined as printed: '116S', '106/104T'.", max_length=16, null=True)),
                ('load_range', models.CharField(blank=True, max_length=16, null=True)),
                ('rim_diameter_in', models.DecimalField(blank=True, decimal_places=2, max_digits=5, null=True)),
                ('product_type_id', models.PositiveSmallIntegerField(blank=True, null=True)),
                ('product_sub_type', models.CharField(blank=True, db_index=True, help_text='Passenger / Light Truck / Commercial / Trailer / ATV-UTV / Farm / OTR / ...', max_length=64, null=True)),
                ('product_status', models.CharField(blank=True, db_index=True, help_text="ProductStatusAvailable / ProductStatusOutOfStock. Out-of-stock sizes are largely absent from the size list, so this is mostly 'Available' plus the fallback SKU of a dead line.", max_length=64, null=True)),
                ('quantity', models.IntegerField(blank=True, help_text='Units SimpleTire showed as on hand.', null=True)),
                ('delivery_days', models.PositiveSmallIntegerField(blank=True, null=True)),
                ('estimated_retail_price_cents', models.IntegerField(blank=True, null=True)),
                ('sale_price_cents', models.IntegerField(blank=True, db_index=True, null=True)),
                ('web_price_cents', models.IntegerField(blank=True, null=True)),
                ('price_label', models.CharField(blank=True, help_text="e.g. '36% off'.", max_length=64, null=True)),
                ('road_hazard_price_cents', models.IntegerField(blank=True, null=True)),
                ('road_hazard_duration_label', models.CharField(blank=True, max_length=64, null=True)),
                ('oversize_fee_cents', models.IntegerField(blank=True, null=True)),
                ('fet_fee_cents', models.IntegerField(blank=True, help_text='Federal Excise Tax. Sent as a number whose unit the API does not state; stored verbatim.', null=True)),
                ('is_run_flat', models.BooleanField(blank=True, null=True)),
                ('is_electric_optimized', models.BooleanField(blank=True, null=True)),
                ('is_oversized', models.BooleanField(blank=True, null=True)),
                ('is_installable', models.BooleanField(blank=True, null=True)),
                ('simple_score', models.DecimalField(blank=True, decimal_places=2, max_digits=4, null=True)),
                ('handling_durability_score', models.DecimalField(blank=True, decimal_places=2, max_digits=4, null=True)),
                ('longevity_score', models.DecimalField(blank=True, decimal_places=2, max_digits=4, null=True)),
                ('traction_score', models.DecimalField(blank=True, decimal_places=2, max_digits=4, null=True)),
                ('spec_category', models.CharField(blank=True, db_index=True, help_text="SimpleTire's tread category: All Season / All Terrain / Winter / Mud Terrain / UHP / ...", max_length=64, null=True)),
                ('spec_vehicle', models.CharField(blank=True, max_length=64, null=True)),
                ('spec_sidewall', models.CharField(blank=True, help_text='Blackwall / Outlined White Lettering / Tubeless / Tube-Type / Blue Stripe / ...', max_length=64, null=True)),
                ('spec_tread_design', models.CharField(blank=True, help_text='Symmetrical / Asymmetrical / Directional.', max_length=32, null=True)),
                ('spec_load_range', models.CharField(blank=True, help_text="Printed form: 'Standard (SL)', 'E (10 Ply)'.", max_length=32, null=True)),
                ('spec_ply_rating', models.PositiveSmallIntegerField(blank=True, help_text="Parsed out of 'E (10 Ply)'. NULL for SL/XL, which state no ply count.", null=True)),
                ('spec_load_index', models.PositiveSmallIntegerField(blank=True, help_text='Single tire. Dual, when given, goes to spec_load_index_dual.', null=True)),
                ('spec_load_index_dual', models.PositiveSmallIntegerField(blank=True, null=True)),
                ('spec_max_load_lb', models.PositiveIntegerField(blank=True, null=True)),
                ('spec_max_load_dual_lb', models.PositiveIntegerField(blank=True, null=True)),
                ('spec_speed_rating', models.CharField(blank=True, help_text="Letter symbol from 'Max Speed', e.g. S, H, W, A8.", max_length=8, null=True)),
                ('spec_max_speed_mph', models.PositiveSmallIntegerField(blank=True, null=True)),
                ('spec_tread_depth_32nds', models.DecimalField(blank=True, decimal_places=2, help_text="In 32nds, as printed ('11/32nds').", max_digits=5, null=True)),
                ('spec_overall_diameter_in', models.DecimalField(blank=True, decimal_places=2, max_digits=6, null=True)),
                ('spec_section_width_in', models.DecimalField(blank=True, decimal_places=2, max_digits=6, null=True)),
                ('spec_max_psi', models.PositiveSmallIntegerField(blank=True, help_text="From the 'Inflation Pressure' spec. Never derived from load range.", null=True)),
                ('spec_rim_width_min_in', models.DecimalField(blank=True, decimal_places=2, max_digits=5, null=True)),
                ('spec_rim_width_max_in', models.DecimalField(blank=True, decimal_places=2, help_text='Equal to the min when \'Rim Range\' names a single width (\'8.25"\').', max_digits=5, null=True)),
                ('spec_tire_weight_lb', models.DecimalField(blank=True, decimal_places=2, max_digits=7, null=True)),
                ('spec_utqg', models.CharField(blank=True, help_text="Verbatim, e.g. '460AA'.", max_length=16, null=True)),
                ('spec_utqg_treadwear', models.PositiveSmallIntegerField(blank=True, null=True)),
                ('spec_utqg_traction', models.CharField(blank=True, max_length=4, null=True)),
                ('spec_utqg_temperature', models.CharField(blank=True, max_length=4, null=True)),
                ('spec_wet_traction', models.CharField(blank=True, max_length=8, null=True)),
                ('spec_mileage_warranty', models.CharField(blank=True, help_text="As printed: 'N/A', '65k'.", max_length=32, null=True)),
                ('spec_mileage_warranty_miles', models.PositiveIntegerField(blank=True, help_text="'65k' -> 65000. NULL when the line reads N/A.", null=True)),
                ('spec_is_3pmsf', models.BooleanField(blank=True, help_text="Three-Peak Mountain Snowflake. NULL = unpublished, not 'uncertified'.", null=True)),
                ('spec_is_studdable', models.BooleanField(blank=True, null=True)),
                ('spec_commercial_position', models.CharField(blank=True, help_text='Steer / Drive / Trailer / All Position.', max_length=32, null=True)),
                ('spec_commercial_application', models.CharField(blank=True, help_text='Urban / Regional / Long Haul / Mixed Service.', max_length=32, null=True)),
                ('spec_smartway_verified', models.CharField(blank=True, max_length=32, null=True)),
                ('raw_specs', models.JSONField(blank=True, encoder=django.core.serializers.json.DjangoJSONEncoder, help_text='siteProductSpecs verbatim: [{name, values, description, cta, flair}, ...].', null=True)),
                ('specs_map', models.JSONField(blank=True, encoder=django.core.serializers.json.DjangoJSONEncoder, help_text='raw_specs flattened to {spec name: joined value} -- the shape to query when a spec has no column.', null=True)),
                ('raw_size', models.JSONField(blank=True, encoder=django.core.serializers.json.DjangoJSONEncoder, help_text="This SKU's siteProductLineAvailableSizeList entry, incl. its own thin specList.", null=True)),
                ('raw_size_detail', models.JSONField(blank=True, encoder=django.core.serializers.json.DjangoJSONEncoder, help_text='siteProductLineSizeDetail verbatim.', null=True)),
                ('raw_product_line', models.JSONField(blank=True, encoder=django.core.serializers.json.DjangoJSONEncoder, help_text='siteProductLine verbatim, minus the hero/CMS image fields nothing will ever read.', null=True)),
                ('scraped_at', models.DateTimeField(db_index=True, default=django.utils.timezone.now, help_text='When this row was last fetched. Drives --resume.')),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('updated_at', models.DateTimeField(auto_now=True)),
            ],
            options={
                'db_table': 'simpletire_skus',
            },
        ),
        migrations.AddIndex(
            model_name='simpletiresku',
            index=models.Index(fields=['brand_slug', 'product_line_slug'], name='simpletire_brand_line_idx'),
        ),
        migrations.AddIndex(
            model_name='simpletiresku',
            index=models.Index(fields=['size_display', 'brand_name'], name='simpletire_size_brand_idx'),
        ),
    ]
