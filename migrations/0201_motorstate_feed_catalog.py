# Adds the Motor State FTP feed's catalog columns to motorstate_products and the two
# feed-sourced columns to motorstate_company_pricing. Every added column is nullable: each
# dealer's feed file carries only the columns that account is entitled to, so image,
# categories and long description arrive on enriched accounts only.
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('src', '0200_wheel_spec_card_fields'),
    ]

    operations = [
        migrations.AddField(
            model_name='motorstatecompanypricing',
            name='feed_updated_at',
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='motorstatecompanypricing',
            name='vendor_msrp',
            field=models.DecimalField(blank=True, decimal_places=2, max_digits=12, null=True),
        ),
        migrations.AddField(
            model_name='motorstateproduct',
            name='aaia_code',
            field=models.CharField(blank=True, max_length=64, null=True),
        ),
        migrations.AddField(
            model_name='motorstateproduct',
            name='acquired_date',
            field=models.DateField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='motorstateproduct',
            name='air_restricted',
            field=models.BooleanField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='motorstateproduct',
            name='canada_restricted',
            field=models.BooleanField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='motorstateproduct',
            name='category_level_1',
            field=models.CharField(blank=True, max_length=255, null=True),
        ),
        migrations.AddField(
            model_name='motorstateproduct',
            name='category_level_2',
            field=models.CharField(blank=True, max_length=255, null=True),
        ),
        migrations.AddField(
            model_name='motorstateproduct',
            name='category_level_3',
            field=models.CharField(blank=True, max_length=255, null=True),
        ),
        migrations.AddField(
            model_name='motorstateproduct',
            name='emissions_warning',
            field=models.BooleanField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='motorstateproduct',
            name='feed_updated_at',
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='motorstateproduct',
            name='height',
            field=models.DecimalField(blank=True, decimal_places=5, max_digits=12, null=True),
        ),
        migrations.AddField(
            model_name='motorstateproduct',
            name='image_url',
            field=models.TextField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='motorstateproduct',
            name='length',
            field=models.DecimalField(blank=True, decimal_places=5, max_digits=12, null=True),
        ),
        migrations.AddField(
            model_name='motorstateproduct',
            name='long_description',
            field=models.TextField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='motorstateproduct',
            name='notes',
            field=models.TextField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='motorstateproduct',
            name='oversized',
            field=models.BooleanField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='motorstateproduct',
            name='ship_alone',
            field=models.BooleanField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='motorstateproduct',
            name='state_restricted',
            field=models.CharField(blank=True, max_length=255, null=True),
        ),
        migrations.AddField(
            model_name='motorstateproduct',
            name='status_type',
            field=models.CharField(blank=True, max_length=8, null=True),
        ),
        migrations.AddField(
            model_name='motorstateproduct',
            name='truck_freight_only',
            field=models.BooleanField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='motorstateproduct',
            name='upc',
            field=models.CharField(blank=True, max_length=64, null=True),
        ),
        migrations.AddField(
            model_name='motorstateproduct',
            name='weight',
            field=models.DecimalField(blank=True, decimal_places=5, max_digits=12, null=True),
        ),
        migrations.AddField(
            model_name='motorstateproduct',
            name='width',
            field=models.DecimalField(blank=True, decimal_places=5, max_digits=12, null=True),
        ),
        migrations.AddIndex(
            model_name='motorstateproduct',
            index=models.Index(fields=['upc'], name='ms_products_upc_idx'),
        ),
        migrations.AddIndex(
            model_name='motorstateproduct',
            index=models.Index(fields=['feed_updated_at'], name='ms_products_feed_upd_idx'),
        ),
    ]
