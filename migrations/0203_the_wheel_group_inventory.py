"""
Create ``thewheelgroup_inventory``: raw per-SKU rows from TWG's real relay inventory CSV (real
DealerCost + per-warehouse on-hand), matched against SKUs that already exist as
TheWheelGroupPart from the mastersheet -- TWG's relay file spans a wider catalog (center caps,
lug nuts, tires, third-party brands) than the wheel mastersheet's 11 house brands, and a row for
anything outside that has nothing to attach to.

No company FK: stock is the same regardless of which dealer's relay account delivered the file
(TWG's shared warehouse network, not a per-customer allocation), same role Turn14BrandInventory
plays for Turn14 -- this feeds the global ProviderPartInventory, decoded to human-readable
warehouse names at propagation time (see master_parts.sync_provider_inventory_from_the_wheel_group).

Depends on 0201 (the last migration actually on origin/main as of this change), not the locally
present but unpushed 0202 -- see 0195's own merge-migration precedent for why.
"""
import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('src', '0201_motorstate_feed_catalog'),
    ]

    operations = [
        migrations.CreateModel(
            name='TheWheelGroupInventory',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('warehouse_qty', models.JSONField(blank=True, null=True)),
                ('total_onhand', models.IntegerField(default=0)),
                ('source_filename', models.CharField(blank=True, max_length=255, null=True)),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('updated_at', models.DateTimeField(auto_now=True)),
                ('part', models.OneToOneField(on_delete=django.db.models.deletion.CASCADE, related_name='relay_inventory', to='src.thewheelgrouppart')),
            ],
            options={
                'db_table': 'thewheelgroup_inventory',
            },
        ),
    ]
