# Records which answer from POST /api/address/validate/ the user proceeded with when they
# requested a quote (see src.enums.AddressValidationStatus and src/integrations/address/).
#
# Nullable with no default on purpose: "validation never ran" (saved location, ship-to-my-shop,
# or a client that predates the feature) has to stay distinguishable from UNVERIFIED, which
# means we did ask the provider and it could not confirm the address. Backfilling existing rows
# to UNVERIFIED would assert something about addresses nobody ever checked.
#
# Depends on 0203 -- the last migration actually on origin/main -- not the locally present but
# unpushed 0202, same reasoning 0203 itself documents.
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('src', '0203_the_wheel_group_inventory'),
    ]

    operations = [
        migrations.AddField(
            model_name='purchaseorder',
            name='ship_to_validation_status',
            field=models.PositiveSmallIntegerField(blank=True, null=True),
        ),
    ]
