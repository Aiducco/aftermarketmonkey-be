# Shop systems per space, step 3 of 3: `space` becomes NOT NULL on the shop-system connection and the three
# ShopMonkey sync tables (0257 filled every row; Model.save() fills it for new ones). Small tables, so the
# table rewrite and the foreign key re-validation are cheap.
import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("src", "0257_shop_systems_backfill"),
    ]

    operations = [
        migrations.AlterField(
            model_name="companyshopmanagementproviders",
            name="space",
            field=models.OneToOneField(
                on_delete=django.db.models.deletion.PROTECT,
                related_name="shop_connection",
                to="src.space",
            ),
        ),
        migrations.AlterField(
            model_name="shopmonkeysyncvendor",
            name="space",
            field=models.ForeignKey(
                on_delete=django.db.models.deletion.PROTECT,
                related_name="shopmonkey_sync_vendors",
                to="src.space",
            ),
        ),
        migrations.AlterField(
            model_name="shopmonkeysyncbrand",
            name="space",
            field=models.ForeignKey(
                on_delete=django.db.models.deletion.PROTECT,
                related_name="shopmonkey_sync_brands",
                to="src.space",
            ),
        ),
        migrations.AlterField(
            model_name="shopmonkeysyncexecution",
            name="space",
            field=models.ForeignKey(
                on_delete=django.db.models.deletion.PROTECT,
                related_name="shopmonkey_sync_executions",
                to="src.space",
            ),
        ),
    ]
