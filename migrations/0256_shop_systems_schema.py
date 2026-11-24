# Shop systems per space, step 1 of 3 (schema only): a NULLABLE `space` on the shop-system connection
# (one-to-one: a space has at most one connection, and a connection belongs to one space) and on the three
# ShopMonkey sync tables, plus `credentials_fingerprint` on the connection (a hash of the account-identity
# fields, so one account cannot serve two spaces). The company-wide uniques on (company, provider) and
# (company, brand) are replaced by uniques per space and per account. Nothing is backfilled here: 0257 fills
# the data and 0258 makes `space` NOT NULL (separate migrations: Postgres foreign keys are DEFERRABLE
# INITIALLY DEFERRED, and an UPDATE followed by ALTER TABLE in one transaction fails with "pending trigger
# events"). Fingerprints are NULL until 0257, and NULLs never collide in a unique, so the new unique holds.
import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("src", "0255_space_providers_backfill"),
    ]

    operations = [
        migrations.AddField(
            model_name="companyshopmanagementproviders",
            name="space",
            field=models.OneToOneField(
                null=True,
                on_delete=django.db.models.deletion.PROTECT,
                related_name="shop_connection",
                to="src.space",
            ),
        ),
        migrations.AddField(
            model_name="companyshopmanagementproviders",
            name="credentials_fingerprint",
            field=models.CharField(blank=True, max_length=64, null=True),
        ),
        migrations.AlterUniqueTogether(
            name="companyshopmanagementproviders",
            unique_together=set(),
        ),
        migrations.AddConstraint(
            model_name="companyshopmanagementproviders",
            constraint=models.UniqueConstraint(
                fields=("company", "provider", "credentials_fingerprint"), name="shop_connection_account_unique"
            ),
        ),
        migrations.AddField(
            model_name="shopmonkeysyncvendor",
            name="space",
            field=models.ForeignKey(
                null=True,
                on_delete=django.db.models.deletion.PROTECT,
                related_name="shopmonkey_sync_vendors",
                to="src.space",
            ),
        ),
        migrations.AlterUniqueTogether(
            name="shopmonkeysyncvendor",
            unique_together=set(),
        ),
        migrations.AddConstraint(
            model_name="shopmonkeysyncvendor",
            constraint=models.UniqueConstraint(fields=("space", "provider"), name="shopmonkey_vendor_space_provider"),
        ),
        migrations.AddField(
            model_name="shopmonkeysyncbrand",
            name="space",
            field=models.ForeignKey(
                null=True,
                on_delete=django.db.models.deletion.PROTECT,
                related_name="shopmonkey_sync_brands",
                to="src.space",
            ),
        ),
        migrations.AlterUniqueTogether(
            name="shopmonkeysyncbrand",
            unique_together=set(),
        ),
        migrations.AddConstraint(
            model_name="shopmonkeysyncbrand",
            constraint=models.UniqueConstraint(fields=("space", "brand"), name="shopmonkey_brand_space_brand"),
        ),
        migrations.AddField(
            model_name="shopmonkeysyncexecution",
            name="space",
            field=models.ForeignKey(
                null=True,
                on_delete=django.db.models.deletion.PROTECT,
                related_name="shopmonkey_sync_executions",
                to="src.space",
            ),
        ),
    ]
