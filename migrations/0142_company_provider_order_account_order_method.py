from django.db import migrations, models


class Migration(migrations.Migration):
    """
    Adds order_method/order_method_name to CompanyProviderOrderAccount — see
    src.enums.OrderMethod. Defaults to API (1) so every existing account keeps its exact
    current adapter-resolution behavior; see src.integrations.orders.registry.get_adapter().

    Hand-written rather than generated via makemigrations: this branch has unrelated,
    not-yet-migrated model drift from other in-progress work (visible via
    `makemigrations --dry-run` picking up changes to premierparts/tirerack/vossen/etc. this
    migration does not touch) — auto-generating here would have bundled that unrelated drift
    into this migration. Scoped to exactly the two fields this feature adds.
    """

    dependencies = [
        ("src", "0141_premier_parts_brand_override"),
    ]

    operations = [
        migrations.AddField(
            model_name="companyproviderorderaccount",
            name="order_method",
            field=models.PositiveSmallIntegerField(default=1),
        ),
        migrations.AddField(
            model_name="companyproviderorderaccount",
            name="order_method_name",
            field=models.CharField(default="API", max_length=16),
        ),
    ]
