# Hand-trimmed from the auto-generated migration: makemigrations picked up a large amount of
# pre-existing drift unrelated to this change (id-field alterations, constraint removals, Meta
# option changes, plus a spurious LeerLead/RealTruckLead delete caused by unrelated concurrent
# WIP in models.py at generation time) -- only the ProductGroup/ProductGroupMember operations
# are kept here.
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):
    dependencies = [
        ("src", "0168_master_parts_gtin_core_index"),
    ]

    operations = [
        migrations.CreateModel(
            name="ProductGroup",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("group_key", models.TextField()),
                ("display_name", models.TextField()),
                ("method", models.CharField(max_length=16)),
                (
                    "grouping_confidence",
                    models.DecimalField(decimal_places=2, max_digits=3),
                ),
                ("sku_count", models.IntegerField()),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "db_table": "product_group",
            },
        ),
        migrations.CreateModel(
            name="ProductGroupMember",
            fields=[
                (
                    "master_part",
                    models.OneToOneField(
                        on_delete=django.db.models.deletion.CASCADE,
                        primary_key=True,
                        related_name="product_group_membership",
                        serialize=False,
                        to="src.masterpart",
                    ),
                ),
            ],
            options={
                "db_table": "product_group_member",
            },
        ),
        migrations.AddField(
            model_name="productgroupmember",
            name="group",
            field=models.ForeignKey(
                on_delete=django.db.models.deletion.CASCADE,
                related_name="members",
                to="src.productgroup",
            ),
        ),
        migrations.AddField(
            model_name="productgroup",
            name="brand",
            field=models.ForeignKey(
                on_delete=django.db.models.deletion.CASCADE,
                related_name="product_groups",
                to="src.brands",
            ),
        ),
        migrations.AlterUniqueTogether(
            name="productgroup",
            unique_together={("brand", "group_key")},
        ),
    ]
