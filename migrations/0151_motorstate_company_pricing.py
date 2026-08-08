"""
Split MotorStateProduct into a global catalog table + per-company MotorStateCompanyPricing,
matching the Keystone / Meyer / Quadratec shape.

Motor State's /api/Product returns catalog and account pricing in one payload, so the original
table carried both and was keyed (company, part_number) -- which duplicated every catalog field
once per connected company. This migration lifts the price columns into their own per-company
table, collapses the product rows to one per part number (lowest id wins), and makes
part_number unique.
"""
from django.db import migrations, models
import django.db.models.deletion


def forwards(apps, schema_editor):
    """Copy prices into MotorStateCompanyPricing, then collapse duplicate product rows."""
    connection = schema_editor.connection

    with connection.cursor() as cursor:
        # 1. One surviving product row per part number (lowest id).
        cursor.execute(
            """
            CREATE TEMPORARY TABLE _ms_keep AS
            SELECT part_number, MIN(id) AS keep_id
            FROM motorstate_products
            GROUP BY part_number
            """
        )
        cursor.execute("CREATE INDEX ON _ms_keep (part_number)")

        # 2. Pricing row per (surviving product, company). DISTINCT ON keeps one row per pair
        #    when the same company somehow has several rows for a part number.
        cursor.execute(
            """
            INSERT INTO motorstate_company_pricing (
                product_id, company_id,
                customer_price, customer_price_non_promotional, base_price,
                list_price, map_price, is_map_restricted,
                special_order_charge, drop_ship_charge,
                created_at, updated_at
            )
            SELECT DISTINCT ON (k.keep_id, p.company_id)
                k.keep_id, p.company_id,
                p.customer_price, p.customer_price_non_promotional, p.base_price,
                p.list_price, p.map_price, p.is_map_restricted,
                p.special_order_charge, p.drop_ship_charge,
                p.created_at, p.updated_at
            FROM motorstate_products p
            JOIN _ms_keep k ON k.part_number = p.part_number
            ORDER BY k.keep_id, p.company_id, p.id
            """
        )

        # 3. Drop the now-redundant duplicate catalog rows.
        cursor.execute(
            """
            DELETE FROM motorstate_products p
            USING _ms_keep k
            WHERE k.part_number = p.part_number AND p.id <> k.keep_id
            """
        )
        cursor.execute("DROP TABLE _ms_keep")


def backwards(apps, schema_editor):
    """
    Restore prices onto the product rows. Only the pricing of the lowest company_id is
    recoverable per part number -- the pre-split per-company duplicate catalog rows are gone.
    """
    with schema_editor.connection.cursor() as cursor:
        cursor.execute(
            """
            UPDATE motorstate_products p
            SET customer_price = cp.customer_price,
                customer_price_non_promotional = cp.customer_price_non_promotional,
                base_price = cp.base_price,
                list_price = cp.list_price,
                map_price = cp.map_price,
                is_map_restricted = cp.is_map_restricted,
                special_order_charge = cp.special_order_charge,
                drop_ship_charge = cp.drop_ship_charge,
                company_id = cp.company_id
            FROM (
                SELECT DISTINCT ON (product_id) *
                FROM motorstate_company_pricing
                ORDER BY product_id, company_id
            ) cp
            WHERE cp.product_id = p.id
            """
        )


class Migration(migrations.Migration):
    dependencies = [
        ("src", "0150_motorstate_brand_mapping"),
    ]

    operations = [
        # New per-company pricing table (created before the data move so forwards() can fill it).
        migrations.CreateModel(
            name="MotorStateCompanyPricing",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True, primary_key=True, serialize=False, verbose_name="ID"
                    ),
                ),
                ("customer_price", models.DecimalField(decimal_places=2, max_digits=12, null=True)),
                (
                    "customer_price_non_promotional",
                    models.DecimalField(decimal_places=2, max_digits=12, null=True),
                ),
                ("base_price", models.DecimalField(decimal_places=2, max_digits=12, null=True)),
                ("list_price", models.DecimalField(decimal_places=2, max_digits=12, null=True)),
                ("map_price", models.DecimalField(decimal_places=2, max_digits=12, null=True)),
                ("is_map_restricted", models.BooleanField(default=False)),
                ("special_order_charge", models.DecimalField(decimal_places=2, max_digits=12, null=True)),
                ("drop_ship_charge", models.DecimalField(decimal_places=2, max_digits=12, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "company",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="motorstate_company_pricing",
                        to="src.company",
                    ),
                ),
                (
                    "product",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="company_pricing",
                        to="src.motorstateproduct",
                    ),
                ),
            ],
            options={
                "db_table": "motorstate_company_pricing",
            },
        ),
        # Move the data across, then collapse duplicate catalog rows.
        migrations.RunPython(forwards, backwards),
        # The old composite key and per-company index must go before the company column does.
        migrations.AlterUniqueTogether(name="motorstateproduct", unique_together=set()),
        migrations.RemoveIndex(model_name="motorstateproduct", name="ms_products_company_brand_idx"),
        migrations.RemoveField(model_name="motorstateproduct", name="company"),
        migrations.RemoveField(model_name="motorstateproduct", name="customer_price"),
        migrations.RemoveField(model_name="motorstateproduct", name="customer_price_non_promotional"),
        migrations.RemoveField(model_name="motorstateproduct", name="base_price"),
        migrations.RemoveField(model_name="motorstateproduct", name="list_price"),
        migrations.RemoveField(model_name="motorstateproduct", name="map_price"),
        migrations.RemoveField(model_name="motorstateproduct", name="is_map_restricted"),
        migrations.RemoveField(model_name="motorstateproduct", name="special_order_charge"),
        migrations.RemoveField(model_name="motorstateproduct", name="drop_ship_charge"),
        migrations.AlterField(
            model_name="motorstateproduct",
            name="part_number",
            field=models.CharField(max_length=128, unique=True),
        ),
        migrations.AddIndex(
            model_name="motorstateproduct",
            index=models.Index(fields=["brand"], name="ms_products_brand_idx"),
        ),
        migrations.AlterUniqueTogether(
            name="motorstatecompanypricing",
            unique_together={("product", "company")},
        ),
    ]
