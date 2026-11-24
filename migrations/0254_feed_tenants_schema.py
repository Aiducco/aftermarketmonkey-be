# Feed tenants, step 1 of 2 (schema only): `company.parent_company` (a feed tenant's real company),
# `spaces.feed_company` (the tenant that holds a space's own distributor connections) and the
# `space_providers` table (which connection each space uses per distributor). All nullable or new, so
# nothing is backfilled here: 0255 fills `space_providers` for the spaces that already exist.
import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("src", "0253_spaces_not_null"),
    ]

    operations = [
        migrations.AddField(
            model_name="company",
            name="parent_company",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.PROTECT,
                related_name="feed_tenants",
                to="src.company",
            ),
        ),
        migrations.AddField(
            model_name="space",
            name="feed_company",
            field=models.OneToOneField(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.PROTECT,
                related_name="feed_of_space",
                to="src.company",
            ),
        ),
        migrations.CreateModel(
            name="SpaceProvider",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "company_provider",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="space_providers",
                        to="src.companyproviders",
                    ),
                ),
                (
                    "provider",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="space_providers",
                        to="src.providers",
                    ),
                ),
                (
                    "space",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="space_providers",
                        to="src.space",
                    ),
                ),
            ],
            options={
                "db_table": "space_providers",
            },
        ),
        migrations.AddConstraint(
            model_name="spaceprovider",
            constraint=models.UniqueConstraint(fields=("space", "provider"), name="space_provider_unique"),
        ),
    ]
