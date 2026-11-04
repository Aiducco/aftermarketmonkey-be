from django.db import migrations


class Migration(migrations.Migration):
    """
    provider_parts.is_special_order (added in 0246) has no database-level default -- only the
    Django model's default=False, which Python-level model instantiation applies but a raw
    pgbulk upsert from a provider that doesn't yet set this column at all does not. Confirmed
    live: Meyer's master-parts ingest hit a NOT NULL violation inserting a brand-new
    ProviderPart row. Giving the column a real Postgres default means every other provider's
    existing insert path keeps working unmodified and gets False for free, until each one is
    actually updated to compute a real value the way Keystone's now does.
    """

    dependencies = [
        ("src", "0246_provider_part_special_order"),
    ]

    operations = [
        migrations.RunSQL(
            sql="ALTER TABLE provider_parts ALTER COLUMN is_special_order SET DEFAULT false;",
            reverse_sql="ALTER TABLE provider_parts ALTER COLUMN is_special_order DROP DEFAULT;",
        ),
    ]
