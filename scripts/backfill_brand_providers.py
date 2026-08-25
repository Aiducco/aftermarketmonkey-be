"""
Backfill BrandProviders rows for (brand, provider) pairs that demonstrably have parts.

Two known causes leave real pairs unrecorded (see the audit in this script's PR):

* Meyer -- a March 2026 batch created BrandMeyerBrandMapping rows without the matching
  BrandProviders rows, and meyer.sync_unmapped_meyer_brands_to_brands only ever revisits
  *unmapped* MeyerBrand rows, so those pairs never self-heal.
* Premier -- premier.resolve_wheelpros_bucket_brands corrects the "Wheel Pros" bucket
  per part via PremierParts.brand_override_id, a path that never had a BrandProviders
  upsert; the feed-brand-level sync that does create them cannot see an override brand.

Purely additive and idempotent: only inserts pairs that are missing, never updates or
deletes. Run with --dry-run (default) to preview; --apply to write.

    python manage.py shell -c "exec(open('scripts/backfill_brand_providers.py').read())"
    python scripts/backfill_brand_providers.py --apply
"""
import argparse
import os
import sys

import django

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "settings")
django.setup()

from django.db import connection, transaction  # noqa: E402

from src import models as src_models  # noqa: E402

MISSING_PAIRS_SQL = """
    with real as (
        select distinct mp.brand_id, pp.provider_id
        from provider_parts pp
        join master_parts mp on mp.id = pp.master_part_id
    )
    select r.brand_id, r.provider_id, b.name, p.name
    from real r
    join brands b on b.id = r.brand_id
    join providers p on p.id = r.provider_id
    where not exists (
        select 1 from brand_providers bp
        where bp.brand_id = r.brand_id and bp.provider_id = r.provider_id
    )
    order by p.name, b.name
"""


def find_missing_pairs():
    with connection.cursor() as cur:
        cur.execute(MISSING_PAIRS_SQL)
        return cur.fetchall()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Write the rows (default is a dry run).")
    parser.add_argument("--provider", help="Restrict to one provider name, e.g. 'Meyer'.")
    args = parser.parse_args()

    rows = find_missing_pairs()
    if args.provider:
        rows = [r for r in rows if r[3] == args.provider]

    by_provider = {}
    for _, _, _, provider_name in rows:
        by_provider[provider_name] = by_provider.get(provider_name, 0) + 1

    print("Database: {}@{}".format(
        connection.settings_dict["NAME"], connection.settings_dict["HOST"]
    ))
    print("Missing (brand, provider) pairs that have parts: {}".format(len(rows)))
    for provider_name, count in sorted(by_provider.items(), key=lambda kv: -kv[1]):
        print("  {:<32} {:>5}".format(provider_name, count))

    print("\nSample (first 15):")
    for brand_id, provider_id, brand_name, provider_name in rows[:15]:
        print("  brand {:>5} {:<34} -> {}".format(brand_id, brand_name[:34], provider_name))

    if not args.apply:
        print("\nDry run -- nothing written. Re-run with --apply to insert.")
        return

    objs = [
        src_models.BrandProviders(brand_id=brand_id, provider_id=provider_id)
        for brand_id, provider_id, _, _ in rows
    ]
    with transaction.atomic():
        created = src_models.BrandProviders.objects.bulk_create(objs, ignore_conflicts=True)
    print("\nInserted {} BrandProviders row(s) (ignore_conflicts on).".format(len(created)))

    remaining = find_missing_pairs()
    print("Missing pairs after backfill: {}".format(len(remaining)))


if __name__ == "__main__":
    main()
