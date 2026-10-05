"""
Fills RealTruck emails from the Google Maps lead table, where both cover the same business.

realtruck_leads and lead were harvested independently, so a shop whose site defeated one crawl
may well have been cracked by the other -- different day, different IP, different page set. Of
the 838 RealTruck domains with no email, 139 already have one on the Google side, and most of
those addresses are Reoon-verified. That is 286 leads for zero credits and zero requests, which
is worth doing before paying to guess at addresses.

Matching is by registrable website domain. That is deliberately conservative: two businesses
sharing a domain ARE the same operation (or branches of it), whereas name matching across two
differently-normalised datasets would produce false merges, and a false merge here means mailing
a stranger.

Only writes to leads with no email at all; never overwrites.

Usage:
  python scripts/backfill_realtruck_emails_from_google.py --dry-run
  python scripts/backfill_realtruck_emails_from_google.py
  python scripts/backfill_realtruck_emails_from_google.py --verified-only
"""
import argparse
import os
import re
import sys
from collections import defaultdict

import django

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "settings")
django.setup()

from django.db.models import Q  # noqa: E402
from src.models import Lead, LeadEmail, RealTruckLead, RealTruckLeadEmail  # noqa: E402

SENDABLE = ["safe", "role_account", "catch_all"]

# Shared platforms are not a shared business -- two shops both linking Facebook are not one company.
GENERIC = {
    "facebook.com", "m.facebook.com", "instagram.com", "linkedin.com", "twitter.com", "x.com",
    "yelp.com", "google.com", "sites.google.com", "wixsite.com", "business.site",
    "godaddysites.com", "squarespace.com", "weebly.com", "wordpress.com", "linktr.ee",
}


def domain_of(url: str) -> str:
    u = re.sub(r"^https?://", "", (url or "").strip().lower())
    return re.sub(r"^www\.", "", u.split("/")[0].split("?")[0].split(":")[0])


def copy_verdicts() -> None:
    """Reuse Reoon verdicts already bought on the Google side.

    A verification result is a property of the ADDRESS -- the mailbox either exists or it does
    not -- so re-buying it for the RealTruck table would spend credits to learn something already
    known. This matters because the send-list export only trusts verification rows: without this,
    a copied address sits in the lead row and is still treated as unusable.

    Runs over every qualified RealTruck lead, not just the ones this script just filled, so it
    also picks up addresses harvested earlier that happen to appear on the Google side.
    """
    verdicts = {}
    for e, st, iv, role, mx in LeadEmail.objects.values_list(
            "email", "status", "is_valid", "is_role_based", "mx_found"):
        verdicts.setdefault((e or "").strip().lower(), (st, iv, role, mx))

    have = {(l, e) for l, e in RealTruckLeadEmail.objects.values_list("lead_id", "email")}
    new = []
    for lead_id, emails in RealTruckLead.objects.filter(is_qualified=True).exclude(
            Q(emails__isnull=True) | Q(emails=[])).values_list("id", "emails"):
        for e in (emails or []):
            e = (e or "").strip().lower()
            v = verdicts.get(e)
            if not v or (lead_id, e) in have:
                continue
            st, iv, role, mx = v
            new.append(RealTruckLeadEmail(lead_id=lead_id, email=e, status=st,
                                          is_valid=iv, is_role_based=role, mx_found=mx))
            have.add((lead_id, e))
    RealTruckLeadEmail.objects.bulk_create(new, batch_size=500, ignore_conflicts=True)
    sendable = sum(1 for r in new if r.status in SENDABLE)
    print(f"Copied {len(new):,} existing Reoon verdicts across "
          f"({sendable:,} sendable) -- 0 credits spent.")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--verdicts-only", action="store_true",
                    help="Skip the address copy; only reuse Reoon verdicts already paid for")
    ap.add_argument("--verified-only", action="store_true",
                    help="Only copy addresses Reoon has confirmed sendable")
    args = ap.parse_args()

    if args.verdicts_only:
        copy_verdicts()
        return 0

    gap = RealTruckLead.objects.filter(is_qualified=True).filter(
        Q(emails__isnull=True) | Q(emails=[]))
    need = defaultdict(list)
    for lead_id, website in gap.values_list("id", "website"):
        d = domain_of(website)
        if d and d not in GENERIC:
            need[d].append(lead_id)
    print(f"RealTruck qualified leads with no email: {gap.count():,} across {len(need):,} domains")

    allowed = None
    if args.verified_only:
        allowed = {e.strip().lower() for e in LeadEmail.objects.filter(
            status__in=SENDABLE).values_list("email", flat=True)}
        print(f"Restricting to {len(allowed):,} Reoon-verified sendable addresses")

    found = defaultdict(set)
    for website, emails in Lead.objects.exclude(
            Q(website__isnull=True) | Q(website="")).exclude(
            Q(emails__isnull=True) | Q(emails=[])).values_list("website", "emails"):
        d = domain_of(website)
        if d not in need:
            continue
        for e in (emails or []):
            e = (e or "").strip().lower()
            if "@" in e and (allowed is None or e in allowed):
                found[d].add(e)

    n_leads = sum(len(need[d]) for d in found)
    n_addrs = sum(len(v) for v in found.values())
    print(f"Matched on the Google side: {len(found):,} domains -> "
          f"{n_leads:,} leads, {n_addrs:,} addresses")

    if args.dry_run:
        print("\nDRY RUN -- nothing written. Sample:")
        for d in list(found)[:10]:
            print(f"  {d:36} {sorted(found[d])[:3]}")
        return 0

    written = 0
    for d, emails in found.items():
        addrs = sorted(emails)
        for lead_id in need[d]:
            RealTruckLead.objects.filter(id=lead_id).update(
                emails=addrs, emails_not_found=False)
            written += 1
    print(f"\nUpdated {written:,} RealTruck leads.")
    copy_verdicts()

    remaining = RealTruckLead.objects.filter(is_qualified=True).filter(
        Q(emails__isnull=True) | Q(emails=[])).count()
    print(f"Qualified RealTruck leads still without an email: {remaining:,}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
