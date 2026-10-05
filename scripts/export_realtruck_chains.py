"""
Exports RealTruck multi-location businesses -- the chains worth one conversation instead of N.

A chain is worth more than the sum of its shops: one conversation covers every buying location,
and the buying decision usually sits at a head office that a per-store mailing never reaches.
realtruck_leads stores one row per location with no group key, so the chains have to be inferred.

Grouping is the union-find from find_multi_location_leads.py -- shared website domain OR shared
name once the branch suffix is stripped ("JX Truck Center - Wausau" -> "JX Truck Center"). Either
signal alone misses cases: domain misses chains whose branches run separate sites, name misses
rebranded ones, and both are needed to avoid reporting the same chain twice.

Counts here are what the DATA shows, which is a floor, not the company's true size: a chain only
appears at the size RealTruck's dealer list happens to cover. The stored location_count column
disagrees (462 rows at >=3 vs 1,687 here) because it was an LLM's reading of each site, not a
count of actual rows -- this file is the one to trust for group size.

Defaults to every lead, not just qualified ones, since an unqualified branch is still a location
of the chain and still evidence of its size. --qualified-only narrows it.

Usage:
  python scripts/export_realtruck_chains.py
  python scripts/export_realtruck_chains.py --min-locations 5 --qualified-only
  python scripts/export_realtruck_chains.py --detailed --out chains_by_location.csv
"""
import argparse
import csv
import os
import sys
from collections import Counter

import django

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "settings")
django.setup()

from src.models import RealTruckLead, RealTruckLeadEmail  # noqa: E402
from scripts.export_realtruck_send_list import SENDABLE, company_name, group_leads  # noqa: E402
from scripts.find_multi_location_leads import domain_of  # noqa: E402

TIER_ORDER = {"A": 0, "B": 1, "C": 2}

# Franchise networks: the locations share a brand but are INDEPENDENTLY OWNED, so one row is not
# one buying decision and head office will not place orders for them. They still belong in the
# export -- a 34-location brand is worth knowing about -- but they are N prospects, not one, and
# should be worked per location. Flagged rather than split because the data cannot tell a
# franchisee from a company store, and guessing either way silently loses prospects.
FRANCHISE_BRANDS = {
    "line-x", "linex", "line", "rhino linings", "rhino lining", "ziebart",
    "ziebart rhino linings", "ziebart tidy car", "tint world", "rack attack", "rack n road",
    "maaco", "ding king", "auto one", "auto trim design", "cap-it", "cap it",
}

FIELDS = ["id", "name", "website", "city", "state", "country", "phone", "emails", "is_qualified",
          "business_typology", "confidence_score", "outreach_priority", "priority_tier",
          "website_quality", "is_preferred", "brand_count"]


def is_franchise(name: str) -> bool:
    n = (name or "").lower().strip()
    return any(b == n or n.startswith(b + " ") or b in n for b in FRANCHISE_BRANDS)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--min-locations", type=int, default=3)
    ap.add_argument("--out", default="realtruck_chains.csv")
    ap.add_argument("--qualified-only", action="store_true")
    ap.add_argument("--detailed", action="store_true",
                    help="One row per location instead of one row per chain")
    args = ap.parse_args()

    qs = RealTruckLead.objects.all()
    if args.qualified_only:
        qs = qs.filter(is_qualified=True)
    rows = list(qs.values(*FIELDS))

    # Verified addresses, so a chain row carries something you can actually send to.
    verified = {}
    for lead_id, email, status in RealTruckLeadEmail.objects.filter(
            status__in=SENDABLE).values_list("lead_id", "email", "status"):
        verified.setdefault(lead_id, []).append((SENDABLE.index(status), email.strip().lower()))

    groups = [g for g in group_leads(rows) if len(g) >= args.min_locations]
    for g in groups:
        g.sort(key=lambda m: (m["state"] or "", m["city"] or ""))
    # Biggest chains first -- that is the order you would work them in.
    groups.sort(key=lambda g: (-len(g), (g[0]["name"] or "").lower()))

    print(f"RealTruck leads scanned:        {len(rows):,}"
          f"{' (qualified only)' if args.qualified_only else ''}")
    print(f"Chains with >={args.min_locations} locations:      {len(groups):,}")
    print(f"Leads inside those chains:     {sum(len(g) for g in groups):,}")

    with open(args.out, "w", newline="") as fh:
        w = csv.writer(fh)
        if args.detailed:
            w.writerow(["chain", "chain_locations", "likely_franchise", "lead_id",
                        "location_name", "website", "city", "state", "country", "phone",
                        "is_qualified", "typology", "confidence", "outreach_priority",
                        "priority_tier", "emails"])
            for g in groups:
                chain = company_name(g[0]["name"])
                for m in g:
                    w.writerow([
                        chain, len(g), "yes" if is_franchise(chain) else "",
                        m["id"], m["name"], m["website"] or "",
                        m["city"] or "", m["state"] or "", m["country"] or "", m["phone"] or "",
                        "yes" if m["is_qualified"] else ("no" if m["is_qualified"] is False else ""),
                        m["business_typology"] or "", m["confidence_score"] or "",
                        m["outreach_priority"] if m["outreach_priority"] is not None else "",
                        m["priority_tier"] or "", ";".join(m["emails"] or []),
                    ])
        else:
            w.writerow(["chain", "locations", "countries", "states", "state_count",
                        "likely_franchise", "qualified_locations", "best_email", "send_tier",
                        "all_emails", "website", "typologies", "avg_confidence", "best_priority",
                        "priority_tier", "is_preferred", "locations_detail", "phones", "lead_ids"])
            for g in groups:
                # The chain's identity comes from its highest-priority location: chains list the
                # flagship site, and a branch row may carry a stale or branch-specific domain.
                lead = max(g, key=lambda m: (m["outreach_priority"] or 0,
                                             m["confidence_score"] or 0))
                addrs = sorted({a for m in g for a in verified.get(m["id"], [])})
                scores = [m["confidence_score"] for m in g if m["confidence_score"] is not None]
                prios = [m["outreach_priority"] for m in g if m["outreach_priority"] is not None]
                tiers = sorted((m["priority_tier"] for m in g if m["priority_tier"]),
                               key=lambda t: TIER_ORDER.get(t, 3))
                sites = sorted({domain_of(m["website"]) for m in g if domain_of(m["website"])})
                states = sorted({m["state"] for m in g if m["state"]})
                chain = company_name(lead["name"])
                w.writerow([
                    chain, len(g),
                    ";".join(sorted({m["country"] for m in g if m["country"]})),
                    ";".join(states), len(states),
                    "yes" if is_franchise(chain) else "",
                    sum(1 for m in g if m["is_qualified"]),
                    addrs[0][1] if addrs else "",
                    SENDABLE[addrs[0][0]] if addrs else "",
                    ";".join(a for _, a in addrs[1:]),
                    sites[0] if len(sites) == 1 else ";".join(sites),
                    ";".join(sorted({m["business_typology"] for m in g if m["business_typology"]})),
                    round(sum(scores) / len(scores)) if scores else "",
                    max(prios) if prios else "", tiers[0] if tiers else "",
                    "yes" if any(m["is_preferred"] for m in g) else "",
                    " | ".join(f"{m['city']}, {m['state']}" for m in g),
                    ";".join(sorted({m["phone"] for m in g if m["phone"]})),
                    ";".join(m["id"] for m in g),
                ])

    with_email = sum(1 for g in groups if any(verified.get(m["id"]) for m in g))
    print(f"\nWrote {args.out}  --  "
          f"{'one row per location' if args.detailed else f'{len(groups):,} chains, one row each'}")
    print(f"  with a verified sendable address: {with_email:,} / {len(groups):,}")
    sizes = Counter(len(g) for g in groups)
    print("  size spread: " + ", ".join(f"{k}x{v}" for k, v in sorted(sizes.items())))
    return 0


if __name__ == "__main__":
    sys.exit(main())
