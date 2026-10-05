"""
Exports multi-location businesses -- the chains worth one conversation instead of N.

A chain is worth more than the sum of its shops: one conversation covers every buying location,
and the decision usually sits at a head office that a per-store mailing never reaches. Neither
lead table stores a group key, so the chains have to be inferred from one row per location.

Grouping is the shared union-find in export_realtruck_send_list.group_leads -- shared website
domain OR shared name once the branch suffix is stripped. Read the guards there before trusting
any size: a naive version invented an 82-location "Peterbilt" out of three competing dealer
groups, and turned 83 shops that merely linked leonardusa.com into one company.

Sizes are what the DATA shows, a floor rather than the company's true footprint: a chain appears
only at the size this source happens to cover. Google Maps and RealTruck disagree for that reason
(Leonard is 69 locations in one, 77 in the other), so neither number is wrong -- they are two
partial views.

  --source realtruck   RealTruck dealer list. Carries priority scores and preferred-dealer flags.
  --source google      Google Maps scrape. No priority scoring, so rating/review volume stands in
                       as the "is this a serious operation" signal.

Defaults to grouping over EVERY lead, not just qualified ones: an unqualified branch is still a
location of the chain and still evidence of its size. For Google that would otherwise bury the
real prospects under national chains we do not sell to, so a chain is only emitted if at least one
of its locations qualified (--min-qualified 0 to see them all).

Usage:
  python scripts/export_lead_chains.py --source google
  python scripts/export_lead_chains.py --source realtruck --min-locations 5
  python scripts/export_lead_chains.py --source google --detailed --out by_location.csv
"""
import argparse
import csv
import os
import sys
from collections import Counter, defaultdict

import django

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "settings")
django.setup()

from src.models import Lead, LeadEmail, RealTruckLead, RealTruckLeadEmail  # noqa: E402
from scripts.export_realtruck_send_list import SENDABLE, company_name, group_leads  # noqa: E402
from scripts.find_multi_location_leads import domain_of  # noqa: E402

TIER_ORDER = {"A": 0, "B": 1, "C": 2}

# Franchise networks: locations share a brand but are INDEPENDENTLY OWNED, so one row is not one
# buying decision and head office will not order for them. Flagged rather than split because the
# data cannot tell a franchisee from a company store, and guessing either way loses prospects
# silently. Worked per location, from the --detailed file.
FRANCHISE_BRANDS = {
    "line-x", "linex", "line", "rhino linings", "rhino lining", "ziebart",
    "ziebart rhino linings", "ziebart tidy car", "tint world", "rack attack", "rack n road",
    "maaco", "ding king", "auto one", "auto trim design", "cap-it", "cap it", "midas",
    "meineke", "precision tune", "rnr tire express", "matco tools", "snap-on", "u-haul",
    "trailer hitches at u-haul", "batteries plus", "tuffy", "monro", "mr. transmission",
}

# Columns every source has. Anything beyond this is declared per source below.
COMMON = ["id", "name", "website", "city", "state", "phone", "emails", "is_qualified",
          "business_typology", "confidence_score"]

SOURCES = {
    "realtruck": {
        "model": RealTruckLead,
        "email_model": RealTruckLeadEmail,
        "extra": ["country", "outreach_priority", "priority_tier", "website_quality",
                  "is_preferred", "brand_count"],
        # (header, fn(group, flagship)) -- the source-specific tail of each chain row.
        "cols": [
            ("countries", lambda g, f: ";".join(sorted({m["country"] for m in g if m["country"]}))),
            ("best_priority", lambda g, f: max(
                (m["outreach_priority"] for m in g if m["outreach_priority"] is not None),
                default="")),
            ("priority_tier", lambda g, f: next(
                iter(sorted((m["priority_tier"] for m in g if m["priority_tier"]),
                            key=lambda t: TIER_ORDER.get(t, 3))), "")),
            ("is_preferred", lambda g, f: "yes" if any(m["is_preferred"] for m in g) else ""),
        ],
    },
    "google": {
        "model": Lead,
        "email_model": LeadEmail,
        "extra": ["rating", "review_count", "category", "google_maps_url"],
        # No priority scoring exists for Google leads, so the rating is the only quality signal
        # available. review_count would be the better one but is populated on 66 of 19,595
        # qualified leads, so it is deliberately not reported rather than shipped mostly blank.
        "cols": [
            ("avg_rating", lambda g, f: round(
                sum(float(m["rating"]) for m in g if m["rating"] is not None)
                / max(sum(1 for m in g if m["rating"] is not None), 1), 2)
                if any(m["rating"] is not None for m in g) else ""),
            ("categories", lambda g, f: ";".join(sorted(
                {m["category"] for m in g if m["category"]})[:5])),
            ("maps_url", lambda g, f: f["google_maps_url"] or ""),
        ],
    },
}


def qualified_ratio(group) -> float:
    """Qualified share of the locations we have actually judged.

    Les Schwab scores 1.00 and Big Tex 1.00; NAPA 0.19, Rush Truck Centers 0.02 and Carquest 0.00.
    A count alone cannot tell them apart -- NAPA has nine qualified locations, more than most real
    prospects -- because the count scales with the chain's size rather than its fit.
    """
    q = sum(1 for m in group if m["is_qualified"])
    rejected = sum(1 for m in group if m["is_qualified"] is False)
    processed = q + rejected
    return q / processed if processed else 0.0


def chain_name(group, flagship) -> str:
    """What most of the locations call themselves.

    Naming the group after its flagship row labelled 5,358 NAPA stores "Jerry's Auto Supply" --
    one independent that happened to score highest. The modal name is what the chain actually is.
    """
    counts = defaultdict(int)
    for m in group:
        n = company_name(m["name"])
        if n:
            counts[n] += 1
    if not counts:
        return company_name(flagship["name"])
    # Ties go to the longer name: "Big Tex Trailer World" over a truncated "Big Tex".
    return max(counts, key=lambda n: (counts[n], len(n)))


def is_franchise(name: str) -> bool:
    n = (name or "").lower().strip()
    return any(b == n or n.startswith(b + " ") or b in n for b in FRANCHISE_BRANDS)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", default="google", choices=sorted(SOURCES))
    ap.add_argument("--min-locations", type=int, default=3)
    ap.add_argument("--min-qualified", type=int, default=None,
                    help="Emit a chain only if this many of its locations qualified. Defaults to "
                         "--min-locations: a chain is a prospect because it has that many "
                         "QUALIFIED sites, not because one store out of thousands slipped "
                         "through. 0 disables the filter.")
    ap.add_argument("--min-qualified-ratio", type=float, default=0.4,
                    help="Fraction of a chain's PROCESSED locations that must have qualified. "
                         "This is what separates a chain we sell to from a national retailer a "
                         "false positive dragged in; 0 disables it.")
    ap.add_argument("--out", default=None)
    ap.add_argument("--qualified-only", action="store_true",
                    help="Group over qualified leads only, so sizes count qualified locations")
    ap.add_argument("--detailed", action="store_true",
                    help="One row per location instead of one row per chain")
    args = ap.parse_args()

    # O'Reilly Auto Parts appears with 5,682 locations and ONE qualified; NAPA with 5,358 and
    # nine. Both are correctly grouped and neither is a prospect. Requiring as many qualified
    # locations as locations is what separates a chain we sell to (Les Schwab 545/546, Tint World
    # 144/145) from a national retailer a false positive dragged in.
    min_qualified = args.min_locations if args.min_qualified is None else args.min_qualified
    cfg = SOURCES[args.source]
    out_path = args.out or f"{args.source}_chains{'_by_location' if args.detailed else ''}.csv"
    fields = COMMON + cfg["extra"]

    qs = cfg["model"].objects.all()
    if args.qualified_only:
        qs = qs.filter(is_qualified=True)
    rows = list(qs.values(*fields))
    print(f"{args.source}: scanned {len(rows):,} leads"
          f"{' (qualified only)' if args.qualified_only else ''}")

    # Verified addresses, so a chain row carries something you can actually send to.
    verified = {}
    for lead_id, email, status in cfg["email_model"].objects.filter(
            status__in=SENDABLE).values_list("lead_id", "email", "status"):
        verified.setdefault(lead_id, []).append((SENDABLE.index(status), email.strip().lower()))

    groups = [g for g in group_leads(rows) if len(g) >= args.min_locations]
    print(f"  businesses with >={args.min_locations} locations: {len(groups):,}")
    if min_qualified:
        before = len(groups)
        groups = [g for g in groups
                  if sum(1 for m in g if m["is_qualified"]) >= min_qualified]
        print(f"  with >={min_qualified} qualified locations: {len(groups):,} "
              f"({before - len(groups):,} dropped)")
    if args.min_qualified_ratio:
        # Measured against PROCESSED locations only. Counting never-qualified rows as rejections
        # would punish a real chain for the parts of it the pipeline has not reached yet: NAPA has
        # 5,311 of its 5,358 locations unprocessed, and judging it on those says nothing.
        before = len(groups)
        groups = [g for g in groups if qualified_ratio(g) >= args.min_qualified_ratio]
        print(f"  with >={args.min_qualified_ratio:.0%} of processed locations qualified: "
              f"{len(groups):,} ({before - len(groups):,} dropped as non-target chains)")
    for g in groups:
        g.sort(key=lambda m: (m["state"] or "", m["city"] or ""))
    groups.sort(key=lambda g: (-len(g), (g[0]["name"] or "").lower()))
    print(f"  leads inside those chains: {sum(len(g) for g in groups):,}")

    with open(out_path, "w", newline="") as fh:
        w = csv.writer(fh)
        if args.detailed:
            w.writerow(["chain", "chain_locations", "likely_franchise", "lead_id",
                        "location_name", "website", "city", "state", "phone", "is_qualified",
                        "typology", "confidence", "emails"])
            for g in groups:
                chain = chain_name(g, g[0])
                fr = "yes" if is_franchise(chain) else ""
                for m in g:
                    w.writerow([
                        chain, len(g), fr, m["id"], m["name"], m["website"] or "",
                        m["city"] or "", m["state"] or "", m["phone"] or "",
                        "yes" if m["is_qualified"] else
                        ("no" if m["is_qualified"] is False else ""),
                        m["business_typology"] or "", m["confidence_score"] or "",
                        ";".join(m["emails"] or []),
                    ])
        else:
            w.writerow(["chain", "locations", "states", "state_count", "likely_franchise",
                        "qualified_locations", "rejected_locations", "unprocessed_locations",
                        "qualified_ratio", "best_email", "send_tier", "all_emails",
                        "website", "typologies", "avg_confidence"]
                       + [h for h, _ in cfg["cols"]]
                       + ["locations_detail", "phones", "lead_ids"])
            for g in groups:
                # Identity comes from the chain's strongest location: a branch row may carry a
                # stale or branch-specific domain, and chains list the flagship site.
                flagship = max(g, key=lambda m: (m["confidence_score"] or 0,
                                                 m.get("review_count") or 0))
                chain = chain_name(g, flagship)
                addrs = sorted({a for m in g for a in verified.get(m["id"], [])})
                scores = [m["confidence_score"] for m in g if m["confidence_score"] is not None]
                sites = sorted({domain_of(m["website"]) for m in g if domain_of(m["website"])})
                states = sorted({m["state"] for m in g if m["state"]})
                w.writerow([
                    chain, len(g), ";".join(states), len(states),
                    "yes" if is_franchise(chain) else "",
                    sum(1 for m in g if m["is_qualified"]),
                    sum(1 for m in g if m["is_qualified"] is False),
                    sum(1 for m in g if m["is_qualified"] is None),
                    f"{qualified_ratio(g):.2f}",
                    addrs[0][1] if addrs else "",
                    SENDABLE[addrs[0][0]] if addrs else "",
                    ";".join(a for _, a in addrs[1:]),
                    sites[0] if len(sites) == 1 else ";".join(sites),
                    ";".join(sorted({m["business_typology"] for m in g
                                     if m["business_typology"]})),
                    round(sum(scores) / len(scores)) if scores else "",
                ] + [fn(g, flagship) for _, fn in cfg["cols"]] + [
                    " | ".join(f"{m['city']}, {m['state']}" for m in g),
                    ";".join(sorted({m["phone"] for m in g if m["phone"]})),
                    ";".join(str(m["id"]) for m in g),
                ])

    with_email = sum(1 for g in groups if any(verified.get(m["id"]) for m in g))
    print(f"\nWrote {out_path}  --  "
          f"{'one row per location' if args.detailed else f'{len(groups):,} chains, one row each'}")
    print(f"  with a verified sendable address: {with_email:,} / {len(groups):,}")
    print(f"  flagged likely_franchise:         "
          f"{sum(1 for g in groups if is_franchise(company_name(g[0]['name']))):,}")
    sizes = Counter(len(g) for g in groups)
    print("  size spread: " + ", ".join(f"{k}x{v}" for k, v in sorted(sizes.items())[:16]) + " ...")
    return 0


if __name__ == "__main__":
    sys.exit(main())
