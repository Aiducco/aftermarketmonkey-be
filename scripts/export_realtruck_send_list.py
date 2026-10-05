"""
Builds the RealTruck outreach send list: one row per unique business, one sendable address each.

Three things have to happen before this is safe to feed a sending tool, and each is a separate
failure if skipped:

  dedupe      Multi-location chains appear once per location in realtruck_leads. Toys For Trucks
              is 11 rows. Sending 11 copies to the same head-office inbox is how a domain gets
              burned, so locations are collapsed with the same union-find (shared domain OR
              shared normalised name) used by find_multi_location_leads.py.
  dedupe #2   Two businesses that survived the above can still share an address -- separate
              brands behind one owner, or a shared info@ on a parent domain. A second pass drops
              any address already claimed by an earlier row, so no address is sent to twice.
  verify      Only Reoon-verified addresses go out. Unverified harvested addresses are excluded
              by default (--include-unverified adds them, marked, at your own bounce risk).

send_tier ranks what Reoon returned, best first, so the sending tool can throttle by risk:
  safe          mailbox confirmed to exist. Send freely.
  role_account  info@/sales@ -- real mailbox, but a shared one. Fine for B2B, lower engagement.
  catch_all     domain accepts everything, so the mailbox is unproven. Highest bounce risk.
Statuses invalid/disabled/disposable/unknown are never exported.

Usage:
  python scripts/export_realtruck_send_list.py
  python scripts/export_realtruck_send_list.py --tiers safe,role_account
  python scripts/export_realtruck_send_list.py --include-unverified --out all.csv
"""
import argparse
import csv
import os
import re
import sys
from collections import defaultdict

import django

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "settings")
django.setup()

from src.models import RealTruckLead, RealTruckLeadEmail  # noqa: E402
from scripts.find_multi_location_leads import (  # noqa: E402
    GENERIC_DOMAINS, MIN_NAME_LEN, SUFFIX_RE, domain_of, normalize_name,
)

def company_name(name: str) -> str:
    """Strip the branch suffix, preserving case: "Music City 4X4 - Hendersonville" -> the company.

    Only used for collapsed chains. The row stands for every location, so a merge tag rendering
    "Hi DFW Truck & Auto Accessories - Arlington" in an email sent to head office is wrong.
    """
    n = (name or "").strip()
    for _ in range(2):  # "Name - Ford - Dallas" needs two passes
        stripped = SUFFIX_RE.sub("", n).strip(" ,-")
        if not stripped or stripped == n:
            break
        n = stripped
    return re.sub(r"\s+", " ", n)


# Reoon statuses worth sending to, best first. Order is the tier ranking.
SENDABLE = ["safe", "role_account", "catch_all"]
UNVERIFIED = "unverified"


# A name key that is only an OEM or franchise brand says nothing about who owns the business.
# Allstate Peterbilt, Larson Group and Rush Truck Centers are three competing dealer groups whose
# names all reduce to "peterbilt"; merging on that produced one fake 82-location company spanning
# 26 states. Same for the Kenworth and Freightliner dealer networks, and for franchise coatings
# brands whose locations are independently owned.
BRAND_ONLY_NAMES = {
    "peterbilt", "kenworth", "freightliner", "western star", "international", "navistar",
    "volvo", "mack", "hino", "isuzu", "ford", "chevrolet", "chevy", "gmc", "ram", "dodge",
    "toyota", "nissan", "jeep", "line", "line-x", "linex", "ziebart", "rhino linings",
    "rhino lining", "tint world", "leonard", "leonard usa", "maaco", "ding king",
}

# A domain shared by this many leads, under this many different business names, is a vendor or
# franchise portal rather than one company's website: independent shops whose listing points at
# the corporate site. leonardusa.com carries 83 leads under 11 names, linex.com 40 under 12.
# Merging on it invents a chain and, worse, discards the other 82 shops as duplicate locations.
PORTAL_MIN_LEADS = 8
PORTAL_MIN_NAMES = 4


def portal_domains(rows):
    """Domains that are a shared platform rather than one business's own site."""
    names = defaultdict(set)
    for r in rows:
        d = domain_of(r["website"])
        if d:
            names[d].add(normalize_name(r["name"]))
    counts = defaultdict(int)
    for r in rows:
        d = domain_of(r["website"])
        if d:
            counts[d] += 1
    return {d for d, ns in names.items()
            if counts[d] >= PORTAL_MIN_LEADS and len(ns) >= PORTAL_MIN_NAMES}


def group_leads(rows):
    """Union-find over shared domain / normalised name. Returns [[row, ...], ...].

    Neither signal is trusted blind: see portal_domains() and BRAND_ONLY_NAMES for the two ways
    a naive version invents chains that do not exist.
    """
    portals = portal_domains(rows) | GENERIC_DOMAINS
    by_domain, by_name = defaultdict(list), defaultdict(list)
    for r in rows:
        d = domain_of(r["website"])
        if d and d not in portals:
            by_domain[d].append(r)
        n = normalize_name(r["name"])
        if len(n) >= MIN_NAME_LEN and n not in BRAND_ONLY_NAMES:
            by_name[n].append(r)

    parent = {r["id"]: r["id"] for r in rows}

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    for buckets in (by_domain, by_name):
        for members in buckets.values():
            for m in members[1:]:
                ra, rb = find(members[0]["id"]), find(m["id"])
                if ra != rb:
                    parent[rb] = ra

    comps = defaultdict(list)
    for r in rows:
        comps[find(r["id"])].append(r)
    return list(comps.values())


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="realtruck_send_list.csv")
    ap.add_argument("--tiers", default=",".join(SENDABLE),
                    help=f"Comma-separated subset of {SENDABLE}, best-first. All three are "
                         "exported by default and labelled in the send_tier column; narrow with "
                         "e.g. --tiers safe,role_account to leave out the highest-risk one.")
    ap.add_argument("--include-unverified", action="store_true",
                    help="Also emit businesses whose only addresses were never Reoon-verified")
    args = ap.parse_args()

    tiers = [t.strip() for t in args.tiers.split(",") if t.strip()]
    bad = set(tiers) - set(SENDABLE)
    if bad:
        print(f"Unknown tier(s): {', '.join(sorted(bad))}. Choose from {SENDABLE}")
        return 1
    rank = {t: i for i, t in enumerate(tiers)}
    rank[UNVERIFIED] = len(tiers)

    rows = list(RealTruckLead.objects.filter(is_qualified=True).values(
        "id", "name", "website", "city", "state", "phone", "emails",
        "business_typology", "confidence_score", "outreach_priority", "priority_tier",
        "website_quality", "is_preferred", "all_brands",
    ))
    if not rows:
        print("No qualified RealTruck leads -- has qualify_leads run?")
        return 1

    # Verified addresses, keyed by lead. A lead can hold several at different tiers.
    verified = defaultdict(list)
    for lead_id, email, status in RealTruckLeadEmail.objects.filter(
            status__in=tiers).values_list("lead_id", "email", "status"):
        verified[lead_id].append((rank[status], email.strip().lower(), status))

    # Tracked separately so a tier filter cannot masquerade as "we have no address for these".
    excluded_tiers = set(SENDABLE) - set(tiers)
    filtered_out = set(RealTruckLeadEmail.objects.filter(
        status__in=excluded_tiers).values_list("lead_id", flat=True)) if excluded_tiers else set()

    groups = group_leads(rows)
    print(f"Qualified leads:          {len(rows):,}")
    print(f"Unique businesses:        {len(groups):,}  "
          f"({len(rows) - len(groups):,} rows collapsed as extra locations)")

    out, no_email, tier_filtered = [], 0, 0
    for mem in groups:
        mem.sort(key=lambda m: (m["state"] or "", m["city"] or ""))
        cands = []
        for m in mem:
            cands += verified[m["id"]]
        if not cands and args.include_unverified:
            cands = [(rank[UNVERIFIED], e.strip().lower(), UNVERIFIED)
                     for m in mem for e in (m["emails"] or []) if e and "@" in e]
        if not cands:
            if any(m["id"] in filtered_out for m in mem):
                tier_filtered += 1
            else:
                no_email += 1
            continue
        cands.sort()

        # The business's own identity comes from the location with the best-scoring website,
        # falling back to the first -- chains list the flagship site, not an arbitrary branch.
        lead = max(mem, key=lambda m: (m["outreach_priority"] or 0, m["confidence_score"] or 0))
        states = sorted({m["state"] for m in mem if m["state"]})
        sites = sorted({(m["website"] or "").strip() for m in mem if m["website"]})
        out.append({
            "primary": cands[0], "cands": cands,
            "all_emails": sorted({e for _, e, _ in cands}),
            "lead": lead, "mem": mem, "states": states, "sites": sites,
        })

    # Second dedupe pass: an address reached by one business must not be sent to under another.
    # Best-tier, highest-priority business keeps it.
    out.sort(key=lambda r: (r["primary"][0], -(r["lead"]["outreach_priority"] or 0)))
    seen, final, collided = set(), [], 0
    for r in out:
        addrs = [e for e in r["all_emails"] if e not in seen]
        if not addrs:
            collided += 1
            continue
        if r["primary"][1] not in addrs:
            # Re-pick from the surviving addresses carrying THEIR own tier -- reusing the old
            # row's status would mislabel the replacement in the column used to throttle sending.
            kept = {a for a in addrs}
            r["primary"] = min(c for c in r["cands"] if c[1] in kept)
        seen.update(addrs)
        r["all_emails"] = addrs
        final.append(r)

    final.sort(key=lambda r: (r["primary"][0], -(r["lead"]["outreach_priority"] or 0),
                              r["lead"]["name"] or ""))

    with open(args.out, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["email", "send_tier", "business_name", "location_name", "website", "city", "state",
                    "all_states", "locations", "typology", "confidence", "outreach_priority",
                    "priority_tier", "website_quality", "is_preferred", "phone",
                    "other_emails", "lead_id"])
        for r in final:
            lead, mem = r["lead"], r["mem"]
            _, email, status = r["primary"]
            name = company_name(lead["name"]) if len(mem) > 1 else (lead["name"] or "")
            w.writerow([
                email, status, name, lead["name"],
                r["sites"][0] if r["sites"] else "",
                lead["city"] or "", lead["state"] or "", ";".join(r["states"]), len(mem),
                lead["business_typology"] or "", lead["confidence_score"] or "",
                lead["outreach_priority"] if lead["outreach_priority"] is not None else "",
                lead["priority_tier"] or "", lead["website_quality"] or "",
                "yes" if lead["is_preferred"] else "",
                next((m["phone"] for m in mem if m["phone"]), ""),
                ";".join(e for e in r["all_emails"] if e != email),
                lead["id"],
            ])

    by_tier = defaultdict(int)
    for r in final:
        by_tier[r["primary"][2]] += 1
    print(f"No sendable address:      {no_email:,}")
    if tier_filtered:
        print(f"Held back by --tiers ({','.join(sorted(excluded_tiers))}): {tier_filtered:,}")
    print(f"Dropped, address already claimed by another business: {collided:,}")
    print(f"\nWrote {args.out}  --  {len(final):,} businesses, one address each")
    for t in tiers + ([UNVERIFIED] if args.include_unverified else []):
        if by_tier[t]:
            print(f"  {t:14} {by_tier[t]:>5}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
