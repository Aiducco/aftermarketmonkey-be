"""
Finds qualified leads that look like multi-location businesses -- chains and franchise groups,
which are worth more than a single shop because one conversation covers several buying locations.

Two independent signals, because neither catches everything on its own:

  domain  Several qualified rows share one website. Strongest signal, but misses chains whose
          locations each run their own site, and would false-positive on shared platforms
          (Facebook, Wix), which is why GENERIC_DOMAINS is excluded.
  name    Several qualified rows share a business name once the location suffix is stripped
          ("JX Truck Center - Wausau" -> "JX Truck Center"). Catches separate-domain chains,
          but would false-positive on short generic names, hence MIN_NAME_LEN.

A group is reported if either signal fires. Groups are keyed so the same chain found by both
signals is reported once.

Usage:
  python scripts/find_multi_location_leads.py                    # qualified RealTruck leads
  python scripts/find_multi_location_leads.py --source google    # qualified Google Maps leads
  python scripts/find_multi_location_leads.py --min-locations 3
  python scripts/find_multi_location_leads.py --out chains.csv
  python scripts/find_multi_location_leads.py --detailed   # one row per location
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

from src.models import Lead, RealTruckLead  # noqa: E402

SOURCES = {"google": Lead, "realtruck": RealTruckLead}

# Shared platforms -- two dealers both linking Facebook are not one company.
GENERIC_DOMAINS = {
    "facebook.com", "m.facebook.com", "instagram.com", "linkedin.com", "twitter.com", "x.com",
    "yelp.com", "google.com", "sites.google.com", "wixsite.com", "business.site",
    "godaddysites.com", "squarespace.com", "weebly.com", "wordpress.com",
}

# Below this, a stripped name is too generic to treat as a chain ("Auto", "Truck Shop").
MIN_NAME_LEN = 8

# Location suffixes: "Name - Wausau", "Name (Dallas)", "Name of Phoenix", "Name #4"
SUFFIX_RE = re.compile(
    r"\s*(?:[-–—|]\s*.+|\(.*\)|\bof\s+[A-Z][\w.\- ]+|#\s*\d+|\bno\.?\s*\d+)\s*$",
    re.IGNORECASE,
)


def domain_of(url: str) -> str:
    u = (url or "").strip().lower()
    u = re.sub(r"^https?://", "", u)
    u = u.split("/")[0].split("?")[0].split(":")[0]
    return re.sub(r"^www\.", "", u)


def normalize_name(name: str) -> str:
    n = (name or "").strip()
    for _ in range(2):  # "Name - Ford - Dallas" needs two passes
        stripped = SUFFIX_RE.sub("", n).strip(" ,-")
        if stripped == n:
            break
        n = stripped
    n = re.sub(r"\s+", " ", n)
    n = re.sub(r"[.,]+$", "", n)
    return n.lower().strip()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", default="realtruck", choices=sorted(SOURCES))
    ap.add_argument("--min-locations", type=int, default=2)
    ap.add_argument("--out", default="multi_location_leads.csv")
    ap.add_argument("--detailed", action="store_true",
                    help="One row per location instead of one row per group")
    args = ap.parse_args()

    model = SOURCES[args.source]
    rows = list(
        model.objects.filter(is_qualified=True).values(
            "id", "name", "website", "city", "state", "phone",
            "business_typology", "confidence_score",
        )
    )
    if not rows:
        print("No qualified leads found -- has qualify_leads run for this source?")
        return 1

    by_domain, by_name = defaultdict(list), defaultdict(list)
    for r in rows:
        d = domain_of(r["website"])
        if d and d not in GENERIC_DOMAINS:
            by_domain[d].append(r)
        n = normalize_name(r["name"])
        if len(n) >= MIN_NAME_LEN:
            by_name[n].append(r)

    # Union the two signals into connected components. Keying groups by their member set is not
    # enough: Toys For Trucks matched 10 rows by domain and 11 by name (one location runs its own
    # site), which reported the same chain twice. Anything linked by either signal is one group.
    parent = {r["id"]: r["id"] for r in rows}

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[rb] = ra

    labels = defaultdict(set)  # component root -> {(signal, key)}
    for signal, buckets in (("domain", by_domain), ("name", by_name)):
        for key, members in buckets.items():
            if len(members) < 2:
                continue
            for m in members[1:]:
                union(members[0]["id"], m["id"])
            labels[find(members[0]["id"])].add((signal, key))

    components = defaultdict(list)
    for r in rows:
        components[find(r["id"])].append(r)

    groups = []
    for root, members in components.items():
        if len(members) < args.min_locations:
            continue
        lbls = labels.get(root, set())
        # Prefer the domain label as the group's display name; fall back to the name label.
        key = next((k for s, k in sorted(lbls) if s == "domain"),
                   next((k for _, k in sorted(lbls)), members[0]["name"]))
        groups.append({
            "key": key,
            "signals": {s for s, _ in lbls},
            "members": members,
        })

    ordered = sorted(groups, key=lambda g: (-len(g["members"]), g["key"]))
    in_groups = {m["id"] for g in ordered for m in g["members"]}

    print(f"Qualified {args.source} leads:            {len(rows)}")
    print(f"Multi-location groups (>={args.min_locations} sites):  {len(ordered)}")
    print(f"Qualified leads inside a group:      {len(in_groups)}\n")
    print(f"{'sites':>5}  {'states':>6}  {'via':<11}  group")
    print("-" * 92)
    for g in ordered:
        states = sorted({m["state"] for m in g["members"] if m["state"]})
        print(f"{len(g['members']):>5}  {len(states):>6}  {'+'.join(sorted(g['signals'])):<11}  "
              f"{g['key'][:40]:<40} {','.join(states[:8])}")

    with open(args.out, "w", newline="") as fh:
        w = csv.writer(fh)
        if args.detailed:
            # One row per location, for when you need to work the individual sites.
            w.writerow(["group", "matched_via", "locations", "states", "lead_id", "name",
                        "website", "city", "state", "phone", "typology", "confidence"])
            for g in ordered:
                states = sorted({m["state"] for m in g["members"] if m["state"]})
                for m in sorted(g["members"], key=lambda x: (x["state"] or "", x["city"] or "")):
                    w.writerow([
                        g["key"], "+".join(sorted(g["signals"])), len(g["members"]), ";".join(states),
                        m["id"], m["name"], m["website"], m["city"], m["state"], m["phone"],
                        m["business_typology"], m["confidence_score"],
                    ])
        else:
            # Default: one row per chain. The locations are collapsed into single cells so each
            # row is one prospect to contact, not N rows you have to mentally re-group.
            w.writerow(["group", "matched_via", "locations", "states", "typologies",
                        "avg_confidence", "business_name", "website", "locations_detail", "phones"])
            for g in ordered:
                mem = sorted(g["members"], key=lambda x: (x["state"] or "", x["city"] or ""))
                states = sorted({m["state"] for m in mem if m["state"]})
                typ = sorted({m["business_typology"] for m in mem if m["business_typology"]})
                scores = [m["confidence_score"] for m in mem if m["confidence_score"] is not None]
                sites = sorted({(m["website"] or "").strip() for m in mem if m["website"]})
                w.writerow([
                    g["key"],
                    "+".join(sorted(g["signals"])),
                    len(mem),
                    ";".join(states),
                    ";".join(typ),
                    round(sum(scores) / len(scores)) if scores else "",
                    mem[0]["name"],
                    sites[0] if len(sites) == 1 else ";".join(sites),
                    " | ".join(f"{m['city']}, {m['state']}" for m in mem),
                    ";".join(sorted({m["phone"] for m in mem if m["phone"]})),
                ])
    print(f"\nWrote {args.out}  ({'one row per location' if args.detailed else 'one row per group'})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
