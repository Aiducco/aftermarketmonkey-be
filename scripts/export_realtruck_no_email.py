"""
Exports the qualified RealTruck businesses we still have no email address for.

These are not failures of reach -- every one has a live website and has already been through the
v2 harvester, which found nothing. A 60-lead resample hit 1. What is left are shops whose site
carries a contact form and no address, so the list exists to be worked by hand or fed to a
guess-and-verify run, not re-crawled.

One row per business, not per location: chains are collapsed the same way as the send list, so
699 domains do not arrive as 779 rows.

has_mx is the column that matters. A domain with no MX record cannot receive mail at all, so no
amount of guessing or manual digging will produce a working address there -- filter those out
before spending anyone's time. It costs one free DNS lookup per domain to know.

Usage:
  python scripts/export_realtruck_no_email.py
  python scripts/export_realtruck_no_email.py --mx-only --out worth_chasing.csv
  python scripts/export_realtruck_no_email.py --no-mx-check     # skip DNS, faster
"""
import argparse
import csv
import os
import sys
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

import django

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "settings")
django.setup()

from django.db.models import Q  # noqa: E402
from src.models import RealTruckLead  # noqa: E402
from scripts.export_realtruck_send_list import company_name, group_leads  # noqa: E402
from scripts.find_multi_location_leads import domain_of  # noqa: E402

TIER_ORDER = {"A": 0, "B": 1, "C": 2}


def mx_map(domains, workers=40):
    """{domain: bool} -- does this domain accept mail at all. Free, and the single best filter."""
    try:
        import dns.resolver
    except ImportError:
        print("dnspython not installed; skipping MX check (pip install dnspython)")
        return {}
    res = dns.resolver.Resolver()
    res.timeout, res.lifetime = 3, 4

    def probe(d):
        try:
            return d, bool(res.resolve(d, "MX"))
        except Exception:
            return d, False

    with ThreadPoolExecutor(max_workers=workers) as ex:
        return dict(ex.map(probe, domains))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="realtruck_no_email.csv")
    ap.add_argument("--mx-only", action="store_true", help="Only domains that can receive mail")
    ap.add_argument("--no-mx-check", action="store_true")
    args = ap.parse_args()

    rows = list(RealTruckLead.objects.filter(is_qualified=True).filter(
        Q(emails__isnull=True) | Q(emails=[])).values(
        "id", "name", "website", "city", "state", "phone", "business_typology",
        "confidence_score", "outreach_priority", "priority_tier", "website_quality",
        "is_preferred", "all_brands"))
    if not rows:
        print("Every qualified RealTruck lead has an email.")
        return 0

    groups = group_leads(rows)
    print(f"Qualified leads with no email: {len(rows):,}")
    print(f"Unique businesses:             {len(groups):,}")

    mx = {}
    if not args.no_mx_check:
        doms = {d for r in rows if (d := domain_of(r["website"]))}
        print(f"Checking MX for {len(doms):,} domains...")
        mx = mx_map(doms)
        print(f"  can receive mail: {sum(mx.values()):,} | cannot: {len(mx) - sum(mx.values()):,}")

    out = []
    for mem in groups:
        mem.sort(key=lambda m: (m["state"] or "", m["city"] or ""))
        lead = max(mem, key=lambda m: (m["outreach_priority"] or 0, m["confidence_score"] or 0))
        d = domain_of(lead["website"]) or next(
            (domain_of(m["website"]) for m in mem if domain_of(m["website"])), "")
        has_mx = mx.get(d)
        if args.mx_only and has_mx is False:
            continue
        out.append((lead, mem, d, has_mx))

    # Worth-chasing order: mail-capable first, then the priority score already computed.
    out.sort(key=lambda t: (t[3] is False,
                            TIER_ORDER.get(t[0]["priority_tier"], 3),
                            -(t[0]["outreach_priority"] or 0),
                            t[0]["name"] or ""))

    with open(args.out, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["website", "domain", "has_mx", "business_name", "city", "state", "all_states",
                    "locations", "typology", "confidence", "outreach_priority", "priority_tier",
                    "website_quality", "is_preferred", "phone", "lead_id"])
        for lead, mem, d, has_mx in out:
            states = sorted({m["state"] for m in mem if m["state"]})
            sites = sorted({(m["website"] or "").strip() for m in mem if m["website"]})
            w.writerow([
                sites[0] if sites else "", d,
                "" if has_mx is None else ("yes" if has_mx else "no"),
                company_name(lead["name"]) if len(mem) > 1 else (lead["name"] or ""),
                lead["city"] or "", lead["state"] or "", ";".join(states), len(mem),
                lead["business_typology"] or "", lead["confidence_score"] or "",
                lead["outreach_priority"] if lead["outreach_priority"] is not None else "",
                lead["priority_tier"] or "", lead["website_quality"] or "",
                "yes" if lead["is_preferred"] else "",
                next((m["phone"] for m in mem if m["phone"]), ""), lead["id"],
            ])

    print(f"\nWrote {args.out}  --  {len(out):,} businesses")
    if mx:
        chase = sum(1 for *_, h in out if h)
        print(f"  worth chasing (has MX): {chase:,}")
        print(f"  no mail server at all : {len(out) - chase:,}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
