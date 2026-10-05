"""
Builds an Instantly-ready send list from the qualified leads that have a verified address.

One row per BUSINESS, not per lead: locations sharing a website domain are one shop to contact,
and mailing each branch separately is the same conversation started five times. The address kept
for a business is its best-ranked one -- safe over role_account over catch_all -- because that is
the order of how confidently the mailbox is known to exist.

Supplier addresses are excluded. A dealer's site lists the brands it stocks, so a search for that
dealer surfaces the manufacturer's contact page, and an address harvested there looks like the
shop's until you notice the same one under a dozen unrelated shops. See
``search_lead_emails._is_vendor_address`` -- the same test is applied here so an older row
harvested before that check existed cannot reach a send list.

Icebreaker text is assembled from stored fields only (location count, dealer tier, stocked
brands, city). Nothing in it is inferred or invented; it is a scaffold to rewrite, not a
finished line.

First/Last/Title are left empty on purpose. These records are businesses, and guessing a person's
name from an address local-part puts a wrong name in a cold email, which is worse than none.

Usage:
  python scripts/export_instantly_list.py --source realtruck --max-locations 3
  python scripts/export_instantly_list.py --source realtruck --min-locations 4
  python scripts/export_instantly_list.py --source google --min-locations 1 --max-locations 3
  python scripts/export_instantly_list.py --source realtruck --exclude-catchall
"""
import argparse
import collections
import csv
import os
import re
import sys

import django

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "settings")
django.setup()

from src.models import Lead, LeadEmail, RealTruckLead, RealTruckLeadEmail  # noqa: E402
from src.management.commands.search_lead_emails import (  # noqa: E402
    _brand_tokens, _learned_vendor_domains, _is_vendor_address, domain_of, GENERIC_DOMAINS,
)

SOURCES = {"google": (Lead, LeadEmail), "realtruck": (RealTruckLead, RealTruckLeadEmail)}
RANK = {"safe": 0, "role_account": 1, "catch_all": 2}
TYPOLOGY_FIRST = ["Off-Road"]          # everything else follows alphabetically


def brands_of(row, n=3):
    src = row.get("preferred_brands") or row.get("all_brands") or ""
    return [b.strip() for b in src.split(";") if b.strip()][:n]


def icebreaker(row, locations):
    bits = brands_of(row)
    if locations > 1:
        line = f"noticed you're running {locations} locations"
    elif row.get("is_preferred"):
        line = "saw you're a RealTruck preferred dealer"
    elif row.get("brand_count"):
        line = f"saw you carry {row['brand_count']} of the lines we track"
    else:
        line = "came across your shop"
    if bits:
        line += f" — {', '.join(bits[:2])} on the shelf"
    if row.get("city"):
        line += f" out of {row['city']}"
    return line


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", default="realtruck", choices=sorted(SOURCES))
    ap.add_argument("--min-locations", type=int, default=1)
    ap.add_argument("--max-locations", type=int, default=10_000)
    ap.add_argument("--exclude-catchall", action="store_true",
                    help="Drop businesses whose only address is accept-all (unconfirmable)")
    ap.add_argument("--exclude-domains", default="",
                    help="Comma-separated domains (or substrings) to drop from the list entirely")
    ap.add_argument("--dedupe-against", default=None, choices=sorted(SOURCES),
                    help="Drop any business whose domain also appears in this other source's "
                         "lead table -- avoids mailing the same shop twice from two sources")
    ap.add_argument("--include-no-email", action="store_true",
                    help="Also include qualified businesses with no verified sendable address -- "
                         "for manual follow-up. Email/Email Status are blank unless an unverified "
                         "candidate address exists, in which case it is shown in Candidate Emails")
    ap.add_argument("--out", default=None)
    args = ap.parse_args()
    excluded = [d.strip().lower() for d in args.exclude_domains.split(",") if d.strip()]

    lead_model, email_model = SOURCES[args.source]

    dupe_domains = set()
    if args.dedupe_against:
        other_model, _ = SOURCES[args.dedupe_against]
        for w in other_model.objects.filter(is_qualified=True).values_list("website", flat=True):
            d = domain_of(w)
            if d and d not in GENERIC_DOMAINS:
                dupe_domains.add(d)

    brand_tokens = _brand_tokens()
    vendor_domains = frozenset(_learned_vendor_domains())

    statuses = ["safe", "role_account"] + ([] if args.exclude_catchall else ["catch_all"])
    meta = {i: (n, domain_of(w))
            for i, n, w in lead_model.objects.values_list("id", "name", "website")}

    best = {}
    for lead_id, email, status, free in email_model.objects.filter(
            verified_at__isnull=False, status__in=statuses
    ).values_list("lead_id", "email", "status", "is_free_email"):
        name, site = meta.get(lead_id, ("", ""))
        if _is_vendor_address(email or "", site, name, brand_tokens, vendor_domains):
            continue
        if lead_id not in best or RANK[status] < best[lead_id][0]:
            best[lead_id] = (RANK[status], email, status, bool(free))

    # Google's Lead and RealTruck's RealTruckLead were built independently and disagree on
    # names for the same concept (zipcode vs zip_code, full_address vs address), and Lead has no
    # outreach-priority scoring at all. Resolve every field per source rather than assume one
    # table's shape fits both.
    model_fields = {f.name for f in lead_model._meta.get_fields()}

    def pick(*candidates):
        return next((c for c in candidates if c in model_fields), None)

    zip_field = pick("zipcode", "zip_code")
    address_field = pick("full_address", "address")
    priority_field = pick("outreach_priority")     # Lead has no equivalent -- None is fine
    tier_field = pick("priority_tier")
    quality_field = pick("website_quality")

    fields = ["id", "name", "website", "phone", "city", "state", "confidence_score",
              "business_typology"]
    fields += [f for f in (zip_field, address_field, priority_field, tier_field, quality_field) if f]
    optional = [f for f in ("is_preferred", "is_real_pro", "preferred_brands", "all_brands",
                            "brand_count") if hasattr(lead_model, f)]

    base_qs = lead_model.objects.filter(is_qualified=True)
    if not args.include_no_email:
        base_qs = base_qs.filter(id__in=best)

    # Every lead's raw (unverified) candidate addresses, for the no-email rows -- shown as a
    # starting point for manual work, never as something already confirmed sendable.
    raw_emails = {i: (ems or []) for i, ems in
                  lead_model.objects.filter(is_qualified=True).values_list("id", "emails")}

    groups = collections.defaultdict(list)
    for row in base_qs.values_list(*(fields + optional)):
        d = dict(zip(fields + optional, row))
        key = domain_of(d["website"])
        if excluded and any(x in key for x in excluded):
            continue
        if key in dupe_domains:
            continue
        groups[key if (key and key not in GENERIC_DOMAINS) else f"id:{d['id']}"].append(d)

    rows = []
    for key, leads in groups.items():
        locations = len(leads)
        if not (args.min_locations <= locations <= args.max_locations):
            continue
        leads.sort(key=lambda d: -(d.get(priority_field) or 0))
        d = leads[0]
        has_verified = d["id"] in best
        if has_verified:
            _, email, status, free = best[d["id"]]
            candidates = ""
        else:
            email, status, free = "", "needs manual research", False
            # Any unverified address found across the group's leads -- a lead to run down by
            # hand, not something to email as-is.
            JUNK_LOCALS = {"example", "test", "yourname", "yourcompany", "name", "email"}
            JUNK_DOMAINS = {"example.com", "mysite.com", "yoursite.com", "domain.com", "email.com"}
            cands = sorted({
                e for x in leads for e in raw_emails.get(x["id"], [])
                if e and e.split("@")[-1] not in JUNK_DOMAINS
                and e.split("@")[0] not in JUNK_LOCALS
            })
            candidates = "; ".join(cands)
            if cands:
                status = "unverified candidate(s)"

        rows.append({
            "Email": email, "First Name": "", "Last Name": "",
            "Company Name": d["name"], "Website": d["website"], "Phone": d["phone"], "Title": "",
            "Icebreaker": icebreaker(d, locations),
            "City": d["city"], "State": d["state"], "Zip": d.get(zip_field, ""),
            "Full Address": d.get(address_field, ""),
            "Typology": d["business_typology"] or "Unclassified", "Locations": locations,
            "Email Status": status, "Candidate Emails": candidates,
            "Free Mailbox": "yes" if free else "no",
            "Priority": d.get(priority_field), "Tier": d.get(tier_field),
            "Site Quality": d.get(quality_field), "AI Confidence": d["confidence_score"],
            "RealTruck Preferred": "yes" if d.get("is_preferred") else "no",
            "RealPro": "yes" if d.get("is_real_pro") else "no",
            "Brand Count": d.get("brand_count"), "Top Brands": "; ".join(brands_of(d, 5)),
            "Domain": key, "Lead IDs": ";".join(str(x["id"]) for x in leads),
        })

    order = {t: i for i, t in enumerate(TYPOLOGY_FIRST)}
    STATUS_ORDER = {**RANK, "unverified candidate(s)": 8, "needs manual research": 9}
    rows.sort(key=lambda r: (order.get(r["Typology"], len(order)), r["Typology"],
                             STATUS_ORDER.get(r["Email Status"], 9), -(r["Priority"] or 0 if r["Priority"] else 0)))

    out = args.out or f"{args.source}_instantly_{args.min_locations}-{args.max_locations}loc.csv"
    with open(out, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)

    print(f"{len(rows)} businesses -> {out}")
    seen = []
    for r in rows:
        if r["Typology"] not in seen:
            seen.append(r["Typology"])
    for t in seen:
        print(f"   {sum(1 for r in rows if r['Typology'] == t):5d}  {t}")
    print("  status   :", dict(collections.Counter(r["Email Status"] for r in rows)))
    print("  locations:", dict(sorted(collections.Counter(r["Locations"] for r in rows).items())))


if __name__ == "__main__":
    main()
