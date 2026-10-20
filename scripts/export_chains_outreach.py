"""
One combined send list of the MULTI-LOCATION businesses held back from the automatic outreach.

The Instantly campaigns deliberately took only small shops -- Google at 1-2 locations (6,376
businesses) and RealTruck at 1-3 (1,111) -- because a chain is a different conversation: the
buying decision sits at a head office, and a templated per-store mail is the wrong opener for a
40-location account. This builds the other half of that split, both sources in one sheet, so the
chains can be worked by hand.

Grouping uses export_realtruck_send_list.group_leads, NOT the website-domain grouping in
export_instantly_list.py. That difference is the reason location counts here disagree with the
September files: domain-only grouping treats a vendor portal as one company, which turned 83
shops that merely link leonardusa.com into a single "83-location business" and silently discarded
the other 82. Read the guards in that module before trusting any count.

Already-contacted addresses are excluded by reading the campaign CSVs themselves (--sent), since
the database records no send history. A business is dropped only if the exact address went out;
an untouched chain that happens to share a domain with a contacted shop is kept.

Usage:
  python scripts/export_chains_outreach.py
  python scripts/export_chains_outreach.py --min-locations 4 --exclude-catchall
  python scripts/export_chains_outreach.py --sent google_instantly_1-2loc.csv,realtruck_instantly.csv
"""
import argparse
import collections
import csv
import os
import sys

import django

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "settings")
django.setup()

from src.models import Lead, LeadEmail, RealTruckLead, RealTruckLeadEmail  # noqa: E402
from src.management.commands.search_lead_emails import (  # noqa: E402
    _brand_tokens, _learned_vendor_domains, _is_vendor_address, domain_of,
)
from scripts.export_instantly_list import brands_of, icebreaker  # noqa: E402
from scripts.export_realtruck_send_list import company_name, group_leads  # noqa: E402
from scripts.export_lead_chains import chain_name, is_franchise, qualified_ratio  # noqa: E402

SOURCES = {"google": (Lead, LeadEmail), "realtruck": (RealTruckLead, RealTruckLeadEmail)}
RANK = {"safe": 0, "role_account": 1, "catch_all": 2}

# The campaigns that already ran. Addresses in these are not offered again.
DEFAULT_SENT = "google_instantly_1-2loc.csv,realtruck_instantly.csv"

# Lists prepared earlier for these same chains, under the buggy domain-only grouping. Rows that
# appeared there are flagged rather than dropped -- they were prepared, not necessarily sent.
PREPARED = "realtruck_instantly_3plus.csv,realtruck_instantly_4plus.csv"

COLUMNS = ["Email", "First Name", "Last Name", "Company Name", "Website", "Phone", "Title",
           "Icebreaker", "City", "State", "Locations", "States", "Source", "Typology",
           "Email Status", "Candidate Emails", "Likely Franchise", "Seen In Earlier List",
           "Qualified Locations", "Processed Locations", "Unprocessed Locations",
           "Qualified Ratio", "AI Confidence", "Priority", "Tier",
           "RealTruck Preferred", "Brand Count", "Top Brands", "Domains", "Locations Detail",
           "Phones", "Lead IDs"]


def read_emails(paths):
    """Addresses from a set of campaign CSVs. Missing files are reported, not fatal."""
    out, missing = set(), []
    for p in [x.strip() for x in paths.split(",") if x.strip()]:
        if not os.path.exists(p):
            missing.append(p)
            continue
        with open(p) as fh:
            for row in csv.DictReader(fh):
                e = (row.get("Email") or "").strip().lower()
                if e:
                    out.add(e)
    return out, missing


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--min-locations", type=int, default=3)
    ap.add_argument("--min-qualified", type=int, default=1,
                    help="Qualified locations a chain needs. One is enough because the ratio "
                         "below does the real filtering; requiring three drops RealTruck chains "
                         "whose other branches were never qualified rather than rejected.")
    ap.add_argument("--min-qualified-ratio", type=float, default=0.4,
                    help="Share of a chain's PROCESSED locations that must have qualified. "
                         "Without it the sheet is topped by national retailers: O'Reilly enters "
                         "at 5,682 locations on one stray qualification, NAPA at 5,358 on nine.")
    ap.add_argument("--exclude-catchall", action="store_true",
                    help="Drop businesses whose only address is accept-all (unconfirmable)")
    ap.add_argument("--sent", default=DEFAULT_SENT,
                    help="Campaign CSVs whose addresses have already been mailed")
    ap.add_argument("--include-no-email", action="store_true", default=True)
    ap.add_argument("--out", default="chains_outreach_3plus.csv")
    args = ap.parse_args()

    min_qualified = args.min_qualified
    sent, missing = read_emails(args.sent)
    for p in missing:
        print(f"  WARNING: --sent file not found, nothing excluded from it: {p}")
    print(f"already-mailed addresses: {len(sent):,}")
    prepared, _ = read_emails(PREPARED)

    statuses = ["safe", "role_account"] + ([] if args.exclude_catchall else ["catch_all"])
    brand_tokens = _brand_tokens()
    vendor_domains = frozenset(_learned_vendor_domains())

    rows = []
    already_mailed = 0
    for source, (lead_model, email_model) in sorted(SOURCES.items()):
        model_fields = {f.name for f in lead_model._meta.get_fields()}
        opt = [f for f in ("is_preferred", "preferred_brands", "all_brands", "brand_count",
                           "outreach_priority", "priority_tier") if f in model_fields]
        fields = ["id", "name", "website", "phone", "city", "state", "emails", "is_qualified",
                  "confidence_score", "business_typology"] + opt

        meta = {i: (n, domain_of(w))
                for i, n, w in lead_model.objects.values_list("id", "name", "website")}
        best = {}
        for lead_id, email, status, free in email_model.objects.filter(
                verified_at__isnull=False, status__in=statuses
        ).values_list("lead_id", "email", "status", "is_free_email"):
            name, site = meta.get(lead_id, ("", ""))
            # A dealer's site lists the brands it stocks, so a harvest can pick up the
            # manufacturer's address and it looks like the shop's until the same one shows up
            # under a dozen unrelated shops.
            if _is_vendor_address(email or "", site, name, brand_tokens, vendor_domains):
                continue
            if lead_id not in best or RANK[status] < best[lead_id][0]:
                best[lead_id] = (RANK[status], email, status, bool(free))

        all_rows = list(lead_model.objects.values(*fields))
        groups = [g for g in group_leads(all_rows) if len(g) >= args.min_locations]
        sized = len(groups)
        # A chain belongs in an outreach list only if it is a chain we actually sell to. Both
        # tests are needed: the count alone keeps NAPA (nine qualified locations, more than most
        # real prospects have) because it scales with the chain's size rather than its fit.
        groups = [g for g in groups
                  if sum(1 for m in g if m["is_qualified"]) >= min_qualified
                  and qualified_ratio(g) >= args.min_qualified_ratio]
        print(f"{source}: {sized:,} businesses with >={args.min_locations} locations "
              f"-> {len(groups):,} are target chains "
              f"({sized - len(groups):,} dropped as national/non-target)")

        for g in groups:
            g.sort(key=lambda m: (m["state"] or "", m["city"] or ""))
            flagship = max(g, key=lambda m: ((m.get("outreach_priority") or 0),
                                             m["confidence_score"] or 0))
            picks = sorted((best[m["id"]] for m in g if m["id"] in best))
            email = status = ""
            free = False
            if picks:
                _, email, status, free = picks[0]
            cands = sorted({e for m in g for e in (m["emails"] or []) if e and "@" in e}
                           - {email})
            if email and email.lower() in sent:
                already_mailed += 1
                continue
            states = sorted({m["state"] for m in g if m["state"]})
            chain = chain_name(g, flagship)
            rows.append({
                "Email": email, "First Name": "", "Last Name": "",
                "Company Name": chain, "Website": flagship["website"] or "",
                "Phone": next((m["phone"] for m in g if m["phone"]), ""), "Title": "",
                "Icebreaker": icebreaker(flagship, len(g)),
                "City": flagship["city"] or "", "State": flagship["state"] or "",
                "Locations": len(g), "States": ";".join(states), "Source": source,
                "Typology": flagship["business_typology"] or "Unclassified",
                "Email Status": status or ("unverified candidate(s)" if cands
                                           else "needs manual research"),
                "Candidate Emails": "; ".join(cands),
                "Likely Franchise": "yes" if is_franchise(chain) else "",
                "Seen In Earlier List": "yes" if email and email.lower() in prepared else "",
                "Qualified Locations": sum(1 for m in g if m["is_qualified"]),
                # The ratio's denominator. 545-of-546 and 3-of-3 both read "1.00" and are not the
                # same evidence, so the sample size travels with the number.
                "Processed Locations": sum(1 for m in g if m["is_qualified"] is not None),
                "Unprocessed Locations": sum(1 for m in g if m["is_qualified"] is None),
                "Qualified Ratio": f"{qualified_ratio(g):.2f}",
                "AI Confidence": flagship["confidence_score"] or "",
                "Priority": flagship.get("outreach_priority") or "",
                "Tier": flagship.get("priority_tier") or "",
                "RealTruck Preferred": "yes" if flagship.get("is_preferred") else "",
                "Brand Count": flagship.get("brand_count") or "",
                "Top Brands": "; ".join(brands_of(flagship, 5)),
                "Domains": ";".join(sorted({domain_of(m["website"]) for m in g
                                            if domain_of(m["website"])})),
                "Locations Detail": " | ".join(f"{m['city']}, {m['state']}" for m in g),
                "Phones": ";".join(sorted({m["phone"] for m in g if m["phone"]})),
                "Lead IDs": ";".join(str(m["id"]) for m in g),
            })

    # Biggest chains first, and within a size the ones you can actually mail today.
    STATUS_ORDER = {**RANK, "unverified candidate(s)": 8, "needs manual research": 9}
    rows.sort(key=lambda r: (-r["Locations"], STATUS_ORDER.get(r["Email Status"], 9),
                             r["Company Name"].lower()))

    # One address per row across the whole sheet: the same head office must not be mailed twice
    # because two sources found the same chain.
    seen, final, collisions = set(), [], 0
    for r in rows:
        e = r["Email"].lower()
        if e and e in seen:
            collisions += 1
            continue
        if e:
            seen.add(e)
        final.append(r)

    with open(args.out, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=COLUMNS)
        w.writeheader()
        w.writerows(final)

    mailable = sum(1 for r in final if r["Email"])
    print(f"\nWrote {args.out}  --  {len(final):,} chains")
    print(f"  dropped, address already mailed:     {already_mailed:,}")
    print(f"  dropped, duplicate across sources:   {collisions:,}")
    print(f"  mailable now (verified address):     {mailable:,}")
    print(f"  need manual research:                {len(final) - mailable:,}")
    print("  by source:", dict(collections.Counter(r["Source"] for r in final)))
    print("  status   :", dict(collections.Counter(r["Email Status"] for r in final)))
    print("  franchise:", sum(1 for r in final if r["Likely Franchise"]))
    thin = sum(1 for r in final if int(r["Processed Locations"]) < 3)
    print(f"  resting on <3 judged locations (thin evidence): {thin:,}")


if __name__ == "__main__":
    main()
