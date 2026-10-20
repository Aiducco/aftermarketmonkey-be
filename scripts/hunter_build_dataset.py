"""
Turns the Hunter checkpoint into a contact dataset, with every name-resolved row audited.

Hunter resolves a company NAME to a domain on its own, and when it guesses wrong it says so
nowhere: "Friend Tire Company" came back as bendtireco.com and "JMK" as jmk.org.pl, each with a
real staff list belonging to somebody else. Mailing those is worse than missing them, so each row
carries how its domain was decided and whether the domain agrees with the company name:

  domain_match = given     we supplied the domain; Hunter did not guess
  domain_match = ok        Hunter guessed, and the name's distinctive words appear in the domain
  domain_match = CHECK     Hunter guessed, and they do not -- verify before mailing

Rows are one person each. People with no first name are kept but marked, since a generic
info@ mailbox is still worth having while being useless for a personalised opener.

Usage:
  python scripts/hunter_build_dataset.py
  python scripts/hunter_build_dataset.py --min-confidence 80 --out high_conf.csv
"""
import argparse
import csv
import json
import os
import re
import sys

CHECKPOINT = "data/distributors/hunter_raw.jsonl"
OUT = "data/distributors/people.csv"

NOISE = {"inc", "llc", "ltd", "co", "company", "corp", "the", "and", "of", "wholesale",
         "distributors", "distributing", "distribution", "group", "enterprises", "usa", "online"}
# Titles worth reaching first: the people who own data feeds and integrations.
PRIORITY_WORDS = ("ecommerce", "e-commerce", "digital", "technology", "it ", "cto", "cio",
                  "data", "integration", "product", "operations", "supply", "inventory",
                  "partnership", "business development", "sales", "president", "owner",
                  "founder", "director", "vp", "vice president", "chief")

COLUMNS = ["Email", "First Name", "Last Name", "Full Name", "Position", "Seniority",
           "Department", "Company", "Queried Company", "Domain", "Tier", "Confidence",
           "Email Type",
           "Domain Match", "Queried By", "Query", "LinkedIn", "Twitter", "Phone",
           "Priority Role", "Sources"]


def tokens(name):
    n = re.sub(r"\(.*?\)", " ", (name or "").lower()).replace("&", " and ")
    return [t for t in re.sub(r"[^a-z0-9 ]", " ", n).split() if t and t not in NOISE]


# These are US and Canadian distributors. Anything else is a sign Hunter matched a same-named
# company somewhere else entirely -- "JMK" resolved to jmk.org.pl, a Polish organisation.
PLAUSIBLE_TLDS = (".com", ".net", ".us", ".ca", ".org")


def domain_verdict(company, domain, queried_by):
    if queried_by == "domain":
        return "given"
    domain = (domain or "").lower()
    if not domain or not domain.endswith(PLAUSIBLE_TLDS) or domain.count(".") > 1:
        return "CHECK"
    label = domain.rsplit(".", 1)[0].replace("-", "")
    toks = [t for t in tokens(company) if len(t) > 2]
    if not toks or not label:
        return "CHECK"
    if len(toks) == 1:
        # One distinctive word cannot corroborate itself: every domain containing "freedomsoft"
        # contains it, whoever owns it. Only an exact label match is evidence.
        return "ok" if label == toks[0] else "CHECK"
    hits = sum(1 for t in toks if t in label)
    return "ok" if hits >= 2 else "CHECK"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--min-confidence", type=int, default=0)
    ap.add_argument("--out", default=OUT)
    ap.add_argument("--include-check", action="store_true", default=True,
                    help="Keep rows whose domain could not be verified (marked CHECK)")
    args = ap.parse_args()

    if not os.path.exists(CHECKPOINT):
        sys.exit(f"no checkpoint at {CHECKPOINT} -- run hunter_find_people.py first")

    rows, empty = [], []
    for line in open(CHECKPOINT):
        try:
            rec = json.loads(line)
        except json.JSONDecodeError:
            continue
        data = rec.get("data") or {}
        domain = data.get("domain") or ""
        emails = data.get("emails") or []
        verdict = domain_verdict(rec["company"], domain, rec.get("queried_by"))
        if not emails:
            empty.append((rec["company"], domain or "-", verdict))
            continue
        org = data.get("organization") or ""
        for e in emails:
            conf = e.get("confidence") or 0
            if conf < args.min_confidence:
                continue
            pos = (e.get("position") or "").strip()
            rows.append({
                "Email": e.get("value") or "",
                "First Name": e.get("first_name") or "",
                "Last Name": e.get("last_name") or "",
                "Full Name": " ".join(x for x in (e.get("first_name"), e.get("last_name")) if x),
                "Position": pos,
                "Seniority": e.get("seniority") or "",
                "Department": e.get("department") or "",
                "Company": org or rec["company"],
                "Queried Company": rec["company"],
                "Domain": domain,
                "Tier": rec.get("tier", ""),
                "Confidence": conf,
                "Email Type": e.get("type") or "",
                "Domain Match": verdict,
                "Queried By": rec.get("queried_by", ""),
                "Query": rec.get("query", ""),
                "LinkedIn": e.get("linkedin") or "",
                "Twitter": e.get("twitter") or "",
                "Phone": e.get("phone_number") or "",
                "Priority Role": "yes" if any(w in pos.lower() for w in PRIORITY_WORDS) else "",
                "Sources": len(e.get("sources") or []),
            })

    if not args.include_check:
        rows = [r for r in rows if r["Domain Match"] != "CHECK"]

    # Named, senior, high-confidence contacts first -- that is the order you would work them.
    SEN = {"executive": 0, "senior": 1, "junior": 2, "": 3}
    rows.sort(key=lambda r: (r["Domain Match"] == "CHECK",
                             not r["Priority Role"],
                             SEN.get(r["Seniority"], 3),
                             -r["Confidence"], r["Company"].lower()))

    with open(args.out, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=COLUMNS)
        w.writeheader()
        w.writerows(rows)

    import collections
    print(f"{args.out}: {len(rows):,} contacts across "
          f"{len({r['Company'] for r in rows})} companies\n")
    print(f"  named (first name present): {sum(1 for r in rows if r['First Name']):,}")
    print(f"  generic mailbox only      : {sum(1 for r in rows if not r['First Name']):,}")
    print(f"  priority role             : {sum(1 for r in rows if r['Priority Role']):,}")
    print(f"  confidence >=80           : {sum(1 for r in rows if r['Confidence'] >= 80):,}")
    print("  domain match:", dict(collections.Counter(r["Domain Match"] for r in rows)))
    check = sorted({(r["Queried Company"], r["Company"], r["Domain"])
                    for r in rows if r["Domain Match"] == "CHECK"})
    if check:
        n = sum(1 for r in rows if r["Domain Match"] == "CHECK")
        print(f"\n  VERIFY BEFORE MAILING -- {n} contacts at {len(check)} companies where Hunter")
        print("  resolved the name to something that does not match it (asked -> got):")
        for asked, got, dom in check:
            same = asked.lower().replace(" ", "") == (got or "").lower().replace(" ", "")
            mark = "" if same else "  <-- different company"
            print(f"    {asked[:30]:32} -> {(got or '?')[:26]:28} {dom[:24]:26}{mark}")
    if empty:
        print(f"\n  {len(empty)} companies returned no contacts (no credit charged by Hunter):")
        for comp, dom, v in empty:
            print(f"    {comp[:34]:36} -> {dom}  [{v}]")


if __name__ == "__main__":
    main()
