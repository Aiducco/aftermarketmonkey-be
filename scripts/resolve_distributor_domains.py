"""
Resolves distributor company names to websites WITHOUT spending Hunter credits.

Hunter's domain-search accepts a company name and will resolve it itself, but a wrong match
still costs a credit and returns a stranger's staff list -- which is worse than no answer,
because nothing downstream reveals that the people belong to a different company. On a 50-search
plan that is unaffordable. So candidates are generated from the name, resolved over DNS, fetched,
and then CONFIRMED by matching the company's own words against what the site says about itself.

Every row carries how it was decided:
  confirmed   the page names the company in words that are not already in the domain
  blocked     a bot wall (Cloudflare, "Access Denied") hid the content -- usually the right
              domain, just unverifiable from here
  parked      registrar parking or a for-sale listing
  weak        a real page that belongs to somebody else
  unresolved  no candidate answered DNS

Only `confirmed` goes to Hunter unattended. The evidence test deliberately ignores any word that
is already in the domain: "barrons" appears on barrons.com whoever owns it, so matching it proved
nothing and resolved Barron's Wholesale Tire to a financial magazine.

Usage:
  python scripts/resolve_distributor_domains.py
  python scripts/resolve_distributor_domains.py --only 2-priority,1-named
  python scripts/resolve_distributor_domains.py --workers 16
"""
import argparse
import csv
import os
import re
import socket
import sys
from concurrent.futures import ThreadPoolExecutor

import requests
import urllib3

urllib3.disable_warnings()

IN_CSV = "data/distributors/companies.csv"
OUT_CSV = "data/distributors/domains.csv"

# Dropped before building candidates: present in a legal name, never in a domain.
NOISE = {"inc", "inc.", "llc", "l.l.c", "ltd", "ltd.", "co", "co.", "company", "corp",
         "corporation", "the", "and", "of", "wholesale", "distributors", "distributing",
         "distribution", "group", "enterprises"}
# Too common across this industry to prove identity on their own -- but still part of the trade
# name, so they stay in the candidate DOMAIN (eastcoastwheels.com), they just do not count as
# evidence in the match test.
WEAK_TOKENS = {"tire", "tires", "wheel", "wheels", "auto", "automotive", "truck", "trucks",
               "parts", "supply", "service", "services", "performance", "diesel", "online"}

# Seeing one of these means the page is about this industry, which is what lets a single
# distinctive word ("Atturo") stand as proof instead of a coincidence.
INDUSTRY = ("tire", "tyre", "wheel", "distributor", "distribution", "wholesale", "aftermarket",
            "truck", "automotive", "auto parts", "suspension", "diesel", "dealer", "fitment",
            "retread", "warehouse", "jobber")

# A domain can resolve and serve a perfectly good page that belongs to nobody: registrar
# parking, a for-sale listing, or a bot wall that hides the real content.
PARKED = ("for sale", "buy this domain", "domain is for sale", "this domain", "spaceship",
          "hover realname", "godaddy", "sedo", "afternic", "dan.com", "namecheap parking",
          "under construction", "coming soon", "default web site page", "parked")
BOT_WALL = ("just a moment", "access denied", "attention required", "checking your browser",
            "enable javascript and cookies", "403 forbidden", "request unsuccessful")

TLDS = [".com", ".net", ".us"]
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/131.0 Safari/537.36")
TITLE_RE = re.compile(r"<title[^>]*>(.*?)</title>", re.I | re.S)
TAG_RE = re.compile(r"<[^>]+>")


def tokens(name):
    n = re.sub(r"\(.*?\)", " ", name.lower())
    n = n.replace("&", " and ").replace("'", "").replace("/", " ")
    n = re.sub(r"[^a-z0-9 ]", " ", n)
    return [t for t in n.split() if t and t not in NOISE]


def candidates(name):
    """Longest, most specific forms first.

    Order matters more than coverage: a bare first word like "capital" or "barrons" belongs to
    some unrelated company on .com, so trying it before "capitaltire" is how a tire distributor
    gets resolved to a trading platform. Short stems stay in the list, but last.
    """
    t = tokens(name)
    if not t:
        return []
    strong = [x for x in t if x not in WEAK_TOKENS] or t
    stems = ["".join(t), "-".join(t)]
    if len(t) > 2:
        stems.append("".join(t[:2]))
    # The distinctive head plus one industry word is the commonest real pattern.
    if strong != t:
        stems.append("".join(strong) + "tire")
        stems.append("".join(strong))
    if len(t) > 1:
        stems.append("".join(t[:1]) + "".join(w for w in t[1:] if w not in WEAK_TOKENS))
    # Bare single word last, and only when the name really is one word.
    if len(strong) == 1:
        stems.append(strong[0])
    out, seen = [], set()
    for s in stems:
        s = re.sub(r"-+", "-", s).strip("-")
        if len(s) < 3:
            continue
        for tld in TLDS:
            d = s + tld
            if d not in seen:
                seen.add(d)
                out.append(d)
    return out[:14]


def resolves(domain):
    try:
        socket.getaddrinfo(domain, None)
        return True
    except OSError:
        return False


def fetch_text(domain):
    for scheme in ("https://", "http://"):
        try:
            r = requests.get(scheme + domain, timeout=8, verify=False, allow_redirects=True,
                             headers={"User-Agent": UA})
            if r.status_code < 500 and r.text:
                return r.text[:60_000], r.url
        except requests.RequestException:
            continue
    return None, None


def label_carries_name(name, domain):
    """Does the domain itself contain any distinctive word of the company name?

    Stripping legal and category words can leave nothing but an industry term: "Wholesale Tire
    Inc" reduces to "tire", which resolved to tire.com -- a generic domain belonging to neither
    that company nor the other one that reduced to it. A domain that carries none of the name's
    distinctive words is not that company's, however well the page scores.
    """
    label = (domain or "").rsplit(".", 1)[0].replace("-", "")
    all_tok = tokens(name)
    strong = [x for x in all_tok if x not in WEAK_TOKENS]
    if strong:
        return any(x in label for x in strong)
    # Nothing distinctive in the name at all -- then a bare industry word is not identification.
    return label not in WEAK_TOKENS


def confirms(name, html, domain):
    """Does the page identify itself as this company? Returns (verdict, title, hits, needed).

    verdict is "confirmed", "parked", "blocked" or "weak".
    """
    t = TITLE_RE.search(html or "")
    title = TAG_RE.sub(" ", t.group(1)) if t else ""
    body = TAG_RE.sub(" ", (html or "")[:20_000])
    hay = " ".join(re.sub(r"[^a-z0-9 ]", " ", f"{title} {body}".lower()).split())

    all_tok = tokens(name)
    strong = [x for x in all_tok if x not in WEAK_TOKENS] or all_tok
    # A token that IS the domain stem proves nothing: every parking page and every unrelated
    # business at barrons.com contains the word "barrons". Only evidence beyond the URL counts.
    stem = domain.rsplit(".", 1)[0].replace("-", "")
    evidence = [tok for tok in strong if tok not in stem]
    hits = sum(1 for tok in evidence if tok in hay)
    industry = any(w in hay for w in INDUSTRY)

    if any(p in hay[:3000] for p in PARKED):
        return "parked", title.strip()[:90], hits, len(evidence)
    if any(b in hay[:3000] for b in BOT_WALL) or len(hay) < 200:
        return "blocked", title.strip()[:90], hits, len(evidence)

    if evidence:
        # Two independent words, or one word plus the right industry.
        ok = hits >= 2 or (hits >= 1 and industry)
    else:
        # The whole name is the domain stem, so the page itself must supply the context, and
        # the other words of the name must appear.
        others = [tok for tok in all_tok if tok not in stem]
        ok = industry and (not others or any(tok in hay for tok in others))
    return ("confirmed" if ok else "weak"), title.strip()[:90], hits, len(evidence)


# Which near-miss to report when nothing confirms. A bot wall usually means the domain is right
# and only unverifiable, so it outranks a page that loaded fine but names someone else.
FALLBACK_RANK = {"blocked": 0, "weak": 1, "parked": 2, "dead": 3}


def work(row):
    name = row["company"]
    best = None
    for d in candidates(name):
        if not resolves(d):
            continue
        html, final = fetch_text(d)
        if html is None:
            cand = (d, "dead", "", 0, 0, "")
        else:
            verdict, title, hits, need = confirms(name, html, d)
            if verdict == "confirmed" and not label_carries_name(name, d):
                verdict = "weak"
            if verdict == "confirmed":
                return {**row, "domain": d, "status": "confirmed", "site_title": title,
                        "match": f"{hits}/{need}", "final_url": final or ""}
            cand = (d, verdict, title, hits, need, final or "")
        if best is None or FALLBACK_RANK[cand[1]] < FALLBACK_RANK[best[1]]:
            best = cand
    if best:
        return {**row, "domain": best[0], "status": best[1], "site_title": best[2],
                "match": f"{best[3]}/{best[4]}", "final_url": best[5]}
    return {**row, "domain": "", "status": "unresolved", "site_title": "", "match": "",
            "final_url": ""}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", default="", help="Comma-separated tiers to resolve")
    ap.add_argument("--workers", type=int, default=12)
    ap.add_argument("--out", default=OUT_CSV)
    args = ap.parse_args()

    rows = list(csv.DictReader(open(IN_CSV)))
    if args.only:
        keep = {t.strip() for t in args.only.split(",") if t.strip()}
        rows = [r for r in rows if r["tier"] in keep]
    print(f"resolving {len(rows)} companies [{args.workers} workers]", flush=True)

    done = []
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        for i, res in enumerate(ex.map(work, rows), 1):
            done.append(res)
            if i % 25 == 0:
                print(f"  [{i}/{len(rows)}]", flush=True)

    # Existing file is merged, so resolving one tier does not discard the others.
    prior = {}
    if os.path.exists(args.out):
        for r in csv.DictReader(open(args.out)):
            prior[r["company"]] = r
    for r in done:
        prior[r["company"]] = r

    cols = ["company", "tier", "also_in_tiers", "domain", "status", "match", "site_title",
            "final_url"]
    with open(args.out, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        for r in sorted(prior.values(), key=lambda x: (x["tier"], x["company"])):
            w.writerow(r)

    import collections
    c = collections.Counter(r["status"] for r in done)
    print(f"\n{args.out}: {len(prior)} companies on file")
    for k in ("confirmed", "blocked", "weak", "parked", "dead", "unresolved"):
        if c[k]:
            print(f"  {k:11} {c[k]}")
    print("  confirmed -> safe for Hunter. blocked -> domain probably right but bot-walled,")
    print("  eyeball it. weak/parked -> the page belongs to someone else; fill in by hand.")


if __name__ == "__main__":
    main()
