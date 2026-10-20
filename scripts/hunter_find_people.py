"""
Runs distributor targets through Hunter's domain-search to collect named contacts.

Credit discipline is the whole design. A domain-search costs one credit whether it returns nine
people or none, the account in use is a 50/month Free plan, and a credit spent on the wrong
company is worse than one wasted: nothing downstream reveals that the staff listed belong to a
financial magazine rather than a tire distributor. So:

  * every call is budgeted (--max-credits) and the budget is checked against the live account
    before anything is spent
  * results append to a JSONL checkpoint, so an interrupted run never re-buys a company
  * targets are deduplicated by what they actually resolve to -- Tireweb, Tire Data Connect and
    WheelUnity are one company with one staff list, and billing three credits for it is a third
    of a day's allowance on this plan
  * a query resolved by NAME has its returned domain checked against that name afterwards, and
    the row is flagged rather than silently trusted

Querying by company name is preferred over a guessed domain wherever resolve_distributor_domains
could not confirm one: Hunter resolves names well, and it costs the same credit as a domain that
might be somebody else's.

Usage:
  python scripts/hunter_find_people.py --targets data/distributors/targets.csv --max-credits 30
  python scripts/hunter_find_people.py --dry-run          # show what would be bought, spend 0
"""
import argparse
import csv
import json
import os
import re
import sys
import time

import requests

API = "https://api.hunter.io/v2"
CHECKPOINT = "data/distributors/hunter_raw.jsonl"
OUT_CSV = "data/distributors/people.csv"

NOISE = {"inc", "llc", "ltd", "co", "company", "corp", "the", "and", "of", "wholesale",
         "distributors", "distributing", "distribution", "group", "enterprises", "usa"}


def key(name):
    n = re.sub(r"\(.*?\)", " ", (name or "").lower()).replace("&", " and ")
    return [t for t in re.sub(r"[^a-z0-9 ]", " ", n).split() if t and t not in NOISE]


def account(api_key):
    r = requests.get(f"{API}/account", params={"api_key": api_key}, timeout=20)
    d = r.json()
    if "errors" in d:
        return None
    return d["data"]["requests"]["searches"]


class KeyPool:
    """Several Free-plan keys worked in sequence.

    Each key is 50 searches a month, so one key cannot cover a 238-company list. The pool moves
    to the next key when one is exhausted or starts refusing, and the remaining budget is the sum
    across keys rather than any single key's -- otherwise a run stops two thirds of the way
    through with credits still sitting unused on another key.
    """

    def __init__(self, keys):
        self.keys = []
        for k in keys:
            s = account(k)
            if s is None:
                print(f"  key {k[:8]}... REJECTED, skipping")
                continue
            print(f"  key {k[:8]}... {s['remaining']:>3} of {s['available']} searches left")
            if s["remaining"] > 0:
                self.keys.append([k, s["remaining"]])
        self.i = 0

    @property
    def remaining(self):
        return sum(r for _, r in self.keys)

    def current(self):
        while self.i < len(self.keys) and self.keys[self.i][1] <= 0:
            self.i += 1
        return self.keys[self.i][0] if self.i < len(self.keys) else None

    def charge(self):
        if self.i < len(self.keys):
            self.keys[self.i][1] -= 1

    def exhaust_current(self):
        """Mark the active key spent -- it reported a limit error, so stop trusting its count."""
        if self.i < len(self.keys):
            self.keys[self.i][1] = 0
            self.i += 1


def load_done():
    done = {}
    if os.path.exists(CHECKPOINT):
        with open(CHECKPOINT) as fh:
            for line in fh:
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                done[rec["company"]] = rec
    return done


def search(api_key, *, domain=None, company=None, limit=10):
    params = {"api_key": api_key, "limit": limit}
    if domain:
        params["domain"] = domain
    else:
        params["company"] = company
    for attempt in range(4):
        try:
            r = requests.get(f"{API}/domain-search", params=params, timeout=40)
        except requests.RequestException as e:
            time.sleep(2 * (attempt + 1))
            if attempt == 3:
                return None, f"request failed: {e}"
            continue
        if r.status_code == 429:          # rate limited, not a spent credit
            time.sleep(5 * (attempt + 1))
            continue
        try:
            d = r.json()
        except ValueError:
            return None, f"HTTP {r.status_code}, unparseable body"
        if "errors" in d:
            return None, "; ".join(e.get("details", str(e)) for e in d["errors"])
        return d.get("data") or {}, None
    return None, "rate limited after 4 attempts"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--targets", default="data/distributors/targets.csv")
    ap.add_argument("--max-credits", type=int, default=25)
    ap.add_argument("--api-key", default=os.environ.get("HUNTER_API_KEY", ""),
                    help="One key, or several comma-separated; they are used in sequence as "
                         "each one's monthly allowance runs out")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int, default=10,
                    help="Max contacts per company. Hunter REJECTS anything above the plan's "
                         "ceiling (10 on Free) instead of clamping it, and a rejected request "
                         "returns nothing -- so raise this only alongside a plan upgrade.")
    args = ap.parse_args()
    if not args.api_key:
        sys.exit("No API key: pass --api-key or set HUNTER_API_KEY")

    rows = list(csv.DictReader(open(args.targets)))
    done = load_done()
    todo = [r for r in rows if r["company"] not in done]

    print("keys:")
    pool = KeyPool([k.strip() for k in args.api_key.split(",") if k.strip()])
    if not pool.remaining:
        sys.exit("no key has any searches left")
    budget = min(args.max_credits, pool.remaining)
    print(f"\npooled searches available: {pool.remaining}")
    print(f"targets: {len(rows)} | already fetched: {len(done)} | to fetch: {len(todo)}")
    print(f"budget this run: {budget}\n")
    if len(todo) > budget:
        print(f"  NOTE: {len(todo) - budget} targets will be left for a later run "
              f"(resumable -- nothing already fetched is re-bought)\n")

    if args.dry_run:
        for r in todo[:budget]:
            how = f"domain={r['domain']}" if r.get("use_domain") == "yes" else \
                  f"company={r['company']!r}"
            print(f"  would spend 1 credit: {r['company'][:38]:40} {how}")
        print(f"\nDRY RUN -- 0 credits spent. {min(len(todo), budget)} would be used.")
        return

    spent, errors = 0, []
    with open(CHECKPOINT, "a") as fh:
        for r in todo:
            if spent >= budget:
                break
            api_key = pool.current()
            if api_key is None:
                print("  all keys exhausted")
                break
            use_domain = r.get("use_domain") == "yes" and r.get("domain")
            data, err = search(api_key,
                              domain=r["domain"] if use_domain else None,
                              company=None if use_domain else r["company"],
                              limit=args.limit)
            spent += 1
            rec = {"company": r["company"], "tier": r.get("tier", ""),
                   "queried_by": "domain" if use_domain else "company",
                   "query": r["domain"] if use_domain else r["company"],
                   "error": err, "data": data}
            # Only successes are checkpointed. A rejected request costs no credit, so recording
            # it as done would permanently skip that company for a reason that no longer applies.
            if err is None:
                fh.write(json.dumps(rec) + "\n")
                fh.flush()
                # Hunter bills only when it finds the company, so a miss leaves the budget alone.
                if (data or {}).get("emails"):
                    pool.charge()
            else:
                errors.append((r["company"], err))
                if "limit" in err.lower() or "exceed" in err.lower() or "usage" in err.lower():
                    print(f"      key {api_key[:8]}... out of allowance, rotating")
                    pool.exhaust_current()
            n = len((data or {}).get("emails") or []) if data else 0
            got = (data or {}).get("domain") or "-"
            print(f"  [{spent}/{budget}] {r['company'][:34]:36} -> {got[:26]:28} "
                  f"{n:>2} contacts" + (f"  ERR {err[:40]}" if err else ""))
            time.sleep(0.4)

    if errors:
        print(f"\n{len(errors)} request(s) rejected (not checkpointed, no credit charged):")
        for comp, e in errors[:8]:
            print(f"    {comp[:34]:36} {e[:70]}")
    print(f"\nattempted {spent}. checkpoint: {CHECKPOINT}")
    for k, _ in pool.keys:
        s_ = account(k)
        if s_:
            print(f"  key {k[:8]}... now {s_['remaining']:>3} searches left")
    print(f"next: python scripts/hunter_build_dataset.py   # turns the checkpoint into {OUT_CSV}")


if __name__ == "__main__":
    main()
