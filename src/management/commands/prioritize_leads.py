"""
Scores qualified leads for outreach order — who to call first, not just who is worth calling.

"Qualified" is binary and says nothing about whether a shop is a two-bay operation or a regional
chain with an e-commerce site. This produces a 0-100 `outreach_priority` from two kinds of signal:

  LLM read of the website (60% of the score)
    A dealer that runs online ordering, publishes a team page, offers financing, lists service
    bays and names its brand partnerships is visibly a bigger, more professional operation than
    one with a single-page site and a phone number. The model reports these as discrete booleans
    plus a 0-100 website_quality, rather than being asked for a vague "how good is this" number —
    discrete observations are far more consistent across leads than holistic judgements.

  Hard signals already in the table (40%)
    location_count   how many qualified rows share the domain — a real multi-site operator
    is_preferred     RealTruck's own preferred-dealer flag
    is_double_warranty / is_next_gen   further RealTruck tiering
    brand_count      how many brands the dealer is authorised for
    confidence_score how sure the qualifier was
    verified email   a contactable lead outranks one you cannot reach

Tiers are just buckets of the composite: A >= 70, B >= 45, C below.

Usage:
  python manage.py prioritize_leads --source realtruck                 # all qualified, unscored
  python manage.py prioritize_leads --source realtruck --rescore       # redo scored ones too
  python manage.py prioritize_leads --source realtruck --limit 50      # sample first
  python manage.py prioritize_leads --source realtruck --pack 5        # businesses per request
"""
import json
import re
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from urllib.parse import urlparse

from django.core.management.base import BaseCommand

from src.integrations.llm import azure_llm
from src.management.commands.qualify_leads import _scrape
from src.models import Lead, RealTruckLead

SOURCES = {"google": Lead, "realtruck": RealTruckLead}
BATCH_SIZE = 50

SYSTEM_PROMPT = """You assess how established and professional an automotive shop is, from the text of its website.

You are NOT deciding whether they are a good prospect — that is already settled. You are judging
how ADVANCED the business appears, so a sales team knows who to approach first. A larger, more
professional operation is a higher-value first contact.

For each business, report what you can actually observe in the text. Do not speculate: if the text
does not show something, mark it false rather than guessing.

Signals of an advanced operation:
- Online store / e-commerce / "add to cart" / online ordering
- Online appointment or quote booking
- Financing or payment plans offered
- A team, staff or "meet the crew" page, or named technicians
- Multiple service bays, a large facility, or several locations
- Named brand partnerships or authorised-dealer status
- Certifications, awards, "since 19xx", years in business
- Fleet or commercial accounts

website_quality is 0-100 for how substantial and professionally run the site suggests the business
is: 0-30 a bare single page or placeholder, 40-60 a normal small shop site with services listed,
70-85 a well-built site with rich content, 90-100 a serious e-commerce or multi-location operation.

Respond ONLY with a JSON object of this shape:
{
  "results": [{
    "id": "<the id you were given>",
    "website_quality": 0-100,
    "has_ecommerce": bool,
    "has_online_booking": bool,
    "has_financing": bool,
    "has_team_page": bool,
    "mentions_multiple_bays_or_locations": bool,
    "named_brand_partnerships": integer,
    "years_established": integer or null,
    "serves_fleet_or_commercial": bool,
    "summary": "one sentence on how advanced this operation appears"
  }]
}"""


def _domain(url: str) -> str:
    u = (url or "").strip().lower()
    u = re.sub(r"^https?://", "", u).split("/")[0].split(":")[0]
    return re.sub(r"^www\.", "", u)


def _composite(lead, llm: dict, location_count: int) -> tuple[int, dict]:
    """
    Blend the LLM's read with hard signals. Weights are deliberately blunt: the aim is a sane
    ordering for a sales queue, not a calibrated probability.
    """
    wq = int(llm.get("website_quality") or 0)

    # Website sophistication, 0-60.
    feature_hits = sum(bool(llm.get(k)) for k in (
        "has_ecommerce", "has_online_booking", "has_financing",
        "has_team_page", "mentions_multiple_bays_or_locations", "serves_fleet_or_commercial"))
    web = wq * 0.45 + min(feature_hits, 6) * 2.5          # 0-45 + 0-15

    # Hard signals, 0-40.
    hard = 0.0
    hard += min(location_count - 1, 5) * 3                # up to 15 for a multi-site operator
    hard += 8 if getattr(lead, "is_preferred", False) else 0
    hard += 4 if getattr(lead, "is_double_warranty", False) else 0
    hard += 4 if getattr(lead, "is_next_gen", False) else 0
    hard += min((getattr(lead, "brand_count", 0) or 0) / 25.0, 1) * 5
    hard += min(int(llm.get("named_brand_partnerships") or 0) / 10.0, 1) * 4

    score = max(0, min(100, round(web + hard)))
    signals = {
        "website_quality": wq,
        "feature_hits": feature_hits,
        "location_count": location_count,
        "is_preferred": bool(getattr(lead, "is_preferred", False)),
        "is_double_warranty": bool(getattr(lead, "is_double_warranty", False)),
        "is_next_gen": bool(getattr(lead, "is_next_gen", False)),
        "brand_count": getattr(lead, "brand_count", None),
        "named_brand_partnerships": llm.get("named_brand_partnerships"),
        "years_established": llm.get("years_established"),
        "has_ecommerce": bool(llm.get("has_ecommerce")),
        "has_online_booking": bool(llm.get("has_online_booking")),
        "has_financing": bool(llm.get("has_financing")),
        "has_team_page": bool(llm.get("has_team_page")),
        "multi_bay_or_location": bool(llm.get("mentions_multiple_bays_or_locations")),
        "fleet_commercial": bool(llm.get("serves_fleet_or_commercial")),
    }
    return score, signals


def _tier(score: int) -> str:
    return "A" if score >= 70 else ("B" if score >= 45 else "C")


def _assess_pack(client, items, model=None) -> dict:
    """items: [(pk, website, text)]. Returns {pk: llm_dict}."""
    blocks = "\n\n".join(f"--- BUSINESS id={pk} url={site} ---\n{text}" for pk, site, text in items)
    parsed, _err = azure_llm.complete_json(
        client, SYSTEM_PROMPT,
        f"Assess each business below and return one result per id.\n\n{blocks}",
        max_tokens=min(900 * len(items), 8192), model=model)
    out = {}
    if parsed:
        for entry in parsed.get("results") or []:
            rid = str(entry.get("id", ""))
            for pk, _s, _t in items:
                if str(pk) == rid:
                    out[pk] = entry
    return out


def _process_pack(leads, client, loc_counts, model=None, pack_size: int = 5) -> list:
    scraped, out = [], []
    for lead in leads:
        site = (lead.website or "").strip()
        if "://" not in site:
            site = f"http://{site}"
        text = _scrape(site)
        if text:
            scraped.append((lead, site, text))
        else:
            out.append((lead, None))
    if not scraped:
        return out

    got = _assess_pack(client, [(l.pk, s, t) for l, s, t in scraped], model) if pack_size > 1 else {}
    for lead, site, text in scraped:
        llm = got.get(lead.pk)
        if llm is None:
            llm = _assess_pack(client, [(lead.pk, site, text)], model).get(lead.pk)
        if llm is None:
            out.append((lead, None))
            continue
        score, signals = _composite(lead, llm, loc_counts.get(_domain(lead.website), 1))
        out.append((lead, {
            "outreach_priority": score,
            "priority_tier": _tier(score),
            "website_quality": int(llm.get("website_quality") or 0),
            "location_count": loc_counts.get(_domain(lead.website), 1),
            "priority_signals": signals,
            "priority_reasoning": llm.get("summary"),
            "prioritized_at": datetime.now(timezone.utc),
        }))
    return out


class Command(BaseCommand):
    help = "Score qualified leads 0-100 for outreach order"

    def add_arguments(self, parser):
        parser.add_argument("--source", default="realtruck", choices=sorted(SOURCES))
        parser.add_argument("--rescore", action="store_true", help="Redo leads already scored")
        parser.add_argument("--limit", type=int, default=None)
        parser.add_argument("--pack", type=int, default=5)
        parser.add_argument("--workers", type=int, default=4)
        parser.add_argument("--model", default=None)

    def handle(self, *args, **options):
        model = SOURCES[options["source"]]
        qs = model.objects.filter(is_qualified=True).exclude(website__isnull=True).exclude(website="")
        if not options["rescore"]:
            qs = qs.filter(outreach_priority__isnull=True)
        if options["limit"]:
            qs = qs[:options["limit"]]
        leads = list(qs)
        if not leads:
            self.stdout.write("Nothing to score.")
            return

        # How many qualified rows share each domain — the multi-location signal.
        loc_counts = Counter(
            _domain(w) for w in model.objects.filter(is_qualified=True)
            .exclude(website__isnull=True).exclude(website="").values_list("website", flat=True)
            if _domain(w)
        )

        client = azure_llm.client(max_retries=8)
        pack = max(1, options["pack"])
        packs = [leads[i:i + pack] for i in range(0, len(leads), pack)]
        self.stdout.write(f"Scoring {len(leads)} {options['source']} leads "
                          f"[{options['workers']} workers, pack {pack}, "
                          f"{options['model'] or azure_llm.deployment()}]\n")

        tiers, done, failed, pending = Counter(), 0, 0, []
        with ThreadPoolExecutor(max_workers=options["workers"]) as ex:
            futures = [ex.submit(_process_pack, p, client, loc_counts, options["model"], pack)
                       for p in packs]
            for fut in as_completed(futures):
                try:
                    results = fut.result()
                except Exception as e:
                    self.stdout.write(self.style.WARNING(f"  pack error: {e}"))
                    failed += pack
                    continue
                for lead, upd in results:
                    done += 1
                    if not upd:
                        failed += 1
                        continue
                    tiers[upd["priority_tier"]] += 1
                    pending.append((lead.pk, upd))
                    self.stdout.write(
                        f"  [{done}] {upd['priority_tier']} {upd['outreach_priority']:>3}  "
                        f"web={upd['website_quality']:>3} loc={upd['location_count']}  "
                        f"{lead.name[:38]}")
                    if len(pending) >= BATCH_SIZE:
                        self._flush(model, pending); pending.clear()
        if pending:
            self._flush(model, pending)

        self.stdout.write(self.style.SUCCESS(
            f"\nDone. A: {tiers['A']}  B: {tiers['B']}  C: {tiers['C']}  failed: {failed}"))

    def _flush(self, model, pending):
        for pk, upd in pending:
            model.objects.filter(pk=pk).update(**upd)
