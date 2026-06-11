"""
Searches Google Places API for off-road / 4x4 shops and stores leads.

Search mode:
  --by-city  (default) — one search per major city e.g. "4x4 custom shop near Houston, TX"
  --by-zip             — one search per ZIP code  e.g. "4x4 custom shop near 77001"

Required env var: GOOGLE_PLACES_API_KEY  (set in .env)

Usage:
  python manage.py find_4x4_leads --state TX
  python manage.py find_4x4_leads --state CA --by-zip
  python manage.py find_4x4_leads --state TX --skip 10
"""
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pgbulk
import requests
from django.conf import settings
from django.core.management.base import BaseCommand

from src.models import Lead, USZipCode

PLACES_TEXT_SEARCH_URL = "https://maps.googleapis.com/maps/api/place/textsearch/json"
PLACES_DETAILS_URL = "https://maps.googleapis.com/maps/api/place/details/json"

SEARCH_QUERIES = [
    # "off-road outfitter near {location}",
    "4x4 custom shop near {location}",
    # "truck accessories store near {location}",
    # "podiatrist near {location}",
]

CATEGORY = "Dedicated Off-Road & 4x4 Outfitters"
# CATEGORY = "Podiatrist"


EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")
EMAIL_IGNORE_DOMAINS = {"example.com", "sentry.io", "wix.com", "squarespace.com", "wordpress.com"}

DETAILS_FIELDS = (
    "name,formatted_phone_number,website,url,rating,"
    "user_ratings_total,formatted_address,geometry,business_status,place_id"
)

WORKERS = 5
API_DELAY = 0.2
MIN_REVIEWS_FOR_DETAILS = 50

LEAD_UPDATE_FIELDS = [
    "name", "address", "city", "state", "zip_code",
    "latitude", "longitude", "phone", "website", "email",
    "rating", "review_count", "google_maps_url", "business_status",
    "search_query", "source_zip", "category", "updated_at",
]
# status / notes excluded — don't overwrite CRM data on re-runs


class Command(BaseCommand):
    help = "Find 4x4/off-road shops via Google Places API and store as leads"

    def add_arguments(self, parser):
        parser.add_argument("--state", default="TX", help="Two-letter state code (default: TX)")
        parser.add_argument("--by-zip", action="store_true", help="Search by ZIP code instead of city name")
        parser.add_argument("--by-county", action="store_true", help="Search using only the first ZIP code per county")
        parser.add_argument("--skip", type=int, default=0, help="Skip first N entries (for resuming)")

    def handle(self, *args, **options):
        api_key = getattr(settings, "GOOGLE_PLACES_API_KEY", "")
        if not api_key:
            self.stdout.write(self.style.ERROR("GOOGLE_PLACES_API_KEY is not set in .env / settings"))
            return

        state = options["state"].upper()
        by_zip = options["by_zip"]
        by_county = options["by_county"]

        if by_county:
            # One ZIP per county — pick the lowest ZIP code in each county
            from django.db.models import Min
            county_zips = (
                USZipCode.objects.filter(state=state, county__isnull=False)
                .exclude(county="")
                .values("county")
                .annotate(first_zip=Min("zip_code"))
                .order_by("county")
            )
            targets = [row["first_zip"] for row in county_zips]
            label = "counties (1 ZIP each)"
        elif by_zip:
            targets = list(
                USZipCode.objects.filter(state=state, is_major_city=True)
                .order_by("city", "zip_code")
                .values_list("zip_code", flat=True)
            )
            label = "ZIP codes"
        else:
            targets = list(
                USZipCode.objects.filter(state=state, is_major_city=True)
                .order_by("city")
                .values_list("city", flat=True)
                .distinct()
            )
            # Format as "City, ST" for the search query
            targets = [f"{city}, {state}" for city in targets]
            label = "cities"

        if not targets:
            self.stdout.write(self.style.ERROR(f"No data found for {state}. Run: python manage.py seed_zipcodes --state {state}"))
            return

        if options["skip"]:
            if options["skip"] >= len(targets):
                self.stdout.write(self.style.ERROR(f"--skip {options['skip']} is >= total {label} ({len(targets)}). Nothing to process."))
                return
            targets = targets[options["skip"]:]
            self.stdout.write(f"Skipping first {options['skip']} {label}, resuming from: {targets[0]}...")

        mode = "county" if by_county else ("ZIP" if by_zip else "city")
        self.stdout.write(f"Processing {len(targets)} {label} [{mode} mode] with {WORKERS} workers...")

        total_upserted = 0

        with ThreadPoolExecutor(max_workers=WORKERS) as executor:
            futures = {
                executor.submit(self._process, api_key, target, idx, len(targets), state): target
                for idx, target in enumerate(targets, 1)
            }
            for future in as_completed(futures):
                target = futures[future]
                try:
                    total_upserted += future.result()
                except Exception as e:
                    self.stdout.write(self.style.ERROR(f"  Error processing {target}: {e}"))

        self.stdout.write(self.style.SUCCESS(f"\nDone. Total upserted: {total_upserted}"))

    # ------------------------------------------------------------------
    # Worker (handles one city or ZIP)
    # ------------------------------------------------------------------

    def _process(self, api_key, location, idx, total, state) -> int:
        self.stdout.write(f"[{idx}/{total}] {location}")

        # Collect results across all search queries, dedupe by place_id
        places_by_id: dict[str, dict] = {}
        for query_template in SEARCH_QUERIES:
            query = query_template.format(location=location)
            results = self._text_search(api_key, query)
            for place in results:
                pid = place.get("place_id")
                if pid and pid not in places_by_id:
                    places_by_id[pid] = place
            self.stdout.write(f"  [{location}] '{query_template.split(' near')[0]}' -> {len(results)} results")

        if not places_by_id:
            return 0

        all_place_ids = list(places_by_id.keys())
        self.stdout.write(f"  [{location}] {len(all_place_ids)} unique results")

        # Only process place_ids not already in DB
        existing_ids = set(
            Lead.objects.filter(place_id__in=all_place_ids).values_list("place_id", flat=True)
        )
        new_places = [p for p in places_by_id.values() if p["place_id"] not in existing_ids]

        if not new_places:
            self.stdout.write(f"  [{location}] all already in DB, skipping")
            return 0

        rich = sum(1 for p in new_places if (p.get("user_ratings_total") or 0) >= MIN_REVIEWS_FOR_DETAILS)
        self.stdout.write(f"  [{location}] {len(new_places)} new ({rich} with {MIN_REVIEWS_FOR_DETAILS}+ reviews, {len(new_places)-rich} basic only)")

        leads = []
        for place in new_places:
            place_id = place["place_id"]
            review_count = place.get("user_ratings_total") or 0

            if review_count >= MIN_REVIEWS_FOR_DETAILS:
                details = self._get_place_details(api_key, place_id) or place
                email = None
                website = details.get("website")
                if website:
                    email = self._scrape_email(website)
            else:
                details = place
                email = None

            lead_data = self._build_lead_data(details, place_id, location, email)
            if not lead_data.get("state"):
                continue
            leads.append(Lead(**lead_data))

        if not leads:
            return 0

        pgbulk.upsert(Lead, leads, unique_fields=["place_id"], update_fields=LEAD_UPDATE_FIELDS)
        self.stdout.write(self.style.SUCCESS(f"  [{location}] -> Upserted {len(leads)} leads"))
        return len(leads)

    # ------------------------------------------------------------------
    # API / scraping helpers
    # ------------------------------------------------------------------

    def _text_search(self, api_key, query):
        params = {"query": query, "key": api_key, "type": "establishment"}
        self.stdout.write(f"    -> GET {PLACES_TEXT_SEARCH_URL} query='{query}'")
        try:
            resp = requests.get(PLACES_TEXT_SEARCH_URL, params=params, timeout=15)
            self.stdout.write(f"    <- HTTP {resp.status_code}")
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as e:
            self.stdout.write(self.style.ERROR(f"    !! Request failed: {e}"))
            return []

        status = data.get("status")
        error_message = data.get("error_message", "")
        result_count = len(data.get("results", []))

        if status == "OK":
            self.stdout.write(f"    <- status=OK  results={result_count}")
        elif status == "ZERO_RESULTS":
            self.stdout.write(f"    <- status=ZERO_RESULTS")
        else:
            self.stdout.write(self.style.ERROR(
                f"    !! Places API status={status}"
                + (f"  error_message='{error_message}'" if error_message else "")
            ))
            return []

        time.sleep(API_DELAY)
        return data.get("results", [])

    def _get_place_details(self, api_key, place_id):
        params = {"place_id": place_id, "fields": DETAILS_FIELDS, "key": api_key}
        try:
            resp = requests.get(PLACES_DETAILS_URL, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as e:
            self.stdout.write(self.style.ERROR(f"    !! Details request failed [{place_id}]: {e}"))
            return None

        status = data.get("status")
        error_message = data.get("error_message", "")
        time.sleep(API_DELAY)

        if status != "OK":
            self.stdout.write(self.style.WARNING(
                f"    !! Details status={status} [{place_id}]"
                + (f"  error_message='{error_message}'" if error_message else "")
            ))
            return None

        result = data.get("result", {})
        self.stdout.write(
            f"    <- details OK  name='{result.get('name', '')}'"
            f"  phone='{result.get('formatted_phone_number', '-')}'"
            f"  website='{result.get('website', '-')}'"
        )
        return result

    def _scrape_email(self, website_url):
        try:
            headers = {"User-Agent": "Mozilla/5.0 (compatible; LeadBot/1.0)"}
            resp = requests.get(website_url, timeout=8, headers=headers, allow_redirects=True)
            if resp.status_code == 200:
                email = self._extract_email(resp.text)
                if email:
                    return email

            if not website_url.rstrip("/").endswith("/contact"):
                contact_url = website_url.rstrip("/") + "/contact"
                resp2 = requests.get(contact_url, timeout=8, headers=headers, allow_redirects=True)
                if resp2.status_code == 200:
                    return self._extract_email(resp2.text)
        except Exception:
            pass
        return None

    def _extract_email(self, html):
        for email in EMAIL_RE.findall(html):
            domain = email.split("@")[-1].lower()
            if domain in EMAIL_IGNORE_DOMAINS:
                continue
            if any(email.endswith(ext) for ext in (".png", ".jpg", ".gif", ".svg", ".webp")):
                continue
            return email.lower()
        return None

    def _build_lead_data(self, details, place_id, source_location, email):
        address = details.get("formatted_address", "")
        geometry = details.get("geometry", {}).get("location", {})
        city, state, zip_from_addr = self._parse_address(address)

        return {
            "place_id": place_id,
            "name": details.get("name", ""),
            "address": address or None,
            "city": city,
            "state": state,
            "zip_code": zip_from_addr,
            "latitude": geometry.get("lat"),
            "longitude": geometry.get("lng"),
            "phone": details.get("formatted_phone_number"),
            "website": details.get("website"),
            "email": email,
            "rating": details.get("rating"),
            "review_count": details.get("user_ratings_total"),
            "google_maps_url": details.get("url"),
            "business_status": details.get("business_status"),
            "search_query": source_location,
            "source_zip": source_location if source_location.isdigit() else None,
            "category": CATEGORY,
        }

    @staticmethod
    def _parse_address(address):
        city = state = zip_code = None
        if not address:
            return city, state, zip_code

        address = re.sub(r",?\s*USA\s*$", "", address.strip())

        m = re.search(r",\s*([^,]+),\s*([A-Z]{2})\s+(\d{5}(?:-\d{4})?)\s*$", address)
        if m:
            return m.group(1).strip(), m.group(2).strip(), m.group(3).strip()

        m2 = re.search(r",\s*([^,]+),\s*([A-Z]{2})\s*$", address)
        if m2:
            return m2.group(1).strip(), m2.group(2).strip(), None

        return city, state, zip_code
