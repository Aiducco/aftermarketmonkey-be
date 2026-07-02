"""
Bulk-ingests business leads scraped by the standalone google-maps-scraper tool
(RapidAPI Maps Data API, nationwide zip-by-zip search) into the Lead table.

Expects the scraper's output/ directory to contain the per-query CSVs produced
by that tool (place_id,name,address,phone,website,rating,reviews_count,lat,lng,category).

Usage:
  python manage.py ingest_google_maps_leads --dir ~/Documents/code/google-maps-scraper/output
"""
from decimal import Decimal, InvalidOperation
from pathlib import Path
import csv

import pgbulk
from django.core.management.base import BaseCommand, CommandError
from django.db.models import F
from django.db.models.expressions import RawSQL
from django.db.models.functions import Coalesce

from src.models import Lead

QUERY_FILES = {
    "off-road-outfitter-us.csv": "off-road outfitter",
    "truck-accessories-store-us.csv": "truck accessories store",
    "4x4-custom-shop-us.csv": "4x4 custom shop",
    "lift-kit-installation-us.csv": "lift kit installation",
}

BATCH_SIZE = 2000


def _fill_only(field_name: str) -> pgbulk.UpdateField:
    """Only overwrite this field when the incoming CSV row actually has a value —
    a blank/null from this scrape source must never blank out a value another
    source (e.g. Google Places details) already populated."""
    return pgbulk.UpdateField(
        field_name,
        expression=Coalesce(RawSQL(f'EXCLUDED."{field_name}"', []), F(field_name)),
    )


UPDATE_FIELDS = [
    "name",
    _fill_only("address"),
    _fill_only("phone"),
    _fill_only("website"),
    _fill_only("rating"),
    _fill_only("review_count"),
    _fill_only("latitude"),
    _fill_only("longitude"),
    "category", "search_query", "updated_at",
]
# status / notes / email / AI qualification fields excluded — never overwrite CRM/enrichment data on re-ingest


class Command(BaseCommand):
    help = "Bulk upsert leads from google-maps-scraper CSV output into the Lead table"

    def add_arguments(self, parser):
        parser.add_argument("--dir", required=True, help="Path to the scraper's output/ directory")

    def handle(self, *args, **options):
        out_dir = Path(options["dir"]).expanduser()
        if not out_dir.is_dir():
            raise CommandError(f"Directory not found: {out_dir}")

        total = 0
        for filename, search_query in QUERY_FILES.items():
            path = out_dir / filename
            if not path.exists():
                self.stdout.write(self.style.WARNING(f"Skipping missing file: {path}"))
                continue
            total += self._ingest_file(path, search_query)

        self.stdout.write(self.style.SUCCESS(f"\nDone. Total upserted: {total}"))

    def _ingest_file(self, path: Path, search_query: str) -> int:
        default_category = search_query.title()
        rows = []
        with path.open(newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                place_id = (row.get("place_id") or "").strip()
                name = (row.get("name") or "").strip()
                if not place_id or not name:
                    continue
                website = (row.get("website") or "").strip() or None
                if website and len(website) > 512:
                    website = None
                rows.append(Lead(
                    place_id=place_id,
                    name=name[:512],
                    address=(row.get("address") or "").strip() or None,
                    phone=(row.get("phone") or "").strip()[:64] or None,
                    website=website,
                    rating=self._to_decimal(row.get("rating"), max_digits=3, decimal_places=1),
                    review_count=self._to_int(row.get("reviews_count")),
                    latitude=self._to_decimal(row.get("lat"), max_digits=9, decimal_places=6),
                    longitude=self._to_decimal(row.get("lng"), max_digits=9, decimal_places=6),
                    category=(row.get("category") or "").strip() or default_category,
                    search_query=search_query,
                ))

        self.stdout.write(f"{path.name}: {len(rows)} rows parsed")

        upserted = 0
        for i in range(0, len(rows), BATCH_SIZE):
            batch = rows[i:i + BATCH_SIZE]
            pgbulk.upsert(Lead, batch, unique_fields=["place_id"], update_fields=UPDATE_FIELDS)
            upserted += len(batch)
            self.stdout.write(f"  ...{upserted}/{len(rows)}")

        self.stdout.write(self.style.SUCCESS(f"{path.name}: upserted {upserted} leads (query='{search_query}')"))
        return upserted

    @staticmethod
    def _to_decimal(value, max_digits, decimal_places):
        if value is None or value == "":
            return None
        try:
            d = Decimal(value).quantize(Decimal(1).scaleb(-decimal_places))
        except (InvalidOperation, ValueError):
            return None
        if len(d.as_tuple().digits) > max_digits:
            return None
        return d

    @staticmethod
    def _to_int(value):
        if value is None or value == "":
            return None
        try:
            return int(float(value))
        except ValueError:
            return None
