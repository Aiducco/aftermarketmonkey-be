"""
Fetches ZIP codes for a given US state from the free GeoNames postal code dataset
and seeds the us_zip_code table.

Source: https://download.geonames.org/export/zip/US.zip  (public domain)
TSV columns: country_code, postal_code, place_name, admin_name1, admin_code1,
             admin_name2, admin_code2, admin_name3, admin_code3, latitude, longitude, accuracy

Usage:
  python manage.py seed_zipcodes             # defaults to TX
  python manage.py seed_zipcodes --state TX
  python manage.py seed_zipcodes --state CA
  python manage.py seed_zipcodes --all-states
  python manage.py seed_zipcodes --state TX --clear
"""
import io
import zipfile

import requests
from django.core.management.base import BaseCommand

from src.models import USZipCode

GEONAMES_URL = "https://download.geonames.org/export/zip/US.zip"

MAJOR_CITIES_BY_STATE = {
    "TX": {
        "houston", "dallas", "san antonio", "austin", "fort worth",
        "el paso", "arlington", "corpus christi", "plano", "laredo",
        "lubbock", "garland", "irving", "amarillo", "grand prairie",
        "mckinney", "frisco", "pasadena", "mesquite", "killeen",
        "mcallen", "waco", "carrollton", "beaumont", "abilene",
        "denton", "odessa", "midland", "round rock", "richardson",
    },
    "CA": {
        "los angeles", "san diego", "san jose", "san francisco", "fresno",
        "sacramento", "long beach", "oakland", "bakersfield", "anaheim",
        "riverside", "stockton", "irvine", "chula vista", "fremont",
        "modesto", "fontana", "moreno valley", "glendale", "huntington beach",
        "santa ana", "santa clarita", "garden grove", "oceanside", "rancho cucamonga",
    },
    "FL": {
        "jacksonville", "miami", "tampa", "orlando", "st. petersburg",
        "hialeah", "tallahassee", "fort lauderdale", "port st. lucie", "cape coral",
        "pembroke pines", "hollywood", "miramar", "gainesville", "coral springs",
        "clearwater", "palm bay", "pompano beach", "west palm beach", "lakeland",
    },
    "AZ": {
        "phoenix", "tucson", "mesa", "chandler", "scottsdale",
        "glendale", "gilbert", "tempe", "peoria", "surprise",
        "yuma", "avondale", "flagstaff", "goodyear", "lake havasu city",
        "buckeye", "casa grande", "sierra vista", "maricopa", "prescott",
    },
    "CO": {
        "denver", "colorado springs", "aurora", "fort collins", "lakewood",
        "thornton", "arvada", "westminster", "pueblo", "centennial",
        "boulder", "highlands ranch", "greeley", "longmont", "loveland",
        "broomfield", "castle rock", "commerce city", "parker", "northglenn",
    },
    "GA": {
        "atlanta", "columbus", "savannah", "athens", "sandy springs",
        "roswell", "macon", "johns creek", "albany", "warner robins",
        "alpharetta", "marietta", "augusta", "smyrna", "valdosta",
        "peachtree city", "gainesville", "dalton", "rome", "canton",
    },
    "TN": {
        "nashville", "memphis", "knoxville", "chattanooga", "clarksville",
        "murfreesboro", "franklin", "jackson", "johnson city", "bartlett",
        "hendersonville", "kingsport", "collierville", "smyrna", "cleveland",
        "brentwood", "germantown", "morristown", "la vergne", "cookeville",
    },
    "NC": {
        "charlotte", "raleigh", "greensboro", "durham", "winston-salem",
        "fayetteville", "cary", "wilmington", "high point", "concord",
        "asheville", "gastonia", "jacksonville", "chapel hill", "rocky mount",
        "huntersville", "apex", "burlington", "kannapolis", "wilson",
    },
    "AL": {
        "birmingham", "huntsville", "montgomery", "mobile", "tuscaloosa",
        "hoover", "dothan", "auburn", "decatur", "madison",
        "florence", "gadsden", "vestavia hills", "prattville", "phenix city",
        "alabaster", "bessemer", "enterprise", "homewood", "northport",
    },
    "OK": {
        "oklahoma city", "tulsa", "norman", "broken arrow", "lawton",
        "edmond", "moore", "midwest city", "enid", "stillwater",
        "muskogee", "owasso", "bartlesville", "shawnee", "yukon",
        "ardmore", "ponca city", "duncan", "sapulpa", "del city",
    },
    "UT": {
        "salt lake city", "west valley city", "provo", "west jordan", "orem",
        "sandy", "ogden", "st. george", "layton", "south jordan",
        "lehi", "millcreek", "taylorsville", "logan", "murray",
        "draper", "bountiful", "riverton", "herriman", "eagle mountain",
    },
    "NV": {
        "las vegas", "henderson", "reno", "north las vegas", "sparks",
        "carson city", "fernley", "elko", "mesquite", "boulder city",
        "sunrise manor", "spring valley", "enterprise", "paradise", "whitney",
        "summerlin south", "winchester", "centennial hills", "henderson", "laughlin",
    },
    "MT": {
        "billings", "missoula", "great falls", "bozeman", "butte",
        "helena", "kalispell", "havre", "anaconda", "miles city",
        "belgrade", "livingston", "laurel", "whitefish", "lewistown",
        "glendive", "sidney", "polson", "hamilton", "columbia falls",
    },
    "ID": {
        "boise", "meridian", "nampa", "idaho falls", "pocatello",
        "caldwell", "coeur d'alene", "twin falls", "lewiston", "post falls",
        "rexburg", "moscow", "eagle", "kuna", "ammon",
        "chubbuck", "garden city", "hayden", "blackfoot", "jerome",
    },
    "MI": {
        "detroit", "grand rapids", "warren", "sterling heights", "ann arbor",
        "lansing", "flint", "dearborn", "livonia", "troy",
        "westland", "clinton township", "canton", "pontiac", "southfield",
        "dearborn heights", "kalamazoo", "muskegon", "battle creek", "saginaw",
    },
    "OH": {
        "columbus", "cleveland", "cincinnati", "toledo", "akron",
        "dayton", "parma", "canton", "youngstown", "lorain",
        "hamilton", "springfield", "kettering", "elyria", "newark",
        "lakewood", "cuyahoga falls", "mentor", "euclid", "middletown",
    },
}


class Command(BaseCommand):
    help = "Download GeoNames data and seed US ZIP codes for a given state"

    def add_arguments(self, parser):
        parser.add_argument(
            "--state",
            default="TX",
            help="Two-letter state code to seed (default: TX)",
        )
        parser.add_argument(
            "--all-states",
            action="store_true",
            help="Seed all US states",
        )
        parser.add_argument(
            "--clear",
            action="store_true",
            help="Delete existing ZIP codes for the target state(s) before re-seeding",
        )

    def handle(self, *args, **options):
        self.stdout.write("Downloading GeoNames US postal code dataset...")
        try:
            resp = requests.get(GEONAMES_URL, timeout=60)
            resp.raise_for_status()
        except requests.RequestException as e:
            self.stdout.write(self.style.ERROR(f"Download failed: {e}"))
            raise

        target_state = None if options["all_states"] else options["state"].upper()

        if options["clear"]:
            qs = USZipCode.objects.all() if target_state is None else USZipCode.objects.filter(state=target_state)
            deleted, _ = qs.delete()
            label = "all states" if target_state is None else target_state
            self.stdout.write(f"Cleared {deleted} existing ZIP codes for {label}.")

        label = target_state or "all states"
        self.stdout.write(f"Parsing ZIP codes for {label}...")
        zip_buffer = io.BytesIO(resp.content)
        records = []

        with zipfile.ZipFile(zip_buffer) as zf:
            with zf.open("US.txt") as f:
                for line in f:
                    parts = line.decode("utf-8").strip().split("\t")
                    if len(parts) < 11:
                        continue

                    country, postal_code, place_name = parts[0], parts[1], parts[2]
                    admin_code1 = parts[4]  # state code e.g. "TX"
                    county = parts[5]
                    lat_str, lon_str = parts[9], parts[10]

                    if target_state and admin_code1 != target_state:
                        continue

                    try:
                        lat = float(lat_str) if lat_str else None
                        lon = float(lon_str) if lon_str else None
                    except ValueError:
                        lat, lon = None, None

                    major_cities = MAJOR_CITIES_BY_STATE.get(admin_code1, set())
                    records.append(
                        USZipCode(
                            zip_code=postal_code,
                            city=place_name,
                            state=admin_code1,
                            county=county or None,
                            latitude=lat,
                            longitude=lon,
                            is_major_city=place_name.lower() in major_cities,
                        )
                    )

        self.stdout.write(f"Found {len(records)} ZIP codes. Saving to database...")

        chunk_size = 500
        update_fields = ["city", "state", "county", "latitude", "longitude", "is_major_city"]

        for i in range(0, len(records), chunk_size):
            USZipCode.objects.bulk_create(
                records[i : i + chunk_size],
                update_conflicts=True,
                unique_fields=["zip_code"],
                update_fields=update_fields,
            )

        major_count = sum(1 for r in records if r.is_major_city)
        self.stdout.write(
            self.style.SUCCESS(
                f"Done. Upserted {len(records)} ZIP codes. Major-city ZIPs: {major_count}"
            )
        )
