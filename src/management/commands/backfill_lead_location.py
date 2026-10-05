"""
Fills city/state on leads that carry coordinates but no location text.

Google Maps ingestion left 151,724 of 155,463 leads with latitude/longitude and NOTHING else --
no address, no zip_code, no source_zip, no city -- which makes the data unusable for geographic
targeting. Coordinates are the only signal, so each point is resolved against the nearest US
postal-code centroid.

Reference data is pgeocode's US set (41,490 postcodes, all 52 state codes), NOT the us_zip_code
table in this database. That table looks like a geo reference but is a scraping seed list: 16,135
rows covering only 16 states. Using it snapped Virginia leads onto "Eden, NC" -- confidently
wrong, and wrong in a way that reads as correct downstream. If you are tempted to swap it back in,
run `select count(distinct state) from us_zip_code` first.

Accuracy: 41k centroids put the nearest one within a few km almost everywhere, so the town is
right and the state is right except for leads sitting within a couple of km of a state line.
Points with no centroid inside the search window -- outside the US, or bad coordinates -- are
left untouched rather than snapped to something distant.

Only ever writes into EMPTY fields; an existing city or state is never overwritten.

Usage:
  python manage.py backfill_lead_location --source google --dry-run
  python manage.py backfill_lead_location --source google
"""
from collections import defaultdict

from django.core.management.base import BaseCommand
from django.db.models import Q

from src.models import Lead, LeerLead, RealTruckLead

# Each table names its coordinate columns differently.
SOURCES = {
    "google": (Lead, "latitude", "longitude"),
    "realtruck": (RealTruckLead, "lat", "lng"),
    "leer": (LeerLead, "lat", "lng"),
}

# Search the lead's own 1-degree cell plus its 8 neighbours (~110km). Wide enough that no US
# point misses a centroid, tight enough to keep each scan to a few hundred candidates.
NEIGHBOURS = [(dla, dlo) for dla in (-1, 0, 1) for dlo in (-1, 0, 1)]


def load_grid(stdout=None):
    """Nearest-neighbour lookup structure over US postal-code centroids, bucketed by degree."""
    import pgeocode

    df = pgeocode.Nominatim("us")._data
    df = df.dropna(subset=["latitude", "longitude", "state_code"])
    grid = defaultdict(list)
    for la, lo, city, state in zip(df.latitude, df.longitude,
                                   df.place_name, df.state_code):
        grid[(round(la), round(lo))].append((float(la), float(lo), city, state))
    if stdout:
        n = sum(len(v) for v in grid.values())
        states = {s for cell in grid.values() for *_, s in cell}
        stdout.write(f"loaded {n:,} postal centroids across {len(states)} states")
    return grid


def locate(grid, lat, lng):
    """(city, state) of the nearest centroid, or None if nothing is within ~1.5 degrees."""
    best, bd = None, None
    la_c, lo_c = round(lat), round(lng)
    for dla, dlo in NEIGHBOURS:
        for zla, zlo, city, state in grid.get((la_c + dla, lo_c + dlo), ()):
            d = (zla - lat) ** 2 + (zlo - lng) ** 2
            if bd is None or d < bd:
                bd, best = d, (city, state)
    return best


class Command(BaseCommand):
    help = "Derive city/state from coordinates using US postal-code centroids"

    def add_arguments(self, parser):
        parser.add_argument("--source", default="google", choices=sorted(SOURCES))
        parser.add_argument("--dry-run", action="store_true")
        parser.add_argument("--batch", type=int, default=2000)

    def handle(self, *args, **opts):
        model, la_f, lo_f = SOURCES[opts["source"]]
        grid = load_grid(self.stdout)

        qs = (model.objects.filter(Q(state__isnull=True) | Q(state=""))
              .exclude(**{f"{la_f}__isnull": True})
              .exclude(**{f"{lo_f}__isnull": True})
              .only(model._meta.pk.name, la_f, lo_f, "city", "state"))
        total = qs.count()
        self.stdout.write(f"{total:,} {opts['source']} leads to locate\n")

        resolved = unresolved = 0
        pending = []
        for i, lead in enumerate(qs.iterator(chunk_size=opts["batch"]), 1):
            hit = locate(grid, float(getattr(lead, la_f)), float(getattr(lead, lo_f)))
            if not hit:
                unresolved += 1
                continue
            city, state = hit
            lead.state = state
            if not lead.city:
                lead.city = city
            pending.append(lead)
            resolved += 1

            if len(pending) >= opts["batch"]:
                if not opts["dry_run"]:
                    model.objects.bulk_update(pending, ["city", "state"])
                pending.clear()
                self.stdout.write(f"  [{i:,}/{total:,}] resolved={resolved:,} "
                                  f"unresolved={unresolved:,}", ending="\r")
                self.stdout.flush()

        if pending and not opts["dry_run"]:
            model.objects.bulk_update(pending, ["city", "state"])

        self.stdout.write(self.style.SUCCESS(
            f"\n{'DRY RUN -- ' if opts['dry_run'] else ''}resolved {resolved:,} | "
            f"no centroid nearby {unresolved:,}"))
