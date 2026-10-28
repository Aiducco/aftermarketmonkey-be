"""
Drop ``motorstate_availability``.

The table was written by a GET /api/ProductAvailabilityChange poll that also served as the
part-number "spine" for the API catalog path -- the only way to enumerate Motor State part
numbers, since their API has no bulk product export. The FTP feed replaced all of it: one CSV
per dealer carries the whole catalog, and brand resolves from the part number's own
three-character prefix rather than from the spine.

It had been frozen since 2026-09-02 (251,630 rows) while still being read as a current
"shared availability spine", and the inventory sync's own docstring already said it "is no
longer read here". The API path that filled it last ran manually on 2026-08-08 and was never
scheduled.

Irreversible on purpose: the data is two months stale and nothing reads it. Re-creating the
table would not re-create its contents, and the path that produced them is gone.
"""
from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ("src", "0234_ntw_locations"),
    ]

    operations = [
        migrations.DeleteModel(name="MotorStateAvailability"),
    ]
