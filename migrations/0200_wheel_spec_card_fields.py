"""
Thirteen columns the wheel detail card needs and no feed we ingest fills yet.

The card has a fixed shape -- the client renders every row and blanks a null -- so the columns have
to exist before the endpoint can promise them. Most are NULL against Wheel Pros, which publishes
none of this; The Wheel Group already publishes warranties, lug seat, TPMS and weight for its 2,072
wheels, and the other feeds are next, so these are the places that data lands.

Two of them earn their separateness:

``structural_warranty`` and ``finish_warranty`` are two columns because wheels warrant the casting
and the coating on very different terms -- lifetime against one year is common -- and that gap is a
real difference between a $200 and a $400 wheel. Folding them into one field discards what the
buyer is paying for.

``is_simulated_beadlock`` is not ``is_beadlock``. A simulated beadlock has the bolts and the look
and does not clamp the bead; someone airing down for a trail needs to know which they bought, so
the two must never collapse into one flag.

``max_psi`` is beadlock-only. On a normal wheel it means nothing and the card hides the row.

All additive and nullable (``style_tags`` defaults to an empty list), so this is safe on the live
table: 43,272 rows, no rewrite, nothing reads the columns until a feed fills them.
"""
import django.contrib.postgres.fields
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0199_wheelpros_part_number_index"),
    ]

    operations = [
        migrations.AddField(
            model_name='wheelspec',
            name='caps_included',
            field=models.BooleanField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='wheelspec',
            name='finish_warranty',
            field=models.CharField(blank=True, max_length=64, null=True),
        ),
        migrations.AddField(
            model_name='wheelspec',
            name='hub_rings',
            field=models.CharField(blank=True, help_text="included / required / not_needed. 'Required' and absent are very different at fitting time.", max_length=24, null=True),
        ),
        migrations.AddField(
            model_name='wheelspec',
            name='is_directional',
            field=models.BooleanField(blank=True, help_text='Sold as left and right. Ordering four of one hand is a returns problem, so it belongs on the card.', null=True),
        ),
        migrations.AddField(
            model_name='wheelspec',
            name='is_hub_centric',
            field=models.BooleanField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='wheelspec',
            name='is_simulated_beadlock',
            field=models.BooleanField(blank=True, help_text='Looks like a beadlock, does not clamp the bead. Distinct from is_beadlock and not interchangeable.', null=True),
        ),
        migrations.AddField(
            model_name='wheelspec',
            name='lug_thread_size',
            field=models.CharField(blank=True, help_text='M14 x 1.5, 1/2-20. Wrong thread will not start.', max_length=24, null=True),
        ),
        migrations.AddField(
            model_name='wheelspec',
            name='lugs_included',
            field=models.BooleanField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='wheelspec',
            name='max_psi',
            field=models.PositiveSmallIntegerField(blank=True, help_text='Beadlock rings have a torque/pressure limit. Meaningless on a normal wheel -- hide it there.', null=True),
        ),
        migrations.AddField(
            model_name='wheelspec',
            name='piece_count',
            field=models.PositiveSmallIntegerField(blank=True, help_text="1-piece cast, or a 2/3-piece bolt-together. Read from the '3PC' marker brands put in the name.", null=True),
        ),
        migrations.AddField(
            model_name='wheelspec',
            name='structural_warranty',
            field=models.CharField(blank=True, max_length=64, null=True),
        ),
        migrations.AddField(
            model_name='wheelspec',
            name='style_tags',
            field=django.contrib.postgres.fields.ArrayField(base_field=models.TextField(), blank=True, default=list, help_text='What the wheel is for, as pills: ["Off-road", "Truck"].', size=None),
        ),
        migrations.AddField(
            model_name='wheelspec',
            name='tier',
            field=models.CharField(blank=True, choices=[('budget', 'Budget'), ('mid', 'Mid'), ('premium', 'Premium'), ('flagship', 'Flagship')], max_length=16, null=True),
        ),
    ]
