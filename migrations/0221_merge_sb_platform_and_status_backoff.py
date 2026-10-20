# Merge node: the S&B branch (0215 -> 0218) and the platform branch (0216 -> 0217 -> 0219
# -> 0220) were developed in parallel and left the graph with two leaves, which makes
# `migrate` refuse to run at all. No operations -- this only rejoins them.

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('src', '0218_local_merge_sb_platform'),
        ('src', '0220_company_provider_status_backoff'),
    ]

    operations = [
    ]
