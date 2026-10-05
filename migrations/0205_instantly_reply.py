# The local record of every reply received in Instantly and what was pushed to FreshSales for it.
# See docs/INSTANTLY_FRESHSALES_SYNC_PLAN.md and src.models.InstantlyReply for why the row carries
# both halves: it is what makes the sync idempotent without a stored cursor.
#
# This migration also merges two pre-existing leaf nodes. 0202_lead_email_search_at and
# 0203_the_wheel_group_inventory both branch off 0201, which leaves the graph with two leaves and
# makes `manage.py migrate` refuse to run at all. Depending on both resolves that; nothing here
# alters either branch.
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0202_lead_email_search_at"),
        ("src", "0204_purchase_order_ship_to_validation_status"),
    ]

    operations = [
        migrations.CreateModel(
            name="InstantlyReply",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("instantly_email_id", models.TextField(unique=True)),
                ("thread_id", models.TextField(blank=True, null=True)),
                ("campaign_id", models.TextField(blank=True, db_index=True, null=True)),
                ("campaign_name", models.TextField(blank=True, null=True)),
                ("lead_email", models.EmailField(db_index=True, max_length=255)),
                ("from_name", models.TextField(blank=True, null=True)),
                ("eaccount", models.TextField(blank=True, null=True)),
                ("subject", models.TextField(blank=True, null=True)),
                ("body_text", models.TextField(blank=True, null=True)),
                ("email_timestamp", models.DateTimeField(blank=True, db_index=True, null=True)),
                ("instantly_created_at", models.DateTimeField(blank=True, db_index=True, null=True)),
                (
                    "interest_status",
                    models.IntegerField(
                        blank=True,
                        choices=[
                            (-3, "Lost"),
                            (-2, "Wrong Person"),
                            (-1, "Not Interested"),
                            (0, "Out of Office"),
                            (1, "Interested"),
                            (2, "Meeting Booked"),
                            (3, "Meeting Completed"),
                            (4, "Closed"),
                        ],
                        null=True,
                    ),
                ),
                ("interest_checked_at", models.DateTimeField(blank=True, null=True)),
                ("is_positive", models.BooleanField(default=False)),
                ("is_auto_reply", models.BooleanField(default=False)),
                ("lead_payload", models.JSONField(blank=True, default=dict)),
                ("freshsales_account_id", models.TextField(blank=True, null=True)),
                ("freshsales_contact_id", models.TextField(blank=True, null=True)),
                ("freshsales_note_id", models.TextField(blank=True, null=True)),
                ("freshsales_deal_id", models.TextField(blank=True, null=True)),
                ("contact_synced_at", models.DateTimeField(blank=True, null=True)),
                ("deal_created_at", models.DateTimeField(blank=True, null=True)),
                ("sync_attempts", models.IntegerField(default=0)),
                ("last_error", models.TextField(blank=True, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "db_table": "instantly_reply",
                "ordering": ["-email_timestamp"],
            },
        ),
        migrations.AddIndex(
            model_name="instantlyreply",
            index=models.Index(fields=["contact_synced_at"], name="instantly_reply_synced_idx"),
        ),
        migrations.AddIndex(
            model_name="instantlyreply",
            index=models.Index(fields=["is_positive", "deal_created_at"], name="instantly_reply_deal_idx"),
        ),
    ]
