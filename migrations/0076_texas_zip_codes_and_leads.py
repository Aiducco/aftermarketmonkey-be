from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0075_wheelpros_feed_type_and_cost_usd"),
    ]

    operations = [
        migrations.CreateModel(
            name="USZipCode",
            fields=[
                ("id", models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("zip_code", models.CharField(max_length=10, unique=True)),
                ("city", models.CharField(max_length=128)),
                ("state", models.CharField(max_length=2)),
                ("county", models.CharField(blank=True, max_length=128, null=True)),
                ("latitude", models.DecimalField(blank=True, decimal_places=6, max_digits=9, null=True)),
                ("longitude", models.DecimalField(blank=True, decimal_places=6, max_digits=9, null=True)),
                ("population", models.IntegerField(blank=True, null=True)),
                ("is_major_city", models.BooleanField(default=False)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
            ],
            options={"db_table": "us_zip_code"},
        ),
        migrations.AddIndex(
            model_name="uszipcode",
            index=models.Index(fields=["state"], name="uszip_state_idx"),
        ),
        migrations.AddIndex(
            model_name="uszipcode",
            index=models.Index(fields=["state", "city"], name="uszip_state_city_idx"),
        ),
        migrations.AddIndex(
            model_name="uszipcode",
            index=models.Index(fields=["is_major_city"], name="uszip_major_idx"),
        ),
        migrations.CreateModel(
            name="Lead",
            fields=[
                ("id", models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("place_id", models.CharField(max_length=255, unique=True)),
                ("name", models.CharField(max_length=512)),
                ("address", models.TextField(blank=True, null=True)),
                ("city", models.CharField(blank=True, max_length=128, null=True)),
                ("state", models.CharField(blank=True, max_length=64, null=True)),
                ("zip_code", models.CharField(blank=True, max_length=10, null=True)),
                ("latitude", models.DecimalField(blank=True, decimal_places=6, max_digits=9, null=True)),
                ("longitude", models.DecimalField(blank=True, decimal_places=6, max_digits=9, null=True)),
                ("phone", models.CharField(blank=True, max_length=64, null=True)),
                ("website", models.URLField(blank=True, max_length=512, null=True)),
                ("email", models.EmailField(blank=True, max_length=255, null=True)),
                ("rating", models.DecimalField(blank=True, decimal_places=1, max_digits=3, null=True)),
                ("review_count", models.IntegerField(blank=True, null=True)),
                ("google_maps_url", models.URLField(blank=True, max_length=512, null=True)),
                ("business_status", models.CharField(blank=True, max_length=64, null=True)),
                ("search_query", models.CharField(blank=True, max_length=255, null=True)),
                ("source_zip", models.CharField(blank=True, max_length=10, null=True)),
                ("category", models.CharField(blank=True, max_length=128, null=True)),
                ("status", models.IntegerField(choices=[(0, "Pending"), (1, "Contacted"), (2, "Qualified"), (3, "Disqualified"), (4, "Converted")], default=0)),
                ("notes", models.TextField(blank=True, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={"db_table": "lead"},
        ),
        migrations.AddIndex(
            model_name="lead",
            index=models.Index(fields=["state", "city"], name="lead_state_city_idx"),
        ),
        migrations.AddIndex(
            model_name="lead",
            index=models.Index(fields=["category"], name="lead_category_idx"),
        ),
        migrations.AddIndex(
            model_name="lead",
            index=models.Index(fields=["status"], name="lead_status_idx"),
        ),
        migrations.AddIndex(
            model_name="lead",
            index=models.Index(fields=["rating"], name="lead_rating_idx"),
        ),
    ]
