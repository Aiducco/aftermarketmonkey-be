import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0174_ml_part_terminology_classification_status"),
    ]

    operations = [
        migrations.CreateModel(
            name="ApiRateBucket",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("bucket_key", models.CharField(max_length=255, unique=True)),
                ("count", models.IntegerField(default=0)),
                ("limit_value", models.IntegerField()),
                ("expires_at", models.DateTimeField(db_index=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "db_table": "api_rate_buckets",
            },
        ),
        migrations.AddField(
            model_name="integrationpricingsyncjob",
            name="not_before",
            field=models.DateTimeField(blank=True, db_index=True, null=True),
        ),
    ]
