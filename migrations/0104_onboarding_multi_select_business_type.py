from django.db import migrations, models


def _forwards_migrate_business_type(apps, schema_editor):
    Company = apps.get_model("src", "Company")
    for company in Company.objects.exclude(business_type_legacy__isnull=True).exclude(business_type_legacy=""):
        company.business_type = [company.business_type_legacy]
        company.save(update_fields=["business_type"])


def _backwards_migrate_business_type(apps, schema_editor):
    Company = apps.get_model("src", "Company")
    for company in Company.objects.exclude(business_type=[]):
        first = company.business_type[0] if company.business_type else None
        company.business_type_legacy = first
        company.save(update_fields=["business_type_legacy"])


class Migration(migrations.Migration):
    dependencies = [
        ("src", "0103_brandfiltercache"),
    ]

    operations = [
        migrations.AddField(
            model_name="company",
            name="city",
            field=models.CharField(blank=True, max_length=128, null=True),
        ),
        migrations.AddField(
            model_name="company",
            name="postal_code",
            field=models.CharField(blank=True, max_length=32, null=True),
        ),
        migrations.AddField(
            model_name="userprofile",
            name="role",
            field=models.CharField(blank=True, max_length=32, null=True),
        ),
        # Convert business_type from a single CharField string to a JSONField list,
        # preserving existing values via a rename + add + data-migrate + drop.
        migrations.RenameField(
            model_name="company",
            old_name="business_type",
            new_name="business_type_legacy",
        ),
        migrations.AddField(
            model_name="company",
            name="business_type",
            field=models.JSONField(blank=True, default=list),
        ),
        migrations.RunPython(_forwards_migrate_business_type, _backwards_migrate_business_type),
        migrations.RemoveField(
            model_name="company",
            name="business_type_legacy",
        ),
    ]
