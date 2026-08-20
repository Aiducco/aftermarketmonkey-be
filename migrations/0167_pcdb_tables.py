# Hand-trimmed from the auto-generated migration: makemigrations picked up a large amount of
# pre-existing drift unrelated to this change (id-field alterations, constraint removals, Meta
# option changes, plus a spurious LeerLead/RealTruckLead delete caused by unrelated concurrent
# WIP in models.py at generation time) -- only the 20 new Pcdb* raw mirror tables plus
# PcdbTerminologyFlat and its 2 indexes are kept here. Depends on the last actually-committed
# migration (0163-0166 are uncommitted local WIP, not safe to depend on -- see the migration
# dependency chain fix in commit fff1801 earlier this session for why that matters).
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("src", "0162_company_manual_trial_granted_at"),
    ]

    operations = [
        migrations.CreateModel(
            name="PcdbACESCodedValues",
            fields=[
                (
                    "aces_coded_value_id",
                    models.IntegerField(primary_key=True, serialize=False),
                ),
                ("element", models.TextField(blank=True, null=True)),
                ("attribute", models.TextField(blank=True, null=True)),
                ("code_value", models.TextField(blank=True, null=True)),
                ("code_description", models.TextField(blank=True, null=True)),
                ("culture_id", models.CharField(blank=True, max_length=16, null=True)),
                ("effective_date_time", models.DateTimeField(blank=True, null=True)),
                ("end_date_time", models.DateTimeField(blank=True, null=True)),
            ],
            options={
                "db_table": "pcdb_aces_coded_values",
            },
        ),
        migrations.CreateModel(
            name="PcdbAlias",
            fields=[
                ("alias_id", models.IntegerField(primary_key=True, serialize=False)),
                ("alias_name", models.TextField(blank=True, null=True)),
                ("culture_id", models.CharField(blank=True, max_length=16, null=True)),
                ("effective_date_time", models.DateTimeField(blank=True, null=True)),
                ("end_date_time", models.DateTimeField(blank=True, null=True)),
            ],
            options={
                "db_table": "pcdb_alias",
            },
        ),
        migrations.CreateModel(
            name="PcdbCategories",
            fields=[
                ("category_id", models.IntegerField(primary_key=True, serialize=False)),
                ("category_name", models.TextField(blank=True, null=True)),
                ("culture_id", models.CharField(blank=True, max_length=16, null=True)),
                ("effective_date_time", models.DateTimeField(blank=True, null=True)),
                ("end_date_time", models.DateTimeField(blank=True, null=True)),
            ],
            options={
                "db_table": "pcdb_categories",
            },
        ),
        migrations.CreateModel(
            name="PcdbPartCategory",
            fields=[
                (
                    "part_category_id",
                    models.IntegerField(primary_key=True, serialize=False),
                ),
                ("part_terminology_id", models.IntegerField(db_index=True)),
                ("subcategory_id", models.IntegerField(blank=True, null=True)),
                ("category_id", models.IntegerField(blank=True, null=True)),
                ("effective_date_time", models.DateTimeField(blank=True, null=True)),
                ("end_date_time", models.DateTimeField(blank=True, null=True)),
            ],
            options={
                "db_table": "pcdb_part_category",
            },
        ),
        migrations.CreateModel(
            name="PcdbPartPosition",
            fields=[
                (
                    "part_position_id",
                    models.IntegerField(primary_key=True, serialize=False),
                ),
                ("part_terminology_id", models.IntegerField(db_index=True)),
                ("position_id", models.IntegerField(blank=True, null=True)),
                ("effective_date_time", models.DateTimeField(blank=True, null=True)),
                ("end_date_time", models.DateTimeField(blank=True, null=True)),
            ],
            options={
                "db_table": "pcdb_part_position",
            },
        ),
        migrations.CreateModel(
            name="PcdbParts",
            fields=[
                (
                    "part_terminology_id",
                    models.IntegerField(primary_key=True, serialize=False),
                ),
                ("part_terminology_name", models.TextField(blank=True, null=True)),
                (
                    "part_terminology_description",
                    models.TextField(blank=True, null=True),
                ),
                ("culture_id", models.CharField(blank=True, max_length=16, null=True)),
                ("effective_date_time", models.DateTimeField(blank=True, null=True)),
                ("end_date_time", models.DateTimeField(blank=True, null=True)),
            ],
            options={
                "db_table": "pcdb_parts",
            },
        ),
        migrations.CreateModel(
            name="PcdbPartsRelationship",
            fields=[
                (
                    "parts_relationship_id",
                    models.IntegerField(primary_key=True, serialize=False),
                ),
                ("part_terminology_id", models.IntegerField(db_index=True)),
                (
                    "related_part_terminology_id",
                    models.IntegerField(blank=True, null=True),
                ),
                ("effective_date_time", models.DateTimeField(blank=True, null=True)),
                ("end_date_time", models.DateTimeField(blank=True, null=True)),
            ],
            options={
                "db_table": "pcdb_parts_relationship",
            },
        ),
        migrations.CreateModel(
            name="PcdbPartsSupersession",
            fields=[
                (
                    "parts_supersession_id",
                    models.IntegerField(primary_key=True, serialize=False),
                ),
                ("old_part_terminology_id", models.IntegerField(db_index=True)),
                ("old_part_terminology_name", models.TextField(blank=True, null=True)),
                (
                    "new_part_terminology_id",
                    models.IntegerField(blank=True, db_index=True, null=True),
                ),
                ("new_part_terminology_name", models.TextField(blank=True, null=True)),
                ("note", models.TextField(blank=True, null=True)),
                ("culture_id", models.CharField(blank=True, max_length=16, null=True)),
                ("effective_date_time", models.DateTimeField(blank=True, null=True)),
                ("end_date_time", models.DateTimeField(blank=True, null=True)),
            ],
            options={
                "db_table": "pcdb_parts_supersession",
            },
        ),
        migrations.CreateModel(
            name="PcdbPartsToAlias",
            fields=[
                (
                    "parts_to_alias_id",
                    models.IntegerField(primary_key=True, serialize=False),
                ),
                ("part_terminology_id", models.IntegerField(db_index=True)),
                ("alias_id", models.IntegerField(blank=True, null=True)),
                ("effective_date_time", models.DateTimeField(blank=True, null=True)),
                ("end_date_time", models.DateTimeField(blank=True, null=True)),
            ],
            options={
                "db_table": "pcdb_parts_to_alias",
            },
        ),
        migrations.CreateModel(
            name="PcdbPartsToUse",
            fields=[
                (
                    "parts_to_use_id",
                    models.IntegerField(primary_key=True, serialize=False),
                ),
                ("part_terminology_id", models.IntegerField(db_index=True)),
                ("use_id", models.IntegerField(blank=True, null=True)),
                ("effective_date_time", models.DateTimeField(blank=True, null=True)),
                ("end_date_time", models.DateTimeField(blank=True, null=True)),
            ],
            options={
                "db_table": "pcdb_parts_to_use",
            },
        ),
        migrations.CreateModel(
            name="PcdbPIESCode",
            fields=[
                (
                    "code_value_id",
                    models.IntegerField(primary_key=True, serialize=False),
                ),
                ("code_value", models.TextField(blank=True, null=True)),
                ("code_description", models.TextField(blank=True, null=True)),
                ("code_format", models.TextField(blank=True, null=True)),
                ("field_format", models.TextField(blank=True, null=True)),
                ("source", models.TextField(blank=True, null=True)),
                ("source_website_link", models.TextField(blank=True, null=True)),
                ("culture_id", models.CharField(blank=True, max_length=16, null=True)),
                ("effective_date_time", models.DateTimeField(blank=True, null=True)),
                ("end_date_time", models.DateTimeField(blank=True, null=True)),
            ],
            options={
                "db_table": "pcdb_pies_code",
            },
        ),
        migrations.CreateModel(
            name="PcdbPIESEXPICode",
            fields=[
                (
                    "expi_code_id",
                    models.IntegerField(primary_key=True, serialize=False),
                ),
                ("expi_code", models.TextField(blank=True, null=True)),
                ("expi_code_description", models.TextField(blank=True, null=True)),
                ("expi_group_id", models.IntegerField(blank=True, null=True)),
                ("culture_id", models.CharField(blank=True, max_length=16, null=True)),
                ("effective_date_time", models.DateTimeField(blank=True, null=True)),
                ("end_date_time", models.DateTimeField(blank=True, null=True)),
            ],
            options={
                "db_table": "pcdb_pies_expi_code",
            },
        ),
        migrations.CreateModel(
            name="PcdbPIESEXPIGroup",
            fields=[
                (
                    "expi_group_id",
                    models.IntegerField(primary_key=True, serialize=False),
                ),
                ("expi_group_code", models.TextField(blank=True, null=True)),
                ("expi_group_description", models.TextField(blank=True, null=True)),
                ("culture_id", models.CharField(blank=True, max_length=16, null=True)),
                ("effective_date_time", models.DateTimeField(blank=True, null=True)),
                ("end_date_time", models.DateTimeField(blank=True, null=True)),
            ],
            options={
                "db_table": "pcdb_pies_expi_group",
            },
        ),
        migrations.CreateModel(
            name="PcdbPIESField",
            fields=[
                ("field_id", models.IntegerField(primary_key=True, serialize=False)),
                ("segment_id", models.IntegerField(blank=True, null=True)),
                ("reference_field_number", models.TextField(blank=True, null=True)),
                ("field_name", models.TextField(blank=True, null=True)),
                ("culture_id", models.CharField(blank=True, max_length=16, null=True)),
                ("effective_date_time", models.DateTimeField(blank=True, null=True)),
                ("end_date_time", models.DateTimeField(blank=True, null=True)),
            ],
            options={
                "db_table": "pcdb_pies_field",
            },
        ),
        migrations.CreateModel(
            name="PcdbPIESReferenceFieldCode",
            fields=[
                (
                    "reference_field_code_id",
                    models.IntegerField(primary_key=True, serialize=False),
                ),
                ("field_id", models.IntegerField(blank=True, null=True)),
                ("code_value_id", models.IntegerField(blank=True, null=True)),
                ("expi_code_id", models.IntegerField(blank=True, null=True)),
                ("reference_notes", models.TextField(blank=True, null=True)),
                ("culture_id", models.CharField(blank=True, max_length=16, null=True)),
                ("effective_date_time", models.DateTimeField(blank=True, null=True)),
                ("end_date_time", models.DateTimeField(blank=True, null=True)),
            ],
            options={
                "db_table": "pcdb_pies_reference_field_code",
            },
        ),
        migrations.CreateModel(
            name="PcdbPIESSegment",
            fields=[
                ("segment_id", models.IntegerField(primary_key=True, serialize=False)),
                ("segment_abb", models.TextField(blank=True, null=True)),
                ("segment_name", models.TextField(blank=True, null=True)),
                ("segment_description", models.TextField(blank=True, null=True)),
                ("culture_id", models.CharField(blank=True, max_length=16, null=True)),
                ("effective_date_time", models.DateTimeField(blank=True, null=True)),
                ("end_date_time", models.DateTimeField(blank=True, null=True)),
            ],
            options={
                "db_table": "pcdb_pies_segment",
            },
        ),
        migrations.CreateModel(
            name="PcdbPositions",
            fields=[
                ("position_id", models.IntegerField(primary_key=True, serialize=False)),
                ("position", models.TextField(blank=True, null=True)),
                ("culture_id", models.CharField(blank=True, max_length=16, null=True)),
                ("effective_date_time", models.DateTimeField(blank=True, null=True)),
                ("end_date_time", models.DateTimeField(blank=True, null=True)),
            ],
            options={
                "db_table": "pcdb_positions",
            },
        ),
        migrations.CreateModel(
            name="PcdbSubCategories",
            fields=[
                (
                    "subcategory_id",
                    models.IntegerField(primary_key=True, serialize=False),
                ),
                ("subcategory_name", models.TextField(blank=True, null=True)),
                ("culture_id", models.CharField(blank=True, max_length=16, null=True)),
                ("effective_date_time", models.DateTimeField(blank=True, null=True)),
                ("end_date_time", models.DateTimeField(blank=True, null=True)),
            ],
            options={
                "db_table": "pcdb_subcategories",
            },
        ),
        migrations.CreateModel(
            name="PcdbTerminologyFlat",
            fields=[
                (
                    "part_terminology_id",
                    models.IntegerField(primary_key=True, serialize=False),
                ),
                ("name", models.TextField()),
                ("category_id", models.IntegerField(blank=True, null=True)),
                ("category_name", models.TextField(blank=True, null=True)),
                ("subcategory_id", models.IntegerField(blank=True, null=True)),
                ("subcategory_name", models.TextField(blank=True, null=True)),
                ("description", models.TextField(blank=True, null=True)),
                ("aliases", models.JSONField(blank=True, default=list)),
                ("aces_valid", models.BooleanField()),
                ("pies_valid", models.BooleanField()),
                ("superseded_by", models.IntegerField(blank=True, null=True)),
                ("is_active", models.BooleanField()),
            ],
            options={
                "db_table": "pcdb_terminology_flat",
            },
        ),
        migrations.CreateModel(
            name="PcdbUse",
            fields=[
                ("use_id", models.IntegerField(primary_key=True, serialize=False)),
                ("use_description", models.TextField(blank=True, null=True)),
                ("culture_id", models.CharField(blank=True, max_length=16, null=True)),
                ("effective_date_time", models.DateTimeField(blank=True, null=True)),
                ("end_date_time", models.DateTimeField(blank=True, null=True)),
            ],
            options={
                "db_table": "pcdb_use",
            },
        ),
        migrations.CreateModel(
            name="PcdbVersion",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("database_name", models.TextField(blank=True, null=True)),
                ("version", models.TextField(blank=True, null=True)),
                ("publication_date", models.DateTimeField(blank=True, null=True)),
            ],
            options={
                "db_table": "pcdb_version",
            },
        ),
        migrations.AddIndex(
            model_name="pcdbterminologyflat",
            index=models.Index(
                fields=["category_id"], name="pcdb_term_flat_category_idx"
            ),
        ),
        migrations.AddIndex(
            model_name="pcdbterminologyflat",
            index=models.Index(fields=["is_active"], name="pcdb_term_flat_active_idx"),
        ),
    ]
