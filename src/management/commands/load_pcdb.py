"""
Load an AutoCare PCdb JSON export (e.g. AutoCare_PCdb_enUS_JSON_20260730/) into the raw Pcdb*
mirror tables, then compute PcdbTerminologyFlat from them. See src/models.py for the table
docstrings and why there's no CodeMaster.json in this loader (PartCategory + PartPosition
replace it -- verified against an older export that shipped both).

Each raw table is fully truncated and reloaded from its JSON file every run -- this is a
versioned reference dataset snapshot (see Version.json), not something to incrementally diff.

Usage:
    python manage.py load_pcdb --source-dir ~/Downloads/AutoCare_PCdb_enUS_JSON_20260730
"""
import json
import os

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.utils.dateparse import parse_datetime

from src import models as src_models

CHUNK_SIZE = 5_000

# (json filename, model, {json_key: model_field}) -- every raw table is a literal mirror, one
# row per JSON object, no transformation. *DateTime keys are auto-detected and parsed.
RAW_TABLES = [
    ("Categories.json", src_models.PcdbCategories, {
        "CategoryID": "category_id", "CategoryName": "category_name", "CultureID": "culture_id",
    }),
    ("SubCategories.json", src_models.PcdbSubCategories, {
        "SubCategoryID": "subcategory_id", "SubCategoryName": "subcategory_name", "CultureID": "culture_id",
    }),
    ("Parts.json", src_models.PcdbParts, {
        "PartTerminologyID": "part_terminology_id",
        "PartTerminologyName": "part_terminology_name",
        "PartTerminologyDescription": "part_terminology_description",
        "CultureID": "culture_id",
    }),
    ("PartCategory.json", src_models.PcdbPartCategory, {
        "PartCategoryID": "part_category_id", "PartTerminologyID": "part_terminology_id",
        "SubCategoryID": "subcategory_id", "CategoryID": "category_id",
    }),
    ("Positions.json", src_models.PcdbPositions, {
        "PositionID": "position_id", "Position": "position", "CultureID": "culture_id",
    }),
    ("PartPosition.json", src_models.PcdbPartPosition, {
        "PartPositionID": "part_position_id", "PartTerminologyID": "part_terminology_id",
        "PositionID": "position_id",
    }),
    ("Alias.json", src_models.PcdbAlias, {
        "AliasID": "alias_id", "AliasName": "alias_name", "CultureID": "culture_id",
    }),
    ("PartsToAlias.json", src_models.PcdbPartsToAlias, {
        "PartsToAliasID": "parts_to_alias_id", "PartTerminologyID": "part_terminology_id",
        "AliasID": "alias_id",
    }),
    ("Use.json", src_models.PcdbUse, {
        "UseID": "use_id", "UseDescription": "use_description", "CultureID": "culture_id",
    }),
    ("PartsToUse.json", src_models.PcdbPartsToUse, {
        "PartsToUseID": "parts_to_use_id", "PartTerminologyID": "part_terminology_id", "UseID": "use_id",
    }),
    ("PartsRelationship.json", src_models.PcdbPartsRelationship, {
        "PartsRelationshipID": "parts_relationship_id", "PartTerminologyID": "part_terminology_id",
        "RelatedPartTerminologyID": "related_part_terminology_id",
    }),
    ("PartsSupersession.json", src_models.PcdbPartsSupersession, {
        "PartsSupersessionID": "parts_supersession_id",
        "OldPartTerminologyID": "old_part_terminology_id",
        "OldPartTerminologyName": "old_part_terminology_name",
        "NewPartTerminologyID": "new_part_terminology_id",
        "NewPartTerminologyName": "new_part_terminology_name",
        "Note": "note", "CultureID": "culture_id",
    }),
    ("ACESCodedValues.json", src_models.PcdbACESCodedValues, {
        "ACESCodedValueID": "aces_coded_value_id", "Element": "element", "Attribute": "attribute",
        "CodeValue": "code_value", "CodeDescription": "code_description", "CultureID": "culture_id",
    }),
    ("PIESSegment.json", src_models.PcdbPIESSegment, {
        "SegmentID": "segment_id", "SegmentAbb": "segment_abb", "SegmentName": "segment_name",
        "SegmentDescription": "segment_description", "CultureID": "culture_id",
    }),
    ("PIESField.json", src_models.PcdbPIESField, {
        "FieldID": "field_id", "SegmentID": "segment_id",
        "ReferenceFieldNumber": "reference_field_number", "FieldName": "field_name", "CultureID": "culture_id",
    }),
    ("PIESCode.json", src_models.PcdbPIESCode, {
        "CodeValueID": "code_value_id", "CodeValue": "code_value", "CodeDescription": "code_description",
        "CodeFormat": "code_format", "FieldFormat": "field_format", "Source": "source",
        "SourceWebsiteLink": "source_website_link", "CultureID": "culture_id",
    }),
    ("PIESEXPIGroup.json", src_models.PcdbPIESEXPIGroup, {
        "EXPIGroupID": "expi_group_id", "EXPIGroupCode": "expi_group_code",
        "EXPIGroupDescription": "expi_group_description", "CultureID": "culture_id",
    }),
    ("PIESEXPICode.json", src_models.PcdbPIESEXPICode, {
        "EXPICodeID": "expi_code_id", "EXPICode": "expi_code", "EXPICodeDescription": "expi_code_description",
        "EXPIGroupID": "expi_group_id", "CultureID": "culture_id",
    }),
    ("PIESReferenceFieldCode.json", src_models.PcdbPIESReferenceFieldCode, {
        "ReferenceFieldCodeID": "reference_field_code_id", "FieldID": "field_id",
        "CodeValueID": "code_value_id", "EXPICodeID": "expi_code_id", "ReferenceNotes": "reference_notes",
        "CultureID": "culture_id",
    }),
    ("Version.json", src_models.PcdbVersion, {
        "DatabaseName": "database_name", "Version": "version", "PublicationDate": "publication_date",
    }),
]

DATETIME_JSON_KEYS = {"EffectiveDateTime", "EndDateTime", "PublicationDate"}


def _convert_row(raw, field_map):
    kwargs = {}
    for json_key, model_field in field_map.items():
        value = raw.get(json_key)
        if json_key in DATETIME_JSON_KEYS and value:
            value = parse_datetime(value)
        kwargs[model_field] = value
    return kwargs


class Command(BaseCommand):
    help = "Load an AutoCare PCdb JSON export into the raw Pcdb* tables and compute PcdbTerminologyFlat."

    def add_arguments(self, parser):
        parser.add_argument("--source-dir", required=True, help="Path to the unzipped PCdb JSON export directory.")
        parser.add_argument(
            "--skip-flat", action="store_true", help="Load raw tables only, skip computing PcdbTerminologyFlat.",
        )

    def handle(self, *args, **options):
        source_dir = os.path.expanduser(options["source_dir"])
        if not os.path.isdir(source_dir):
            raise CommandError(f"Not a directory: {source_dir}")

        for filename, model, field_map in RAW_TABLES:
            path = os.path.join(source_dir, filename)
            if not os.path.isfile(path):
                self.stdout.write(self.style.WARNING(f"Skipping {filename}: not found in {source_dir}"))
                continue

            with open(path) as f:
                rows = json.load(f)

            self.stdout.write(f"Loading {filename} -> {model._meta.db_table} ({len(rows):,} rows)...")
            with transaction.atomic():
                model.objects.all().delete()
                objs = [model(**_convert_row(row, field_map)) for row in rows]
                model.objects.bulk_create(objs, batch_size=CHUNK_SIZE)

        if options["skip_flat"]:
            self.stdout.write(self.style.SUCCESS("Raw tables loaded (skipped terminology_flat)."))
            return

        from src.integrations.services import pcdb as pcdb_services

        result = pcdb_services.build_terminology_flat()

        self.stdout.write(self.style.SUCCESS(
            "terminology_flat built: {terminology_count:,} terminologies, "
            "{with_category_count:,} with a category, {inactive_count:,} superseded (inactive), "
            "{aces_invalid_count:,} not ACES-valid, {with_aliases_count:,} with aliases, "
            "{skipped_alias_refs} alias references skipped (missing AliasID).".format(**result)
        ))
        if result["supersession_cycles"]:
            self.stdout.write(self.style.WARNING(
                "{} supersession cycle(s) excluded from resolution (left terminal/active) -- see logs:".format(
                    len(result["supersession_cycles"])
                )
            ))
            for cycle in result["supersession_cycles"]:
                self.stdout.write(self.style.WARNING("  {}".format(cycle)))
