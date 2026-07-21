"""
Imports the Auto Care Association VCdb vehicle reference dataset into vcdb_vehicles.

Joins Vehicle.json -> BaseVehicle.json -> Make.json / Model.json / SubModel.json into one
flattened (vehicle_id, year, make, model, submodel) row per VCdb vehicle, plus an unambiguous
engine (VehicleToEngineConfig -> EngineConfig -> EngineBase/FuelType) and drive_type
(VehicleToDriveType -> DriveType) when a vehicle has exactly one option for that attribute —
left blank otherwise, since VCdb vehicles are commonly offered with multiple engine/drivetrain
choices and a bare VehicleID can't disambiguate which one a given fitment row means. Source JSON
files come from an AutoCare VCdb export (e.g. AutoCare_VCdb_NA_LDPS_enUS_JSON_<date>) — this repo
does not ship that dataset, so point --vcdb-path at wherever it's checked out, or set
VCDB_JSON_PATH.

Usage:
  python manage.py import_vcdb_vehicles
  python manage.py import_vcdb_vehicles --vcdb-path /path/to/AutoCare_VCdb_NA_LDPS_enUS_JSON_20250828
  python manage.py import_vcdb_vehicles --batch-size 2000
"""
import json
import os
from pathlib import Path

import pgbulk
from django.core.management.base import BaseCommand, CommandError

from src.models import VcdbVehicle

DEFAULT_BATCH_SIZE = 5000

VCDB_UPDATE_FIELDS = [
    "base_vehicle_id", "year", "make", "model", "submodel", "region_id",
    "engine", "drive_type", "updated_at",
]


def _default_vcdb_path() -> Path:
    env_path = os.environ.get("VCDB_JSON_PATH")
    if env_path:
        return Path(env_path)
    # Sibling checkout: ../autoparts-be/autocare.org/AutoCare_VCdb_NA_LDPS_enUS_JSON_20250828
    repo_root = Path(__file__).resolve().parents[4]
    return repo_root / "autoparts-be" / "autocare.org" / "AutoCare_VCdb_NA_LDPS_enUS_JSON_20250828"


def _format_engine(engine_base: dict, fuel_type_name: str) -> str:
    liter = engine_base.get("Liter")
    cylinders = engine_base.get("Cylinders")
    parts = []
    if liter:
        parts.append(f"{liter}L")
    if cylinders:
        parts.append(f"{cylinders}cyl")
    if fuel_type_name:
        parts.append(fuel_type_name)
    return " ".join(parts)


class Command(BaseCommand):
    help = "Import the AutoCare VCdb vehicle dataset (Vehicle/BaseVehicle/Make/Model/SubModel/engine/drive type) into vcdb_vehicles"

    def add_arguments(self, parser):
        parser.add_argument(
            "--vcdb-path",
            default=None,
            help="Directory containing the VCdb JSON files (defaults to $VCDB_JSON_PATH or a sibling autoparts-be checkout)",
        )
        parser.add_argument(
            "--batch-size",
            type=int,
            default=DEFAULT_BATCH_SIZE,
            help=f"pgbulk upsert batch size (default: {DEFAULT_BATCH_SIZE})",
        )

    def handle(self, *args, **options):
        vcdb_path = Path(options["vcdb_path"]) if options["vcdb_path"] else _default_vcdb_path()
        batch_size = options["batch_size"]

        if not vcdb_path.is_dir():
            raise CommandError(f"VCdb path not found: {vcdb_path}")

        self.stdout.write(f"Loading VCdb JSON from {vcdb_path} ...")
        vehicle = self._load_json(vcdb_path / "Vehicle.json")
        base_vehicle = self._load_json(vcdb_path / "BaseVehicle.json")
        make = self._load_json(vcdb_path / "Make.json")
        model = self._load_json(vcdb_path / "Model.json")
        submodel = self._load_json(vcdb_path / "SubModel.json")
        v2engine = self._load_json(vcdb_path / "VehicleToEngineConfig.json")
        engine_config = self._load_json(vcdb_path / "EngineConfig.json")
        engine_base = self._load_json(vcdb_path / "EngineBase.json")
        fuel_type = self._load_json(vcdb_path / "FuelType.json")
        v2drive = self._load_json(vcdb_path / "VehicleToDriveType.json")
        drive_type = self._load_json(vcdb_path / "DriveType.json")

        base_vehicle_by_id = {b["BaseVehicleID"]: b for b in base_vehicle}
        make_by_id = {m["MakeID"]: m["MakeName"] for m in make}
        model_by_id = {m["ModelID"]: m["ModelName"] for m in model}
        submodel_by_id = {s["SubModelID"]: s["SubModelName"] for s in submodel}

        engine_base_by_id = {e["EngineBaseID"]: e for e in engine_base}
        fuel_type_by_id = {f["FuelTypeID"]: f["FuelTypeName"] for f in fuel_type}
        engine_config_by_id = {e["EngineConfigID"]: e for e in engine_config}
        drive_type_by_id = {d["DriveTypeID"]: d["DriveTypeName"] for d in drive_type}

        self.stdout.write("Resolving unambiguous engine per vehicle...")
        engine_options_by_vehicle_id: dict = {}
        for row in v2engine:
            ec = engine_config_by_id.get(row["EngineConfigID"])
            if not ec:
                continue
            eb = engine_base_by_id.get(ec["EngineBaseID"])
            if not eb:
                continue
            engine_str = _format_engine(eb, fuel_type_by_id.get(ec.get("FuelTypeID"), ""))
            if not engine_str:
                continue
            engine_options_by_vehicle_id.setdefault(row["VehicleID"], set()).add(engine_str)
        engine_by_vehicle_id = {
            vid: next(iter(opts)) for vid, opts in engine_options_by_vehicle_id.items() if len(opts) == 1
        }

        self.stdout.write("Resolving unambiguous drive type per vehicle...")
        drive_options_by_vehicle_id: dict = {}
        for row in v2drive:
            dt_name = drive_type_by_id.get(row["DriveTypeID"])
            if not dt_name:
                continue
            drive_options_by_vehicle_id.setdefault(row["VehicleID"], set()).add(dt_name)
        drive_type_by_vehicle_id = {
            vid: next(iter(opts)) for vid, opts in drive_options_by_vehicle_id.items() if len(opts) == 1
        }

        self.stdout.write(
            f"Vehicle={len(vehicle)} BaseVehicle={len(base_vehicle)} Make={len(make)} "
            f"Model={len(model)} SubModel={len(submodel)}. "
            f"Unambiguous engine for {len(engine_by_vehicle_id)} vehicles, "
            f"unambiguous drive type for {len(drive_type_by_vehicle_id)} vehicles. Building rows..."
        )

        records = []
        skipped = 0
        for v in vehicle:
            bv = base_vehicle_by_id.get(v["BaseVehicleID"])
            if not bv:
                skipped += 1
                continue
            make_name = make_by_id.get(bv["MakeID"])
            model_name = model_by_id.get(bv["ModelID"])
            if not make_name or not model_name:
                skipped += 1
                continue
            vehicle_id = v["VehicleID"]
            records.append(
                VcdbVehicle(
                    vehicle_id=vehicle_id,
                    base_vehicle_id=bv["BaseVehicleID"],
                    year=bv["YearID"],
                    make=make_name,
                    model=model_name,
                    submodel=submodel_by_id.get(v.get("SubmodelID"), ""),
                    region_id=v.get("RegionID"),
                    engine=engine_by_vehicle_id.get(vehicle_id, ""),
                    drive_type=drive_type_by_vehicle_id.get(vehicle_id, ""),
                )
            )

        if skipped:
            self.stdout.write(self.style.WARNING(f"Skipped {skipped} vehicle rows with unresolved BaseVehicle/Make/Model."))

        self.stdout.write(f"Upserting {len(records)} vehicles in batches of {batch_size}...")
        total = 0
        num_batches = (len(records) + batch_size - 1) // batch_size
        for i in range(0, len(records), batch_size):
            batch = records[i : i + batch_size]
            pgbulk.upsert(
                VcdbVehicle,
                batch,
                unique_fields=["vehicle_id"],
                update_fields=VCDB_UPDATE_FIELDS,
                returning=False,
            )
            total += len(batch)
            batch_num = (i // batch_size) + 1
            self.stdout.write(f"  batch {batch_num}/{num_batches} ({total}/{len(records)} rows)")

        self.stdout.write(self.style.SUCCESS(f"Done. Upserted {total} VCdb vehicles."))

    @staticmethod
    def _load_json(path: Path):
        if not path.is_file():
            raise CommandError(f"Missing VCdb file: {path}")
        with open(path) as f:
            return json.load(f)
