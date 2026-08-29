"""
Tests for ``src.integrations.services.wheelpros_vehicles`` and the Vehicle API client.

Pure logic only -- no network, no database. Three things are worth pinning down:

* the scalar parser, because Wheel Pros sends **every** axle value as a display string
  (``'+35'``, ``'18"'``, ``'5x114.3'``) and the typed columns are all guesses at those shapes;
* the payload -> row mapping, because model rows and submodel rows share one table and their
  identity is assembled from two sources (the payload, falling back to the request); and
* the client's 403 handling, because "the account is not entitled to this API" must fail loudly
  and immediately rather than be retried 500,000 times.
"""
import decimal
import json
import pathlib
import tempfile

from django.test import SimpleTestCase

from src.integrations.clients.wheelpros import exceptions as wheelpros_exceptions
from src.integrations.clients.wheelpros.vehicle_client import WheelProsVehicleApiClient
from src.integrations.services import wheelpros_vehicles as service


def _axle(**overrides):
    """A front-axle object shaped like the spec's, trimmed to the fields under test."""
    axle = {
        "code": "F1",
        "vehiclePressureSensor": "Direct TPMS",
        "boltPatternMm": "6x135",
        "oeWidthIn": "8.5",
        "maxWidthIn": "10",
        "oeTireTx": "275/65R18",
        "oeHexTx": "21mm",
        "nutBolt": "Nut",
        "centerBoreMm": "87.1",
        "minWheelLoad": "2600",
        "sensorPartNumberOe": "JL3T-1A180-AA",
        "hubCode": "H12",
        "maxBs": "6.25",
        "maxFs": "5.75",
        "hubClearanceMm": "12.5",
        "yFactor": "1.25",
        "yFactor25": "1.5",
        "yFactor50": "1.75",
        "diameter": {"oeDiameterIn": "18", "minDiameterIn": "17", "maxDiameterIn": "22"},
        "caliper": {
            "peakDepth": "42.5",
            "depth90mm": "30",
            "depth100mm": "31",
            "depth106mm": "32",
            "depth119mm": "33",
            "depth134mm": "34",
            "depth160mm": "35",
        },
        "offset": {
            "oeOffset": "+44",
            "offsetMinMm": "-12",
            "offsetMaxMm": "50",
            "liftOffsetMinMm": "-24",
            "liftOffsetMaxMm": "55",
        },
        "lug": {"amLugStyle": "Conical", "lugNutSizeTx": "M14x1.5", "lugCnt": "6"},
    }
    axle.update(overrides)
    return axle


def _payload(**overrides):
    payload = {
        "id": 123456,
        "make": "Ford",
        "model": "F-150",
        "year": 2024,
        "properties": {"staggered": False},
        "axles": {"front": _axle(), "rear": _axle(boltPatternMm="6x135", oeWidthIn="9.5")},
    }
    payload.update(overrides)
    return payload


class DecimalParsingTests(SimpleTestCase):
    """``_decimal`` is the single point where a display string becomes a number. It has to be
    generous about formatting and strict about meaning."""

    def _parse(self, value, *, max_digits=8, decimal_places=2):
        return service._decimal(value, max_digits=max_digits, decimal_places=decimal_places, field="test")

    def test_plain_and_signed_numbers(self):
        self.assertEqual(self._parse("8.5"), decimal.Decimal("8.50"))
        self.assertEqual(self._parse("+44"), decimal.Decimal("44.00"))
        self.assertEqual(self._parse("-12"), decimal.Decimal("-12.00"))
        self.assertEqual(self._parse(35), decimal.Decimal("35.00"))

    def test_strips_units_and_separators(self):
        self.assertEqual(self._parse('18"'), decimal.Decimal("18.00"))
        self.assertEqual(self._parse("114.3 mm"), decimal.Decimal("114.30"))
        self.assertEqual(self._parse("2,600"), decimal.Decimal("2600.00"))

    def test_zero_is_a_value_not_a_blank(self):
        self.assertEqual(self._parse("0"), decimal.Decimal("0.00"))

    def test_unpublished_markers_become_none(self):
        for blank in ("", "  ", "N/A", "na", "None", "-", "--", "TBD", None):
            self.assertIsNone(self._parse(blank), blank)

    def test_bolt_pattern_is_not_mistaken_for_a_measurement(self):
        # The failure this guards against is silent: stripping the 'x' would yield 5114.3 and
        # write a plausible-looking wrong number into a numeric column.
        self.assertIsNone(self._parse("5x114.3"))
        self.assertIsNone(self._parse("275/65R18"))
        self.assertIsNone(self._parse("Conical"))

    def test_overflow_is_dropped_rather_than_raised(self):
        # A DataError here would abort a multi-hour crawl; the raw string survives on raw_axle.
        self.assertIsNone(self._parse("99999999", max_digits=6, decimal_places=2))

    def test_rounds_to_the_column_scale(self):
        self.assertEqual(self._parse("8.567"), decimal.Decimal("8.57"))


class BooleanParsingTests(SimpleTestCase):
    def test_real_booleans_pass_through(self):
        self.assertIs(service._bool(True), True)
        self.assertIs(service._bool(False), False)

    def test_string_forms(self):
        self.assertIs(service._bool("true"), True)
        self.assertIs(service._bool("no"), False)

    def test_unknown_is_none_not_false(self):
        # "we don't know" and "not staggered" are different facts.
        self.assertIsNone(service._bool("maybe"))
        self.assertIsNone(service._bool(None))
        self.assertIsNone(service._bool("N/A"))


class BuildVehicleTests(SimpleTestCase):
    def setUp(self):
        self.ref = service.ModelRef(year=2024, make="ford", model="f-150", vehicle_types=("wheel", "tire"))

    def test_identity_prefers_the_payloads_canonical_spelling(self):
        vehicle = service.build_vehicle(_payload(), ref=self.ref)
        self.assertEqual((vehicle.year, vehicle.make, vehicle.model), (2024, "Ford", "F-150"))
        self.assertEqual(vehicle.submodel, "")
        self.assertEqual(vehicle.external_id, 123456)
        self.assertIs(vehicle.staggered, False)
        self.assertEqual(vehicle.vehicle_types, ["wheel", "tire"])

    def test_identity_falls_back_to_the_request_when_the_payload_omits_it(self):
        vehicle = service.build_vehicle({"id": 9}, ref=self.ref)
        self.assertEqual((vehicle.year, vehicle.make, vehicle.model), (2024, "ford", "f-150"))

    def test_submodel_row(self):
        vehicle = service.build_vehicle(
            _payload(subModel="Raptor"), ref=self.ref, submodel="raptor"
        )
        self.assertEqual(vehicle.submodel, "Raptor")

    def test_submodel_falls_back_to_the_requested_name(self):
        vehicle = service.build_vehicle(_payload(), ref=self.ref, submodel="King Ranch")
        self.assertEqual(vehicle.submodel, "King Ranch")

    def test_raw_payload_is_kept_verbatim(self):
        payload = _payload()
        self.assertEqual(service.build_vehicle(payload, ref=self.ref).raw_payload, payload)


class BuildAxlesTests(SimpleTestCase):
    def test_both_positions_are_mapped(self):
        axles = service.build_axles(_payload(), vehicle_id=7)
        self.assertEqual([a.position for a in axles], ["front", "rear"])
        self.assertTrue(all(a.vehicle_id == 7 for a in axles))

    def test_flat_fields(self):
        front = service.build_axles(_payload(), vehicle_id=7)[0]
        self.assertEqual(front.code, "F1")
        self.assertEqual(front.bolt_pattern_mm, "6x135")
        self.assertEqual(front.oe_tire, "275/65R18")
        self.assertEqual(front.nut_bolt, "Nut")
        self.assertEqual(front.pressure_sensor, "Direct TPMS")
        self.assertEqual(front.sensor_part_number_oe, "JL3T-1A180-AA")
        self.assertEqual(front.center_bore_mm, decimal.Decimal("87.10"))
        self.assertEqual(front.oe_width_in, decimal.Decimal("8.50"))
        self.assertEqual(front.min_wheel_load, decimal.Decimal("2600.00"))
        self.assertEqual(front.y_factor_50, decimal.Decimal("1.750"))

    def test_nested_groups_are_flattened(self):
        front = service.build_axles(_payload(), vehicle_id=7)[0]
        self.assertEqual(front.oe_diameter_in, decimal.Decimal("18.00"))
        self.assertEqual(front.max_diameter_in, decimal.Decimal("22.00"))
        self.assertEqual(front.caliper_peak_depth, decimal.Decimal("42.50"))
        self.assertEqual(front.caliper_depth_119mm, decimal.Decimal("33.00"))
        self.assertEqual(front.oe_offset_mm, decimal.Decimal("44.00"))
        self.assertEqual(front.offset_min_mm, decimal.Decimal("-12.00"))
        self.assertEqual(front.lift_offset_max_mm, decimal.Decimal("55.00"))
        self.assertEqual(front.lug_count, 6)
        self.assertEqual(front.lug_nut_size, "M14x1.5")
        self.assertEqual(front.lug_style_am, "Conical")

    def test_staggered_vehicles_keep_distinct_rows(self):
        front, rear = service.build_axles(_payload(), vehicle_id=7)
        self.assertEqual(front.oe_width_in, decimal.Decimal("8.50"))
        self.assertEqual(rear.oe_width_in, decimal.Decimal("9.50"))

    def test_missing_position_yields_no_row(self):
        # Two rows of NULLs would be indistinguishable from a vehicle with no published fitment.
        axles = service.build_axles(_payload(axles={"front": _axle()}), vehicle_id=7)
        self.assertEqual([a.position for a in axles], ["front"])

    def test_no_axles_at_all(self):
        self.assertEqual(service.build_axles({"id": 1}, vehicle_id=7), [])
        self.assertEqual(service.build_axles(_payload(axles=None), vehicle_id=7), [])

    def test_unpublished_fields_are_null(self):
        front = service.build_axles(
            _payload(axles={"front": _axle(hubCode="N/A", centerBoreMm="", lug={})}), vehicle_id=7
        )[0]
        self.assertIsNone(front.hub_code)
        self.assertIsNone(front.center_bore_mm)
        self.assertIsNone(front.lug_count)

    def test_raw_axle_is_kept_verbatim(self):
        axle = _axle()
        front = service.build_axles(_payload(axles={"front": axle}), vehicle_id=7)[0]
        self.assertEqual(front.raw_axle, axle)


class ModelRefTests(SimpleTestCase):
    def test_key_is_case_and_whitespace_insensitive(self):
        # The API's path params are case-insensitive and its listings are not consistent, so a
        # resumed run must not re-crawl the same model under a different spelling.
        a = service.ModelRef(year=2024, make="Ford", model="F-150")
        b = service.ModelRef(year=2024, make="ford", model=" f-150 ")
        self.assertEqual(a.key, b.key)

    def test_different_years_are_different_units(self):
        self.assertNotEqual(
            service.ModelRef(year=2024, make="Ford", model="F-150").key,
            service.ModelRef(year=2025, make="Ford", model="F-150").key,
        )


class CheckpointTests(SimpleTestCase):
    def test_round_trip_and_torn_line_tolerance(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "nested" / "checkpoint.jsonl"
            ref = service.ModelRef(year=2024, make="Ford", model="F-150")
            with service._Checkpoint(path) as checkpoint:
                checkpoint.record(ref, status="ok", vehicles=4)

            # A hard kill mid-write leaves a partial line; it must be ignored, not fatal.
            with path.open("a", encoding="utf-8") as handle:
                handle.write('{"key": "2025|ford|bron')

            self.assertEqual(service._Checkpoint(path).completed_keys(), {ref.key})
            self.assertEqual(json.loads(path.read_text().splitlines()[0])["vehicles"], 4)

    def test_absent_file_is_an_empty_resume_set(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.assertEqual(service._Checkpoint(pathlib.Path(tmp) / "missing.jsonl").completed_keys(), set())

    def test_dry_run_records_nothing(self):
        with service._Checkpoint(None) as checkpoint:
            checkpoint.record(service.ModelRef(year=2024, make="Ford", model="F-150"), status="ok", vehicles=1)
        self.assertEqual(service._Checkpoint(None).completed_keys(), set())


class VehicleClientTests(SimpleTestCase):
    def _client(self):
        return WheelProsVehicleApiClient(credentials={"username": "u", "password": "p"})

    def test_credentials_are_required(self):
        with self.assertRaises(ValueError):
            WheelProsVehicleApiClient(credentials={"username": "u"})

    def test_unknown_environment_is_rejected(self):
        with self.assertRaises(ValueError):
            WheelProsVehicleApiClient(credentials={"username": "u", "password": "p"}, environment="prod")

    def test_path_segments_are_fully_encoded(self):
        # A raw slash in "RAM ProMaster 1500" style names would silently address another route.
        self.assertEqual(WheelProsVehicleApiClient._quote("Chevrolet Silverado 1500"), "Chevrolet%20Silverado%201500")
        self.assertEqual(WheelProsVehicleApiClient._quote("A/B"), "A%2FB")

    def test_type_params(self):
        self.assertEqual(WheelProsVehicleApiClient._type_params(None), {})
        self.assertEqual(WheelProsVehicleApiClient._type_params("wheel"), {"type": "wheel"})
        with self.assertRaises(ValueError):
            WheelProsVehicleApiClient._type_params("truck")

    def test_403_raises_permission_error_immediately(self):
        """The account-not-entitled case. It must not be retried, and the message has to name
        the account so the fix (ask Wheel Pros to grant it) is obvious from the log alone."""
        client = self._client()
        client._cached_token = "token"
        client._token_expires_at = float("inf")
        calls = []

        class _Response:
            status_code = 403
            text = '{"Message":"User is not authorized to access this resource"}'

        class _Session:
            def get(self_inner, *args, **kwargs):
                calls.append(args)
                return _Response()

        client._thread_state.session = _Session()

        with self.assertRaises(wheelpros_exceptions.WheelProsVehiclePermissionError) as caught:
            client.get_years()
        self.assertEqual(len(calls), 1, "a 403 must not be retried")
        self.assertIn("not entitled", str(caught.exception))
        self.assertIn("u", str(caught.exception))

    def test_404_is_a_skip_not_a_failure(self):
        client = self._client()
        client._cached_token = "token"
        client._token_expires_at = float("inf")

        class _Response:
            status_code = 404
            text = "not found"

        class _Session:
            def get(self_inner, *args, **kwargs):
                return _Response()

        client._thread_state.session = _Session()
        with self.assertRaises(wheelpros_exceptions.WheelProsVehicleNotFound):
            client.get_model_info(2024, "Ford", "Nonexistent")

    def test_listing_responses_are_coerced_and_filtered(self):
        client = self._client()
        client._cached_token = "token"
        client._token_expires_at = float("inf")

        class _Response:
            status_code = 200

            @staticmethod
            def json():
                return [2026, "2025", "", None, "notayear"]

        class _Session:
            def get(self_inner, *args, **kwargs):
                return _Response()

        client._thread_state.session = _Session()
        self.assertEqual(client.get_years(), [2026, 2025])


class TokenBucketTests(SimpleTestCase):
    def test_rate_must_be_positive(self):
        with self.assertRaises(ValueError):
            service._TokenBucket(0)

    def test_acquire_spaces_requests_out(self):
        import time

        bucket = service._TokenBucket(50.0)  # 20ms apart
        started = time.monotonic()
        for _ in range(4):
            bucket.acquire()
        # Three gaps of 20ms; allow slack for a slow CI box but prove it is not free.
        self.assertGreater(time.monotonic() - started, 0.04)
