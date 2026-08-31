"""
Tests for the SimpleTire -> tire_specs merge.

Every fixture below is a real string from ``simpletire_skus`` or ``tire_specs``. The cases that
matter most are the ones asserting what the merge *refuses* to do: the field ownership guard, the
size gate on each match tier, and the NULL-is-not-a-value rules. Those are the failures that would
be silent in production -- a widened merge does not raise, it just overwrites good data.
"""
import decimal

from django.test import SimpleTestCase

from src.integrations.services import simpletire_sync as sync
from src.integrations.services import tire_reparse
from src.models import TireSpec


class LoadRangeTests(SimpleTestCase):
    databases = []

    def test_letter_range_yields_the_ply_rating_too(self):
        """Their display string packs both facts; reading only the letter throws half of it away."""
        self.assertEqual(sync.parse_load_range("E (10 Ply)"), ("E", 10, None))
        self.assertEqual(sync.parse_load_range("H (16 Ply)"), ("H", 16, None))

    def test_worded_designations(self):
        self.assertEqual(sync.parse_load_range("Standard (SL)"), ("SL", None, None))
        self.assertEqual(sync.parse_load_range("Extra (XL)"), ("XL", None, None))
        self.assertEqual(sync.parse_load_range("Light (LL)"), ("LL", None, None))

    def test_designations_we_have_no_code_for_are_reported_not_guessed(self):
        """HL is a real designation above XL and K a real load range, but load_range_ply has
        neither. Coercing them to a neighbour would be worse than leaving the field alone."""
        self.assertEqual(sync.parse_load_range("HL").code, None)
        self.assertEqual(sync.parse_load_range("HL").unmapped, "HL")
        self.assertEqual(sync.parse_load_range("K (20 Ply)").code, None)
        # ...but the ply rating in the string is still usable.
        self.assertEqual(sync.parse_load_range("K (20 Ply)").ply_rating, 20)

    def test_european_truck_markings_are_not_load_ranges(self):
        self.assertEqual(sync.parse_load_range("3*").code, None)
        self.assertEqual(sync.parse_load_range("3*").unmapped, "3*")

    def test_empty(self):
        self.assertEqual(sync.parse_load_range(None), (None, None, None))
        self.assertEqual(sync.parse_load_range(""), (None, None, None))


class SidewallTests(SimpleTestCase):
    databases = []

    def test_appearance_and_construction_are_separated(self):
        self.assertEqual(sync.split_sidewall("Blackwall"), ("Blackwall", None))
        self.assertEqual(sync.split_sidewall("Outlined White Lettering"), ("Outlined White Lettering", None))
        self.assertEqual(sync.split_sidewall("Tubeless"), (None, True))
        self.assertEqual(sync.split_sidewall("Tube-Type"), (None, False))

    def test_appearance_does_not_imply_construction(self):
        """A Blackwall tire is not thereby tubeless -- the source simply did not say."""
        self.assertIsNone(sync.split_sidewall("Blackwall")[1])


class CategoryAxisTests(SimpleTestCase):
    databases = []

    def test_season_is_lifted_out_of_a_non_season_answer(self):
        """The 822-row case: we say Highway Terrain, they say All Season. Both true."""
        self.assertEqual(sync._merge_category("HT", "All Season"), ("HT", "ALL_SEASON", None))

    def test_a_two_axis_category_upgrades_a_season_only_answer(self):
        """The 732-row case: we only had the season, they name the performance tier as well."""
        self.assertEqual(sync._merge_category("ALL_SEASON", "UHP All Season"), ("UHP", "ALL_SEASON", None))

    def test_a_coarser_answer_never_replaces_a_finer_one(self):
        """Their taxonomy has no Rugged Terrain. Taking theirs would flatten 269 RT rows to AT."""
        tread, season, note = sync._merge_category("RT", "All Terrain")
        self.assertEqual(tread, "RT")
        self.assertEqual(note, "category-conflict:RT->AT")

    def test_fills_an_empty_category(self):
        self.assertEqual(sync._merge_category(None, "Winter"), (None, "WINTER", None))
        self.assertEqual(sync._merge_category(None, "Mud Terrain"), ("MT", None, None))

    def test_unmapped_category_changes_nothing(self):
        """'Sport' is MC_STREET on a bike and ATV_SPORT on a quad; guessing would mislabel one."""
        self.assertEqual(sync._merge_category("AT", "Sport"), ("AT", None, None))


class ModelNameTests(SimpleTestCase):
    databases = []

    def test_takes_theirs(self):
        self.assertEqual(sync._merge_model_name("NT555 G2", "NT555RII", [])[0], "NT555RII")

    def test_keeps_ours_as_an_alias_when_it_is_the_same_product(self):
        name, aliases = sync._merge_model_name("Terra Grappler", "Terra Grappler G2", [])
        self.assertEqual(name, "Terra Grappler G2")
        self.assertEqual(aliases, ["Terra Grappler"])

    def test_does_not_alias_a_name_for_a_different_product(self):
        """Aliasing 'NT555 G2' onto the NT555RII would point a real search at the wrong tire."""
        self.assertEqual(sync._merge_model_name("NT555 G2", "NT555RII", [])[1], [])

    def test_does_not_duplicate_an_alias_the_enrichment_already_wrote(self):
        _, aliases = sync._merge_model_name("Terra Grappler", "Terra Grappler G2", ["terra grappler"])
        self.assertEqual(aliases, ["terra grappler"])

    def test_keeps_ours_when_they_have_no_name(self):
        self.assertEqual(sync._merge_model_name("Ridge Grappler", None, [])[0], "Ridge Grappler")


def _sku(**kwargs):
    row = {
        "id": 1,
        "brand_name": "Nitto",
        "part_number": "207-110",
        "size_display": "285/70R17",
        "product_line_name": "Ridge Grappler",
        "spec_category": None,
        "spec_vehicle": None,
        "spec_sidewall": None,
        "spec_load_range": None,
        "spec_ply_rating": None,
        "spec_is_studdable": None,
    }
    row.update({k: None for k in sync.TAKE_THEIRS.values() if k not in row})
    row.update(kwargs)
    return row


class MatchTests(SimpleTestCase):
    databases = []

    def _catalog(self, *rows):
        return sync.build_index(list(rows))

    def test_tier_1_brand_and_part_number(self):
        found = sync.match(
            brand="NITTO",
            part_number="207110",
            size_display="285/70R17",
            model_name=None,
            catalog=self._catalog(_sku()),
        )
        self.assertEqual(found.tier, 1)

    def test_tier_1_is_rejected_when_the_sizes_disagree(self):
        """Same brand, same MPN, different tire: one of the two catalogs is wrong, so trust
        neither rather than picking."""
        found = sync.match(
            brand="NITTO",
            part_number="207-110",
            size_display="265/70R17",
            model_name=None,
            catalog=self._catalog(_sku()),
        )
        self.assertIsNone(found)

    def test_brand_alias_is_applied(self):
        catalog = self._catalog(_sku(brand_name="Yokohama", part_number="110101682"))
        found = sync.match(
            brand="YOKOHAMA TIRE",
            part_number="110101682",
            size_display="285/70R17",
            model_name=None,
            catalog=catalog,
        )
        self.assertEqual(found.tier, 1)

    def test_tier_2_needs_the_size_to_agree(self):
        """7,814 of our part numbers exist under some other brand of theirs because short numeric
        MPNs collide across manufacturers. The size is what separates a real match from those."""
        catalog = self._catalog(_sku(brand_name="Atturo", part_number="118431"))
        self.assertEqual(
            sync.match(
                brand="MICHELIN", part_number="118431", size_display="285/70R17", model_name=None, catalog=catalog
            ).tier,
            2,
        )
        self.assertIsNone(
            sync.match(
                brand="MICHELIN", part_number="118431", size_display="225/65R16", model_name=None, catalog=catalog
            )
        )

    def test_tier_2_refuses_an_ambiguous_part_number(self):
        catalog = self._catalog(
            _sku(id=1, brand_name="Atturo", part_number="118431"),
            _sku(id=2, brand_name="Summit", part_number="118431"),
        )
        self.assertIsNone(
            sync.match(
                brand="MICHELIN", part_number="118431", size_display="285/70R17", model_name=None, catalog=catalog
            )
        )

    def test_tier_3_brand_model_and_size(self):
        catalog = self._catalog(_sku(part_number="different"))
        found = sync.match(
            brand="NITTO",
            part_number="207-110",
            size_display="285/70R17",
            model_name="Ridge Grappler",
            catalog=catalog,
        )
        self.assertEqual(found.tier, 3)

    def test_tier_3_refuses_when_more_than_one_sku_fits(self):
        catalog = self._catalog(
            _sku(id=1, part_number="a"),
            _sku(id=2, part_number="b"),
        )
        self.assertIsNone(
            sync.match(
                brand="NITTO", part_number="zzz", size_display="285/70R17", model_name="Ridge Grappler", catalog=catalog
            )
        )


class MergeTests(SimpleTestCase):
    databases = []

    def test_a_null_on_their_side_does_not_erase_ours(self):
        spec = TireSpec(max_psi=44, search_aliases=[])
        updates = sync.build_updates(spec, _sku())
        self.assertNotIn("max_psi", updates)

    def test_studdable_is_a_positive_only_claim(self):
        """Their column is True or NULL, never False. Reading NULL as 'not studdable' would
        overwrite 1,203 of our answers with an absence of evidence."""
        spec = TireSpec(is_studdable=True, search_aliases=[])
        self.assertNotIn("is_studdable", sync.build_updates(spec, _sku(spec_is_studdable=None)))
        spec = TireSpec(is_studdable=False, search_aliases=[])
        self.assertEqual(sync.build_updates(spec, _sku(spec_is_studdable=True))["is_studdable"], True)

    def test_vehicle_class_is_fill_only(self):
        """Their SUV/Crossover has no equivalent of ours and folds to passenger, which is coarser
        than an answer we already have."""
        spec = TireSpec(vehicle_class="light_truck", search_aliases=[])
        self.assertNotIn("vehicle_class", sync.build_updates(spec, _sku(spec_vehicle="Passenger")))
        spec = TireSpec(vehicle_class=None, search_aliases=[])
        self.assertEqual(sync.build_updates(spec, _sku(spec_vehicle="SUV/Crossover"))["vehicle_class"], "passenger")

    def test_fractional_tread_depth_survives(self):
        """26% of published depths are fractional; an integer column silently rounded them until
        migration 0190."""
        spec = TireSpec(search_aliases=[])
        updates = sync.build_updates(spec, _sku(spec_tread_depth_32nds=decimal.Decimal("7.20")))
        self.assertEqual(updates["tread_depth_32nds"], decimal.Decimal("7.20"))

    def test_the_merge_never_erases_a_value_another_source_supplied(self):
        """_merge_category returns no season for "All Terrain", and writing that None over a
        season TDG had already set blanked 3,593 rows the first time this sync was re-run."""
        spec = TireSpec(season_category_id="WINTER", search_aliases=[])
        updates = sync.build_updates(spec, _sku(spec_category="All Terrain"))
        self.assertNotIn("season_category_id", updates)

    def test_a_null_is_still_allowed_to_fill_nothing(self):
        spec = TireSpec(season_category_id=None, search_aliases=[])
        self.assertNotIn("season_category_id", sync.build_updates(spec, _sku(spec_category="All Terrain")))

    def test_unchanged_values_are_not_reported_as_changes(self):
        spec = TireSpec(max_psi=44, model_name="Ridge Grappler", search_aliases=[])
        updates = sync.build_updates(spec, _sku(spec_max_psi=44))
        self.assertEqual(updates, {})


class OwnershipTests(SimpleTestCase):
    databases = []

    def test_the_merge_cannot_write_a_field_it_must_not_own(self):
        self.assertEqual(set(sync.WRITE_FIELDS) & sync._NEVER_WRITE, set())

    def test_run_flat_is_never_merged(self):
        """Their is_run_flat is False on all 58,124 scraped rows, including tires whose own model
        name says Run Flat. Merging it would replace 558 correct values with a default."""
        self.assertIn("is_run_flat", sync._NEVER_WRITE)
        spec = TireSpec(is_run_flat=True, search_aliases=[])
        self.assertNotIn("is_run_flat", sync.build_updates(spec, _sku()))

    def test_nominal_diameter_and_speed_are_left_to_the_parser(self):
        """Ours is the diameter the size prints (35.0 for a 35X12.50R20) and drives '35 inch'
        search; theirs is measured (33.07). Same name, different quantity."""
        self.assertIn("overall_diameter_in", sync._NEVER_WRITE)
        self.assertIn("max_speed_mph", sync._NEVER_WRITE)

    def test_reparse_leaves_catalog_owned_fields_alone(self):
        """The other half of the contract: without this, the next parser fix reverts the merge."""
        for field in sync.TAKE_THEIRS:
            if field in tire_reparse.CATALOG_OWNED:
                self.assertNotIn(field, tire_reparse.PARSER_FIELDS_CATALOG)
                self.assertNotIn(field, tire_reparse.RESOLVED_FIELDS_CATALOG)
        self.assertIn("load_range", tire_reparse.CATALOG_OWNED)
        self.assertIn("max_load_lb", tire_reparse.CATALOG_OWNED)

    def test_reparse_still_owns_the_size_block_on_catalog_rows(self):
        """A match is only accepted when both sides agree on the dimensions, so the size stays
        ours and a parser fix must still reach these rows."""
        for field in ("notation", "section_width_mm", "rim_diameter_in", "size_display"):
            self.assertIn(field, tire_reparse.PARSER_FIELDS_CATALOG)
