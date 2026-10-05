"""
Tests for the two-stage PCdb terminology classifier.

Four things here are worth pinning, all of them decisions that fail silently rather than loudly.
Which distributor's blurb becomes the prompt decides what the model even gets to read, and the
"informative" check is what stops a rich-provider rank from being trusted when that provider
shipped nothing but an echo of the description. The taxonomy's subcategory-name pooling and its
worked examples are the two production-confirmed fixes the routing accuracy rests on -- both are
invisible in a passing run and only show up as a slow drift in wrong terminologies. The recursive
batch halving is the difference between 15 hard errors and 0 on a 300-part run. And the assembler
is where "the model refused" and "the pipeline broke" get told apart, which is the whole point of
the status column.

No database: every function under test takes its rows as arguments precisely so this file doesn't
need one.
"""
import decimal
import json

from django.test import SimpleTestCase

from src.integrations.services import part_terminology_classification as terminology


def _term(part_terminology_id, name, category, subcategory, description=None):
    return {
        "part_terminology_id": part_terminology_id,
        "name": name,
        "category_name": category,
        "subcategory_name": subcategory,
        "description": description,
    }


class ProviderContextTests(SimpleTestCase):
    databases = []

    TURN_14 = 1
    ATECH = 7

    def test_the_richest_provider_wins_regardless_of_row_order(self):
        """ATECH is the confirmed-worst tier and Turn 14 the best, so the answer must not depend
        on which ProviderPart row the database happened to return first."""
        turn14 = (self.TURN_14, [{"label": "Category", "value": "Suspension Lift Kit"}])
        atech = (self.ATECH, [{"label": "sku", "value": "RC-100"}])
        for rows in ([turn14, atech], [atech, turn14]):
            context = terminology.build_part_context("ROUGH COUNTRY", "6IN LIFT KIT", rows)
            self.assertIn("TURN_14", context)
            self.assertNotIn("ATECH", context)

    def test_a_top_ranked_provider_that_only_echoes_the_description_is_skipped(self):
        """Rank alone is not enough -- a provider can be well-ranked and still ship nothing but
        the sku/brand/description we already have, which adds no signal to the prompt."""
        rows = [
            (self.TURN_14, [{"label": "description", "value": "6IN LIFT KIT"},
                            {"label": "brand", "value": "ROUGH COUNTRY"}]),
            (self.ATECH, [{"label": "Part Type", "value": "Suspension Leveling Kit"}]),
        ]
        context = terminology.build_part_context("ROUGH COUNTRY", "6IN LIFT KIT", rows)
        self.assertIn("Suspension Leveling Kit", context)

    def test_a_brand_echoed_in_a_different_case_is_still_recognised_as_an_echo(self):
        """The bug that silently disabled the fall-through. Brands are stored upper-case and
        echoed by distributors in title case, and a case-sensitive strip left the brand in the
        residual, scoring a pure echo as informative. Real consequence: Wiseco piston ring sets
        took Turn 14's echo over Motor State's "Single Cyl. Piston Ring Set 3.810 Bore", and Stage
        2 then refused them because nothing in the text said "piston"."""
        echo = "SKU: wis3810A; Brand: Wiseco; Description: Wiseco 3.810inch 1 Cyl. Ring Set"
        self.assertFalse(_is_informative := terminology._is_informative(
            echo, "Wiseco 3.810inch 1 Cyl. Ring Set", "WISECO"))

    def test_a_provider_that_names_the_part_type_beats_one_that_echoes_it(self):
        """The end-to-end consequence of the check above, on the real rows that exposed it."""
        rows = [
            (self.TURN_14, [{"label": "SKU", "value": "wis3810A"}, {"label": "Brand", "value": "Wiseco"},
                            {"label": "Description", "value": "Wiseco 3.810inch 1 Cyl. Ring Set"}]),
            (23, [{"label": "Description", "value": "Single Cyl. Piston Ring Set 3.810 Bore"}]),
        ]
        context = terminology.build_part_context("WISECO", "Wiseco 3.810inch 1 Cyl. Ring Set", rows)
        self.assertIn("Piston Ring Set", context)

    def test_motor_state_is_no_longer_ranked_below_every_unverified_distributor(self):
        """Its feed carries a long_description that names the part type outright. The old
        no-signal placement came from product_type.py, which is about wheel/tire/part -- a
        different question from terminology, and answered before that feed moved to FTP."""
        self.assertLess(
            terminology._provider_rank("MOTOR_STATE_DISTRIBUTING"),
            terminology._provider_rank("SOME_UNVERIFIED_DISTRIBUTOR"),
        )

    def test_a_part_with_no_provider_rows_still_produces_a_prompt(self):
        self.assertEqual(
            terminology.build_part_context("WEATHERTECH", "CARGO LINER", []),
            "WEATHERTECH CARGO LINER",
        )

    def test_an_unknown_provider_kind_does_not_crash_the_run(self):
        """Provider kinds are added ahead of this pipeline knowing about them; an unrecognised one
        should sort mid-table, not raise partway through a 3M-row catalog."""
        context = terminology.build_part_context(
            "ACME", "WIDGET", [(9999, [{"label": "Part Type", "value": "Brake Caliper Bracket"}])]
        )
        self.assertIn("UNKNOWN_9999", context)
        self.assertGreater(terminology._provider_rank("UNKNOWN_9999"), terminology._provider_rank("TURN_14"))
        self.assertLess(terminology._provider_rank("UNKNOWN_9999"), terminology._provider_rank("ATECH"))


class TaxonomyTests(SimpleTestCase):
    databases = []

    # "Fuel Injection System and Related Components" really does exist under two categories with
    # non-overlapping term lists. This is the collision fix #1 exists for.
    ROWS = [
        _term(1, "Fuel Injector", "Air and Fuel Delivery", "Fuel Injection System and Related Components"),
        _term(2, "Fuel Rail", "Air and Fuel Delivery", "Fuel Injection System and Related Components"),
        _term(3, "Engine Control Module", "Engine", "Fuel Injection System and Related Components"),
        _term(4, "Cargo Net", "Interior", "Trunk Lid and Compartment"),
        _term(5, "Cargo Floor Liner Extension Panel", "Interior", "Trunk Lid and Compartment"),
    ]

    def test_candidates_pool_across_every_category_sharing_a_subcategory_name(self):
        """The confirmed save: Stage 1 routing a FUEL INJECTOR to "Engine" instead of "Air and Fuel
        Delivery" still reaches the right term, because Stage 2's candidates are keyed on the
        subcategory NAME alone."""
        taxonomy = terminology.build_taxonomy(self.ROWS)
        pooled = taxonomy.by_subcategory_name["Fuel Injection System and Related Components"]
        self.assertEqual({t["part_terminology_id"] for t in pooled}, {1, 2, 3})

    def test_both_categories_stay_addressable_as_stage_1_pairs(self):
        taxonomy = terminology.build_taxonomy(self.ROWS)
        self.assertIn(("Engine", "Fuel Injection System and Related Components"), taxonomy.by_pair)
        self.assertIn(("Air and Fuel Delivery", "Fuel Injection System and Related Components"), taxonomy.by_pair)

    def test_each_subcategory_carries_worked_examples(self):
        """Fix #2: "Trunk Lid and Compartment" doesn't read as a cargo liner to anyone, model or
        human. The parenthetical examples are the only thing that makes it matchable, so a
        taxonomy rendered without them is broken even though it still parses."""
        taxonomy = terminology.build_taxonomy(self.ROWS)
        self.assertIn("Trunk Lid and Compartment (e.g. Cargo Net", taxonomy.text)

    def test_shortest_names_are_offered_as_examples_first(self):
        """Shortest-first is a proxy for most generic. A deeply specific hardware term as the sole
        example is a worse hint than a plain one."""
        taxonomy = terminology.build_taxonomy(self.ROWS, examples_per_subcategory=1)
        self.assertIn("Trunk Lid and Compartment (e.g. Cargo Net)", taxonomy.text)

    def test_rows_missing_a_category_or_subcategory_are_dropped(self):
        """A pair Stage 1 could never name is a pair Stage 2 could never be routed to, and a None
        label would break rendering the taxonomy at all."""
        taxonomy = terminology.build_taxonomy(self.ROWS + [_term(6, "Orphan", None, None)])
        self.assertEqual(taxonomy.terminology_count, len(self.ROWS))

    def test_the_model_is_told_the_bare_name_but_a_decorated_one_still_parses(self):
        """Stage 1 is asked for the bare subcategory; stripping the parenthetical defensively is
        what keeps an echoed label from being scored as unroutable."""
        self.assertEqual(
            terminology._strip_example_hint("Trunk Lid and Compartment (e.g. Cargo Net, Cargo Box)"),
            "Trunk Lid and Compartment",
        )
        self.assertEqual(terminology._strip_example_hint("Engine"), "Engine")
        self.assertIsNone(terminology._strip_example_hint(None))


class _FakeClient:
    """Stands in for the OpenAI client. Fails any call carrying more than max_parts parts, which is
    what a truncated response looks like from here: valid request, unparseable answer."""

    def __init__(self, max_parts, min_tokens=0):
        self.max_parts = max_parts
        self.min_tokens = min_tokens
        self.batch_sizes = []
        self.budgets = []

    def complete_json(self, cli, system, user, max_tokens=1024, model=None, reasoning_effort=None):
        parts = json.loads(user)["parts"]
        self.batch_sizes.append(len(parts))
        self.budgets.append(max_tokens)
        if len(parts) > self.max_parts or max_tokens < self.min_tokens:
            return None, "JSON parse error: Unterminated string"
        return {"parts": [{"id": p["id"], "terminology_id": p["id"] * 10} for p in parts]}, None


class RetryTests(SimpleTestCase):
    databases = []

    def _run(self, fake, parts):
        with self.settings():
            original = terminology.qwen_llm.complete_json
            terminology.qwen_llm.complete_json = fake.complete_json
            try:
                return terminology.complete_json_with_retry(
                    None, "test-model", "system", {"parts": parts}, per_part_tokens=60
                )
            finally:
                terminology.qwen_llm.complete_json = original

    def test_halving_recurses_past_one_level(self):
        """One level of halving was not enough on real data -- 15 parts hard-failed in a 300-part
        run before this recursed. A batch of 16 that only succeeds at 2 needs three halvings."""
        fake = _FakeClient(max_parts=2)
        parsed, err = self._run(fake, [{"id": i} for i in range(16)])
        self.assertIsNone(err)
        self.assertEqual(len(parsed["parts"]), 16)
        self.assertEqual(min(fake.batch_sizes), 2)

    def test_a_single_part_that_cannot_be_parsed_gives_up_with_the_error(self):
        """There is nothing left to halve, so the error has to survive rather than becoming an
        empty success that would be written as a silent unclassifiable."""
        parsed, err = self._run(_FakeClient(max_parts=0), [{"id": 1}])
        self.assertIsNone(parsed)
        self.assertIn("JSON parse error", err)

    def test_one_poisoned_part_does_not_discard_its_healthy_half(self):
        """Partial recovery is the point: 63 good answers plus one failure beats throwing all 64
        away because a single part could not be generated."""
        class OnePoisoned(_FakeClient):
            def complete_json(self, cli, system, user, max_tokens=1024, model=None, reasoning_effort=None):
                parts = json.loads(user)["parts"]
                if any(p["id"] == 0 for p in parts):
                    return None, "JSON parse error"
                return {"parts": [{"id": p["id"], "terminology_id": p["id"]} for p in parts]}, None

        parsed, err = self._run(OnePoisoned(max_parts=1), [{"id": i} for i in range(8)])
        self.assertIsNone(err)
        self.assertEqual({p["id"] for p in parsed["parts"]}, {1, 2, 3, 4, 5, 6, 7})


    def test_halving_never_lowers_the_budget_below_the_flat_allowance(self):
        """The bug this floor fixes. A reasoning model spends a fixed ~800 tokens thinking before
        it writes any JSON, and halving the batch does not reduce that at all. Recomputing the
        child budget from the part count alone shrank it at every level, so a truncation caused by
        reasoning could never be recovered -- the retry burned 2^6 calls and still failed."""
        fake = _FakeClient(max_parts=64, min_tokens=terminology._RESPONSE_OVERHEAD_TOKENS)
        parsed, err = self._run(fake, [{"id": i} for i in range(8)])
        self.assertIsNone(err)
        self.assertGreaterEqual(min(fake.budgets), terminology._RESPONSE_OVERHEAD_TOKENS)

    def test_a_single_part_still_gets_the_whole_flat_allowance(self):
        """The bottom of the recursion is where the old arithmetic was worst: one part earned a
        300-token budget against a 240-token reasoning cost, leaving 60 for the answer."""
        self.assertGreater(terminology._token_budget(1, 60), terminology._RESPONSE_OVERHEAD_TOKENS)
        self.assertEqual(
            terminology._token_budget(10, 60) - terminology._token_budget(5, 60), 5 * 60
        )

class AssembleRowsTests(SimpleTestCase):
    databases = []

    PAIR = ("Air and Fuel Delivery", "Fuel Injection System and Related Components")

    def test_a_refusal_and_a_failure_are_different_statuses(self):
        """The whole reason this table has a status column. A null terminology because the model
        looked and refused is a usable answer; a null because the call blew up is a rerun."""
        routed = {self.PAIR: [{"id": 1, "stage1_reasoning": "fuel"}, {"id": 2, "stage1_reasoning": "fuel"}]}
        stage2 = {
            1: {"terminology_id": None, "confidence": 0.1, "reasoning": "no candidate is a fuel injector"},
            2: {"terminology_id": None, "confidence": 0.0, "error": "TimeoutError"},
        }
        rows = {r["master_part_id"]: r for r in terminology.assemble_rows(routed, [], stage2, "qwen")}
        self.assertEqual(rows[1]["status"], "unclassifiable")
        self.assertEqual(rows[2]["status"], "error")

    def test_every_part_gets_a_row_including_the_unroutable_ones(self):
        """A part with no row is indistinguishable from a part not yet processed, so a bulk run's
        failures have to stay queryable rather than being absences."""
        unroutable = [{"id": 3, "stage1_confidence": 0.0, "stage1_reasoning": "no signal in title"}]
        routed = {self.PAIR: [{"id": 4, "stage1_reasoning": "fuel", "stage1_confidence": 0.9}]}
        rows = terminology.assemble_rows(routed, unroutable, {4: {"terminology_id": 1, "confidence": 0.9}}, "qwen")
        self.assertEqual({r["master_part_id"] for r in rows}, {3, 4})

    def test_both_stages_reasoning_survives_into_one_auditable_string(self):
        """Every wrong Stage 2 result in testing traced back to a Stage 1 routing miss, so the
        routing rationale has to still be there when someone reads the row months later."""
        routed = {self.PAIR: [{"id": 5, "stage1_reasoning": "fuel system part"}]}
        stage2 = {5: {"terminology_id": 1, "confidence": 0.95, "reasoning": "exact match on Fuel Injector"}}
        row = terminology.assemble_rows(routed, [], stage2, "qwen")[0]
        self.assertEqual(row["reasoning"], "Stage 1: fuel system part | Stage 2: exact match on Fuel Injector")

    def test_confidence_is_coerced_into_what_the_column_can_hold(self):
        """The column is DecimalField(max_digits=3, decimal_places=2). A model that answers 0.923
        or a stray 1.5 should cost the precision, not the row -- confidence is advisory, the
        classification is the payload."""
        self.assertEqual(terminology._coerce_confidence(0.923), decimal.Decimal("0.92"))
        self.assertEqual(terminology._coerce_confidence("0.9"), decimal.Decimal("0.90"))
        self.assertEqual(terminology._coerce_confidence(1.5), decimal.Decimal("1.00"))
        self.assertEqual(terminology._coerce_confidence(-2), decimal.Decimal("0.00"))
        self.assertIsNone(terminology._coerce_confidence(None))
        self.assertIsNone(terminology._coerce_confidence("very confident"))

    def test_a_classified_row_carries_the_pair_it_was_routed_through(self):
        """Stage 1's pick is stored next to the final id specifically so a bad Stage 2 result can
        be traced back to a routing miss."""
        routed = {self.PAIR: [{"id": 6, "stage1_confidence": 0.8}]}
        row = terminology.assemble_rows(routed, [], {6: {"terminology_id": 42, "confidence": 0.9}}, "qwen")[0]
        self.assertEqual(row["status"], "classified")
        self.assertEqual(row["part_terminology_id"], 42)
        self.assertEqual(row["category"], "Air and Fuel Delivery")
        self.assertEqual(row["model_used"], "qwen")


class ResponseIdTests(SimpleTestCase):
    databases = []

    def test_a_string_id_is_still_matched_to_its_part(self):
        """Self-hosted models are looser about types than a hosted API. An answer keyed "101"
        instead of 101 is a good answer, and dropping it would write a false unclassifiable."""
        self.assertEqual(terminology._entry_id({"id": "101"}), 101)
        self.assertEqual(terminology._entry_id({"id": 101}), 101)

    def test_an_unusable_id_falls_through_to_the_missing_from_response_path(self):
        self.assertIsNone(terminology._entry_id({}))
        self.assertEqual(terminology._entry_id({"id": "part-101"}), "part-101")


class OversizedPoolTests(SimpleTestCase):
    databases = []

    def test_the_ceiling_sits_below_the_smallest_pool_that_failed_in_testing(self):
        """1,590 candidates worked against gpt-oss:120b and 1,919 came back empty, so the ceiling
        has to clear the first without reaching the second."""
        self.assertLessEqual(terminology.MAX_STAGE2_CANDIDATES, 1590)

    def test_a_pair_too_big_even_unpooled_becomes_an_error_row_naming_it(self):
        """The retry cannot rescue this -- halving the part batch leaves the candidate list exactly
        as big -- so the part needs a row a human can act on, not fifteen rejected requests. Only
        ('Body', 'Hardware, Fasteners and Fittings') is actually in this position."""
        routed = {("Body", "Hardware, Fasteners and Fittings"): [{"id": 9, "stage1_reasoning": "hardware"}]}
        stage2 = {9: {"terminology_id": None, "confidence": 0.0,
                      "error": "'Body' / 'Hardware, Fasteners and Fittings' has 3522 terms, over the 1500 ceiling even unpooled"}}
        row = terminology.assemble_rows(routed, [], stage2, "qwen")[0]
        self.assertEqual(row["status"], "error")
        self.assertEqual(row["subcategory"], "Hardware, Fasteners and Fittings")
        self.assertIn("3522 terms", row["reasoning"])

    def test_a_narrowed_part_says_so_in_its_reasoning(self):
        """Pooling is what makes a wrong-category Stage 1 pick recoverable. A part judged on its
        own pair alone did not get that safety net, and the row should not look like one that
        did."""
        routed = {("Brake", "Hardware, Fasteners and Fittings"): [
            {"id": 10, "stage1_reasoning": "brake hardware", "stage2_narrowed": 863}
        ]}
        stage2 = {10: {"terminology_id": 77, "confidence": 0.8, "reasoning": "matches"}}
        row = terminology.assemble_rows(routed, [], stage2, "qwen")[0]
        self.assertEqual(row["status"], "classified")
        self.assertIn("saw only this category's 863 terms", row["reasoning"])
