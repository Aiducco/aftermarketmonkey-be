"""
Tests for the Instantly -> FreshSales reply sync.

No database and no network. The repo's .env points at the production database, so a test that
needed Django's test-database machinery would try to create one on the production host; the sync is
therefore written with its decisions in pure functions, and those are what is pinned here.

Four of these are regression tests for places Instantly's **documentation is wrong**, each verified
against the live workspace on 2026-10-03. They are the tests most worth keeping: a future reader
with the docs open would otherwise "fix" the code back to the broken behaviour.

  * ``next_starting_after`` is returned on the last page too -> the walk must stop on an empty page
  * ``is_auto_reply`` is never returned -> auto-replies are derived from the interest label
  * the ``lt_interest_status`` filter is ignored -> leads are looked up one address at a time
  * only ``lead`` (an address) is returned, never ``lead_id`` -> the address is the join key
"""
import datetime
import unittest.mock as mock

from django.core.management import call_command
from django.test import SimpleTestCase, override_settings

from src import models as src_models
from src.integrations.clients.freshsales import client as freshsales_client
from src.integrations.clients.freshsales import exceptions as freshsales_exceptions
from src.integrations.clients.instantly import client as instantly_client
from src.integrations.clients.instantly import exceptions as instantly_exceptions
from src.integrations.services import instantly_freshsales_sync as sync
from src.management.commands import sync_instantly_replies as sync_command

INSTANTLY_SETTINGS = {
    "INSTANTLY_API_KEY": "test-key",
    "INSTANTLY_BASE_URL": "https://api.instantly.ai/api/v2",
    "INSTANTLY_TIMEOUT_SECONDS": 5,
}
FRESHSALES_SETTINGS = {
    "FRESHSALES_API_KEY": "test-key",
    "FRESHSALES_BUNDLE_ALIAS": "aftermarketscout",
    "FRESHSALES_TIMEOUT_SECONDS": 5,
}


def _email(**overrides):
    """An inbound email shaped exactly as the live API returns one -- null fields omitted, not null."""
    payload = {
        "id": "01a06c90-61e7-7ecc-87e1-9129b2e9857d",
        "timestamp_created": "2026-10-02T21:43:14.000Z",
        "timestamp_email": "2026-10-02T21:43:10.000Z",
        "thread_id": "f1d2c3b4-0000-4000-8000-000000000001",
        "campaign_id": "26e0d682-c7a5-40c8-8cad-087bfd3c41c7",
        "lead": "andrew@rhinoutah.com",
        "from_address_email": "andrew@rhinoutah.com",
        "from_address_json": [{"name": "Andrew Ortega", "address": "andrew@rhinoutah.com"}],
        "eaccount": "outreach@example.com",
        "subject": "Re: distributor portals",
        "body": {"text": "Sounds interesting, can you send pricing?", "html": "<p>...</p>"},
        "i_status": 1,
        "ue_type": 2,
    }
    payload.update(overrides)
    return payload


def _reply(**overrides):
    """An unsaved InstantlyReply. Never saved -- these tests do not touch a database."""
    fields = {
        "instantly_email_id": "01a06c90-61e7-7ecc-87e1-9129b2e9857d",
        "lead_email": "andrew@rhinoutah.com",
        "from_name": "Andrew Ortega",
        "eaccount": "outreach@example.com",
        "subject": "Re: distributor portals",
        "body_text": "Sounds interesting, can you send pricing?",
        "campaign_name": "Realtruck - 1/3 locations",
        "interest_status": 1,
        "is_positive": True,
        "email_timestamp": datetime.datetime(2026, 10, 2, 21, 43, 10, tzinfo=datetime.timezone.utc),
        "lead_payload": {
            "companyName": "Rhino Utah",
            "website": "https://rhinoutah.com",
            "phoneNumber": "801-555-0100",
            "City": "Salt Lake City",
            "State": "UT",
            "Zip": "84101",
            "Tier": "A",
            "Typology": "Off-Road",
            "Locations": "2",
        },
    }
    fields.update(overrides)
    return src_models.InstantlyReply(**fields)


class _FakeResponse(object):
    def __init__(self, status_code=200, payload=None, text=""):
        self.status_code = status_code
        self._payload = payload if payload is not None else {}
        self.text = text

    def json(self):
        if self._payload is _Unparseable:
            raise ValueError("not json")
        return self._payload


class _Unparseable(object):
    pass


def _preview_with(emails, synced_ids=(), contact_addresses=(), deal_addresses=()):
    """
    Run ``sync.preview`` over ``emails`` against a stubbed ``InstantlyReply`` manager.

    One helper rather than per-class mocks: ``preview`` makes four distinct queries, and three
    hand-rolled copies of that shape is three things to get subtly wrong.
    """
    client = mock.Mock()
    client.iter_received_emails.return_value = iter(emails)

    def fake_filter(**kwargs):
        result = mock.MagicMock()
        if "contact_synced_at__isnull" in kwargs:
            result.values_list.return_value = list(synced_ids)
        elif "freshsales_contact_id__isnull" in kwargs:
            result.values_list.return_value.distinct.return_value = list(contact_addresses)
        elif "freshsales_deal_id__isnull" in kwargs:
            result.values_list.return_value.distinct.return_value = list(deal_addresses)
        return result

    with mock.patch.object(src_models.InstantlyReply, "objects") as objects:
        objects.values_list.return_value = list(synced_ids)
        objects.filter.side_effect = fake_filter
        return sync.preview(client=client)


# -------------------------------------------------------------------------------------------
# Regression tests for the four documentation defects
# -------------------------------------------------------------------------------------------


@override_settings(**INSTANTLY_SETTINGS)
class PaginationTests(SimpleTestCase):
    databases = []

    def test_an_empty_page_ends_the_walk_even_though_a_cursor_is_still_offered(self):
        """
        Instantly returns ``next_starting_after`` on the LAST page as well, and following it yields
        zero items. A loop that trusts the cursor never terminates -- verified live: page 1 returned
        22 items with a cursor, page 2 returned 0 items with the cursor still present.
        """
        pages = [
            _FakeResponse(payload={"items": [_email(id="a")], "next_starting_after": "cursor-1"}),
            _FakeResponse(payload={"items": [], "next_starting_after": "cursor-2"}),
        ]
        client = instantly_client.InstantlyApiClient()
        # time.sleep is the /emails rate-limit pacing; stubbed so the suite does not pay 3.1s a page.
        with mock.patch.object(instantly_client, "_session") as session, mock.patch.object(
            instantly_client.time, "sleep"
        ):
            session.request.side_effect = pages
            emails = list(client.iter_received_emails())

        self.assertEqual([e["id"] for e in emails], ["a"])
        self.assertEqual(session.request.call_count, 2, "must not keep following the stale cursor")

    def test_pages_are_followed_while_items_keep_coming(self):
        pages = [
            _FakeResponse(payload={"items": [_email(id="a")], "next_starting_after": "c1"}),
            _FakeResponse(payload={"items": [_email(id="b")], "next_starting_after": "c2"}),
            _FakeResponse(payload={"items": []}),
        ]
        client = instantly_client.InstantlyApiClient()
        with mock.patch.object(instantly_client, "_session") as session, mock.patch.object(
            instantly_client.time, "sleep"
        ):
            session.request.side_effect = pages
            emails = list(client.iter_received_emails())

        self.assertEqual([e["id"] for e in emails], ["a", "b"])
        self.assertEqual(session.request.call_args_list[1].kwargs["params"]["starting_after"], "c1")

    def test_the_walk_asks_for_inbound_oldest_first(self):
        client = instantly_client.InstantlyApiClient()
        with mock.patch.object(instantly_client, "_session") as session:
            session.request.return_value = _FakeResponse(payload={"items": []})
            list(client.iter_received_emails(min_timestamp_created="2026-09-01T00:00:00+00:00"))

        params = session.request.call_args.kwargs["params"]
        self.assertEqual(params["email_type"], "received")
        # Oldest-first keeps the watermark behind what was stored, never ahead of what was skipped.
        self.assertEqual(params["sort_order"], "asc")
        self.assertEqual(params["min_timestamp_created"], "2026-09-01T00:00:00+00:00")


class AutoReplyDetectionTests(SimpleTestCase):
    databases = []

    def test_out_of_office_is_detected_from_the_interest_label(self):
        """``is_auto_reply`` is documented but never returned, so the label is the signal. Both
        out-of-office replies in the live account carry i_status=0."""
        self.assertTrue(sync.derive_is_auto_reply(0, "Out of office Re: partstech for offroad shops"))

    def test_an_unlabelled_auto_reply_is_caught_by_its_subject(self):
        """Backstop for an auto-reply Instantly has not labelled yet -- the label arrives late."""
        self.assertTrue(sync.derive_is_auto_reply(None, "Automatic reply: Re: distributor portals"))
        self.assertTrue(sync.derive_is_auto_reply(None, "Out of Office"))

    def test_a_real_reply_is_not_an_auto_reply(self):
        self.assertFalse(sync.derive_is_auto_reply(1, "Re: distributor portals"))
        self.assertFalse(sync.derive_is_auto_reply(None, "Re: switching distributors"))
        self.assertFalse(sync.derive_is_auto_reply(-1, "Re: not for us"))

    def test_an_email_with_no_auto_reply_field_at_all_still_resolves(self):
        """The live API omits the field entirely; row mapping must not depend on it."""
        payload = _email(i_status=0, subject="Out of office Re: x")
        self.assertNotIn("is_auto_reply", payload)
        self.assertTrue(sync.row_fields_from_email(payload)["is_auto_reply"])


@override_settings(**INSTANTLY_SETTINGS)
class LeadLookupTests(SimpleTestCase):
    databases = []

    def test_a_lead_is_looked_up_by_address_not_by_interest_filter(self):
        """
        The documented ``lt_interest_status`` filter is ignored by the live API: asking for
        "Interested" returned 100 leads of which 95 were unlabelled. So the re-check asks per
        address and never sends an interest filter.
        """
        client = instantly_client.InstantlyApiClient()
        with mock.patch.object(instantly_client, "_session") as session:
            session.request.return_value = _FakeResponse(
                payload={"items": [{"email": "andrew@rhinoutah.com", "lt_interest_status": 1}]}
            )
            lead = client.find_lead("andrew@rhinoutah.com")

        body = session.request.call_args.kwargs["json"]
        self.assertEqual(body, {"limit": 1, "search": "andrew@rhinoutah.com"})
        self.assertNotIn("lt_interest_status", body)
        self.assertEqual(lead["lt_interest_status"], 1)

    def test_a_search_hit_for_a_different_address_is_rejected(self):
        """``search`` matched exactly for every address tested, but a substring match would attach
        another shop's interest label to this reply."""
        client = instantly_client.InstantlyApiClient()
        with mock.patch.object(instantly_client, "_session") as session:
            session.request.return_value = _FakeResponse(
                payload={"items": [{"email": "someone@else.com", "lt_interest_status": 1}]}
            )
            self.assertIsNone(client.find_lead("andrew@rhinoutah.com"))


class JoinKeyTests(SimpleTestCase):
    databases = []

    def test_the_address_is_the_join_key_because_lead_id_is_never_returned(self):
        payload = _email()
        self.assertNotIn("lead_id", payload)
        self.assertEqual(sync.row_fields_from_email(payload)["lead_email"], "andrew@rhinoutah.com")

    def test_the_from_address_is_used_when_lead_is_absent(self):
        fields = sync.row_fields_from_email(_email(lead=None))
        self.assertEqual(fields["lead_email"], "andrew@rhinoutah.com")


# -------------------------------------------------------------------------------------------
# Interest labels
# -------------------------------------------------------------------------------------------


class InterestTests(SimpleTestCase):
    databases = []

    def test_only_interested_through_closed_are_positive(self):
        for status in (1, 2, 3, 4):
            self.assertTrue(sync.is_positive_interest(status), status)
        for status in (0, -1, -2, -3, -4, None):
            self.assertFalse(sync.is_positive_interest(status), status)

    def test_each_label_maps_to_the_contact_status_that_says_the_same_thing(self):
        self.assertEqual(sync.contact_status_name(1), "Interested")
        self.assertEqual(sync.contact_status_name(2), "Interested")
        self.assertEqual(sync.contact_status_name(4), "Interested")
        self.assertEqual(sync.contact_status_name(-1), "Unqualified")
        self.assertEqual(sync.contact_status_name(-2), "Unqualified")
        # Out of office and "not labelled yet" both mean we know nothing more than delivery.
        self.assertEqual(sync.contact_status_name(0), "Contacted")
        self.assertEqual(sync.contact_status_name(None), "Contacted")

    def test_an_unrecognised_label_is_not_positive(self):
        """Instantly's docs disagree with themselves on -4/No Show. An unknown value must never
        open a deal on its own."""
        self.assertFalse(sync.is_positive_interest(99))
        self.assertEqual(sync.contact_status_name(99), "Contacted")


# -------------------------------------------------------------------------------------------
# Names, companies, timestamps
# -------------------------------------------------------------------------------------------


class DisplayNameTests(SimpleTestCase):
    databases = []

    def test_a_persons_name_is_split(self):
        self.assertEqual(sync.split_display_name("Miguel Bautista"), ("Miguel", "Bautista"))
        self.assertEqual(sync.split_display_name("Andrew Ortega"), ("Andrew", "Ortega"))

    def test_a_business_display_name_leaves_both_names_blank(self):
        """
        "Sundowner Truck Accessories" is a real From-header name from the account. Split, it puts
        "Sundowner Truck" in front of whoever opens the CRM record. Blank is the safe failure --
        the same reason export_instantly_list.py leaves First/Last empty on outbound mail.
        """
        self.assertEqual(sync.split_display_name("Sundowner Truck Accessories"), ("", ""))
        self.assertEqual(sync.split_display_name("ATO Autosports"), ("", ""))
        self.assertEqual(sync.split_display_name("Bucks 4x4"), ("", ""))

    def test_a_display_name_equal_to_the_company_is_not_a_person(self):
        self.assertEqual(sync.split_display_name("Rhino Utah", company_name="Rhino Utah"), ("", ""))
        self.assertEqual(sync.split_display_name("rhino utah", company_name="Rhino Utah"), ("", ""))

    def test_four_or_more_words_is_treated_as_a_business(self):
        self.assertEqual(sync.split_display_name("Smith Brothers Jeep Outfitters"), ("", ""))

    def test_a_4x4_fragment_inside_a_word_marks_a_business(self):
        """
        "Clarksville MC4x4 Sales" is a real From-header name from the account. It is three words,
        and neither "Clarksville" nor "MC4x4" is a listed business token, so without the substring
        rule it split into first="Clarksville" last="MC4x4 Sales" -- a shop addressed as a person.
        """
        self.assertEqual(sync.split_display_name("Clarksville MC4x4 Sales"), ("", ""))
        self.assertEqual(sync.split_display_name("Bucks 4WD"), ("", ""))

    def test_a_person_with_an_ordinary_surname_is_still_split(self):
        """The business-token list has to stay narrow enough not to swallow real people."""
        for name, expected in (
            ("Miguel Bautista", ("Miguel", "Bautista")),
            ("Axel Rost", ("Axel", "Rost")),
            ("Glenn Garman", ("Glenn", "Garman")),
            ("Mitchell Carter", ("Mitchell", "Carter")),
            ("Zac L", ("Zac", "L")),
        ):
            self.assertEqual(sync.split_display_name(name), expected, name)

    def test_a_mononym_keeps_a_blank_surname(self):
        self.assertEqual(sync.split_display_name("Butch"), ("Butch", ""))

    def test_a_missing_display_name_is_blank_not_guessed(self):
        self.assertEqual(sync.split_display_name(""), ("", ""))
        self.assertEqual(sync.split_display_name(None), ("", ""))

    def test_the_display_name_comes_from_the_from_header(self):
        self.assertEqual(sync.display_name_of(_email()), "Andrew Ortega")
        self.assertEqual(sync.display_name_of(_email(from_address_json=[])), "")
        self.assertEqual(sync.display_name_of({}), "")


@override_settings(FRESHSALES_CREATE_DEALS=True)
class PreviewTests(SimpleTestCase):
    """
    The dry run's counts are what someone checks the CRM against after the first real run, so they
    have to mean "people and opportunities", not "rows processed".

    Deals are enabled here: these assertions are about one-deal-per-address dedupe, which only
    means anything when deals are being created at all. The default-off behaviour is pinned in
    DealsAreManualByDefaultTests.
    """

    databases = []

    @staticmethod
    def _preview(emails):
        return _preview_with(emails)

    def test_repeat_replies_from_one_address_are_one_contact_and_one_deal(self):
        """Three of the live account's addresses replied more than once; 22 replies are 13 people."""
        rows, counts = self._preview(
            [
                _email(id="1", lead="andrew@rhinoutah.com", i_status=1),
                _email(id="2", lead="andrew@rhinoutah.com", i_status=1),
                _email(id="3", lead="andrew@rhinoutah.com", i_status=1),
            ]
        )
        self.assertEqual(counts["replies"], 3)
        self.assertEqual(counts["would_create_contacts"], 1)
        self.assertEqual(counts["would_create_deals"], 1)
        self.assertEqual([r["action"] for r in rows[1:]], ["note on existing contact"] * 2)

    def test_an_auto_reply_is_skipped_and_never_opens_a_deal(self):
        rows, counts = self._preview(
            [_email(id="1", lead="mitch@dasmule.com", i_status=0, subject="Out of office Re: x")]
        )
        self.assertEqual(counts["auto_replies"], 1)
        self.assertEqual(counts["would_create_contacts"], 0)
        self.assertEqual(counts["would_create_deals"], 0)
        self.assertIn("auto-reply", rows[0]["action"])

    def test_a_reply_with_no_address_is_reported_not_pushed(self):
        rows, counts = self._preview([_email(id="1", lead=None, from_address_email=None)])
        self.assertEqual(counts["no_email"], 1)
        self.assertEqual(counts["would_create_contacts"], 0)

    def test_a_negative_reply_is_a_contact_but_not_a_deal(self):
        _, counts = self._preview([_email(id="1", lead="parts@ttspecbf.com", i_status=-1)])
        self.assertEqual(counts["would_create_contacts"], 1)
        self.assertEqual(counts["would_create_deals"], 0)

    def test_an_already_stored_reply_is_marked_not_new(self):
        client = mock.Mock()
        client.iter_received_emails.return_value = iter([_email(id="already-there")])
        with mock.patch.object(src_models.InstantlyReply, "objects") as objects:
            objects.values_list.return_value = ["already-there"]
            rows, counts = sync.preview(client=client)
        self.assertEqual(counts["already_stored"], 1)
        self.assertEqual(counts["new"], 0)
        self.assertFalse(rows[0]["new"])


class LeadPayloadHydrationTests(SimpleTestCase):
    """
    ``GET /emails`` carries no shop details -- company, city, phone and the rest live on the lead.
    So every reply needs its lead read once, including one that arrived already labelled positive
    (12 of the live account's 22 did). Getting this wrong is not subtle but it is quiet: the shop
    still reaches the CRM, just as a sales account named after a gmail address.
    """

    databases = []

    def test_a_positive_reply_with_no_payload_is_still_selected(self):
        captured = {}

        def fake_filter(*args, **kwargs):
            captured.setdefault("q_objects", []).extend(args)
            return mock.MagicMock(exclude=fake_filter, filter=fake_filter, order_by=lambda *a: [])

        with mock.patch.object(src_models.InstantlyReply, "objects") as objects:
            objects.filter.side_effect = fake_filter
            sync.refresh_interest(client=mock.Mock(), recheck_days=60, recheck_hours=6)

        # The selection must be "stale label OR missing payload", so is_positive=True cannot on its
        # own exclude a reply whose payload has never been fetched.
        rendered = " ".join(str(q) for q in captured.get("q_objects", []))
        self.assertIn("lead_payload", rendered)
        self.assertIn("OR", rendered.upper())

    def test_a_freshsales_id_is_sent_as_the_integer_the_api_issued(self):
        self.assertEqual(sync._as_id("127000525268"), 127000525268)
        self.assertEqual(sync._as_id("abc"), "abc")
        self.assertIsNone(sync._as_id(None))


class NotConfiguredTests(SimpleTestCase):
    """
    The cron will hit this state for real: the code deploys through CI, the keys are set on the box
    by hand afterwards. Raising in that window would write a FAILED audit row and send cron mail
    every 15 minutes, which buries a genuine failure in identical noise.
    """

    databases = []

    @override_settings(INSTANTLY_API_KEY="", FRESHSALES_API_KEY="", FRESHSALES_BUNDLE_ALIAS="")
    def test_every_missing_setting_is_named(self):
        self.assertEqual(
            sync_command.Command._missing_settings(),
            ["INSTANTLY_API_KEY", "FRESHSALES_API_KEY", "FRESHSALES_BUNDLE_ALIAS"],
        )

    @override_settings(INSTANTLY_API_KEY="set", FRESHSALES_API_KEY="", FRESHSALES_BUNDLE_ALIAS="set")
    def test_a_partial_configuration_names_only_what_is_missing(self):
        self.assertEqual(sync_command.Command._missing_settings(), ["FRESHSALES_API_KEY"])

    @override_settings(**dict(INSTANTLY_SETTINGS, **FRESHSALES_SETTINGS))
    def test_a_complete_configuration_does_not_skip(self):
        self.assertEqual(sync_command.Command._missing_settings(), [])

    @override_settings(INSTANTLY_API_KEY="", FRESHSALES_API_KEY="", FRESHSALES_BUNDLE_ALIAS="")
    def test_an_unconfigured_run_skips_without_calling_either_vendor(self):
        with mock.patch.object(sync_command.audit_scheduled_tasks, "start_scheduled_task_execution"), mock.patch.object(
            sync_command.audit_scheduled_tasks, "mark_scheduled_task_skipped"
        ) as skipped, mock.patch.object(sync_command.instantly_freshsales_sync, "run") as run:
            call_command("sync_instantly_replies")
        run.assert_not_called()
        self.assertIn("Not configured", skipped.call_args.kwargs["message"])


class CompanyNameTests(SimpleTestCase):
    databases = []

    def test_instantlys_company_variable_wins(self):
        self.assertEqual(
            sync.company_name_for("andrew@rhinoutah.com", {"companyName": "Rhino Utah"}),
            "Rhino Utah",
        )

    def test_the_domain_is_the_fallback(self):
        self.assertEqual(sync.company_name_for("parts@ttspecbf.com", {}), "ttspecbf.com")

    def test_a_free_mailbox_never_becomes_the_company(self):
        """Fourteen unrelated shops replied from gmail in the live account. An account named
        "gmail.com" would collect all of them under one company."""
        self.assertEqual(sync.company_name_for("flatoutauto84@gmail.com", {}), "flatoutauto84@gmail.com")

    def test_an_empty_company_variable_falls_through(self):
        self.assertEqual(sync.company_name_for("parts@ttspecbf.com", {"companyName": "   "}), "ttspecbf.com")


class TimestampTests(SimpleTestCase):
    databases = []

    def test_instantlys_z_suffix_parses_to_aware_utc(self):
        parsed = sync.parse_timestamp("2026-10-02T21:43:14.000Z")
        self.assertEqual(parsed.year, 2026)
        self.assertEqual(parsed.utcoffset(), datetime.timedelta(0))

    def test_junk_is_none_rather_than_an_exception(self):
        """A single malformed timestamp must not take down a whole ingest pass."""
        self.assertIsNone(sync.parse_timestamp("not a date"))
        self.assertIsNone(sync.parse_timestamp(None))
        self.assertIsNone(sync.parse_timestamp(""))


# -------------------------------------------------------------------------------------------
# Row and CRM payload mapping
# -------------------------------------------------------------------------------------------


class RowMappingTests(SimpleTestCase):
    databases = []

    def test_an_email_maps_onto_the_row(self):
        fields = sync.row_fields_from_email(
            _email(), campaign_names={"26e0d682-c7a5-40c8-8cad-087bfd3c41c7": "Realtruck - 1/3 locations"}
        )
        self.assertEqual(fields["instantly_email_id"], "01a06c90-61e7-7ecc-87e1-9129b2e9857d")
        self.assertEqual(fields["lead_email"], "andrew@rhinoutah.com")
        self.assertEqual(fields["from_name"], "Andrew Ortega")
        self.assertEqual(fields["campaign_name"], "Realtruck - 1/3 locations")
        self.assertEqual(fields["body_text"], "Sounds interesting, can you send pricing?")
        self.assertTrue(fields["is_positive"])
        self.assertFalse(fields["is_auto_reply"])

    def test_a_text_only_body_still_yields_text(self):
        """Two of the 22 live replies have a text body and no html."""
        fields = sync.row_fields_from_email(_email(body={"text": "no html here"}))
        self.assertEqual(fields["body_text"], "no html here")

    def test_the_content_preview_is_the_last_resort_for_a_body(self):
        fields = sync.row_fields_from_email(_email(body={}, content_preview="first lines only"))
        self.assertEqual(fields["body_text"], "first lines only")

    def test_an_unknown_campaign_leaves_the_name_empty_rather_than_guessing(self):
        fields = sync.row_fields_from_email(_email(), campaign_names={})
        self.assertIsNone(fields["campaign_name"])


class CrmPayloadTests(SimpleTestCase):
    databases = []

    def test_contact_fields_carry_the_shop_context_instantly_gave_back(self):
        fields = sync.contact_fields(_reply(), contact_status_id=127004315050)
        self.assertEqual(fields["first_name"], "Andrew")
        self.assertEqual(fields["last_name"], "Ortega")
        self.assertEqual(fields["contact_status_id"], 127004315050)
        self.assertEqual(fields["city"], "Salt Lake City")
        self.assertEqual(fields["state"], "UT")
        self.assertEqual(fields["zipcode"], "84101")
        self.assertEqual(fields["work_number"], "801-555-0100")

    def test_empty_contact_fields_are_omitted_not_sent_as_blanks(self):
        """Sending "" would overwrite a value a human has since typed into the CRM."""
        fields = sync.contact_fields(_reply(from_name="", lead_payload={}), contact_status_id=1)
        self.assertNotIn("first_name", fields)
        self.assertNotIn("city", fields)
        self.assertEqual(fields["contact_status_id"], 1)

    def test_sales_account_fields_come_from_the_same_payload(self):
        fields = sync.sales_account_fields(_reply())
        self.assertEqual(fields["website"], "https://rhinoutah.com")
        self.assertEqual(fields["city"], "Salt Lake City")

    def test_the_note_carries_the_reply_and_how_to_read_it(self):
        description = sync.note_description(_reply())
        self.assertIn("Sounds interesting, can you send pricing?", description)
        self.assertIn("Realtruck - 1/3 locations", description)
        self.assertIn("Interested", description)
        self.assertIn("andrew@rhinoutah.com", description)
        self.assertIn("Off-Road", description)

    def test_an_unlabelled_reply_says_so_in_its_note(self):
        description = sync.note_description(_reply(interest_status=None, is_positive=False))
        self.assertIn("unlabelled", description)

    def test_the_deal_is_named_for_the_shop_and_campaign(self):
        self.assertEqual(sync.deal_name(_reply()), "Rhino Utah — Realtruck - 1/3 locations")

    def test_a_deal_without_a_campaign_name_still_reads_sensibly(self):
        self.assertEqual(sync.deal_name(_reply(campaign_name=None)), "Rhino Utah — Instantly reply")


# -------------------------------------------------------------------------------------------
# Transport error handling
# -------------------------------------------------------------------------------------------


@override_settings(**INSTANTLY_SETTINGS)
class InstantlyTransportTests(SimpleTestCase):
    databases = []

    def test_a_rejected_key_is_an_auth_error_naming_the_endpoint(self):
        """401 and 403 are indistinguishable between "wrong key" and "key lacks the scope", so the
        message has to name the endpoint for the second case to be diagnosable."""
        client = instantly_client.InstantlyApiClient()
        for status_code in (401, 403):
            with mock.patch.object(instantly_client, "_session") as session:
                session.request.return_value = _FakeResponse(status_code=status_code)
                with self.assertRaises(instantly_exceptions.InstantlyAuthError) as caught:
                    client.find_lead("andrew@rhinoutah.com")
            self.assertIn("leads/list", str(caught.exception))

    def test_a_429_is_its_own_error_so_the_caller_can_stop_instead_of_retrying(self):
        client = instantly_client.InstantlyApiClient()
        with mock.patch.object(instantly_client, "_session") as session:
            session.request.return_value = _FakeResponse(status_code=429)
            with self.assertRaises(instantly_exceptions.InstantlyRateLimited):
                client.find_lead("andrew@rhinoutah.com")

    def test_an_unparseable_body_is_an_api_exception(self):
        client = instantly_client.InstantlyApiClient()
        with mock.patch.object(instantly_client, "_session") as session:
            session.request.return_value = _FakeResponse(payload=_Unparseable)
            with self.assertRaises(instantly_exceptions.InstantlyAPIException):
                client.find_lead("andrew@rhinoutah.com")

    @override_settings(INSTANTLY_API_KEY="")
    def test_a_missing_key_fails_before_any_request_is_made(self):
        with mock.patch.object(instantly_client, "_session") as session:
            with self.assertRaises(ValueError):
                instantly_client.InstantlyApiClient()
        session.request.assert_not_called()


@override_settings(**FRESHSALES_SETTINGS)
class FreshsalesTransportTests(SimpleTestCase):
    databases = []

    def test_the_base_url_is_built_from_the_bundle_alias(self):
        client = freshsales_client.FreshsalesApiClient()
        self.assertEqual(client.base_url, "https://aftermarketscout.myfreshworks.com/crm/sales/api")

    def test_the_auth_header_is_token_not_bearer(self):
        client = freshsales_client.FreshsalesApiClient()
        with mock.patch.object(freshsales_client, "_session") as session:
            session.request.return_value = _FakeResponse(payload={"contact_statuses": []})
            client.contact_status_ids_by_name()
        headers = session.request.call_args.kwargs["headers"]
        self.assertEqual(headers["Authorization"], "Token token=test-key")

    def test_a_201_means_created_and_a_200_means_updated(self):
        """Both are success. The distinction is reported because a 200 on a contact we believed was
        new means that shop reached the CRM by some other route."""
        client = freshsales_client.FreshsalesApiClient()
        for status_code, expected_created in ((201, True), (200, False)):
            with mock.patch.object(freshsales_client, "_session") as session:
                session.request.return_value = _FakeResponse(status_code=status_code, payload={"contact": {"id": 42}})
                contact_id, created = client.upsert_contact("a@b.com", {"first_name": "A"})
            self.assertEqual(contact_id, "42")
            self.assertEqual(created, expected_created)

    def test_a_contact_is_keyed_on_its_email(self):
        client = freshsales_client.FreshsalesApiClient()
        with mock.patch.object(freshsales_client, "_session") as session:
            session.request.return_value = _FakeResponse(payload={"contact": {"id": 7}})
            client.upsert_contact("andrew@rhinoutah.com", {"first_name": "Andrew"})
        body = session.request.call_args.kwargs["json"]
        self.assertEqual(body["unique_identifier"], {"emails": "andrew@rhinoutah.com"})
        self.assertEqual(body["contact"]["first_name"], "Andrew")

    def test_a_deal_carries_the_three_fields_the_api_requires(self):
        """name, amount and sales_account_id are all mandatory -- a deal cannot be created without
        an account, which is why the sync upserts one first."""
        client = freshsales_client.FreshsalesApiClient()
        with mock.patch.object(freshsales_client, "_session") as session:
            session.request.return_value = _FakeResponse(payload={"deal": {"id": 9}})
            client.create_deal(name="Rhino Utah — Realtruck", amount=0, sales_account_id="5", contact_id="7")
        deal = session.request.call_args.kwargs["json"]["deal"]
        self.assertEqual(deal["name"], "Rhino Utah — Realtruck")
        self.assertEqual(deal["amount"], 0)
        self.assertEqual(deal["sales_account_id"], "5")
        # Pipeline, stage and owner are deliberately absent so the CRM applies its own defaults.
        self.assertNotIn("deal_stage_id", deal)
        self.assertNotIn("deal_pipeline_id", deal)
        self.assertNotIn("owner_id", deal)

    def test_a_renamed_contact_status_fails_loudly(self):
        """Writing a hardcoded numeric id keeps succeeding after the status it referred to has been
        renamed or repurposed, which is worse than raising."""
        client = freshsales_client.FreshsalesApiClient()
        with self.assertRaises(freshsales_exceptions.FreshsalesConfigError) as caught:
            client.resolve_contact_status_id("Interested", {"New": 1, "Contacted": 2})
        self.assertIn("Interested", str(caught.exception))
        self.assertIn("New", str(caught.exception))

    def test_a_status_present_by_name_resolves_to_its_id(self):
        client = freshsales_client.FreshsalesApiClient()
        self.assertEqual(
            client.resolve_contact_status_id("Interested", {"Interested": 127004315050}),
            127004315050,
        )

    def test_a_403_mentions_the_permission_possibility(self):
        """The key inherits its owner's permissions -- GET /selector/owners already 403s for the key
        in use -- so a 403 is as likely to be a permission gap as a bad key."""
        client = freshsales_client.FreshsalesApiClient()
        with mock.patch.object(freshsales_client, "_session") as session:
            session.request.return_value = _FakeResponse(status_code=403)
            with self.assertRaises(freshsales_exceptions.FreshsalesAuthError) as caught:
                client.contact_status_ids_by_name()
        self.assertIn("permission", str(caught.exception).lower())

    def test_an_error_body_is_surfaced_because_it_names_the_offending_field(self):
        client = freshsales_client.FreshsalesApiClient()
        with mock.patch.object(freshsales_client, "_session") as session:
            session.request.return_value = _FakeResponse(
                status_code=400, payload={}, text='{"errors":{"amount":["can\'t be blank"]}}'
            )
            with self.assertRaises(freshsales_exceptions.FreshsalesAPIException) as caught:
                client.create_deal(name="x", amount=0, sales_account_id="1")
        self.assertIn("amount", str(caught.exception))

    def test_calls_are_counted_so_a_run_can_stay_inside_the_hourly_budget(self):
        client = freshsales_client.FreshsalesApiClient()
        with mock.patch.object(freshsales_client, "_session") as session:
            session.request.return_value = _FakeResponse(payload={"contact_statuses": []})
            client.contact_status_ids_by_name()
            client.contact_status_ids_by_name()
        self.assertEqual(client.calls_made, 2)

    @override_settings(FRESHSALES_BUNDLE_ALIAS="")
    def test_a_missing_bundle_alias_fails_with_a_message_that_says_where_to_find_it(self):
        with self.assertRaises(ValueError) as caught:
            freshsales_client.FreshsalesApiClient()
        self.assertIn("myfreshworks.com", str(caught.exception))


@override_settings(FRESHSALES_CREATE_DEALS=True)
class PreviewSubtractsCompletedWorkTests(SimpleTestCase):
    """
    The dry run answers "what happens if I run this". After the backfill the honest answer is
    "nothing", so the preview has to subtract replies already pushed rather than re-propose them.

    Deals enabled, so that "already has a deal" is actually exercised.
    """

    databases = []

    @staticmethod
    def _preview(emails, synced_ids=(), contact_addresses=(), deal_addresses=()):
        return _preview_with(emails, synced_ids, contact_addresses, deal_addresses)

    def test_an_already_synced_reply_proposes_nothing(self):
        rows, counts = self._preview(
            [_email(id="done", lead="andrew@rhinoutah.com", i_status=1)],
            synced_ids=["done"],
            contact_addresses=["andrew@rhinoutah.com"],
            deal_addresses=["andrew@rhinoutah.com"],
        )
        self.assertEqual(counts["already_synced"], 1)
        self.assertEqual(counts["would_create_contacts"], 0)
        self.assertEqual(counts["would_create_deals"], 0)
        self.assertEqual(rows[0]["action"], "already synced")

    def test_a_new_reply_from_a_shop_already_in_the_crm_is_a_note_not_a_second_deal(self):
        _, counts = self._preview(
            [_email(id="fresh", lead="andrew@rhinoutah.com", i_status=1)],
            synced_ids=[],
            contact_addresses=["andrew@rhinoutah.com"],
            deal_addresses=["andrew@rhinoutah.com"],
        )
        self.assertEqual(counts["would_create_contacts"], 0)
        self.assertEqual(counts["would_create_deals"], 0)

    def test_a_genuinely_new_reply_is_still_proposed(self):
        _, counts = self._preview(
            [_email(id="fresh", lead="someone@newshop.com", i_status=1)],
            synced_ids=["other"],
        )
        self.assertEqual(counts["would_create_contacts"], 1)
        self.assertEqual(counts["would_create_deals"], 1)


class DealsAreManualByDefaultTests(SimpleTestCase):
    """
    Deals are opened by hand. The sync records Instantly's verdict and sets the contact's FreshSales
    status from it, so "Interested" is the queue to work from -- but it does not open the
    opportunity, because an auto-created deal in a shared pipeline is somebody else's forecast.
    """

    databases = []

    def test_the_default_is_off(self):
        from django.conf import settings

        self.assertFalse(settings.FRESHSALES_CREATE_DEALS)

    @override_settings(FRESHSALES_CREATE_DEALS=False)
    def test_the_preview_does_not_promise_a_deal_when_disabled(self):
        rows, counts = _preview_with([_email(id="a", lead="andrew@rhinoutah.com", i_status=1)])

        self.assertEqual(counts["would_create_deals"], 0)
        self.assertNotIn("deal", rows[0]["action"])
        # The contact is still created, and still labelled Interested.
        self.assertEqual(counts["would_create_contacts"], 1)
        self.assertEqual(rows[0]["status"], "Interested")

    @override_settings(FRESHSALES_CREATE_DEALS=True)
    def test_the_preview_promises_a_deal_when_explicitly_enabled(self):
        _, counts = _preview_with([_email(id="a", lead="andrew@rhinoutah.com", i_status=1)])

        self.assertEqual(counts["would_create_deals"], 1)

    def test_a_positive_reply_still_maps_to_the_interested_contact_status(self):
        """The signal a human needs in order to create the deal by hand."""
        self.assertEqual(sync.contact_status_name(1), "Interested")
        self.assertEqual(sync.contact_status_name(2), "Interested")
