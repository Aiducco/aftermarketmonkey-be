"""
Tests for mirroring platform signups into FreshSales.

No database and no network -- the repo's .env points at the production database, so the decisions
live in pure functions and those are what is pinned here. See
src/integrations/tests/test_instantly_freshsales_sync.py for the same arrangement.

The cases that matter most are the exclusion rules and the collision with the Instantly sync: both
are about not putting the wrong thing in front of a human, and both were derived from real rows on
production rather than invented.
"""
import datetime
import unittest.mock as mock

from django.contrib.auth.models import User
from django.test import SimpleTestCase, override_settings

from src import models as src_models
from src.integrations.services import platform_crm_sync as sync

INTERNAL_DOMAINS = frozenset(
    {"aftermarketscout.com", "test.com", "example.com", "pentest.local", "m.com", "dmzapps.com", "tridentatx.com"}
)


def _company(**overrides):
    fields = {
        "id": 42,
        "name": "Frontline Off-Road",
        "slug": "frontline-off-road",
        "status": 1,
        "status_name": "ACTIVE",
        "onboarding_step": 4,
        "city": "Tulsa",
        "state_province": "OK",
        "postal_code": "74101",
        "country": "United States",
        "business_type": ["retail_store", "dealership"],
        "subscription_plan": None,
        "is_internal": False,
    }
    fields.update(overrides)
    return src_models.Company(**fields)


def _profile(company=None, **overrides):
    user_fields = {
        "email": "frontlineoutfitter@gmail.com",
        "first_name": "Michael",
        "last_name": "Ponder",
        "date_joined": datetime.datetime(2026, 9, 13, 10, 0, tzinfo=datetime.timezone.utc),
    }
    user_fields.update(overrides.pop("user", {}))
    fields = {"user": User(**user_fields), "company": company or _company(), "is_company_admin": True, "role": "owner"}
    fields.update(overrides)
    return src_models.UserProfile(**fields)


@override_settings(FRESHSALES_INTERNAL_EMAIL_DOMAINS=INTERNAL_DOMAINS)
class InternalExclusionTests(SimpleTestCase):
    """
    Our own staff, demo, support and pentest accounts must never reach the CRM. Every address below
    is real: these are the domains actually present on production.
    """

    databases = []

    def test_our_own_domains_are_internal(self):
        for email in (
            "gojko@aftermarketscout.com",
            "support@aftermarketscout.com",
            "turn14-support@aftermarketscout.com",
            "test@dmzapps.com",
            "gojko@test.com",
            "second-user@example.com",
            "auto.pentest@pentest.local",
            "adam@m.com",
            "info@tridentatx.com",
        ):
            self.assertTrue(sync.is_internal_email(email), email)

    def test_customer_domains_are_not(self):
        for email in (
            "frontlineoutfitter@gmail.com",
            "adam@thoroffroadtx.com",
            "michael.lunsford.1.ctr@us.af.mil",
            "jj@icatx.com",
            "perry@reconoo.com",
        ):
            self.assertFalse(sync.is_internal_email(email), email)

    def test_case_and_whitespace_do_not_defeat_the_check(self):
        self.assertTrue(sync.is_internal_email("  Gojko@AfterMarketScout.COM  "))

    def test_a_missing_address_is_not_internal(self):
        """It is simply not syncable -- the queryset excludes empty emails before this is reached."""
        self.assertFalse(sync.is_internal_email(""))
        self.assertFalse(sync.is_internal_email(None))


class AccountAndContactFieldTests(SimpleTestCase):
    databases = []

    def test_the_account_carries_the_address_onboarding_collected(self):
        fields = sync.account_fields(_company())
        self.assertEqual(fields["name"], "Frontline Off-Road")
        self.assertEqual(fields["city"], "Tulsa")
        self.assertEqual(fields["state"], "OK")
        self.assertEqual(fields["zipcode"], "74101")
        self.assertEqual(fields["country"], "United States")

    def test_blank_account_fields_are_omitted_not_sent_empty(self):
        """A company that skipped the address step must not blank out a CRM record a human filled."""
        fields = sync.account_fields(_company(city=None, state_province="", postal_code=None, country=None))
        self.assertEqual(set(fields), {"name"})

    def test_the_contact_uses_the_name_the_person_typed(self):
        fields = sync.contact_fields(_profile(), contact_status_id=127004315052)
        self.assertEqual(fields["first_name"], "Michael")
        self.assertEqual(fields["last_name"], "Ponder")
        self.assertEqual(fields["contact_status_id"], 127004315052)
        self.assertEqual(fields["job_title"], "Owner")

    def test_a_role_slug_becomes_a_readable_job_title(self):
        fields = sync.contact_fields(_profile(role="parts_manager"), contact_status_id=1)
        self.assertEqual(fields["job_title"], "Parts Manager")

    def test_a_user_with_no_name_sends_no_name(self):
        profile = _profile(user={"email": "a@b.com", "first_name": "", "last_name": ""})
        fields = sync.contact_fields(profile, contact_status_id=1)
        self.assertNotIn("first_name", fields)
        self.assertNotIn("last_name", fields)
        self.assertEqual(fields["contact_status_id"], 1)

    def test_a_profile_with_no_role_omits_the_job_title(self):
        fields = sync.contact_fields(_profile(role=None), contact_status_id=1)
        self.assertNotIn("job_title", fields)


class NoteTests(SimpleTestCase):
    databases = []

    def test_the_note_says_why_the_contact_exists(self):
        text = sync.note_description(_profile())
        self.assertIn("Signed up on the AfterMarketScout platform", text)
        self.assertIn("Frontline Off-Road", text)
        self.assertIn("frontlineoutfitter@gmail.com", text)
        self.assertIn("Michael Ponder", text)
        self.assertIn("step 4 of 4", text)
        self.assertIn("retail store, dealership", text)

    def test_a_paying_company_shows_its_plan_and_billing_state(self):
        company = _company(subscription_plan="hunter", subscription_status="active")
        text = sync.note_description(_profile(company=company))
        self.assertIn("hunter", text)
        self.assertIn("active", text)

    def test_a_free_company_says_none_rather_than_blank(self):
        self.assertIn("Plan:       none", sync.note_description(_profile()))


class CollisionWithInstantlyTests(SimpleTestCase):
    """
    frontlineoutfitter@gmail.com is a real case: an Instantly reply labelled Interested *and* a
    completed signup. Instantly knew the shop as "Frontline Outfitters", the platform knows it as
    "Frontline Off-Road". One business must stay one sales account.
    """

    databases = []

    def test_an_account_from_an_instantly_reply_is_reused(self):
        with mock.patch.object(src_models.InstantlyReply, "objects") as objects:
            objects.filter.return_value.values_list.return_value.first.return_value = "127015863079"
            self.assertEqual(sync._reusable_account_id(["frontlineoutfitter@gmail.com"]), "127015863079")

    def test_a_shop_with_no_instantly_history_gets_a_fresh_account(self):
        with mock.patch.object(src_models.InstantlyReply, "objects") as objects:
            objects.filter.return_value.values_list.return_value.first.return_value = None
            self.assertIsNone(sync._reusable_account_id(["adam@thoroffroadtx.com"]))

    def test_no_addresses_means_nothing_to_reuse(self):
        self.assertIsNone(sync._reusable_account_id([]))

    def test_a_synced_signup_is_recognised_as_a_platform_contact(self):
        """What stops the Instantly sync demoting a signup from Qualified back to a Lead status."""
        with mock.patch.object(src_models.UserProfile, "objects") as objects:
            objects.filter.return_value.exists.return_value = True
            self.assertTrue(sync.is_platform_contact("frontlineoutfitter@gmail.com"))
            # Matched case-insensitively -- CRM and auth records disagree on casing often enough.
            self.assertEqual(objects.filter.call_args.kwargs["user__email__iexact"], "frontlineoutfitter@gmail.com")

    def test_an_unknown_address_is_not_a_platform_contact(self):
        with mock.patch.object(src_models.UserProfile, "objects") as objects:
            objects.filter.return_value.exists.return_value = False
            self.assertFalse(sync.is_platform_contact("parts@ttspecbf.com"))

    def test_an_empty_address_never_queries(self):
        with mock.patch.object(src_models.UserProfile, "objects") as objects:
            self.assertFalse(sync.is_platform_contact(""))
        objects.filter.assert_not_called()


class FreshsalesIdTests(SimpleTestCase):
    databases = []

    def test_a_stored_id_is_sent_as_the_integer_the_api_issued(self):
        self.assertEqual(sync._as_id("127015863079"), 127015863079)
        self.assertEqual(sync._as_id("abc"), "abc")
        self.assertIsNone(sync._as_id(None))
