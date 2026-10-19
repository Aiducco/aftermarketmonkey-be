"""
switch_comped_trial_plan: the self-serve fix for a company on a manually comp'd trial (like
SunCoast Off-Road, confirmed live 2026-10-19 to have zero real Stripe subscriptions) being sent to
the Billing Portal to change plans and finding nothing there to change. No database and no
network -- the decisions live in a pure-ish function against a mocked Company.objects, same
arrangement as the other service-layer tests in this repo.
"""
import datetime
import unittest.mock as mock

from django.test import SimpleTestCase

from src import models as src_models
from src.api.services import billing as billing_services


def _company(**overrides):
    fields = {
        "id": 67,
        "name": "SunCoast Off-Road",
        "slug": "suncoast-off-road-b00938de",
        "status": 1,
        "status_name": "ACTIVE",
        "subscription_plan": "hunter",
        "subscription_status": "trialing",
        "subscription_period_end": datetime.datetime(2026, 10, 31, 10, 33, 26, tzinfo=datetime.timezone.utc),
        "manual_trial_granted_at": datetime.datetime(2026, 10, 10, 10, 33, 26, tzinfo=datetime.timezone.utc),
        "subscription_id": None,
    }
    fields.update(overrides)
    return src_models.Company(**fields)


class SwitchCompedTrialPlanTests(SimpleTestCase):
    databases = []

    def test_company_not_found(self):
        with mock.patch.object(src_models.Company, "objects") as objects:
            objects.get.side_effect = src_models.Company.DoesNotExist
            result, err = billing_services.switch_comped_trial_plan(company_id=999, plan_id="tracker")
        self.assertIsNone(result)
        self.assertEqual(err, "Company not found")

    def test_a_company_with_a_real_subscription_is_refused(self):
        """Must go through the Portal/Checkout instead -- Stripe's webhook owns subscription_plan
        for these, writing it here too would race with that."""
        company = _company(manual_trial_granted_at=None, subscription_id="sub_123")
        company.save = mock.Mock()
        with mock.patch.object(src_models.Company, "objects") as objects:
            objects.get.return_value = company
            result, err = billing_services.switch_comped_trial_plan(company_id=67, plan_id="tracker")
        self.assertIsNone(result)
        self.assertIn("not on a comp'd trial", err)
        company.save.assert_not_called()

    def test_an_invalid_plan_id_is_rejected(self):
        company = _company()
        company.save = mock.Mock()
        with mock.patch.object(src_models.Company, "objects") as objects:
            objects.get.return_value = company
            result, err = billing_services.switch_comped_trial_plan(company_id=67, plan_id="enterprise")
        self.assertIsNone(result)
        self.assertEqual(err, "Invalid plan_id")
        company.save.assert_not_called()

    def test_switching_plan_keeps_the_same_trial_clock(self):
        """The whole point -- picking Tracker instead of Hunter must not grant or reset trial
        time, only change which plan the existing trial applies to."""
        company = _company()
        company.save = mock.Mock()
        original_period_end = company.subscription_period_end
        original_granted_at = company.manual_trial_granted_at

        with mock.patch.object(src_models.Company, "objects") as objects:
            objects.get.return_value = company
            result, err = billing_services.switch_comped_trial_plan(company_id=67, plan_id="tracker")

        self.assertIsNone(err)
        self.assertEqual(company.subscription_plan, "tracker")
        self.assertEqual(company.subscription_status, "trialing")
        self.assertEqual(company.subscription_period_end, original_period_end)
        self.assertEqual(company.manual_trial_granted_at, original_granted_at)
        self.assertEqual(result["subscription_plan"], "tracker")
        self.assertEqual(result["subscription_status"], "trialing")
        company.save.assert_called_once_with(update_fields=["subscription_plan", "subscription_status", "updated_at"])


class GetSubscriptionCompedTrialFlagsTests(SimpleTestCase):
    """is_comped_trial / has_stripe_subscription -- what tells the frontend which action to call."""

    databases = []

    def test_a_comped_trial_is_flagged_and_has_no_stripe_subscription(self):
        company = _company()
        with mock.patch.object(src_models.Company, "objects") as objects:
            objects.filter.return_value.first.return_value = company
            with mock.patch.object(billing_services, "_live_price_display", return_value=("$49/mo", "monthly")):
                subscription = billing_services.get_subscription(67)
        self.assertTrue(subscription["is_comped_trial"])
        self.assertFalse(subscription["has_stripe_subscription"])

    def test_a_real_subscription_is_flagged_the_other_way(self):
        company = _company(manual_trial_granted_at=None, subscription_id="sub_123", subscription_status="active")
        with mock.patch.object(src_models.Company, "objects") as objects:
            objects.filter.return_value.first.return_value = company
            with mock.patch.object(billing_services, "_live_price_display", return_value=("$49/mo", "monthly")):
                subscription = billing_services.get_subscription(67)
        self.assertFalse(subscription["is_comped_trial"])
        self.assertTrue(subscription["has_stripe_subscription"])
