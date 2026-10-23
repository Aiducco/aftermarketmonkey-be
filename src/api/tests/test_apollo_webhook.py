"""Apollo direct-dial callback endpoint."""
import json

from django import test
from django.urls import reverse

from src import models


class ApolloPhoneWebhookTests(test.TestCase):
    def setUp(self) -> None:
        self.url = reverse("apollo_phone_webhook")

    def _post(self, payload, secret="test-secret-123"):
        return self.client.post(
            "{}?secret={}".format(self.url, secret),
            data=json.dumps(payload),
            content_type="application/json",
        )

    def test_rejects_bad_secret(self) -> None:
        response = self._post({"id": "abc"}, secret="wrong")
        self.assertEqual(response.status_code, 403)
        self.assertEqual(models.ApolloPhoneReveal.objects.count(), 0)

    def test_stores_phone_numbers(self) -> None:
        response = self._post(
            {
                "id": "60c504fa892fb60001ab29b2",
                "name": "Zach Debolt",
                "title": "System Analyst",
                "email": "zach.debolt@randysww.com",
                "organization": {"primary_domain": "randysworldwide.com"},
                "phone_numbers": [
                    {"raw_number": "+1 555-123-4567", "sanitized_number": "+15551234567", "type": "work_direct"}
                ],
            }
        )
        self.assertEqual(response.status_code, 200)
        row = models.ApolloPhoneReveal.objects.get(apollo_person_id="60c504fa892fb60001ab29b2")
        self.assertEqual(row.status, "received")
        self.assertEqual(row.name, "Zach Debolt")
        self.assertEqual(row.organization_domain, "randysworldwide.com")
        self.assertEqual(row.phone_numbers[0]["sanitized"], "+15551234567")
        self.assertIsNotNone(row.received_at)

    def test_marks_no_phone_when_payload_has_none(self) -> None:
        self._post({"id": "xyz", "name": "No Number"})
        self.assertEqual(models.ApolloPhoneReveal.objects.get(apollo_person_id="xyz").status, "no_phone")

    def test_updates_existing_requested_row_without_losing_snapshot(self) -> None:
        models.ApolloPhoneReveal.objects.create(
            apollo_person_id="p1", name="Known Name", title="Known Title",
            organization_domain="sdwheel.com", status="requested",
        )
        # The phone callback is sparser than the enrich payload -- the snapshot must survive.
        self._post({"id": "p1", "phone_numbers": [{"raw_number": "+1 555-000-1111"}]})
        row = models.ApolloPhoneReveal.objects.get(apollo_person_id="p1")
        self.assertEqual(row.name, "Known Name")
        self.assertEqual(row.title, "Known Title")
        self.assertEqual(row.status, "received")
        self.assertEqual(row.phone_numbers[0]["raw"], "+1 555-000-1111")

    def test_accepts_people_envelope_and_bare_list(self) -> None:
        self._post({"people": [{"id": "e1", "phone_numbers": [{"raw_number": "1"}]}]})
        self._post([{"id": "e2", "phone_numbers": [{"raw_number": "2"}]}])
        self.assertTrue(models.ApolloPhoneReveal.objects.filter(apollo_person_id="e1").exists())
        self.assertTrue(models.ApolloPhoneReveal.objects.filter(apollo_person_id="e2").exists())

    def test_malformed_body_is_acknowledged_not_retried(self) -> None:
        response = self.client.post(
            "{}?secret=test-secret-123".format(self.url),
            data=b"not json", content_type="application/json",
        )
        self.assertEqual(response.status_code, 200)
