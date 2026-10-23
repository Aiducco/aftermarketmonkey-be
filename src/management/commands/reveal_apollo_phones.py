"""
Request direct dials from Apollo for a company's contacts, then read them back.

    python manage.py reveal_apollo_phones --domain sdwheel.com --only-with-phone-flag
    python manage.py reveal_apollo_phones --person-ids 60c504fa892fb60001ab29b2 --wait 180
    python manage.py reveal_apollo_phones --show sdwheel.com          # read, spend nothing

WHY A WEBHOOK
-------------
Apollo will not hand over a direct dial synchronously. Both people/match and
people/bulk_match reject ``reveal_phone_number`` with HTTP 400
``SEARCH.VALIDATION.WEBHOOK_URL_REQUIRED`` unless ``webhook_url`` is supplied; the number is
resolved asynchronously and POSTed to that URL minutes later, and no endpoint exists to poll
for it. So this command fires the request and then waits on rows landing in
``apollo_phone_reveal`` via /api/apollo/phone-webhook/.

CREDITS -- READ BEFORE RUNNING
------------------------------
Every revealed record costs a credit, and a reveal that resolves to nothing costs one too.
Two guards:
  * ``--only-with-phone-flag`` (default on) skips anyone whose search record does not say
    ``has_direct_phone: "Yes"``, which is free to read and is Apollo's own claim that a number
    exists. On the companies checked so far this cuts the spend roughly in half.
  * ``--dry-run`` prints exactly who would be requested, and costs nothing.

Apollo validates ``webhook_url`` for format only, not reachability -- an unroutable URL
returns HTTP 200 and spends the credit with the result delivered into the void. The command
therefore refuses to run unless APOLLO_WEBHOOK_SECRET is set and the base URL is https.
"""
import json
import time
import typing
import urllib.error
import urllib.request

from django.conf import settings
from django.core.management import base
from django.utils import timezone

from src import models

_SEARCH_URL = "https://api.apollo.io/api/v1/mixed_people/api_search"
_BULK_MATCH_URL = "https://api.apollo.io/api/v1/people/bulk_match"
_BULK_MATCH_LIMIT = 10  # Apollo's documented cap per bulk_match call.


class Command(base.BaseCommand):
    help = "Request Apollo direct dials via the phone webhook, then read the results back."

    def add_arguments(self, parser: typing.Any) -> None:
        parser.add_argument("--domain", help="Company domain whose contacts to reveal.")
        parser.add_argument("--person-ids", nargs="+", help="Explicit Apollo person ids.")
        parser.add_argument("--show", help="Print stored reveals for a domain and exit. Free.")
        parser.add_argument(
            "--only-with-phone-flag",
            action="store_true",
            default=True,
            help="Skip people whose search record is not has_direct_phone=Yes (default).",
        )
        parser.add_argument(
            "--all-contacts",
            action="store_true",
            help="Override the flag filter and request everyone. Costs more credits.",
        )
        parser.add_argument("--dry-run", action="store_true", help="List targets, spend nothing.")
        parser.add_argument(
            "--wait", type=int, default=120, help="Seconds to wait for callbacks (default 120)."
        )

    # ------------------------------------------------------------------ helpers

    def _post(self, url: str, body: dict) -> dict:
        request = urllib.request.Request(
            url,
            data=json.dumps(body).encode(),
            headers={
                "Content-Type": "application/json",
                "accept": "application/json",
                "x-api-key": settings.APOLLO_API_KEY,
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                return json.load(response)
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode(errors="replace")[:400]
            raise base.CommandError("Apollo HTTP {}: {}".format(exc.code, detail))

    def _search_domain(self, domain: str) -> list:
        people, page = [], 1
        while page <= 10:
            payload = self._post(
                _SEARCH_URL,
                {"q_organization_domains_list": [domain], "per_page": 100, "page": page},
            )
            batch = payload.get("people") or []
            if not batch:
                break
            people.extend(batch)
            if len(batch) < 100:
                break
            page += 1
            time.sleep(0.4)
        return people

    def _print_stored(self, domain: str) -> None:
        rows = models.ApolloPhoneReveal.objects.filter(organization_domain=domain).order_by("name")
        if not rows:
            self.stdout.write("No stored reveals for {}.".format(domain))
            return
        for row in rows:
            numbers = ", ".join(p.get("raw") or p.get("sanitized") or "" for p in row.phone_numbers)
            self.stdout.write(
                "{:<26} {:<34} {:<12} {}".format(
                    (row.name or row.apollo_person_id)[:25], (row.title or "")[:33], row.status, numbers
                )
            )

    # ------------------------------------------------------------------ entrypoint

    def handle(self, *args: typing.Any, **options: typing.Any) -> None:
        if options.get("show"):
            self._print_stored(options["show"])
            return

        if not settings.APOLLO_API_KEY:
            raise base.CommandError("APOLLO_API_KEY is unset.")
        if not settings.APOLLO_WEBHOOK_SECRET:
            raise base.CommandError(
                "APOLLO_WEBHOOK_SECRET is unset -- reveals would be delivered to an endpoint "
                "that rejects them, and the credits would be spent anyway."
            )

        base_url = (settings.APOLLO_WEBHOOK_BASE_URL or "").rstrip("/")
        if not base_url.startswith("https://"):
            raise base.CommandError(
                "APOLLO_WEBHOOK_BASE_URL must be a public https origin (got {!r}). Apollo "
                "validates this for format only and spends the credit regardless of whether "
                "the callback can actually be delivered.".format(base_url)
            )
        webhook_url = "{}/api/apollo/phone-webhook/?secret={}".format(
            base_url, settings.APOLLO_WEBHOOK_SECRET
        )

        domain = options.get("domain") or ""
        targets: list = []

        if options.get("person_ids"):
            targets = [{"id": pid} for pid in options["person_ids"]]
        elif domain:
            people = self._search_domain(domain)
            self.stdout.write("Found {} people at {}.".format(len(people), domain))
            if not options.get("all_contacts") and options.get("only_with_phone_flag"):
                people = [p for p in people if p.get("has_direct_phone") == "Yes"]
                self.stdout.write(
                    "{} have has_direct_phone=Yes -- requesting only those.".format(len(people))
                )
            targets = [
                {
                    "id": p["id"],
                    "name": "{} {}".format(
                        p.get("first_name") or "", p.get("last_name") or ""
                    ).strip(),
                    "title": p.get("title") or "",
                }
                for p in people
                if p.get("id")
            ]
        else:
            raise base.CommandError("Pass --domain, --person-ids or --show.")

        if not targets:
            self.stdout.write("Nothing to request.")
            return

        if options.get("dry_run"):
            self.stdout.write("DRY RUN -- {} reveal(s) would be requested:".format(len(targets)))
            for target in targets:
                self.stdout.write(
                    "  {}  {}  {}".format(
                        target["id"], (target.get("name") or "")[:28], (target.get("title") or "")[:34]
                    )
                )
            return

        # Rows go in before the request so a callback arriving mid-batch updates rather than
        # races, and so a dropped reveal is still visible as a stale "requested".
        for target in targets:
            models.ApolloPhoneReveal.objects.update_or_create(
                apollo_person_id=target["id"],
                defaults={
                    "name": target.get("name") or "",
                    "title": target.get("title") or "",
                    "organization_domain": domain,
                    "status": "requested",
                    "requested_at": timezone.now(),
                },
            )

        requested = 0
        for start in range(0, len(targets), _BULK_MATCH_LIMIT):
            chunk = targets[start : start + _BULK_MATCH_LIMIT]
            self._post(
                _BULK_MATCH_URL,
                {
                    "reveal_phone_number": True,
                    "webhook_url": webhook_url,
                    "details": [{"id": t["id"]} for t in chunk],
                },
            )
            requested += len(chunk)
            self.stdout.write("Requested {}/{}...".format(requested, len(targets)))
            time.sleep(0.5)

        ids = [t["id"] for t in targets]
        deadline = time.time() + max(0, options.get("wait") or 0)
        while time.time() < deadline:
            pending = models.ApolloPhoneReveal.objects.filter(
                apollo_person_id__in=ids, status="requested"
            ).count()
            if not pending:
                break
            self.stdout.write("  {} still pending...".format(pending))
            time.sleep(10)

        done = models.ApolloPhoneReveal.objects.filter(apollo_person_id__in=ids)
        with_numbers = [r for r in done if r.phone_numbers]
        self.stdout.write(
            self.style.SUCCESS(
                "{} of {} returned a number.".format(len(with_numbers), len(ids))
            )
        )
        for row in sorted(with_numbers, key=lambda r: r.name or ""):
            numbers = ", ".join(p.get("raw") or p.get("sanitized") or "" for p in row.phone_numbers)
            self.stdout.write("  {:<26} {}".format((row.name or row.apollo_person_id)[:25], numbers))

        still_pending = done.filter(status="requested").count()
        if still_pending:
            self.stdout.write(
                "{} still pending -- Apollo can take a few minutes. Re-read with "
                "--show {}".format(still_pending, domain or "<domain>")
            )
