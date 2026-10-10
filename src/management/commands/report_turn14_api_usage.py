"""
Exact Turn 14 API usage report, read from api_rate_buckets -- every GET, token and quote request
this server has made to Turn 14 already passes through src.integrations.rate_limit.acquire(),
which upserts a row here (src/integrations/clients/turn_14/rate_limit.py's Bucket definitions),
so this is a precise count, not an estimate.

    manage.py report_turn14_api_usage                  # last 24 hours
    manage.py report_turn14_api_usage --hours 1         # last hour
    manage.py report_turn14_api_usage --since 2026-10-10  # since midnight that day (UTC)

Built 2026-10-10 after Turn14 (Dan Ziegler) flagged our token-request volume as high -- see
src/integrations/clients/turn_14/rate_limit.py's get_cached_token/store_token docstrings for the
root cause (a process-local token cache that couldn't survive our cron jobs each running as a
fresh process) and the fix (moved onto the shared Redis cache). This command exists so that
question never again has to be answered by an ad-hoc script -- it's the one place to check
before replying to Turn 14.
"""
import datetime
import typing

from django.core.management.base import BaseCommand
from django.db import connection
from django.utils import timezone

from src import enums as src_enums
from src import models as src_models
from src.integrations import credentials as credentials_helper
from src.integrations import rate_limit as rate_limit_base


class Command(BaseCommand):
    help = "Exact count of Turn 14 API calls (token/get/quote) made by this server, read from api_rate_buckets."

    def add_arguments(self, parser):
        parser.add_argument(
            "--hours", type=float, default=24.0,
            help="Look back this many hours from now (default 24). Ignored if --since is given.",
        )
        parser.add_argument(
            "--since", type=str, default=None,
            help="Look back to this UTC date/datetime instead of --hours, e.g. 2026-10-10 or '2026-10-10 05:00'.",
        )

    def handle(self, *args, **options):
        if options.get("since"):
            raw = options["since"]
            fmt = "%Y-%m-%d %H:%M" if " " in raw else "%Y-%m-%d"
            # Naive datetime, matching this project's USE_TZ = False (timezone.now() is naive
            # here too) -- api_rate_buckets.updated_at is stored the same way.
            since = datetime.datetime.strptime(raw, fmt)
        else:
            since = timezone.now() - datetime.timedelta(hours=options["hours"])

        # identity_for() is a one-way hash (see rate_limit.identity_for's own docstring for why),
        # so the only way to turn a bucket's identity back into a company name is to hash every
        # real Turn14 credential we have on file and match -- same approach used ad-hoc earlier
        # this session, now a permanent part of this report.
        identity_to_company: typing.Dict[str, str] = {}
        for cp in src_models.CompanyProviders.objects.filter(
            provider__kind=src_enums.BrandProviderKind.TURN_14.value,
        ).select_related("company"):
            creds = credentials_helper.get_feed_credentials(cp)
            client_id = (creds or {}).get("client_id")
            if client_id:
                identity_to_company[rate_limit_base.identity_for(client_id)] = cp.company.name
        for account in src_models.CompanyProviderOrderAccount.objects.filter(
            company_provider__provider__kind=src_enums.BrandProviderKind.TURN_14.value,
        ).select_related("company_provider__company"):
            client_id = (account.credentials or {}).get("client_id")
            if client_id:
                identity_to_company.setdefault(
                    rate_limit_base.identity_for(client_id), account.company_provider.company.name
                )

        with connection.cursor() as c:
            c.execute(
                "SELECT bucket_key, count, updated_at FROM api_rate_buckets "
                "WHERE bucket_key LIKE 't14:%%' AND updated_at >= %s",
                [since],
            )
            rows = c.fetchall()

        token_total = 0
        by_metric_identity: typing.Dict[typing.Tuple[str, str], int] = {}
        for bucket_key, count, _updated_at in rows:
            parts = bucket_key.split(":")
            # shape: t14:<metric>:<window>:<identity>:<window_index>, e.g. t14:get:hour:<id>:<n>,
            # or t14:token:minute:ip:<n> (token's "identity" is the constant "ip", not a hash).
            if len(parts) < 4:
                continue
            metric = parts[1]  # get | token | quote
            identity = parts[3]
            if metric == "token":
                token_total += count
                continue
            # Only the "hour" window is counted for get/quote (minute/second/day windows double
            # count the same requests at finer/coarser granularity -- see get_buckets()/
            # quote_buckets() in the client's own rate_limit.py, every GET passes all of them).
            if parts[2] != "hour":
                continue
            by_metric_identity[(metric, identity)] = by_metric_identity.get((metric, identity), 0) + count

        self.stdout.write("Turn 14 API usage since {} ({}):\n".format(
            since.isoformat(), timezone.now() - since
        ))
        self.stdout.write("  Token requests (mints a new OAuth token, metered 10/min per IP, server-wide): {}".format(
            token_total
        ))
        self.stdout.write(
            "    -- expected going forward: roughly 1 per active credential per ~hour "
            "(a token's real validity window), not per cron tick.\n"
        )

        get_total = sum(v for (m, _), v in by_metric_identity.items() if m == "get")
        quote_total = sum(v for (m, _), v in by_metric_identity.items() if m == "quote")
        self.stdout.write("  GET requests (catalog/pricing/inventory/etc, metered 5 000/hour per credential): {}".format(
            get_total
        ))
        self.stdout.write("  Quote requests (order quotes, metered 2/second per credential): {}\n".format(quote_total))

        self.stdout.write("  By company (GET requests):")
        company_totals: typing.Dict[str, int] = {}
        for (metric, identity), count in by_metric_identity.items():
            if metric != "get":
                continue
            name = identity_to_company.get(identity, "unknown (identity={})".format(identity))
            company_totals[name] = company_totals.get(name, 0) + count
        for name, count in sorted(company_totals.items(), key=lambda kv: -kv[1]):
            self.stdout.write("    {:>8}  {}".format(count, name))

        self.stdout.write(
            "\n  Raw bucket rows inspected: {} (each is one hourly/minute/second/day window for "
            "one credential, not one request -- 'count' on the row is the request tally).".format(len(rows))
        )
