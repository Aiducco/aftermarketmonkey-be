"""
Port one company's distributor connections onto another company.

Built for the demo account: company 16 (Trident Motorsports) is the only tenant connected to
most of the catalog, and AftermarketScout's own account needs the same coverage so a demo shows
real pricing from every distributor. The source company is never written to -- this only ever
creates or updates rows belonging to ``--target-id``.

What it does, per source ``CompanyProviders`` row that has feed credentials:

  - target has no row for that provider   -> create one, copy the feed credentials, enqueue a
                                             pricing sync.
  - target has a row that never synced    -> refresh its feed credentials from the source,
    (``initial_sync_completed`` False)       reactivate it, clear the credential-rejection
                                             backoff, enqueue a pricing sync.
  - target has a row that HAS synced       -> left completely alone. A working connection is
                                             never touched, and nothing is re-enqueued for it
                                             (it has already run).

Three things are deliberately not copied verbatim:

``primary``
    Always False on the target. ``primary=True`` does not mean "main distributor" -- it
    designates the one connection whose credentials drive the GLOBAL shared catalog sync for
    that kind (see turn_14_global, keystone.py, meyer.py, the_wheel_group.py, ...). Copying it
    would leave two primary rows for the same kind, which those syncs resolve arbitrarily and
    log a warning about -- i.e. it would change the source company's behaviour.

The source's relay SFTP identity
    Several kinds (The Wheel Group, Tri-State, S&B, RANDYS) store the source company's own
    relay SFTP username/password as their feed credentials. That is a per-tenant identity, not
    a vendor-issued credential: each company gets its own chrooted drop folder
    (relay_sftp_provisioning). Any credential value equal to the source's relay username or
    password is therefore rewritten to the target's own. Pass ``--relay-from-source`` to copy
    them verbatim instead, which points the target's ingest at the source's drop folder -- more
    demo data (whatever files have actually landed there), two tenants reading one folder.
    Credentials that merely happen to be SFTP but are vendor-issued (Meyer/A-Tech/DLG's shared
    account, TireRack, NTW, Elite Wheel) are copied verbatim either way.

Order credentials
    Only with ``--order-accounts``. Order credentials place real orders on the source company's
    vendor accounts -- Rough Country's are an EDI sender ID, A-Tech's email a named rep -- so a
    test checkout on the target would be a real document on a real trading partner. Copied
    accounts come in as order_status WAITING, which
    ``integration_pricing_sync_jobs.run_integration_pricing_sync_job`` promotes to CONNECTED on
    its own once that connection's feed finishes its first sync. Only connections with NO order
    account of their own get them -- see :meth:`Command._copy_order_accounts`.

``--keep-credentials-kinds`` (default: Turn 14) names kinds where an existing, never-synced
target row keeps its OWN feed credentials rather than having the source's copied over. Turn 14
is there because its limits are enforced per client_id and the credentials are DEACTIVATED --
support conversation required to restore -- after exceeding the daily cap twice in 30 days (see
src.integrations.rate_limit). Sharing one client_id across two companies' pricing syncs doubles
the draw on that budget, and on this exact pair the source's Turn 14 row is also the
``primary=True`` global catalog connection, so tripping it would stall the whole platform's
Turn 14 sweep. The target gets activated and enqueued on its own client_id instead.

Usage:
    python manage.py port_company_providers --source-id 16 --target-id 58 --dry-run
    python manage.py port_company_providers --source-id 16 --target-id 58 --order-accounts
"""

import typing

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.utils import timezone

from src import enums as src_enums
from src import models as src_models
from src.api.services import integrations as integrations_services
from src.integrations import credentials as credentials_helper
from src.integrations.services import integration_pricing_sync_jobs

_TURN_14_KIND = src_enums.BrandProviderKind.TURN_14.value


def _parse_kinds(raw: typing.Optional[str]) -> typing.Set[int]:
    if not raw:
        return set()
    return {int(part) for part in raw.split(",") if part.strip()}


def _rewrite_relay_identity(
    feed: typing.Dict[str, typing.Any],
    source: src_models.Company,
    target: src_models.Company,
) -> typing.Tuple[typing.Dict[str, typing.Any], bool]:
    """
    Replace any credential value that IS the source company's relay SFTP username/password with
    the target's own. Returns (credentials, rewritten) -- ``rewritten`` is False for the common
    case of a vendor-issued credential set, where nothing matched.
    """
    if not source.relay_sftp_username:
        return feed, False

    out: typing.Dict[str, typing.Any] = {}
    rewritten = False
    for key, value in feed.items():
        if value and value == source.relay_sftp_username:
            out[key] = target.relay_sftp_username
            rewritten = True
        elif value and source.relay_sftp_password and value == source.relay_sftp_password:
            out[key] = target.relay_sftp_password
            rewritten = True
        else:
            out[key] = value
    return out, rewritten


def _ingesting_status_fields() -> typing.Dict[str, typing.Any]:
    """
    INGESTING == "credentials in place, initial sync not finished yet" -- the same state
    connect_provider leaves a freshly connected row in. Nothing is validated here, so this is a
    starting point rather than a verdict: check_company_provider_connections re-checks every row
    with initial_sync_completed=False every few minutes and will correct it to
    FAILING/WAITING/INGESTING, and the pricing job sets CONNECTED itself once it succeeds.
    """
    return {
        "status": src_enums.CompanyProviderConnectionStatus.INGESTING.value,
        "status_name": src_enums.CompanyProviderConnectionStatus.INGESTING.name,
        "status_reason": None,
        "status_checked_at": timezone.now(),
        "status_consecutive_failures": 0,
        "status_next_check_at": None,
    }


class Command(BaseCommand):
    help = "Copy one company's distributor connections onto another company, enqueueing a pricing sync for each."

    def add_arguments(self, parser):
        parser.add_argument("--source-id", type=int, required=True, help="Company to copy connections FROM.")
        parser.add_argument("--target-id", type=int, required=True, help="Company to copy connections TO.")
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Print the plan without writing anything.",
        )
        parser.add_argument(
            "--order-accounts",
            action="store_true",
            help="Also copy order-placement credentials (see module docstring -- these place real orders).",
        )
        parser.add_argument(
            "--relay-from-source",
            action="store_true",
            help="Copy the source's relay SFTP credentials verbatim instead of using the target's own account.",
        )
        parser.add_argument(
            "--skip-kinds",
            default="",
            help="Comma-separated BrandProviderKind values to leave out entirely.",
        )
        parser.add_argument(
            "--keep-credentials-kinds",
            default=str(_TURN_14_KIND),
            help=(
                "Comma-separated kinds where an existing target row keeps its own feed credentials "
                "instead of the source's (default: Turn 14 -- see module docstring). Pass an empty "
                "string to copy every kind's credentials."
            ),
        )
        parser.add_argument(
            "--no-enqueue",
            action="store_true",
            help="Create/update the rows but don't enqueue pricing sync jobs.",
        )

    def handle(self, *args, **options):
        source_id = options["source_id"]
        target_id = options["target_id"]
        dry_run = options["dry_run"]
        copy_order_accounts = options["order_accounts"]
        relay_from_source = options["relay_from_source"]
        skip_kinds = _parse_kinds(options["skip_kinds"])
        keep_credentials_kinds = _parse_kinds(options["keep_credentials_kinds"])
        enqueue = not options["no_enqueue"]

        if source_id == target_id:
            raise CommandError("--source-id and --target-id must differ.")

        source = src_models.Company.objects.filter(id=source_id).first()
        if not source:
            raise CommandError("Source company {} not found.".format(source_id))
        target = src_models.Company.objects.filter(id=target_id).first()
        if not target:
            raise CommandError("Target company {} not found.".format(target_id))

        self.stdout.write(
            "Porting connections: '{}' (id={}) -> '{}' (id={}){}".format(
                source.name, source.id, target.name, target.id, "  [DRY RUN]" if dry_run else ""
            )
        )

        source_rows = list(source.company_providers.select_related("provider").order_by("provider__kind"))
        target_by_provider = {cp.provider_id: cp for cp in target.company_providers.select_related("provider").all()}

        created: typing.List[str] = []
        refreshed: typing.List[str] = []
        untouched: typing.List[str] = []
        skipped: typing.List[str] = []
        enqueued: typing.List[int] = []
        order_accounts_copied: typing.List[str] = []
        relay_rewritten: typing.List[str] = []

        with transaction.atomic():
            for source_cp in source_rows:
                provider = source_cp.provider
                label = "{} (kind={})".format(provider.name, provider.kind)

                if provider.kind in skip_kinds:
                    skipped.append("{}: --skip-kinds".format(label))
                    continue

                source_feed = credentials_helper.get_feed_credentials(source_cp)
                if not source_feed:
                    skipped.append("{}: source has no feed credentials".format(label))
                    continue

                target_cp = target_by_provider.get(source_cp.provider_id)

                if target_cp and target_cp.initial_sync_completed:
                    untouched.append("{}: target already synced (cp_id={})".format(label, target_cp.id))
                    continue

                keep_target_credentials = (
                    target_cp is not None
                    and provider.kind in keep_credentials_kinds
                    and bool(credentials_helper.get_feed_credentials(target_cp))
                )

                if keep_target_credentials:
                    feed = credentials_helper.get_feed_credentials(target_cp)
                    rewritten = False
                else:
                    feed = dict(source_feed)
                    if relay_from_source:
                        rewritten = False
                    else:
                        feed, rewritten = _rewrite_relay_identity(feed, source, target)
                        if rewritten and not target.relay_sftp_username:
                            raise CommandError(
                                "{} stores the source's relay SFTP identity, but target company {} has no "
                                "relay account provisioned. Provision one first, or pass "
                                "--relay-from-source to read the source's drop folder.".format(label, target.id)
                            )

                if rewritten:
                    relay_rewritten.append(label)

                # "order" is never persisted on CompanyProviders.credentials -- order credentials
                # live on CompanyProviderOrderAccount (see that model's docstring). Legacy source
                # rows still carry an empty one; don't propagate it.
                credentials = {"feed": feed}
                status_fields = _ingesting_status_fields()

                if target_cp:
                    detail = "cp_id={}{}{}".format(
                        target_cp.id,
                        ", kept target credentials" if keep_target_credentials else ", credentials from source",
                        ", reactivated" if not target_cp.active else "",
                    )
                    refreshed.append("{}: {}".format(label, detail))
                    if not dry_run:
                        target_cp.credentials = credentials
                        target_cp.active = True
                        target_cp.initial_sync_completed = False
                        for field, value in status_fields.items():
                            setattr(target_cp, field, value)
                        target_cp.save()
                else:
                    created.append(label)
                    if not dry_run:
                        target_cp = src_models.CompanyProviders.objects.create(
                            company=target,
                            provider=provider,
                            credentials=credentials,
                            # Never copied -- see module docstring.
                            primary=False,
                            active=True,
                            initial_sync_completed=False,
                            **status_fields,
                        )
                        target_by_provider[provider.id] = target_cp

                if copy_order_accounts:
                    order_accounts_copied.extend(self._copy_order_accounts(source_cp, target_cp, label, dry_run))

                if enqueue and integration_pricing_sync_jobs.should_enqueue_pricing_sync(provider.kind):
                    if dry_run:
                        enqueued.append(target_cp.id if target_cp else 0)
                    else:
                        integration_pricing_sync_jobs.enqueue_company_provider_pricing_sync(target_cp.id)
                        enqueued.append(target_cp.id)

            if dry_run:
                transaction.set_rollback(True)

        self._report(
            created=created,
            refreshed=refreshed,
            untouched=untouched,
            skipped=skipped,
            relay_rewritten=relay_rewritten,
            order_accounts_copied=order_accounts_copied,
            enqueued=enqueued,
            dry_run=dry_run,
        )

    def _copy_order_accounts(
        self,
        source_cp: src_models.CompanyProviders,
        target_cp: typing.Optional[src_models.CompanyProviders],
        label: str,
        dry_run: bool,
    ) -> typing.List[str]:
        """
        Copy the source connection's order accounts, but ONLY onto a target connection that has
        no order accounts of its own. A target connection that already has one was set up
        deliberately, with its own credentials (AftermarketScout's Turn 14 and Meyer both carry
        their own "Store 1" account); adding the source's alongside it would be a second,
        redundant way to order from the same distributor on someone else's vendor account, not a
        capability the target was missing. This is the "order credentials the target lacks" case
        and nothing else.

        Copied accounts land on order_status WAITING ("credentials fine, feed hasn't finished
        syncing"), which the pricing job promotes to CONNECTED itself when the feed's first sync
        completes. is_default carries over as-is, since a connection with no accounts has no
        default to conflict with.
        """
        source_accounts = list(source_cp.order_accounts.all())
        if not source_accounts:
            return []

        if target_cp is not None and target_cp.order_accounts.exists():
            return ["{}: target already has its own order account(s) -- not copied".format(label)]

        existing_labels: typing.Set[str] = set()
        has_default = False

        copied = []
        for account in source_accounts:
            if account.label in existing_labels:
                continue
            is_default = account.is_default and not has_default
            copied.append(
                "{}: order account {!r} ({}{})".format(
                    label, account.label, account.order_method_name, ", default" if is_default else ""
                )
            )
            if not dry_run:
                src_models.CompanyProviderOrderAccount.objects.create(
                    company_provider=target_cp,
                    label=account.label,
                    credentials=account.credentials,
                    order_method=account.order_method,
                    order_method_name=account.order_method_name,
                    is_default=is_default,
                    active=account.active,
                    **integrations_services._order_connection_status_fields(
                        src_enums.CompanyProviderOrderConnectionStatus.WAITING,
                        integrations_services._ORDER_WAITING_ON_FEED_REASON,
                    ),
                )
            if is_default:
                has_default = True
            existing_labels.add(account.label)

        if copied and not dry_run:
            # Mirror the new default account's order status onto CompanyProviders.order_status,
            # the read-only summary the catalog listing's Ordering badge reads.
            integrations_services._refresh_default_order_status(target_cp)
        return copied

    def _report(self, **sections) -> None:
        dry_run = sections.pop("dry_run")
        enqueued = sections.pop("enqueued")

        titles = {
            "created": "Created",
            "refreshed": "Refreshed (existed but never synced)",
            "untouched": "Left alone (already synced)",
            "skipped": "Skipped",
            "relay_rewritten": "Relay identity rewritten to the target's own account",
            "order_accounts_copied": "Order accounts",
        }
        for key, title in titles.items():
            rows = sections.get(key) or []
            self.stdout.write("\n{} ({}):".format(title, len(rows)))
            for row in rows:
                self.stdout.write("  - {}".format(row))

        self.stdout.write("\nPricing sync jobs enqueued: {}".format(len(enqueued)))
        if dry_run:
            self.stdout.write(self.style.WARNING("\nDRY RUN -- nothing was written."))
        else:
            self.stdout.write(self.style.SUCCESS("\nDone."))
