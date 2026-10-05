# Deploy handoff — Instantly → FreshSales reply sync

> **Superseded 2026-10-05: this deploy is done.** Code is on `main`, the keys are in
> `.env.app.production`, migration `0205` is applied and the cron is installed. Kept as a record of
> the steps, and as the runbook if the box is ever rebuilt. Note that deal creation is now off by
> default (`FRESHSALES_CREATE_DEALS`), so the "6 deals" figures below describe the original
> backfill, not what a fresh run would do.

For whoever (or whatever) has SSH to production. Self-contained: you do not need the session that
wrote this. Design notes are in [INSTANTLY_FRESHSALES_SYNC_PLAN.md](INSTANTLY_FRESHSALES_SYNC_PLAN.md);
this file is only the deploy.

## What is being deployed

`manage.py sync_instantly_replies` — polls Instantly for replies, creates a FreshSales contact for
every human reply, and a deal for every reply Instantly has labelled Interested / Meeting Booked /
Meeting Completed / Closed. Meant to run on a 15-minute cron.

## State before you start — read this, it changes what you do

| Fact | Consequence for you |
|---|---|
| **Migration `0205` is already applied to the production DB** | `migrate` will report it as applied. Nothing to do; do not try to re-run it |
| **The CRM backfill has already run** — 13 contacts, 13 sales accounts, 6 deals, 20 notes | **Do not run with `--since`.** A plain run is a no-op and that is correct |
| `instantly_reply` already holds all 22 replies | Same — a plain run finds nothing new |
| The code is **uncommitted** on `main` locally | Step 1 |
| The three API keys are **not** on the server | Step 3. This is the only step that makes the sync actually start working |
| No cron installed | Step 5 |

The command is **safe to deploy before the keys exist**: with any of the three unset it exits 0 and
writes a `SKIPPED` audit row naming what is missing. So steps 1–2 cannot break anything.

## Step 1 — commit (local only, do not push yet)

The working tree contains **unrelated work in progress** (`wheel_size.py`, `premier.py`,
`qwen_llm.py`, `wheel_enrichment.py`, `verify_lead_emails.py`, various `scripts/`, the Rough Country
EDI files, and more). **Stage only the paths below.** Do not `git add -A`.

```bash
git add migrations/0202_lead_email_search_at.py migrations/0205_instantly_reply.py conf/settings_base.py .env.app.example src/models.py src/integrations/clients/instantly/ src/integrations/clients/freshsales/ src/integrations/services/instantly_freshsales_sync.py src/management/commands/sync_instantly_replies.py src/integrations/tests/test_instantly_freshsales_sync.py docs/INSTANTLY_FRESHSALES_SYNC_PLAN.md docs/INSTANTLY_FRESHSALES_DEPLOY_HANDOFF.md
```

### Why `0202_lead_email_search_at.py` is in that list — do not drop it

`0202` is from earlier `search_lead_emails` work. It was **applied to the production database but
never committed**, and `email_search_at` is already in committed `src/models.py`. `0205` declares a
dependency on it, so if `0202` is not pushed, the deploy's `migrate` step fails on the box with:

```
django.db.migrations.exceptions.NodeNotFoundError: Migration src.0205_instantly_reply
dependencies reference nonexistent parent node ('src', '0202_lead_email_search_at')
```

Committing the file only records what the production database already has. It applies nothing.

`0205` also **merges two migration leaf nodes**. `0202` and `0203` both branch off `0201`, which
left the graph with two leaves — in that state `manage.py migrate` refuses to run at all
(`Conflicting migrations detected; multiple leaf nodes`). `0205` depends on both, which resolves it.
That fix is load-bearing for this deploy and for every deploy after it.

Verify the staged set before committing — it should list exactly **16** files and nothing else:

```bash
git diff --cached --name-only
```

Then commit:

```bash
git commit -m "$(printf 'Sync Instantly replies into FreshSales as contacts and deals\n\nEvery human reply becomes a FreshSales contact; a reply Instantly has labelled\npositive also becomes a deal in the default pipeline. Polls on a cron rather than\ntaking a webhook: Instantly signs nothing, so there would be no way to verify a\ncaller, and a poll that fails simply runs again.\n\nThe interest label is re-read for 60 days after a reply, because Instantly labels\nmost threads within minutes but not all -- 4 of the account 22 replies are still\nunlabelled, two of them a month old -- and a label applied by hand in Unibox has\nto be able to produce the deal later.\n\nAlso commits 0202_lead_email_search_at, which was applied to production but never\ncommitted, and whose absence would break 0205 deps. 0205 depends on both 0202 and\n0204, merging two migration leaf nodes that currently make migrate refuse to run.\n\nCo-Authored-By: Claude Opus 5 <noreply@anthropic.com>')"
```

## Step 2 — push, which deploys

Pushing `main` triggers `.github/workflows/deploy.yml`: it SSHes in, `git reset --hard`, rebuilds
the image, `docker compose up -d`, then runs `migrate --no-input`.

```bash
git push origin main
```

Expect `migrate` to apply nothing (both migrations are already recorded on this database). Watch the
Actions run finish before continuing.

## Step 3 — put the three keys on the server

**This is the step CI cannot do.** `.gitignore` has `.env.app.*` with only `.env.app.example`
whitelisted, so `.env.app.production` exists solely on the box.

Gojko will give you the three values. Append them (do not rewrite the file — it holds everything
else the app needs):

```bash
nano ~/aftermarketmonkey-be/.env.app.production
```

The three lines to add:

```
INSTANTLY_API_KEY=<value from Gojko>
FRESHSALES_API_KEY=<value from Gojko>
FRESHSALES_BUNDLE_ALIAS=aftermarketscout
```

Everything else has a working default in `conf/settings_base.py`; see
[the plan §8](INSTANTLY_FRESHSALES_SYNC_PLAN.md) for the tuning knobs.

These must be **real environment variables in that file**, not in a `.env`: `conf/settings_base`
reads `os.environ` at import, which happens before `settings.py` calls `load_dotenv()`, so a key
living only in `.env` reads as empty.

## Step 4 — recreate the container so it picks them up

`env_file` is read when a container is **created**, so a restart is not enough:

```bash
cd ~/aftermarketmonkey-be && DEPLOY_ENV=production docker compose up -d --force-recreate app
```

Confirm the settings actually landed inside the container — this prints lengths and the alias, never
the secrets:

```bash
docker compose exec -T app python manage.py shell -c "from django.conf import settings; print('instantly', len(settings.INSTANTLY_API_KEY), '| freshsales', len(settings.FRESHSALES_API_KEY), '| alias', settings.FRESHSALES_BUNDLE_ALIAS)"
```

Expect three non-zero lengths and `alias aftermarketscout`. A `0` means the file edit or the
recreate did not take.

Then a dry run, which writes nothing anywhere. It should list 22 replies and report
`would_create_contacts 13`, `would_create_deals 6` — all of which already exist in the CRM, so this
is confirming the connection, not proposing work:

```bash
docker compose exec -T app python manage.py sync_instantly_replies --dry-run --since 2026-09-01
```

Now a real run. Because the backfill is done, the correct result is **zeros across the board**:

```bash
docker compose exec -T app python manage.py sync_instantly_replies
```

If that shows `contacts_created 0, deals_created 0, notes_created 0, failed 0` you are done with the
app half. Anything non-zero means replies arrived since the backfill, which is also fine — check them
in FreshSales.

## Step 5 — install the cron

Get the real container name first; the one below is the expected default but confirm it:

```bash
docker ps --format '{{.Names}}'
```

```bash
mkdir -p /root/logs
```

Substitute the name you just saw if it differs:

```bash
(crontab -l 2>/dev/null; echo '*/15 * * * * docker exec aftermarketmonkey-be-app-1 python manage.py sync_instantly_replies >> /root/logs/instantly_sync.log 2>&1') | crontab -
```

```bash
crontab -l
```

## Step 6 — verify after 15–30 minutes

Every run writes an audit row, so this answers "is it working" without reading logs:

```bash
docker compose exec -T app python manage.py shell -c "from src.models import ScheduledTaskExecution as S
for e in S.objects.filter(name='sync_instantly_replies')[:5]:
    print(e.created_at, e.status_name, (e.message or e.error_message or '')[:160])"
```

What you want to see:

- `COMPLETED` with mostly zeros — working, nothing new to do
- `SKIPPED  Not configured: ... unset` — step 3 or 4 did not take
- `FAILED` — read the `error_message`; it names the vendor and endpoint

Steady state is ~1 FreshSales call and ~2 Instantly calls per run, far inside both budgets
(FreshSales 1000/hour per account, Instantly 20/minute on `GET /emails`).

## Rollback

Nothing here changes existing behaviour — it is a new table, new files and one new command, with no
hooks into any existing code path. To stop it, remove the cron line:

```bash
crontab -l | grep -v sync_instantly_replies | crontab -
```

That is the whole rollback. Do **not** roll back migration `0205`: it is also the merge node that
keeps `migrate` runnable, and dropping it re-breaks the two-leaf problem. Leaving the table in place
costs nothing.

## Do not do these

- **Do not** run with `--since` — the backfill is done; it would re-check 22 replies for nothing
- **Do not** `git add -A` — the tree has unrelated work in progress
- **Do not** drop `0202_lead_email_search_at.py` from the commit — see step 1
- **Do not** run `manage.py test` with the normal settings. The repo's `.env` points `DATABASE_HOST`
  at **production**, and Django's test runner creates a test database on whatever host it is given.
  Run the suite with a SQLite settings override instead:

```bash
printf 'from settings import *\nDATABASES = {"default": {"ENGINE": "django.db.backends.sqlite3", "NAME": ":memory:"}}\n' > /tmp/test_settings.py && PYTHONPATH=/tmp:. python manage.py test src.integrations.tests.test_instantly_freshsales_sync --settings=test_settings
```

Expect **66 tests, OK**. (Package-level discovery like `src.integrations.tests` does not work in this
repo — `src` has no `__init__.py`, so name test modules explicitly.)

## Afterwards

Two things are Gojko's calls, not yours:

- **Deal amount is `0`.** `FRESHSALES_DEFAULT_DEAL_AMOUNT` sets it; the 6 deals currently total $0 in
  a pipeline whose headline field is `amount`.
- **Both API keys have been pasted into a chat**, so they should be rotated once this is settled.
  Rotating is this file's step 3 plus step 4, nothing more.
