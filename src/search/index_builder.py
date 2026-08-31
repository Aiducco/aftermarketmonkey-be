"""
Build and atomically swap a Meilisearch index, for any product type.

Extracted from ``src.search.tires_index``, whose rebuild logic this reproduces exactly. The point
of sharing it is that the logic is subtle in one specific way that is easy to get wrong twice:
``swap_indexes`` is **asynchronous**, and the first version of the tire rebuild did not wait on the
task or check its status. A failed swap was therefore indistinguishable from a successful one, and
the staging cleanup that followed deleted the only good copy of the data while the job reported
success. That fix should not have to be made independently in every index module.

The other rule worth stating once: **a build that is short does not get swapped in.** A partially
built index replacing a good one looks like inventory disappearing, which is reported as "search is
broken" and diagnosed slowly. Counting first and refusing on a mismatch turns that into a loud
no-op.

``tires_index`` predates this and still carries its own copy; it can adopt this without behaviour
change, but doing so is not free of risk to a live index and is deliberately left as its own step.
"""
import dataclasses
import logging
import time
import typing

logger = logging.getLogger(__name__)


@dataclasses.dataclass(frozen=True)
class IndexSpec:
    """Everything that differs between one product index and another."""

    name: str
    log_prefix: str
    searchable: typing.Sequence[str]
    filterable: typing.Sequence[str]
    sortable: typing.Sequence[str]
    #: Attributes where a one-character difference means a different product, not a near miss.
    typo_disabled: typing.Sequence[str] = ()
    max_values_per_facet: int = 200
    max_total_hits: int = 5000
    primary_key: str = "id"

    @property
    def staging_name(self) -> str:
        return "{}_staging".format(self.name)


def apply_settings(index, spec: IndexSpec) -> None:
    index.update_searchable_attributes(list(spec.searchable))
    index.update_filterable_attributes(list(spec.filterable))
    index.update_sortable_attributes(list(spec.sortable))
    index.update_faceting_settings({"maxValuesPerFacet": spec.max_values_per_facet})
    index.update_pagination_settings({"maxTotalHits": spec.max_total_hits})
    index.update_distinct_attribute(spec.primary_key)
    if spec.typo_disabled:
        index.update_typo_tolerance({"disableOnAttributes": list(spec.typo_disabled)})


def wait(client, task_uid: int, timeout_ms: int = 600_000) -> None:
    client.wait_for_task(task_uid, timeout_in_ms=timeout_ms)


def setup(client, spec: IndexSpec) -> bool:
    """Create and configure the index. Idempotent."""
    try:
        try:
            client.create_index(spec.name, {"primaryKey": spec.primary_key})
        except Exception:
            pass  # already exists -- configuring below is the idempotent part
        apply_settings(client.index(spec.name), spec)
        logger.info("%s index '%s' configured", spec.log_prefix, spec.name)
        return True
    except Exception as exc:
        logger.exception("%s setup failed: %s", spec.log_prefix, exc)
        return False


def live_filterable_attributes(client, spec: IndexSpec) -> typing.Optional[typing.FrozenSet[str]]:
    """
    What the **running** index accepts as a filter, or None if it cannot be read.

    Not the same as ``spec.filterable``: that is what this code wants, and the index only has it
    after ``setup`` runs. Between a deploy and that command the two disagree, and Meilisearch
    rejects an entire multi-search when one requested facet is not filterable -- so a single new
    attribute would take search down rather than hiding one control. Callers intersect against
    this so a not-yet-configured facet is simply absent until the index catches up.
    """
    try:
        return frozenset(client.index(spec.name).get_filterable_attributes() or [])
    except Exception as exc:
        logger.warning("%s could not read filterable attributes from '%s': %s", spec.log_prefix, spec.name, exc)
        return None


def rebuild(
    client,
    spec: IndexSpec,
    *,
    iter_documents: typing.Callable[[], typing.Iterator[typing.List[dict]]],
    expected: int,
) -> typing.Tuple[int, int]:
    """
    Full rebuild into staging, verified, then swapped in atomically. Returns ``(live, expected)``.

    The swap is refused unless the staged count matches ``expected`` exactly.
    """
    started = time.monotonic()
    logger.info(
        "%s rebuild start | live=%s staging=%s expected_docs=%s",
        spec.log_prefix,
        spec.name,
        spec.staging_name,
        expected,
    )

    # Start from empty: a leftover staging index from a failed run would otherwise contribute
    # stale documents to the count check and to the swapped-in result.
    try:
        task = client.delete_index(spec.staging_name)
        wait(client, task.task_uid, timeout_ms=120_000)
    except Exception:
        pass
    client.create_index(spec.staging_name, {"primaryKey": spec.primary_key})
    apply_settings(client.index(spec.staging_name), spec)

    indexed = 0
    task_uids: typing.List[int] = []
    for batch in iter_documents():
        task = client.index(spec.staging_name).add_documents(batch, primary_key=spec.primary_key)
        task_uids.append(task.task_uid)
        indexed += len(batch)
        logger.info("%s staged %s/%s", spec.log_prefix, indexed, expected)
    for task_uid in task_uids:
        wait(client, task_uid)

    actual = client.index(spec.staging_name).get_stats().number_of_documents
    if actual != expected:
        logger.error(
            "%s REFUSING TO SWAP: staging has %s documents, expected %s. Live index '%s' left "
            "untouched; staging kept for inspection.",
            spec.log_prefix,
            actual,
            expected,
            spec.name,
        )
        return actual, expected

    # Meilisearch refuses to swap against an index that does not exist, which is the state on a
    # first-ever build. Create it empty so the swap has something to trade places with.
    try:
        client.create_index(spec.name, {"primaryKey": spec.primary_key})
        apply_settings(client.index(spec.name), spec)
    except Exception:
        pass  # already exists, the normal case after the first build

    # swap_indexes is ASYNCHRONOUS. Without waiting on the task and checking that it succeeded, a
    # failed swap looks exactly like a successful one -- and the staging delete below would then
    # destroy the only good copy of the data while this function reported success.
    swap_task = client.swap_indexes([{"indexes": [spec.name, spec.staging_name]}])
    wait(client, swap_task.task_uid, timeout_ms=120_000)
    status = client.get_task(swap_task.task_uid).status
    if status != "succeeded":
        logger.error(
            "%s SWAP FAILED (status=%s). Live index '%s' is unchanged and staging '%s' is kept "
            "with the freshly built documents -- do not delete it before investigating.",
            spec.log_prefix,
            status,
            spec.name,
            spec.staging_name,
        )
        return 0, expected

    live = client.index(spec.name).get_stats().number_of_documents
    if live != expected:
        logger.error(
            "%s post-swap count is %s, expected %s. Staging '%s' kept for inspection.",
            spec.log_prefix,
            live,
            expected,
            spec.staging_name,
        )
        return live, expected

    # Only now is staging safe to drop: it holds the *previous* documents.
    try:
        task = client.delete_index(spec.staging_name)
        wait(client, task.task_uid, timeout_ms=120_000)
    except Exception as exc:
        logger.warning("%s post-swap staging cleanup failed (non-fatal): %s", spec.log_prefix, exc)

    logger.info(
        "%s rebuild done | %s documents live in '%s' in %.1fs",
        spec.log_prefix,
        live,
        spec.name,
        time.monotonic() - started,
    )
    return live, expected
