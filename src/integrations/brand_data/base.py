"""
What a loader is handed and what it hands back.

A loader's whole job is: get the brand's records, whatever that takes, and yield them one at a
time as the brand wrote them. It does no mapping, no parsing and no writing -- those are the same
for every source and live in ``mapping`` and ``ingest``, so that a new brand costs a transport and
not a pipeline.

Two obligations come with the context object, and both exist because this department's failures
are quiet:

``observe(chunk)``  feed it whatever bytes were read. The digest of those bytes is the run's
                    fingerprint, which is what lets a re-run of an unchanged quarterly spreadsheet
                    cost nothing, and what proves two "different" files were the same file.
``input_label``     say what was actually read -- the path, the URL, the page count. A run whose
                    row count halved is a mystery until you can see it read a different file.
"""
import dataclasses
import hashlib
import typing

from src import models as src_models


class BrandDataError(Exception):
    """Base for this department. Commands turn these into CommandError, not tracebacks."""


class SourceConfigError(BrandDataError):
    """The registry row cannot be run as written -- missing handler, missing path, bad field map."""


class SourceFetchError(BrandDataError):
    """The source could not be read: file missing, endpoint down, response not what it claims."""


@dataclasses.dataclass
class SourceRecord:
    """
    One record as the brand published it.

    ``key`` is filled only when the *transport* knows the identity structurally -- a JSON object's
    ``id``, say. Most of the time it is None and ``ingest`` derives the key from the mapped row,
    which is the more honest place for it: identity is a property of the tire, not of the file it
    arrived in.
    """

    payload: typing.Any
    key: typing.Optional[str] = None
    label: str = ""


@dataclasses.dataclass
class LoaderContext:
    """Everything a loader is allowed to know: its registry row, its config, and the run's options."""

    source: src_models.TireBrandSource
    config: typing.Dict[str, typing.Any] = dataclasses.field(default_factory=dict)
    file_override: str = ""
    limit: typing.Optional[int] = None
    progress: typing.Callable[[str], None] = lambda message: None
    input_label: str = ""

    _digest: "hashlib._Hash" = dataclasses.field(default_factory=hashlib.sha256, repr=False)

    def observe(self, chunk: typing.Union[bytes, str]) -> None:
        """Record bytes read, for the run fingerprint. Cheap; call it with everything."""
        self._digest.update(chunk.encode("utf-8", "replace") if isinstance(chunk, str) else chunk)

    @property
    def fingerprint(self) -> str:
        return self._digest.hexdigest()

    def require(self, key: str) -> typing.Any:
        value = self.config.get(key)
        if value in (None, "", [], {}):
            raise SourceConfigError(f"{self.source.slug}: config is missing {key!r}")
        return value


BrandLoader = typing.Callable[[LoaderContext], typing.Iterable[SourceRecord]]
