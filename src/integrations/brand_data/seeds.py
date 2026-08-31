"""
Putting rows in the registry: declared sources from a file, planned sources from our own catalog.

Two ways in, for two different things.

**Declared.** A source somebody has actually arranged -- a portal, a sheet, an endpoint -- is
written as JSON and upserted on ``slug``. It is a file rather than a form because the config it
carries (column maps, pagination, filters) is code-shaped: it wants review, a diff and a history,
and it is the part most likely to be wrong. Keep the declarations in the repo next to the data
they read, under ``resources/brand_data/``.

**Planned.** Every brand we sell tires for and have no source for is also a row, created from the
catalog itself with ``status='planned'``. Without those the registry answers "what have we set
up", which nobody needs to ask; with them it answers "what is left", which is the only reason to
have it. They are ordered by how many of that brand's tires no reseller catalog has validated, so
the top of the table is the brand where a source would buy the most.

Both paths are idempotent, and neither ever downgrades a row a human has since worked on: seeding
a brand that already has a source leaves the existing row alone.
"""
import dataclasses
import json
import pathlib
import typing

from src import models as src_models
from src.integrations.brand_data import base, coverage, mapping
from src.integrations.brand_data import registry as brand_registry
from src.integrations.services.tire_catalog import brand_key

REQUIRED_FIELDS = ("slug", "brand_name", "method")
WRITABLE_FIELDS = (
    "brand_name",
    "method",
    "handler",
    "status",
    "config",
    "credential_setting",
    "source_url",
    "notes",
    "contact",
    "priority",
    "refresh_interval_days",
)


@dataclasses.dataclass
class SeedResult:
    created: typing.List[str] = dataclasses.field(default_factory=list)
    updated: typing.List[str] = dataclasses.field(default_factory=list)
    unchanged: typing.List[str] = dataclasses.field(default_factory=list)
    linked: typing.List[str] = dataclasses.field(default_factory=list)


def load_declarations(path: typing.Union[str, pathlib.Path]) -> typing.List[typing.Dict[str, typing.Any]]:
    """Read and fully validate a declarations file, reporting every problem in it at once."""
    file_path = pathlib.Path(path)
    if not file_path.exists():
        raise base.SourceConfigError(f"{file_path} does not exist")
    try:
        payload = json.loads(file_path.read_text())
    except json.JSONDecodeError as exc:
        raise base.SourceConfigError(f"{file_path} is not valid JSON: {exc}") from exc

    declarations = payload if isinstance(payload, list) else [payload]
    problems = []
    methods = {value for value, _ in src_models.TireBrandSource.METHOD_CHOICES}
    statuses = {value for value, _ in src_models.TireBrandSource.STATUS_CHOICES}

    for index, declaration in enumerate(declarations):
        if not isinstance(declaration, dict):
            problems.append(f"entry {index} is not an object")
            continue
        label = declaration.get("slug") or f"entry {index}"
        for field in REQUIRED_FIELDS:
            if not declaration.get(field):
                problems.append(f"{label}: missing {field}")
        if declaration.get("method") and declaration["method"] not in methods:
            problems.append(f"{label}: method must be one of {', '.join(sorted(methods))}")
        if declaration.get("status") and declaration["status"] not in statuses:
            problems.append(f"{label}: status must be one of {', '.join(sorted(statuses))}")
        if declaration.get("handler"):
            try:
                brand_registry.get_loader(declaration["handler"])
            except base.SourceConfigError as exc:
                problems.append(f"{label}: {exc}")
        field_map = (declaration.get("config") or {}).get("field_map")
        if field_map is not None:
            try:
                mapping.validate_field_map(field_map)
            except mapping.FieldMapError as exc:
                problems.append(f"{label}: {exc}")
        unknown = sorted(set(declaration) - {"slug"} - set(WRITABLE_FIELDS))
        if unknown:
            problems.append(f"{label}: unknown field(s) {', '.join(unknown)}")

    if problems:
        raise base.SourceConfigError("\n".join(problems))
    return declarations


def upsert_sources(
    declarations: typing.Sequence[typing.Mapping[str, typing.Any]], *, dry_run: bool = False
) -> SeedResult:
    """Create or update registry rows from validated declarations, keyed on ``slug``."""
    result = SeedResult()
    brands = _brands_by_key()

    for declaration in declarations:
        slug = declaration["slug"]
        source = src_models.TireBrandSource.objects.filter(slug=slug).first()
        values = {field: declaration[field] for field in WRITABLE_FIELDS if field in declaration}

        if source is None:
            result.created.append(slug)
            if not dry_run:
                source = src_models.TireBrandSource(slug=slug, **values)
                _link_brand(source, brands, result)
                source.save()
            continue

        changed = [field for field, value in values.items() if getattr(source, field) != value]
        if not changed:
            result.unchanged.append(slug)
        else:
            result.updated.append(f"{slug} ({', '.join(changed)})")
            if not dry_run:
                for field, value in values.items():
                    setattr(source, field, value)
                _link_brand(source, brands, result)
                source.save()
    return result


def plan_from_catalog(
    *, min_tires: int = 25, limit: typing.Optional[int] = None, dry_run: bool = False
) -> typing.Tuple[SeedResult, typing.List[coverage.BrandCoverage]]:
    """
    Add a ``planned`` row for every tire brand in our catalog that has no source yet.

    ``min_tires`` exists because the tail of the brand list is long and mostly noise -- a brand
    with three tires is not worth a data-collection arrangement, and burying the brands that are
    worth one under two hundred of them makes the registry unreadable. Raise it or lower it; the
    default is a starting point, not a finding.
    """
    result = SeedResult()
    brands_with_tires = coverage.catalog_brands(min_tires=min_tires)
    missing = [brand for brand in brands_with_tires if not brand.has_source]
    if limit is not None:
        missing = missing[:limit]

    brands = _brands_by_key()
    for brand in missing:
        slug = _slugify(brand.brand_name)
        if not slug or src_models.TireBrandSource.objects.filter(slug=slug).exists():
            continue
        result.created.append(slug)
        if dry_run:
            continue
        source = src_models.TireBrandSource(
            slug=slug,
            brand_name=brand.brand_name,
            method=src_models.TireBrandSource.METHOD_MANUAL,
            status=src_models.TireBrandSource.STATUS_PLANNED,
            notes=(
                f"Created from our own catalog: {brand.tires} tires, {brand.unvalidated} of them "
                f"unconfirmed by any reseller catalog. Nobody has looked for a data source yet -- "
                f"find one, then set method/handler/config and move this to active."
            ),
        )
        _link_brand(source, brands, result)
        source.save()
    return result, missing


def _brands_by_key() -> typing.Dict[str, src_models.Brands]:
    return {brand_key(brand.name): brand for brand in src_models.Brands.objects.all()}


def _link_brand(
    source: src_models.TireBrandSource, brands: typing.Mapping[str, src_models.Brands], result: SeedResult
) -> None:
    """
    Point the source at our catalog's brand row where the names reduce to the same key.

    Exact-on-the-key only, never fuzzy. A wrong link here would attach a manufacturer's whole spec
    sheet to somebody else's parts, and leaving it NULL costs nothing -- the merge that will use
    it does its own matching, with a size gate.
    """
    if source.brand_id is not None:
        return
    match = brands.get(brand_key(source.brand_name))
    if match is not None:
        source.brand = match
        result.linked.append(source.slug)


def _slugify(name: str) -> str:
    slug = "".join(character.lower() if character.isalnum() else "-" for character in name)
    while "--" in slug:
        slug = slug.replace("--", "-")
    return slug.strip("-")[:128]
