"""Validate ``configs/<platform>_config.json`` against a platform's live extractor registry.

Intended for use at runtime or in tests::

    from port.helpers.port_config_validator import validate_or_raise
    validate_or_raise("instagram")

…and, for researchers, from the command line before a release::

    python -m port.helpers.port_config_validator --all --report

Checks performed
----------------
1. ``configs/<platform>_config.json`` exists.
2. File is valid JSON.
3. Top-level schema: ``platform_info`` (dict) and ``tables`` (list) are present.
4. Per-table schema: required fields have correct types; optional fields have
   correct types when present.
5. UI-content locale coverage: every participant-facing text bundle (a table's
   ``title`` / ``description``, each ``headers`` column label, each
   visualization ``title``) is a locale dict of strings that carries the
   default UI locale.  See ``validate_ui_content``.
6. Registry cross-check: every ``extractor`` value in ``tables`` exists as a
   key in the live ``EXTRACTOR_REGISTRY``.
7. Extractor uniqueness: each extractor name appears exactly once.
8. Table ID uniqueness: each ``id`` appears exactly once.
9. Runtime load: ``load_port_config`` successfully builds a ``list[TableConfig]``
   without errors.

Checks 1–5 read the JSON only.  They deliberately run *before* the platform
module is imported, so a researcher gets the content report even when the live
module cannot be loaded.
"""

import importlib.resources
import json
import logging
from importlib import import_module
from typing import Any, Callable, Iterator, TypedDict

import pandas as pd

from port.helpers.table_extractor import load_port_config
from port.helpers.ui_locale import (
    DEFAULT_UI_LOCALE,
    PROVISIONAL_UI_LOCALES,
    SUPPORTED_UI_LOCALES,
)

logger = logging.getLogger(__name__)

_REQUIRED_FIELDS: list[tuple[str, type]] = [
    ("id", str),
    ("extractor", str),
    ("title", dict),
    ("description", dict),
    ("headers", dict),
]

_OPTIONAL_FIELDS: list[tuple[str, type]] = [
    ("visualizations", list),
    ("extractor_kwargs", dict),
    ("variables", list),
    ("documentation", dict),
]

# Table fields whose value is a single participant-facing text bundle.
_UI_TEXT_FIELDS: tuple[str, ...] = ("title", "description")

# How many example locations an aggregate warning names before it stops.
_AGGREGATE_SAMPLE_LIMIT = 3


class UiContentCoverage(TypedDict):
    """Per-platform UI-content locale coverage, as counted over text bundles."""

    total: int
    present: dict[str, int]
    empty: dict[str, int]
    unknown: dict[str, int]


class ValidationError(Exception):
    """Raised when ``configs/<platform>_config.json`` fails schema or registry validation."""


def read_config(platform: str) -> dict[str, Any]:
    """Read and parse ``configs/<platform>_config.json``.

    Raises
    ------
    FileNotFoundError
        If the config file does not exist.
    ValidationError
        If the file is not valid JSON.
    """
    config_filename = f"{platform}_config.json"
    try:
        ref = importlib.resources.files("port") / "configs" / config_filename
        text = ref.read_text(encoding="utf-8")
    except (FileNotFoundError, TypeError) as exc:
        raise FileNotFoundError(
            f"configs/{config_filename} not found. "
            f"Generate it first by running:  pnpm generate-config {platform}"
        ) from exc

    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValidationError(
            f"configs/{config_filename} is not valid JSON: {exc}"
        ) from exc


def available_platforms() -> list[str]:
    """Return the platform names that have a committed/generated config, sorted."""
    configs = importlib.resources.files("port") / "configs"
    names: list[str] = []
    for entry in configs.iterdir():
        name = entry.name
        if name.endswith("_config.json"):
            names.append(name[: -len("_config.json")])
    return sorted(names)


def _iter_ui_text_bundles(tables: list[Any]) -> Iterator[tuple[str, Any]]:
    """Yield ``(label, value)`` for every participant-facing text bundle in *tables*.

    A "text bundle" is what the researcher authors as ``{"en": ..., "nl": ...}``:
    a table's ``title`` and ``description``, every ``headers`` column label, and
    every visualization ``title``.  The value is yielded as-is — deciding whether
    it is well-formed is the caller's job.

    ``platform_info`` is never visited: its ``languages`` list is DDP *export*
    metadata (which language a participant's exported files are in) and has
    nothing to do with which locales the UI text carries.
    """
    for i, entry in enumerate(tables):
        if not isinstance(entry, dict):
            continue
        prefix = f"tables[{i}]"
        for field in _UI_TEXT_FIELDS:
            if field in entry:
                yield f"{prefix}.{field}", entry[field]

        headers = entry.get("headers")
        if isinstance(headers, dict):
            for column, bundle in headers.items():
                yield f"{prefix}.headers[{column!r}]", bundle

        visualizations = entry.get("visualizations")
        if isinstance(visualizations, list):
            for vi, viz in enumerate(visualizations):
                if isinstance(viz, dict) and "title" in viz:
                    yield f"{prefix}.visualizations[{vi}].title", viz["title"]


def _aggregate(labels: list[str]) -> str:
    """Render up to ``_AGGREGATE_SAMPLE_LIMIT`` example locations as one phrase."""
    shown = ", ".join(labels[:_AGGREGATE_SAMPLE_LIMIT])
    remaining = len(labels) - _AGGREGATE_SAMPLE_LIMIT
    return f"{shown} (+{remaining} more)" if remaining > 0 else shown


def validate_ui_content(tables: list[Any]) -> tuple[list[str], list[str]]:
    """Check UI-content locale coverage across every text bundle in *tables*.

    Errors (a participant would see broken or missing text):

    * the bundle is not a dict — a bare ``"Column A"`` where
      ``{"en": "Column A"}`` belongs;
    * a translation value is not a string;
    * the bundle carries no ``DEFAULT_UI_LOCALE`` entry, which is the last
      locale the renderer falls back to before the ``?text?`` sentinel.

    Warnings (informational, aggregated to one line per problem class — never
    one line per table or per column):

    * empty translation strings, which are a deliberate "render nothing" and so
      are never an error;
    * locale keys outside ``SUPPORTED_UI_LOCALES``, which are never rendered.

    Returns
    -------
    tuple[list[str], list[str]]
        ``(errors, warnings)``.
    """
    errors: list[str] = []
    empty_labels: list[str] = []
    unknown_counts: dict[str, int] = {}

    for label, bundle in _iter_ui_text_bundles(tables):
        if not isinstance(bundle, dict):
            errors.append(
                f"{label}: must be a locale dict such as "
                f"{{\"{DEFAULT_UI_LOCALE}\": \"...\"}}, "
                f"got {type(bundle).__name__}"
            )
            continue

        for locale, text in bundle.items():
            if not isinstance(locale, str):
                errors.append(
                    f"{label}: locale keys must be strings, got "
                    f"{type(locale).__name__} ({locale!r})"
                )
                continue
            if not isinstance(text, str):
                errors.append(
                    f"{label}: text for locale '{locale}' must be a string, "
                    f"got {type(text).__name__}"
                )
                continue
            if text == "":
                empty_labels.append(f"{label}['{locale}']")
            if locale not in SUPPORTED_UI_LOCALES:
                unknown_counts[locale] = unknown_counts.get(locale, 0) + 1

        if DEFAULT_UI_LOCALE not in bundle:
            present = ", ".join(sorted(str(k) for k in bundle)) or "nothing"
            errors.append(
                f"{label}: missing the required '{DEFAULT_UI_LOCALE}' translation "
                f"(carries: {present})"
            )

    warnings: list[str] = []
    if empty_labels:
        warnings.append(
            f"{len(empty_labels)} UI text value(s) are empty and render as blank: "
            f"{_aggregate(empty_labels)}"
        )
    if unknown_counts:
        keys = ", ".join(f"'{k}'" for k in sorted(unknown_counts))
        total = sum(unknown_counts.values())
        warnings.append(
            f"unknown UI locale key(s) {keys} in {total} UI text value(s); "
            f"supported UI locales are {', '.join(SUPPORTED_UI_LOCALES)} — "
            f"anything else is never rendered"
        )
    return errors, warnings


def ui_content_coverage(tables: list[Any]) -> UiContentCoverage:
    """Count UI text bundles per locale: the coverage matrix ``--report`` prints.

    ``present`` counts bundles carrying a non-empty string for a supported
    locale, ``empty`` counts bundles carrying an empty string, and ``unknown``
    counts occurrences of locale keys outside ``SUPPORTED_UI_LOCALES``.
    Malformed bundles are skipped here — ``validate_ui_content`` reports those.
    """
    coverage: UiContentCoverage = {
        "total": 0,
        "present": {locale: 0 for locale in SUPPORTED_UI_LOCALES},
        "empty": {locale: 0 for locale in SUPPORTED_UI_LOCALES},
        "unknown": {},
    }
    for _label, bundle in _iter_ui_text_bundles(tables):
        if not isinstance(bundle, dict):
            continue
        coverage["total"] += 1
        for locale, text in bundle.items():
            if not isinstance(locale, str) or not isinstance(text, str):
                continue
            if locale not in SUPPORTED_UI_LOCALES:
                coverage["unknown"][locale] = coverage["unknown"].get(locale, 0) + 1
                continue
            bucket = "empty" if text == "" else "present"
            coverage[bucket][locale] += 1
    return coverage


def format_coverage_report(platform: str, coverage: UiContentCoverage) -> str:
    """Render *coverage* as the human-readable per-platform matrix."""
    total = coverage["total"]
    lines = [f"{platform}: {total} UI text bundle(s)"]
    lines.append(f"  {'locale':<8}{'present':>9}{'empty':>7}{'coverage':>10}")
    for locale in SUPPORTED_UI_LOCALES:
        present = coverage["present"][locale]
        empty = coverage["empty"][locale]
        pct = (present / total * 100) if total else 0.0
        marker = "*" if locale in PROVISIONAL_UI_LOCALES else " "
        lines.append(f"  {locale + marker:<8}{present:>9}{empty:>7}{pct:>9.1f}%")
    if PROVISIONAL_UI_LOCALES:
        lines.append(
            f"  * provisional UI locale ({', '.join(PROVISIONAL_UI_LOCALES)})"
        )
    for locale, count in sorted(coverage["unknown"].items()):
        lines.append(f"  unknown locale key '{locale}': {count} value(s), never rendered")
    return "\n".join(lines)


def validate(platform: str) -> tuple[list[str], list[str]]:
    """Validate ``configs/<platform>_config.json`` for *platform* using the live module.

    Parameters
    ----------
    platform:
        Platform name, e.g. ``"instagram"``.  Used to import
        ``port.platforms.<platform>`` and retrieve ``EXTRACTOR_REGISTRY``.

    Returns
    -------
    tuple[list[str], list[str]]
        ``(errors, warnings)``.  ``errors`` is empty on success.

    Raises
    ------
    FileNotFoundError
        If the config file does not exist (early-exit condition).
    ValidationError
        If the config file is not valid JSON or otherwise unparseable (early-exit condition).
    """
    errors: list[str] = []
    warnings: list[str] = []

    # 1. Config file exists.  2. File is valid JSON.
    raw = read_config(platform)

    # 3. Top-level schema.
    if not isinstance(raw.get("platform_info"), dict):
        errors.append("top-level 'platform_info' key must be a dict")
    if not isinstance(raw.get("tables"), list):
        errors.append("top-level 'tables' key must be a list")
        return errors, warnings

    tables: list[dict] = raw["tables"]

    # 4. Per-table schema.
    for i, entry in enumerate(tables):
        prefix = f"tables[{i}]"
        for field, expected_type in _REQUIRED_FIELDS:
            if field not in entry:
                errors.append(f"{prefix}: missing required field '{field}'")
            elif not isinstance(entry[field], expected_type):
                errors.append(
                    f"{prefix}: field '{field}' must be {expected_type.__name__}, "
                    f"got {type(entry[field]).__name__}"
                )
        for field, expected_type in _OPTIONAL_FIELDS:
            if field in entry and not isinstance(entry[field], expected_type):
                errors.append(
                    f"{prefix}: optional field '{field}' must be {expected_type.__name__}, "
                    f"got {type(entry[field]).__name__}"
                )

    # 5. UI-content locale coverage (JSON-only; no platform import needed).
    content_errors, content_warnings = validate_ui_content(tables)
    errors.extend(content_errors)
    warnings.extend(content_warnings)

    # Load live module and registry (prerequisite for checks 6–7).
    try:
        platform_module = import_module(f"port.platforms.{platform}")
    except ModuleNotFoundError:
        errors.append(f"Cannot import port.platforms.{platform}")
        return errors, warnings

    registry: dict[str, Callable[..., pd.DataFrame]] | None = getattr(platform_module, "EXTRACTOR_REGISTRY", None)
    if registry is None:
        errors.append(f"port.platforms.{platform} has no EXTRACTOR_REGISTRY")
        return errors, warnings

    extractor_names = [
        name
        for entry in tables
        if isinstance(name := entry.get("extractor"), str)
    ]
    table_ids = [
        tid
        for entry in tables
        if isinstance(tid := entry.get("id"), str)
    ]

    # 6. Registry cross-check.
    for name in extractor_names:
        if name not in registry:
            errors.append(f"extractor '{name}' not found in live EXTRACTOR_REGISTRY")

    # 7. Extractor uniqueness.
    seen_extractors: dict[str, int] = {}
    for name in extractor_names:
        seen_extractors[name] = seen_extractors.get(name, 0) + 1
    for name, count in seen_extractors.items():
        if count > 1:
            errors.append(f"extractor '{name}' appears {count} times (must be exactly once)")

    # 8. Table ID uniqueness.
    seen_ids: dict[str, int] = {}
    for tid in table_ids:
        seen_ids[tid] = seen_ids.get(tid, 0) + 1
    for tid, count in seen_ids.items():
        if count > 1:
            errors.append(f"table id '{tid}' appears {count} times (must be unique)")

    # 9. Runtime load via load_port_config.
    if not errors:
        try:
            load_port_config(registry, platform)
        except Exception as exc:
            errors.append(f"load_port_config() failed at runtime: {exc}")

    return errors, warnings


def validate_or_raise(platform: str) -> None:
    """Validate ``configs/<platform>_config.json`` and raise on any error.

    Logs warnings.  Intended for use at startup or in tests.

    Parameters
    ----------
    platform:
        Platform name, e.g. ``"instagram"``.

    Raises
    ------
    FileNotFoundError
        If the config file does not exist.
    ValidationError
        If the config file is malformed (invalid JSON or schema/registry errors).
    """
    errors, warnings = validate(platform)
    for msg in warnings:
        logger.warning(msg)
    if errors:
        raise ValidationError("\n".join(f"  - {e}" for e in errors))


def _validate_one(platform: str, report: bool) -> bool:
    """Validate one platform, printing its result.  Returns True when it passed."""
    try:
        errors, warnings = validate(platform)
    except (FileNotFoundError, ValidationError) as exc:
        print(f"FAIL {platform}: {exc}")
        return False

    print(f"{'FAIL' if errors else 'OK  '} {platform}")
    for msg in errors:
        print(f"  error:   {msg}")
    for msg in warnings:
        print(f"  warning: {msg}")

    if report:
        try:
            raw = read_config(platform)
        except (FileNotFoundError, ValidationError):
            return not errors
        tables = raw.get("tables")
        if isinstance(tables, list):
            print(format_coverage_report(platform, ui_content_coverage(tables)))
    return not errors


def main(argv: list[str] | None = None) -> int:
    """Command-line entry point.

    ::

        python -m port.helpers.port_config_validator <platform> [--report]
        python -m port.helpers.port_config_validator --all [--report]

    Exits non-zero when any platform has errors.  Warnings never fail the run:
    an empty translation or an unknown locale key is something a researcher
    should see, not something that should block a release.
    """
    import argparse

    parser = argparse.ArgumentParser(
        prog="python -m port.helpers.port_config_validator",
        description=(
            "Validate generated/committed configs/<platform>_config.json files, "
            "including UI-content locale coverage."
        ),
    )
    parser.add_argument("platform", nargs="?", help="Platform name, e.g. instagram")
    parser.add_argument(
        "--all", action="store_true", help="Validate every config in port/configs/"
    )
    parser.add_argument(
        "--report",
        action="store_true",
        help="Also print the per-platform UI-content locale coverage matrix",
    )
    args = parser.parse_args(argv)

    if bool(args.platform) == args.all:
        parser.error("give exactly one of <platform> or --all")

    platforms = available_platforms() if args.all else [args.platform]
    if not platforms:
        print(
            "No configs found in port/configs/. "
            "Generate one first with:  pnpm generate-config <platform>"
        )
        return 1

    passed = [_validate_one(platform, args.report) for platform in platforms]
    failed = passed.count(False)
    if len(platforms) > 1:
        print(f"\n{len(platforms) - failed}/{len(platforms)} platform config(s) valid")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
