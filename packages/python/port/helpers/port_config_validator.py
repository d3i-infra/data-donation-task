"""Validate ``configs/<platform>_config.json`` against a platform's live extractor registry.

Intended for use at runtime or in tests::

    from port.helpers.port_config_validator import validate_or_raise
    validate_or_raise("instagram")

Checks performed
----------------
1. ``configs/<platform>_config.json`` exists.
2. File is valid JSON.
3. Top-level schema: ``platform_info`` (dict) and ``tables`` (list) are present.
4. Per-table schema: required fields have correct types; optional fields have
   correct types when present.
5. Registry cross-check: every ``extractor`` value in ``tables`` exists as a
   key in the live ``EXTRACTOR_REGISTRY``.
6. Extractor uniqueness: each extractor name appears exactly once.
7. Table ID uniqueness: each ``id`` appears exactly once.
8. Runtime load: ``load_port_config`` successfully builds a ``list[TableConfig]``
   without errors.
"""

import importlib.resources
import json
import logging
from importlib import import_module
from typing import Callable

import pandas as pd

from port.helpers.table_extractor import load_port_config

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


class ValidationError(Exception):
    """Raised when ``configs/<platform>_config.json`` fails schema or registry validation."""


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

    # 1. Config file exists.
    config_filename = f"{platform}_config.json"
    try:
        ref = importlib.resources.files("port") / "configs" / config_filename
        text = ref.read_text(encoding="utf-8")
    except (FileNotFoundError, TypeError) as exc:
        raise FileNotFoundError(
            f"configs/{config_filename} not found. "
            f"Generate it first by running:  pnpm generate-config {platform}"
        ) from exc

    # 2. File is valid JSON.
    try:
        raw = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValidationError(
            f"configs/{config_filename} is not valid JSON: {exc}"
        ) from exc

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

    # Load live module and registry (prerequisite for checks 5–6).
    try:
        platform_module = import_module(f"port.platforms.{platform}")
    except ModuleNotFoundError:
        errors.append(f"Cannot import port.platforms.{platform}")
        return errors, warnings

    registry: dict[str, Callable[..., pd.DataFrame]] = getattr(platform_module, "EXTRACTOR_REGISTRY", None)
    if registry is None:
        errors.append(f"port.platforms.{platform} has no EXTRACTOR_REGISTRY")
        return errors, warnings

    extractor_names = [
        entry.get("extractor")
        for entry in tables
        if isinstance(entry.get("extractor"), str)
    ]
    table_ids = [
        entry.get("id")
        for entry in tables
        if isinstance(entry.get("id"), str)
    ]

    # 5. Registry cross-check.
    for name in extractor_names:
        if name not in registry:
            errors.append(f"extractor '{name}' not found in live EXTRACTOR_REGISTRY")

    # 6. Extractor uniqueness.
    seen_extractors: dict[str, int] = {}
    for name in extractor_names:
        seen_extractors[name] = seen_extractors.get(name, 0) + 1
    for name, count in seen_extractors.items():
        if count > 1:
            errors.append(f"extractor '{name}' appears {count} times (must be exactly once)")

    # 7. Table ID uniqueness.
    seen_ids: dict[str, int] = {}
    for tid in table_ids:
        seen_ids[tid] = seen_ids.get(tid, 0) + 1
    for tid, count in seen_ids.items():
        if count > 1:
            errors.append(f"table id '{tid}' appears {count} times (must be unique)")

    # 8. Runtime load via load_port_config.
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
