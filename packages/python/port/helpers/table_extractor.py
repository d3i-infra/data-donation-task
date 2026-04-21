"""
TableConfig dataclass, config loaders, and generic extraction runner.
"""

import importlib.resources
import json
import logging
from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Callable

import pandas as pd

import port.api.props as props
import port.api.d3i_props as d3i_props
from port.api.d3i_props import ExtractionResult

logger = logging.getLogger(__name__)


@dataclass
class TableConfig:
    """Full specification for a single extracted table shown in the UI.

    Parameters
    ----------
    id:
        Unique identifier for the table, used internally by the consent form.
    extractor:
        Callable with signature ``(reader, errors, **kwargs) -> pd.DataFrame``
        that extracts the data for this table.
    title:
        Human-readable table title as a ``props.Translatable`` mapping.
    description:
        Human-readable table description as a ``props.Translatable`` mapping.
    headers:
        Mapping of DataFrame column names to ``props.Translatable`` labels
        shown as column headers in the UI.
    extractor_kwargs:
        Extra keyword arguments forwarded to ``extractor`` beyond the mandatory
        ``reader`` and ``errors`` parameters.
    visualizations:
        Optional list of visualization descriptors passed directly to
        ``PropsUIPromptConsentFormTableViz``.
    variables:
        Optional list of column names to include in the extracted DataFrame.
        ``None`` (default) keeps all columns produced by the extractor.
        Column names not present in the DataFrame are silently ignored.
    """

    id: str
    extractor: Callable[..., pd.DataFrame]
    title: props.Translatable
    description: props.Translatable
    headers: dict[str, props.Translatable]
    extractor_kwargs: dict[str, Any] = field(default_factory=dict)
    visualizations: list[dict[str, Any]] = field(default_factory=list)
    variables: list[str] | None = None


def _build_config(
    raw: dict,
    registry: dict[str, Callable[..., pd.DataFrame]],
) -> list[TableConfig]:
    """Build ``TableConfig`` objects from a parsed config dict.

    Parameters
    ----------
    raw:
        Parsed configuration dict with a top-level ``"tables"`` list.
    registry:
        Mapping from extractor name strings to callable extractor functions.

    Returns
    -------
    list[TableConfig]
        One ``TableConfig`` per entry in ``raw["tables"]``.

    Raises
    ------
    KeyError
        If an entry references an extractor name not present in *registry*.
    """
    configs: list[TableConfig] = []
    for entry in raw["tables"]:
        extractor_fn = registry[entry["extractor"]]
        headers = {
            col: props.Translatable(translations)
            for col, translations in entry["headers"].items()
        }
        configs.append(TableConfig(
            id=entry["id"],
            extractor=extractor_fn,
            title=props.Translatable(entry["title"]),
            description=props.Translatable(entry["description"]),
            headers=headers,
            extractor_kwargs=entry.get("extractor_kwargs", {}),
            visualizations=entry.get("visualizations", []),
            variables=entry.get("variables", None),
        ))
    return configs



def load_port_config(
    registry: dict[str, Callable[..., pd.DataFrame]],
) -> list[TableConfig]:
    """Load the active config from ``port_config.json``.

    Parameters
    ----------
    registry:
        Mapping from extractor name strings to callable extractor functions.

    Returns
    -------
    list[TableConfig]

    Raises
    ------
    ImportError
        If ``port_config.json`` has not been generated yet.
    KeyError
        If a table entry references an extractor name not present in *registry*.
    """
    try:
        ref = importlib.resources.files("port") / "port_config.json"
        raw = json.loads(ref.read_text(encoding="utf-8"))
    except (FileNotFoundError, TypeError) as exc:
        raise ImportError(
            "port_config.json not found. "
            "Generate it first by running:  pnpm generate-config <platform>"
        ) from exc
    return _build_config(raw, registry)


def run_extraction(reader, errors: Counter, config: list[TableConfig]) -> ExtractionResult:
    """Run a config-driven extraction and return non-empty tables.

    Parameters
    ----------
    reader:
        Archive reader passed as the first argument to each extractor.
    errors:
        Mutable counter that accumulates error type counts.  Updated in-place
        by individual extractors.
    config:
        List of ``TableConfig`` objects describing which extractors to run and
        how their output should be presented in the UI.

    Returns
    -------
    ExtractionResult
        Contains only non-empty tables together with the accumulated error counter.
    """
    tables = []
    for table_cfg in config:
        df = table_cfg.extractor(reader, errors, **table_cfg.extractor_kwargs)
        if table_cfg.variables is not None:
            df = df[[c for c in table_cfg.variables if c in df.columns]]
        table = d3i_props.PropsUIPromptConsentFormTableViz(
            id=table_cfg.id,
            data_frame=df,
            title=table_cfg.title,
            description=table_cfg.description,
            headers=table_cfg.headers,
            visualizations=table_cfg.visualizations if table_cfg.visualizations else None,
        )
        tables.append(table)

    return ExtractionResult(
        tables=[t for t in tables if not t.data_frame.empty],
        errors=errors,
    )
