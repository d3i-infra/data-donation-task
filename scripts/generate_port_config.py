#!/usr/bin/env python3
"""Generate port_config.json for a platform from extractor docstrings.

Usage
-----
    python scripts/generate_port_config.py <platform>

Example
-------
    python scripts/generate_port_config.py instagram

The script reads ``packages/python/port/platforms/<platform>.py`` as source
text (no import needed), extracts the ``EXTRACTOR_REGISTRY`` key order and
each extractor's ``Table config::`` JSON block from docstrings, then writes
the assembled config to ``packages/python/port/port_config.json``.
"""

import ast
import json
import re
import sys
import textwrap
from pathlib import Path

_REPO_ROOT = Path(__file__).parent.parent
_PLATFORMS_DIR = _REPO_ROOT / "packages" / "python" / "port" / "platforms"
_OUTPUT_PATH = _REPO_ROOT / "packages" / "python" / "port" / "port_config.json"

_SECTION_RE = re.compile(r"^(\s*)Table config::\s*$", re.MULTILINE)
_TABLE_DOC_RE = re.compile(r"^(\s*)Table documentation::\s*$", re.MULTILINE)
_PLATFORM_INFO_RE = re.compile(r"^(\s*)Platform info::\s*$", re.MULTILINE)


def _parse_json_block(label: str, docstring: str, section_re: re.Pattern) -> dict:
    """Extract and parse a labelled JSON block from a docstring.

    The block starts on the line after the section header and ends when the
    indentation returns to the level of the header (or the string ends).
    """
    match = section_re.search(docstring)
    if not match:
        raise ValueError(f"{label}: missing '{section_re.pattern.split('::')[0].lstrip('^(\\\\s*)')}::' section in docstring")

    header_indent = len(match.group(1))
    block_start = match.end()
    lines = docstring[block_start:].splitlines()

    block_lines = []
    for line in lines:
        if line.strip() == "":
            block_lines.append(line)
            continue
        indent = len(line) - len(line.lstrip())
        if indent > header_indent:
            block_lines.append(line)
        else:
            break

    raw_block = textwrap.dedent("\n".join(block_lines)).strip()
    if not raw_block:
        raise ValueError(f"{label}: JSON block is empty")

    try:
        return json.loads(raw_block)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{label}: invalid JSON in block: {exc}") from exc


def _parse_table_config_block(fn_name: str, docstring: str) -> dict:
    """Extract and parse the ``Table config::`` JSON block from a docstring."""
    return _parse_json_block(fn_name, docstring, _SECTION_RE)


def _parse_table_doc_block(fn_name: str, docstring: str) -> dict | None:
    """Extract and parse the ``Table documentation::`` JSON block, or None if absent."""
    try:
        return _parse_json_block(fn_name, docstring, _TABLE_DOC_RE)
    except ValueError:
        return None


def _get_docstring(node: ast.FunctionDef | ast.Module) -> str:
    """Return the docstring of a function or module AST node, or empty string."""
    if (
        node.body
        and isinstance(node.body[0], ast.Expr)
        and isinstance(node.body[0].value, ast.Constant)
        and isinstance(node.body[0].value.value, str)
    ):
        return node.body[0].value.value
    return ""


def _extract_registry_keys(tree: ast.Module) -> list[str]:
    """Return EXTRACTOR_REGISTRY string keys in declaration order.

    Looks for an annotated assignment whose target name is
    ``EXTRACTOR_REGISTRY`` and whose value is a dict literal with string keys.
    """
    for node in ast.walk(tree):
        if not isinstance(node, ast.AnnAssign):
            continue
        target = node.target
        if not (isinstance(target, ast.Name) and target.id == "EXTRACTOR_REGISTRY"):
            continue
        value = node.value
        if not isinstance(value, ast.Dict):
            continue
        keys = []
        for key in value.keys:
            if isinstance(key, ast.Constant) and isinstance(key.value, str):
                keys.append(key.value)
        return keys
    raise ValueError("EXTRACTOR_REGISTRY not found or not a dict literal in source file")


def build_config(platform: str) -> dict:
    """Build and return the port config dict for *platform*.

    Raises ``SystemExit`` on any parse or validation error.
    """
    source_path = _PLATFORMS_DIR / f"{platform}.py"
    if not source_path.is_file():
        print(f"ERROR: Platform source not found: {source_path}", file=sys.stderr)
        sys.exit(1)

    source = source_path.read_text(encoding="utf-8")
    try:
        tree = ast.parse(source, filename=str(source_path))
    except SyntaxError as exc:
        print(f"ERROR: Syntax error in {source_path}: {exc}", file=sys.stderr)
        sys.exit(1)

    # Extract platform_info from the module docstring.
    module_docstring = _get_docstring(tree)
    try:
        platform_info = _parse_json_block(f"module:{platform}", module_docstring, _PLATFORM_INFO_RE)
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)

    # Build a name → docstring map for all top-level function definitions.
    fn_docstrings: dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            fn_docstrings[node.name] = _get_docstring(node)

    # Get registry key order.
    try:
        registry_keys = _extract_registry_keys(tree)
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)

    # Parse Table config:: block for each registry entry.
    tables = []
    errors = []
    for fn_name in registry_keys:
        docstring = fn_docstrings.get(fn_name, "")
        try:
            entry = _parse_table_config_block(fn_name, docstring)
        except ValueError as exc:
            errors.append(str(exc))
            continue
        entry["extractor"] = fn_name
        doc = _parse_table_doc_block(fn_name, docstring)
        if doc is not None:
            entry["documentation"] = doc
        tables.append(entry)

    if errors:
        for msg in errors:
            print(f"ERROR: {msg}", file=sys.stderr)
        sys.exit(1)

    return {"platform_info": platform_info, "tables": tables}


def generate(platform: str, *, stdout: bool = False) -> None:
    """Generate port_config.json for *platform*.

    Parameters
    ----------
    platform:
        Platform name, e.g. ``"instagram"``.
    stdout:
        When ``True``, print the JSON to stdout instead of writing the file.
    """
    config = build_config(platform)
    serialized = json.dumps(config, indent=2, ensure_ascii=False) + "\n"

    if stdout:
        sys.stdout.write(serialized)
    else:
        _OUTPUT_PATH.write_text(serialized, encoding="utf-8")
        print(f"Written: {_OUTPUT_PATH}")


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(
        description="Generate port_config.json for a platform."
    )
    parser.add_argument("platform", help="Platform name, e.g. instagram")
    parser.add_argument(
        "--stdout",
        action="store_true",
        help="Print JSON to stdout instead of writing port_config.json",
    )
    args = parser.parse_args()
    generate(args.platform, stdout=args.stdout)


if __name__ == "__main__":
    main()
