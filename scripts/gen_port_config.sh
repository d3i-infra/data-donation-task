#!/usr/bin/env bash
# Generate and validate port_config.json for a platform.
#
# Usage: bash scripts/gen_port_config.sh <platform> [--stdout]
# Via pnpm: pnpm generate-config <platform> [--stdout]
set -euo pipefail

platform="${1:?Usage: gen_port_config.sh <platform> [--stdout]  (e.g. instagram)}"
stdout_flag="${2:-}"

# Resolve the repo root relative to this script.
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_PKG="$REPO_ROOT/packages/python"

# Run inside the python package so that `port` is importable.
cd "$PYTHON_PKG"

if [[ "$stdout_flag" == "--stdout" ]]; then
    python3 "$REPO_ROOT/scripts/generate_port_config.py" "$platform" --stdout
    exit 0
fi

echo "Generating configs/${platform}_config.json"
python3 "$REPO_ROOT/scripts/generate_port_config.py" "$platform"

# Validate what was just written: schema, registry cross-check, and UI-content
# locale coverage. This is a separate step chained after generation, never a
# step inside it: the generator stays AST-only (ADR-0028) so desktop tooling can
# read metadata without importing Pyodide-dependent platform modules, whereas
# the validator deliberately imports the live module. Hence also poetry rather
# than bare python3 — the validator needs the project's dependencies.
if ! command -v poetry >/dev/null 2>&1; then
    echo "ERROR: poetry is required to validate the generated config." >&2
    echo "       Install poetry (https://python-poetry.org/), then re-run:" >&2
    echo "         pnpm generate-config ${platform}" >&2
    exit 1
fi

echo "Validating configs/${platform}_config.json"
if ! poetry run python "$REPO_ROOT/scripts/validate_port_config.py" "$platform" --report; then
    echo "" >&2
    echo "ERROR: the generated config has validation errors (listed above)." >&2
    echo "       Fix the 'Table config::' docstring blocks in" >&2
    echo "         packages/python/port/platforms/${platform}.py" >&2
    echo "       then delete the generated file and re-run:" >&2
    echo "         rm packages/python/port/configs/${platform}_config.json" >&2
    echo "         pnpm generate-config ${platform}" >&2
    exit 1
fi

echo "Done."
