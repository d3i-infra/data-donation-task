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
else
    echo "Generating port_config.json for platform: $platform"
    python3 "$REPO_ROOT/scripts/generate_port_config.py" "$platform"
    echo "Done."
fi
