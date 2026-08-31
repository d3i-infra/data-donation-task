#!/usr/bin/env bash
# Builds the production release wheel from a disposable staged copy of
# packages/python, with the e2etest fault-injection platform, the
# e2etest_multifile test platform, and their configs removed from the copy
# before the build. The normal development/Playwright wheel (`pnpm run
# build:wheel`, invoked by `pnpm run build:py`) is untouched by this script
# and still includes both — only the release path excludes them. See
# ADR-0004 (release-wheel boundary) and
# ~/notes/e2etest-release-packaging-recommendation.md for the full rationale.
#
# The source checkout is never mutated: everything the exclusion touches is a
# copy in a mktemp directory, removed on exit (including on a failed or
# interrupted build) via the trap below. A signal or crash mid-build can
# leave a stale temp directory behind, never a damaged working tree.
set -euo pipefail

# Requires poetry >= 2.0: `--clean` and the `-C <dir>` project-directory form
# below are poetry-2-only. The researcher-build path runs this on VMs whose
# poetry was provisioned once at VM-provision time and is never upgraded
# afterward, so a stale VM can still be on 1.x. Fail fast with a clear
# message rather than let a 1.x poetry hit an unknown `--clean`/`-C` flag and
# produce an opaque CLI error partway through a researcher's release build.
poetry_version_output="$(poetry --version 2>/dev/null || true)"
poetry_version="$(printf '%s' "$poetry_version_output" | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -n1 || true)"
poetry_major="${poetry_version%%.*}"
if [ -z "$poetry_major" ] || ! [[ "$poetry_major" =~ ^[0-9]+$ ]] || [ "$poetry_major" -lt 2 ]; then
    echo "ERROR: poetry >= 2.0 required for release wheel builds (found: ${poetry_version_output:-unknown})." >&2
    echo "       This script uses poetry-2-only flags (--clean, -C <dir>). Re-provision or" >&2
    echo "       upgrade poetry where this checkout runs." >&2
    exit 1
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
py_dir="$repo_root/packages/python"

stage_dir=$(mktemp -d)
cleanup() {
    rm -rf -- "${stage_dir:?}"
}
trap cleanup EXIT

cp "$py_dir/pyproject.toml" "$stage_dir/"
cp -R "$py_dir/port" "$stage_dir/port"

e2etest_module="$stage_dir/port/platforms/e2etest.py"
e2etest_config="$stage_dir/port/configs/e2etest_config.json"
e2etest_multifile_module="$stage_dir/port/platforms/e2etest_multifile.py"
e2etest_multifile_config="$stage_dir/port/configs/e2etest_multifile_config.json"

if [ ! -f "$e2etest_module" ]; then
    echo "ERROR: expected test-only file missing from staged copy: port/platforms/e2etest.py" >&2
    echo "       The release-wheel exclusion assumes this file exists so it can be removed." >&2
    echo "       If the e2etest platform was renamed or removed, update this script (and" >&2
    echo "       ADR-0004) to match; do not silently ship without the exclusion." >&2
    exit 1
fi
if [ ! -f "$e2etest_config" ]; then
    echo "ERROR: expected test-only file missing from staged copy: port/configs/e2etest_config.json" >&2
    echo "       The release-wheel exclusion assumes this file exists so it can be removed." >&2
    echo "       If the e2etest platform was renamed or removed, update this script (and" >&2
    echo "       ADR-0004) to match; do not silently ship without the exclusion." >&2
    exit 1
fi
if [ ! -f "$e2etest_multifile_module" ]; then
    echo "ERROR: expected test-only file missing from staged copy: port/platforms/e2etest_multifile.py" >&2
    echo "       The release-wheel exclusion assumes this file exists so it can be removed." >&2
    echo "       If the e2etest_multifile platform was renamed or removed, update this script (and" >&2
    echo "       ADR-0004) to match; do not silently ship without the exclusion." >&2
    exit 1
fi
if [ ! -f "$e2etest_multifile_config" ]; then
    echo "ERROR: expected test-only file missing from staged copy: port/configs/e2etest_multifile_config.json" >&2
    echo "       The release-wheel exclusion assumes this file exists so it can be removed." >&2
    echo "       If the e2etest_multifile platform was renamed or removed, update this script (and" >&2
    echo "       ADR-0004) to match; do not silently ship without the exclusion." >&2
    exit 1
fi

rm -- "$e2etest_module"
rm -- "$e2etest_config"
rm -- "$e2etest_multifile_module"
rm -- "$e2etest_multifile_config"

echo "Building release wheel from staged copy (e2etest and e2etest_multifile excluded)..."
# Poetry keys cached environments by project path (see ADR-0004), and
# `poetry build` otherwise provisions one for whatever path it is pointed at
# even though a plain poetry-core wheel build needs no environment at all.
# Every mktemp staging directory is a distinct path, so left to its default
# behavior this would leak one throwaway virtualenv into the global poetry
# cache per release build, forever. POETRY_VIRTUALENVS_CREATE=false skips
# that provisioning for this one command without touching the developer's
# own poetry config.
POETRY_VIRTUALENVS_CREATE=false poetry -C "$stage_dir" build \
    --format wheel \
    --clean \
    --output "$py_dir/dist"
