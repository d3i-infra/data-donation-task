#!/bin/bash
set -e

# Usage: ./check-deps.sh [release]
# Without arguments: checks deps needed for development (pnpm start)
# With "release": also checks deps needed for release.sh

if [ "$1" != "release" ]; then
  if [ -z "${VITE_PLATFORM:-}" ]; then
    echo "ERROR: VITE_PLATFORM is not set."
    echo "  Use: VITE_PLATFORM=<platform> pnpm start"
    echo "  Or, for e2e tests: VITE_PLATFORM=<platform> pnpm test:e2e"
    echo "  Available platforms: packages/python/port/configs/*_config.json"
    echo "  No config yet? Generate one with: pnpm generate-config <platform>"
    echo "  To get started with the example platform: VITE_PLATFORM=example pnpm start"
    exit 1
  fi

  config="packages/python/port/configs/${VITE_PLATFORM}_config.json"
  if [ ! -f "$config" ]; then
    echo "ERROR: No config found for platform '${VITE_PLATFORM}'."
    echo "  Expected: $config"
    echo "  Generate it with: pnpm generate-config ${VITE_PLATFORM}"
    exit 1
  fi
fi

missing=()
command -v node >/dev/null 2>&1 || missing+=("node (https://nodejs.org/)")
command -v pnpm >/dev/null 2>&1 || missing+=("pnpm (https://pnpm.io/installation)")
command -v python3 >/dev/null 2>&1 || missing+=("python3 (https://www.python.org/)")

if [ "$1" = "release" ]; then
  command -v zip >/dev/null 2>&1 || missing+=("zip")
  command -v git >/dev/null 2>&1 || missing+=("git")
  # release.sh asks poetry where the port package's Python environment lives
  # (`poetry env info --executable`) and validates every platform config with
  # that interpreter before it builds anything, so a missing poetry stops the
  # release before the first zip. Same pointer as the guard in
  # scripts/gen_port_config.sh. (release.sh only *locates* the environment;
  # provisioning it is not release.sh's job — see its probe block.)
  if ! command -v poetry >/dev/null 2>&1; then
    missing+=("poetry (https://python-poetry.org/) — release.sh validates every platform config with it")
  fi
else
  # Dev mode still needs poetry: `pnpm start` builds the port wheel with it.
  command -v poetry >/dev/null 2>&1 || missing+=("poetry (https://python-poetry.org/)")
fi

if [ ${#missing[@]} -ne 0 ]; then
  echo "Error: the following required tools are not installed:"
  for tool in "${missing[@]}"; do
    echo "  - $tool"
  done
  exit 1
fi
