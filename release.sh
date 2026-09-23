#!/bin/bash
set -e

# Check prerequisites
./check-deps.sh release

export NODE_ENV=production

NAME=${PWD##*/}
BRANCH=${1:-$(git branch --show-current)}
BRANCH=${BRANCH//\//-}
TIMESTAMP=$(date '+%Y-%m-%d_%H-%M-%S')

CONFIGS_DIR="packages/python/port/configs"

# e2etest and e2etest_multifile are repository test infrastructure (Playwright's
# error-flow and multi-file fixtures), not researcher platforms: their
# configs must not represent study membership during release, and the
# modules they select must never reach a participant. Reject them before
# anything else runs — see ADR-0004.
if [ "${VITE_PLATFORM:-}" = "e2etest" ]; then
    echo "ERROR: e2etest is test-only and cannot be released." >&2
    echo "       It exists for 'VITE_PLATFORM=e2etest pnpm test:e2e' and" >&2
    echo "       'VITE_PLATFORM=e2etest pnpm start' only." >&2
    exit 1
fi
if [ "${VITE_PLATFORM:-}" = "e2etest_multifile" ]; then
    echo "ERROR: e2etest_multifile is test-only and cannot be released." >&2
    echo "       It exists for 'VITE_PLATFORM=e2etest_multifile pnpm test:e2e:multi'" >&2
    echo "       and 'VITE_PLATFORM=e2etest_multifile pnpm start' only." >&2
    exit 1
fi

# If VITE_PLATFORM is already set, release only that platform
if [ -n "$VITE_PLATFORM" ]; then
    config_file="$CONFIGS_DIR/${VITE_PLATFORM}_config.json"
    if [ ! -f "$config_file" ]; then
        echo "ERROR: No config found for platform '$VITE_PLATFORM' at $config_file."
        echo "Generate it first with:  pnpm generate-config $VITE_PLATFORM"
        exit 1
    fi
    platforms=("$VITE_PLATFORM")
else
    # Discover platforms from configs/<platform>_config.json files
    platforms=()
    for config_file in "$CONFIGS_DIR"/*_config.json; do
        [ -f "$config_file" ] || continue
        basename="${config_file##*/}"          # e.g. chatgpt_config.json
        platform="${basename%_config.json}"    # e.g. chatgpt
        # Documented exception to "every config is a study platform": e2etest
        # and e2etest_multifile are Playwright's error-flow and multi-file
        # fixtures, not something a researcher deploys. See the rejection
        # above and ADR-0004.
        [ "$platform" = "e2etest" ] && continue
        [ "$platform" = "e2etest_multifile" ] && continue
        platforms+=("$platform")
    done

    if [ ${#platforms[@]} -eq 0 ]; then
        echo "ERROR: No platform configs found in $CONFIGS_DIR."
        echo "Generate one first with:  pnpm generate-config <platform>"
        exit 1
    fi
fi

echo "Found ${#platforms[@]} platform(s): ${platforms[*]}"
mkdir -p releases

# The config validator imports the port package's third-party dependencies
# (pandas), so a usable Python environment for packages/python is a
# *precondition* of a release. This script checks for it and stops; it never
# provisions it. Three reasons to probe rather than install:
#
#   * Check, don't provision, is this repo's existing precedent — check-deps.sh
#     reports the missing tool and exits rather than installing it.
#   * dd-script-builder runs the release from a `cp -r` copy of this tree, and
#     its builds are network-free by that repo's ADR-0011. An install here
#     would need the network and would break exactly the case it was added for.
#   * On the production VM the environment belongs to the deployment repo's
#     data-donation-task sync unit, which wipes, re-clones and re-provisions
#     the checkout daily. A release that installed on top of that would be
#     fighting the owner of the state.
#
# Poetry keys its cached environments by project path, so a copied tree has no
# cached environment at all — which is why provisioning creates the venv
# *inside* the tree, at packages/python/.venv, where `cp -r` carries it into
# every build copy. The probe follows the same order: the in-project venv
# first, then whatever environment poetry already associates with this checkout
# (the developer-machine case, where `poetry install` used the global cache).
#
# Nothing here uses `poetry run` — not the probe, not the validation below. In
# a tree with no environment `poetry run` silently creates an empty one as a
# side effect, leaking a directory into the global cache on a machine that was
# only ever asked a question; `poetry env info --executable` only reports — it
# prints nothing and exits non-zero when no environment exists. Resolving the
# interpreter once and then *running it directly* also keeps the probe and the
# validation on the same environment: a second `poetry run` would resolve
# independently, and under `virtualenvs.in-project = false` it would ignore the
# .venv the probe just approved.
#
# The canary is the validator's own import chain (`--help` executes every
# module-level import, then argparse exits 0) rather than a hand-listed
# `import pandas`, so it keeps testing the right thing as dependencies change.
echo "Checking Python environment for config validation..."
PY_PKG_DIR="packages/python"
VALIDATOR="scripts/validate_port_config.py"
VALIDATOR_PYTHON="$PY_PKG_DIR/.venv/bin/python"
if [ ! -x "$VALIDATOR_PYTHON" ]; then
    VALIDATOR_PYTHON=$(poetry -C "$PY_PKG_DIR" env info --executable 2>/dev/null || true)
fi
if [ -z "$VALIDATOR_PYTHON" ] || ! "$VALIDATOR_PYTHON" "$VALIDATOR" --help >/dev/null 2>&1; then
    echo "ERROR: the Python environment used to validate platform configs is" >&2
    echo "       missing or incomplete (packages/python has no environment, or" >&2
    echo "       the validator's dependencies do not import there). Every config" >&2
    echo "       is validated before anything is built, so nothing was built." >&2
    echo "" >&2
    echo "       release.sh only checks for this environment — it never creates" >&2
    echo "       one. Provision it where it belongs:" >&2
    echo "" >&2
    echo "       * On a development machine, in this checkout:" >&2
    echo "             cd packages/python && poetry install" >&2
    echo "" >&2
    echo "       * If this tree is a copy or a deployment of another checkout" >&2
    echo "         (dd-script-builder copies the source tree to build; the VM's" >&2
    echo "         sync service re-clones it daily), then the SOURCE checkout is" >&2
    echo "         what needs provisioning, and it needs an in-project venv so" >&2
    echo "         that copies carry the environment with them:" >&2
    echo "             python3 -m venv packages/python/.venv" >&2
    echo "             cd packages/python && poetry install --no-root --only main" >&2
    echo "         Then take the copy again. Poetry keys cached environments by" >&2
    echo "         project path, so a copy never inherits one from the original" >&2
    echo "         location — only an in-project .venv survives the copy." >&2
    exit 1
fi

for PLATFORM in "${platforms[@]}"; do
    # Configs are hand-editable after generation, so validate every one at
    # release time — schema, extractor registry, and UI-content locale
    # coverage. A researcher must learn here that a table header lost its
    # English text, not from a participant staring at "?text?".
    echo "Validating config for platform: ${PLATFORM}..."
    if ! "$VALIDATOR_PYTHON" "$VALIDATOR" "$PLATFORM" --report; then
        echo "ERROR: config validation failed for '${PLATFORM}' (see above); nothing was built." >&2
        exit 1
    fi

    echo "Building for platform: ${PLATFORM}..."
    export VITE_PLATFORM=$PLATFORM
    pnpm run build:release

    # Defense-in-depth: check the artifact that will actually ship, not just
    # the staged build that produced it. Catches a stale wheel, a copy-step
    # regression, or a future change to the build pipeline that would
    # otherwise let e2etest slip back into a release zip unnoticed. See
    # ADR-0004 and scripts/verify_release_wheel.py.
    echo "Verifying release artifact for platform: ${PLATFORM}..."
    if ! "$VALIDATOR_PYTHON" scripts/verify_release_wheel.py "$PLATFORM" \
        --dist-dir packages/data-collector/dist; then
        echo "ERROR: release artifact verification failed for '${PLATFORM}'; nothing was zipped." >&2
        exit 1
    fi

    RELEASE_NAME="${NAME}_${PLATFORM}_${BRANCH}_${TIMESTAMP}.zip"
    cd packages/data-collector/dist
    zip -r ../../../releases/${RELEASE_NAME} .
    cd ../../..
    echo "Created: releases/${RELEASE_NAME}"
done

echo ""
echo "Done. ${#platforms[@]} platform release(s) created in releases/"
