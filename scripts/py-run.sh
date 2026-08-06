#!/bin/bash
# Location-independent wrapper for Python tooling.
# Works from any cwd and any worktree.
#
# Usage:
#   scripts/py-run.sh test [pytest args...]
#   scripts/py-run.sh typecheck [pyright args...]
#   scripts/py-run.sh verify              # runs both test + typecheck

set -e

REPO_ROOT="$(git rev-parse --show-toplevel)"
PY_DIR="$REPO_ROOT/packages/python"

if [ ! -d "$PY_DIR" ]; then
    echo "ERROR: packages/python not found at $PY_DIR"
    exit 1
fi

cd "$PY_DIR"

# Pyright needs the poetry virtualenv's interpreter to resolve pandas, numpy,
# dateutil and the installed stub packages. The venv path is machine-specific
# (it embeds a hash of the project path), so it must never be checked into
# pyrightconfig.json — resolve it here instead. Works on any machine and in CI,
# where the workflow runs `poetry install` before the typecheck step.
#
# A missing environment is a hard failure, not a warning: without it pyright
# silently falls back to the default interpreter and reports a wall of
# unresolved-import errors that look like real type errors.
#
# Sets the global PYRIGHT_ENV_ARGS array. Deliberately avoids `mapfile`, which
# is a bash-4 builtin and is absent from macOS's stock /bin/bash 3.2.
PYRIGHT_ENV_ARGS=()
resolve_pyright_interpreter() {
    local interpreter
    PYRIGHT_ENV_ARGS=()
    interpreter="$(poetry env info --executable 2>/dev/null || true)"
    if [ -z "$interpreter" ]; then
        echo "ERROR: no poetry environment found for packages/python." >&2
        echo "       Pyright cannot resolve pandas/numpy or the installed stubs" >&2
        echo "       without it. Run:" >&2
        echo "" >&2
        echo "         cd packages/python && poetry install --with test,dev" >&2
        echo "" >&2
        echo "       (and make sure poetry itself is installed and on PATH)." >&2
        exit 1
    fi
    PYRIGHT_ENV_ARGS=(--pythonpath "$interpreter")
}

case "${1:-}" in
    test)
        shift
        poetry run pytest -v "$@"
        ;;
    typecheck)
        shift
        resolve_pyright_interpreter
        [ $# -eq 0 ] && set -- port/platforms/*.py port/helpers/*.py port/api/*.py port/main.py port/script.py
        pnpm exec pyright "${PYRIGHT_ENV_ARGS[@]}" "$@"
        ;;
    verify)
        echo "=== Running tests ==="
        poetry run pytest -v
        echo ""
        echo "=== Running type checks ==="
        resolve_pyright_interpreter
        pnpm exec pyright "${PYRIGHT_ENV_ARGS[@]}" port/platforms/*.py port/helpers/*.py port/api/*.py port/main.py port/script.py
        echo ""
        echo "=== All checks passed ==="
        ;;
    *)
        echo "Usage: scripts/py-run.sh {test|typecheck|verify} [args...]"
        echo ""
        echo "  test [args]     Run pytest with optional arguments"
        echo "  typecheck       Run Pyright type checker"
        echo "  verify          Run both tests and type checks"
        exit 1
        ;;
esac
