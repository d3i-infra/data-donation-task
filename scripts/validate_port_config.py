#!/usr/bin/env python3
"""Desktop launcher for the port-config validator CLI.

``port/__init__.py`` imports ``port.main``, which imports the Pyodide-only
``js`` module, so *any* ``import port...`` fails on a desktop interpreter.
pytest solves this once in ``tests/conftest.py`` (ADR-0015); this launcher is
the same shim for the command line, and it lives here in ``scripts/`` — the
build-time tooling layer, alongside ``generate_port_config.py`` — precisely so
that no environment awareness leaks into ``port/`` itself.

Usage
-----
    python scripts/validate_port_config.py <platform> [--report]
    python scripts/validate_port_config.py --all [--report]

Wherever ``js`` is importable (Pyodide, or a test session that has already
installed the shim) the validator module is runnable directly, no launcher::

    python -m port.helpers.port_config_validator --all --report

All arguments are passed straight through to that module's ``main()``.
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock

_PYTHON_PKG = Path(__file__).resolve().parent.parent / "packages" / "python"
if str(_PYTHON_PKG) not in sys.path:
    sys.path.insert(0, str(_PYTHON_PKG))

sys.modules.setdefault("js", MagicMock())

from port.helpers.port_config_validator import main  # noqa: E402

if __name__ == "__main__":
    raise SystemExit(main())
