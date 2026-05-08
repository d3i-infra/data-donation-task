"""Shared helpers for extractor integration tests.

Usage in a platform-specific test file::

    from extractor_integration_helpers import ExtractorSpec, find_fixture, make_reader

Fixture naming convention
-------------------------
Drop a real DDP zip into ``tests/ddp/`` using the naming pattern::

    <platform>_<anything>.zip

Examples::

    tests/ddp/chatgpt_my_export.zip
    tests/ddp/instagram_2024.zip

``find_fixture("chatgpt")`` returns the first match for ``chatgpt_*.zip``.
The ``ddp/`` directory is git-ignored — real DDPs must never enter version
control (see AD0001).

See Also
--------
docs/decisions/testing/AD0001 : policy against committing real DDP data
docs/decisions/testing/AD0002 : Pyodide mocking strategy for desktop testing
"""
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

import pandas as pd

from port.helpers.extraction_helpers import ZipArchiveReader
import port.helpers.validate as validate

DDP_DIR = Path(__file__).parent / "ddp"


def find_fixture(platform: str) -> Path | None:
    """Return the first ``<platform>_*.zip`` in ``tests/ddp/``, or None."""
    if not DDP_DIR.is_dir():
        return None
    matches = sorted(DDP_DIR.glob(f"{platform}_*.zip"))
    return matches[0] if matches else None


def make_reader(fixture: Path, ddp_categories: list) -> ZipArchiveReader:
    """Validate *fixture* and return a ``ZipArchiveReader`` ready for extraction."""
    errors: Counter = Counter()
    validation = validate.validate_zip(ddp_categories, str(fixture))
    return ZipArchiveReader(str(fixture), validation.archive_members, errors)


@dataclass
class ExtractorSpec:
    """Test-layer descriptor for a single extractor under test.

    Parameters
    ----------
    name:
        Human-readable label used as the pytest parametrize ID.
    extractor:
        Callable with signature ``(reader, errors, **kwargs) -> pd.DataFrame``.
    kwargs:
        Extra keyword arguments forwarded to the extractor beyond ``reader``
        and ``errors``.
    """

    name: str
    extractor: Callable[..., pd.DataFrame]
    kwargs: dict = field(default_factory=dict)

    def run(self, reader: ZipArchiveReader) -> pd.DataFrame:
        errors: Counter = Counter()
        return self.extractor(reader, errors, **self.kwargs)
