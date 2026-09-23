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
control (see ADR-0014).

Multi-part fixture sets
------------------------
A platform whose DDP arrives as several zip parts (ADR-0040's archive-set
pipeline, e.g. Google Takeout) drops a *directory* of parts instead of one
zip, using the pattern::

    <platform>_set_<anything>/*.zip

Examples::

    tests/ddp/google_set_uu-acct/takeout-...-001.zip
    tests/ddp/google_set_uu-acct/takeout-...-002.zip

``find_fixture_sets("google")`` returns every matching directory, sorted.
``open_fixture_set(set_dir)`` builds the ``ArchiveSet`` over its parts.

See Also
--------
docs/decisions/0014-no-real-participant-data-in-version-control.md : policy against committing real DDP data
docs/decisions/0015-mock-the-pyodide-js-module-in-conftest-before-importing-port.md : Pyodide mocking strategy for desktop testing
docs/decisions/0040-present-multi-part-uploads-as-one-archive-set.md : the ArchiveSet pipeline fixture sets exercise
"""
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

import pandas as pd

from port.helpers.archive_set import ArchiveSet
from port.helpers.extraction_helpers import ZipArchiveReader
import port.helpers.validate as validate

DDP_DIR = Path(__file__).parent / "ddp"


def find_fixture(platform: str) -> Path | None:
    """Return the first ``<platform>_*.zip`` in ``tests/ddp/``, or None."""
    if not DDP_DIR.is_dir():
        return None
    matches = sorted(DDP_DIR.glob(f"{platform}_*.zip"))
    return matches[0] if matches else None


def find_fixture_sets(platform: str) -> list[Path]:
    """All ``<platform>_set_*/`` fixture directories in ``tests/ddp/`` (multi-part
    DDPs), sorted. Empty when none are present — callers skip, mirroring
    ``find_fixture``."""
    if not DDP_DIR.is_dir():
        return []
    return sorted(p for p in DDP_DIR.glob(f"{platform}_set_*") if p.is_dir())


class DiskPart:
    """A zip part on disk shaped like the worker's reader adapters: file-like
    with ``.name`` and ``.size``, so ``ArchiveSet`` can order and open it.

    ``seekable()`` matters, not just decoration: ``zipfile.ZipFile.__init__``
    calls it on the object it is handed (stdlib ``zipfile``, ``_SharedFile``
    construction) before it will treat the file as randomly seekable, so a
    part missing this method fails open with an ``AttributeError`` that the
    platform's own exception handling swallows into an opaque error-counter
    entry — this method exists so a fixture set behaves like the real
    browser-upload adapters it stands in for, not a distinct code path.

    A multi-set caller (e.g. ``test_extractor_integration_google.py``'s
    ``_SET_CACHE``) holds its built ``ArchiveSet``, and therefore every
    ``DiskPart`` it wraps, open for the whole test session rather than
    per-test — ``close()`` exists so such a cache *can* release its file
    handles when it chooses to, not because anything closes them today."""

    def __init__(self, path: Path):
        self._fh = open(path, "rb")
        self.name = path.name
        self.size = path.stat().st_size

    def read(self, n=-1):
        return self._fh.read(n)

    def seek(self, offset, whence=0):
        return self._fh.seek(offset, whence)

    def tell(self):
        return self._fh.tell()

    def seekable(self):
        return self._fh.seekable()

    def close(self):
        self._fh.close()


def open_fixture_set(set_dir: Path) -> ArchiveSet:
    """Build the ``ArchiveSet`` over every ``*.zip`` part in *set_dir*."""
    return ArchiveSet([DiskPart(p) for p in sorted(set_dir.glob("*.zip"))])


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
