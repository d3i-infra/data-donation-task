"""Tests for the e2e-only fault-injection platform.

The e2etest platform exists so the Playwright suite can exercise the
consent-gated error flow without shipping a crash trigger in a production
platform (the example platform is both a release artifact and the documented
copy-me template). It delegates validation and extraction to the example
platform and adds one behavior: a deliberate raise when the uploaded archive
contains ``trigger_error.txt``.
"""
import pytest

from port.helpers.validate import StatusCode, ValidateInput


def _validation(members: list[str]) -> ValidateInput:
    v = ValidateInput([StatusCode(id=0, description="Valid zip file")], [])
    v.archive_members = members
    return v


def test_trigger_archive_raises_before_any_io():
    """An archive containing the trigger file raises immediately — no config
    or file access needed, so a dummy path suffices."""
    from port.platforms.e2etest import extraction

    with pytest.raises(RuntimeError, match="Intentional test error"):
        extraction("does-not-exist.zip", _validation(["trigger_error.txt"]))


def test_nested_trigger_file_also_raises():
    from port.platforms.e2etest import extraction

    with pytest.raises(RuntimeError, match="Intentional test error"):
        extraction("does-not-exist.zip", _validation(["subdir/trigger_error.txt"]))


def test_standard_platform_interface():
    """The module satisfies the standard platform interface so script.py can
    dispatch to it like any other platform."""
    from port.platforms import e2etest

    assert callable(e2etest.extraction)
    assert callable(e2etest.process)
    assert "file_stats_to_df" in e2etest.EXTRACTOR_REGISTRY


def test_delegates_to_example_platform(monkeypatch):
    """Validation and extraction behavior come from the example platform, so
    the donation e2e specs exercise the same code paths on e2etest builds."""
    from collections import Counter

    import pandas as pd

    from port.platforms import e2etest, example

    calls = []
    monkeypatch.setattr(
        example, "file_stats_to_df", lambda reader, errors: (calls.append((reader, errors)), pd.DataFrame())[1]
    )
    e2etest.file_stats_to_df("reader-sentinel", Counter())
    assert len(calls) == 1
    assert calls[0][0] == "reader-sentinel"

    flow = e2etest.E2eTestFlow("session-1")
    # Participant-visible identity mimics the example platform (headings,
    # donation key) so the baseline donation specs pass unchanged.
    assert flow.platform_name == "example"
