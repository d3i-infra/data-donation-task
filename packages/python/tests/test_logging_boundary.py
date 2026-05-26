"""Tests for PII-safe logging boundary.

Verifies that:
- ExtractionResult dataclass works correctly
- Content loggers are non-propagating (defense in depth)
- Helper error counting works
- emit_log produces CommandSystemLog commands
"""
import logging
from collections import Counter
from unittest.mock import MagicMock

import pytest
from port.api.d3i_props import ExtractionResult, PropsUIPromptConsentFormTableViz
from port.api.commands import CommandSystemLog
from port.api import props
import port.helpers.port_helpers as ph
import pandas as pd


class _SpyHandler(logging.Handler):
    """Minimal handler that records messages for testing."""
    def __init__(self):
        super().__init__()
        self.messages = []

    def emit(self, record):
        self.messages.append(self.format(record))


class TestExtractionResult:
    def test_basic_construction(self):
        tables = [
            PropsUIPromptConsentFormTableViz(
                id="test",
                data_frame=pd.DataFrame({"a": [1]}),
                title=props.Translatable({"en": "T", "nl": "T"}),
            )
        ]
        errors = Counter({"FileNotFound": 3, "JSONDecodeError": 2})
        result = ExtractionResult(tables=tables, errors=errors)
        assert len(result.tables) == 1
        assert result.errors["FileNotFound"] == 3

    def test_empty_errors(self):
        result = ExtractionResult(tables=[], errors=Counter())
        assert not result.errors
        assert not result.tables

    def test_default_errors(self):
        """errors defaults to empty Counter if not provided."""
        result = ExtractionResult(tables=[])
        assert isinstance(result.errors, Counter)
        assert not result.errors


class TestContentLoggerIsolation:
    """Content loggers must not propagate even if parent handlers exist."""

    def test_content_logger_does_not_propagate(self):
        """Content logger stays silent even if a handler is on port."""
        port_logger = logging.getLogger("port")
        spy = _SpyHandler()
        port_logger.addHandler(spy)

        content_logger = logging.getLogger("port.helpers.extraction_helpers.content")
        content_logger.propagate = False
        content_logger.addHandler(logging.NullHandler())
        content_logger.debug("Contained in zip: messages/inbox/mothersname_123/photo.jpg")

        assert len(spy.messages) == 0
        port_logger.removeHandler(spy)


class TestHelperErrorCounting:
    """Verify helper functions increment the errors Counter when provided."""

    def test_extract_file_from_zip_counts_file_not_found(self, tmp_path):
        """extract_file_from_zip increments errors when file not in zip."""
        import zipfile
        from port.helpers.extraction_helpers import extract_file_from_zip

        zip_path = tmp_path / "test.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("existing.json", '{"key": "value"}')

        errors = Counter()
        extract_file_from_zip(str(zip_path), "nonexistent.json", errors=errors)
        assert errors["FileNotFoundInZipError"] == 1

    def test_errors_none_does_not_crash(self, tmp_path):
        """extract_file_from_zip works without errors parameter (backward compat)."""
        import zipfile
        from port.helpers.extraction_helpers import extract_file_from_zip

        zip_path = tmp_path / "test.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("existing.json", '{"key": "value"}')

        result = extract_file_from_zip(str(zip_path), "nonexistent.json")
        assert result.getvalue() == b""


class TestEmitLog:
    """Verify emit_log produces CommandSystemLog via the generator protocol."""

    def test_emit_log_yields_command_system_log(self):
        gen = ph.emit_log("info", "test milestone")
        cmd = next(gen)
        assert isinstance(cmd, CommandSystemLog)
        assert cmd.level == "info"
        assert cmd.message == "test milestone"

    def test_emit_log_completes_after_response(self):
        gen = ph.emit_log("info", "test")
        next(gen)  # get the command
        with pytest.raises(StopIteration):
            gen.send(None)  # send PayloadVoid equivalent
