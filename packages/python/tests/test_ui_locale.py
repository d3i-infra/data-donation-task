"""
Tests for the session-fixed UI locale helper (port.helpers.ui_locale).

This locale is received once from the host via port.start's #960-shaped
context dict and is unrelated to helpers.validate.Language, which governs
the language of DDP *export* content, not the participant UI.
"""
import pytest

from port.helpers import ui_locale


@pytest.fixture(autouse=True)
def reset_ui_locale():
    """Reset module-level state before and after each test."""
    ui_locale.set_ui_locale(None)
    yield
    ui_locale.set_ui_locale(None)


def test_default_locale_is_en():
    assert ui_locale.get_ui_locale() == "en"


def test_set_and_get_locale():
    ui_locale.set_ui_locale("nl")
    assert ui_locale.get_ui_locale() == "nl"


def test_set_none_defaults_to_en():
    ui_locale.set_ui_locale("nl")
    ui_locale.set_ui_locale(None)
    assert ui_locale.get_ui_locale() == "en"


def test_set_empty_string_defaults_to_en():
    ui_locale.set_ui_locale("nl")
    ui_locale.set_ui_locale("")
    assert ui_locale.get_ui_locale() == "en"


def test_set_non_string_defaults_to_en():
    ui_locale.set_ui_locale("nl")
    ui_locale.set_ui_locale(123)
    assert ui_locale.get_ui_locale() == "en"
