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


def test_set_unsupported_locale_defaults_to_en():
    """Unsupported locales are normalized to the default, not stored verbatim."""
    ui_locale.set_ui_locale("nl")
    ui_locale.set_ui_locale("ro")
    assert ui_locale.get_ui_locale() == "en"


def test_module_constants_form_a_consistent_locale_set():
    """Invariants only — the actual locale values are the sync test's job
    (test_ui_locales_sync.py), so this must never restate them literally."""
    assert ui_locale.SUPPORTED_UI_LOCALES, "supported locales must be non-empty"
    assert len(ui_locale.SUPPORTED_UI_LOCALES) == len(set(ui_locale.SUPPORTED_UI_LOCALES)), (
        "supported locales must not contain duplicates"
    )
    assert ui_locale.DEFAULT_UI_LOCALE in ui_locale.SUPPORTED_UI_LOCALES
    assert set(ui_locale.PROVISIONAL_UI_LOCALES) <= set(ui_locale.SUPPORTED_UI_LOCALES)


class TestNormalizeUiLocale:
    """Mirrors the JS test vectors in packages/data-collector/src/locale/policy.test.ts."""

    def test_normalizes_es_es_to_es(self):
        assert ui_locale.normalize_ui_locale("es-ES") == "es"

    def test_normalizes_en_uppercase_to_en(self):
        assert ui_locale.normalize_ui_locale("EN") == "en"

    def test_normalizes_nl_nl_underscore_to_nl(self):
        assert ui_locale.normalize_ui_locale("nl_NL") == "nl"

    def test_returns_default_for_unsupported_locale_ro(self):
        assert ui_locale.normalize_ui_locale("ro") == ui_locale.DEFAULT_UI_LOCALE

    def test_returns_default_for_empty_string(self):
        assert ui_locale.normalize_ui_locale("") == ui_locale.DEFAULT_UI_LOCALE

    def test_returns_default_for_none(self):
        assert ui_locale.normalize_ui_locale(None) == ui_locale.DEFAULT_UI_LOCALE

    def test_returns_default_for_non_string_input_number(self):
        assert ui_locale.normalize_ui_locale(42) == ui_locale.DEFAULT_UI_LOCALE

    def test_only_uses_supported_ui_locales_for_validation(self):
        assert "en" in ui_locale.SUPPORTED_UI_LOCALES
        assert "ro" not in ui_locale.SUPPORTED_UI_LOCALES
