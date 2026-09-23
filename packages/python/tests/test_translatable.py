"""Tests for Translatable construction-time validation.

Translatable is a data carrier: Python never resolves text, JS renders it.
These tests pin the entry-point junk guard — a wrong shape must fail loudly
at construction rather than reaching the renderer as unusable text.

Locale-set policy (which locales a study must supply) is NOT enforced here;
en-presence is a researcher-facing gate, not a construction-time rule.
"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from port.api.props import PropsUIPromptHelloWorld, Translatable, Translations


class TestTranslatableAcceptsValidBundles:
    """The runtime guard is deliberately looser than the Translations TypedDict.

    Translations declares en/nl required to steer researchers writing new
    content; the constructor only rejects shapes the renderer cannot use.
    The `pyright: ignore` comments below mark exactly that gap — these bundles
    are legal at runtime by design, not oversights.
    """

    def test_en_nl_bundle(self):
        t = Translatable({"en": "Hello", "nl": "Hallo"})
        assert t.translations == {"en": "Hello", "nl": "Hallo"}

    def test_en_only_bundle(self):
        # Platform modules legitimately build partial bundles.
        bundle = Translatable({"en": "Hello"})  # pyright: ignore[reportArgumentType]
        assert bundle.translations == {"en": "Hello"}

    def test_bundle_without_en(self):
        # en-presence is a researcher-facing gate, not a construction-time rule.
        bundle = Translatable({"nl": "Hallo"})  # pyright: ignore[reportArgumentType]
        assert bundle.translations == {"nl": "Hallo"}

    def test_empty_string_value_allowed(self):
        # Deliberate-hit contract: an empty string is a real, intentional
        # translation ("render nothing here"), not a missing one.
        bundle = Translatable({"en": ""})  # pyright: ignore[reportArgumentType]
        assert bundle.translations == {"en": ""}

    def test_empty_dict_allowed(self):
        bundle = Translatable({})  # pyright: ignore[reportArgumentType]
        assert bundle.translations == {}

    def test_all_supported_locales(self):
        # de and it are the locales this task added to Translations.
        bundle: Translations = {"en": "a", "nl": "b", "de": "c", "it": "d", "es": "e"}
        assert Translatable(bundle).translations == bundle


class TestTranslatableRejectsJunk:
    """Every construction here is a type error by design — that is the point."""

    def test_bare_string_raises(self):
        with pytest.raises(TypeError):
            Translatable("bare string")  # pyright: ignore[reportArgumentType]

    def test_none_raises(self):
        with pytest.raises(TypeError):
            Translatable(None)  # pyright: ignore[reportArgumentType]

    def test_list_raises(self):
        with pytest.raises(TypeError):
            Translatable(["en", "Hello"])  # pyright: ignore[reportArgumentType]

    def test_non_string_value_raises(self):
        with pytest.raises(TypeError):
            Translatable({"en": 42})  # pyright: ignore[reportArgumentType]

    def test_nested_dict_value_raises(self):
        with pytest.raises(TypeError):
            Translatable({"en": {"en": "Hello"}})  # pyright: ignore[reportArgumentType]

    def test_none_value_raises(self):
        with pytest.raises(TypeError):
            Translatable({"en": None})  # pyright: ignore[reportArgumentType]

    def test_non_string_key_raises(self):
        with pytest.raises(TypeError):
            Translatable({1: "Hello"})  # pyright: ignore[reportArgumentType]

    def test_error_message_names_the_class(self):
        with pytest.raises(TypeError, match="Translatable"):
            Translatable("bare string")  # pyright: ignore[reportArgumentType]


class TestTranslatableSerialization:
    def test_todict_round_trips_translations(self):
        t = Translatable({"en": "Hello", "nl": "Hallo"})
        assert t.toDict() == {"translations": {"en": "Hello", "nl": "Hallo"}}


class TestHelloWorldSerializesText:
    def test_text_is_serialized_not_passed_through(self):
        prompt = PropsUIPromptHelloWorld(text=Translatable({"en": "Hi", "nl": "Hoi"}))
        d = prompt.toDict()
        assert d["__type__"] == "PropsUIPromptHelloWorld"
        assert d["text"] == {"translations": {"en": "Hi", "nl": "Hoi"}}
