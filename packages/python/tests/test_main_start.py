"""
Tests for port.main.start — the entry point invoked by py_worker.js.

The engine calls it with a single #960-shaped context dict
{"sessionId", "platform", "locale"}.

`start()` delegates to `process()`, which imports platform modules and
validates configs — to keep this test hermetic we monkeypatch
`port.main.process` with a stub generator recording the args it received.
"""
import pytest

from port.main import start
from port.helpers import ui_locale


@pytest.fixture(autouse=True)
def reset_ui_locale():
    ui_locale.set_ui_locale(None)
    yield
    ui_locale.set_ui_locale(None)


@pytest.fixture
def recording_process(monkeypatch):
    """Stub for port.script.process that records its call args."""
    calls = []

    def fake_process(session_id, platform):
        # Record eagerly (matches process()'s real call signature); return a
        # generator so ScriptWrapper's generator-protocol usage still works.
        calls.append((session_id, platform))

        def gen():
            yield

        return gen()

    monkeypatch.setattr("port.main.process", fake_process)
    return calls


def test_extracts_session_platform_locale(recording_process):
    wrapper = start({"sessionId": "abc123", "platform": "example", "locale": "nl"})

    assert recording_process[0] == ("abc123", "example")
    assert wrapper.platform == "example"
    assert ui_locale.get_ui_locale() == "nl"


def test_missing_locale_defaults_en(recording_process):
    wrapper = start({"sessionId": "abc123", "platform": "example"})

    assert recording_process[0] == ("abc123", "example")
    assert wrapper.platform == "example"
    assert ui_locale.get_ui_locale() == "en"
