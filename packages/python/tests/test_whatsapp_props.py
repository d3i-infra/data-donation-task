"""WhatsApp's ``extraction()`` carries ``date_columns`` into the table props (ADR-0043).

``whatsapp.py`` builds ``PropsUIPromptConsentFormTableViz`` itself instead of going
through ``run_extraction`` (it pre-parses a chat file into a DataFrame, so extractors
take ``(df, errors)`` rather than the usual ``(reader, errors)`` — see the module
docstring). That hand-rolled construction dropped ``date_columns`` (final review,
Important 1): the chat table declares ``Timestamp`` as ``iso-8601`` in its
``Table config::`` block, but the prop reaching the front end carried no
``date_columns`` at all, so both of its date charts regressed to fully unplotted.

Builds the config in memory from the docstrings (as ``pnpm generate-config`` does)
rather than reading the gitignored, and here stale, ``configs/whatsapp_config.json``
(task-8-review.md / final-review.md, Important finding 2).
"""
import sys
from pathlib import Path
from unittest.mock import patch

import pandas as pd

_REPO_ROOT = Path(__file__).resolve().parents[3]
_SCRIPTS_DIR = _REPO_ROOT / "scripts"
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

from generate_port_config import build_config  # noqa: E402

import port.helpers.table_extractor as te  # noqa: E402
from port.platforms import whatsapp  # noqa: E402


def _in_memory_config():
    raw = build_config("whatsapp")
    return te._build_config(raw, whatsapp.EXTRACTOR_REGISTRY)


def test_whatsapp_extraction_carries_date_columns_in_props(monkeypatch):
    config = _in_memory_config()
    monkeypatch.setattr(te, "load_port_config", lambda registry, platform: config)

    df = pd.DataFrame({
        "date": ["2026-06-15T20:30:41"],
        "name": ["Alice"],
        "chat_message": ["hi"],
    })
    result = whatsapp.extraction(df)
    tables = {t.id: t for t in result.tables}
    assert "whatsapp_group_chat" in tables
    chat_table = tables["whatsapp_group_chat"]

    assert chat_table.date_columns == {"Timestamp": {"encoding": "iso-8601"}}
    assert chat_table.toDict()["date_columns"] == {"Timestamp": {"encoding": "iso-8601"}}


class TestWhatsAppConfigKey:
    """WhatsAppFlow looks its config up as "whatsapp", matching
    configs/whatsapp_config.json — never as its display name "WhatsApp Group
    Chat" (final review, Minor 8). ``load_reference_timezone`` and
    ``load_public_key_pem`` lowercase-and-append ``_config.json`` to whatever
    they are given, so "WhatsApp Group Chat" resolved to a file that never
    exists and the flow's ``display_timezone``/``public_key_pem`` were always
    None regardless of what the config declared.

    Mirrors ``tests/test_flow_builder.py``'s ``TestDisplayTimezone`` pattern:
    monkeypatch the loaders and assert on what argument they were called with,
    rather than depending on the real (gitignored, locally-generated)
    ``whatsapp_config.json`` carrying a timezone.
    """

    def test_timezone_lookup_uses_the_config_filename_key(self, monkeypatch):
        seen = {}

        def _fake_load_reference_timezone(platform):
            seen["timezone_platform"] = platform
            return "Europe/Amsterdam"

        monkeypatch.setattr(
            "port.helpers.flow_builder.load_reference_timezone",
            _fake_load_reference_timezone,
        )
        with patch("port.helpers.flow_builder.load_public_key_pem", return_value=None):
            flow = whatsapp.WhatsAppFlow("test-session")

        assert seen["timezone_platform"] == "whatsapp"
        assert flow.display_timezone == "Europe/Amsterdam"

    def test_public_key_lookup_uses_the_config_filename_key(self, monkeypatch):
        seen = {}

        def _fake_load_public_key_pem(platform):
            seen["pem_platform"] = platform
            return None

        monkeypatch.setattr(
            "port.helpers.flow_builder.load_public_key_pem",
            _fake_load_public_key_pem,
        )
        with patch("port.helpers.flow_builder.load_reference_timezone", return_value=None):
            whatsapp.WhatsAppFlow("test-session")

        assert seen["pem_platform"] == "whatsapp"

    def test_display_name_and_donation_key_are_unchanged(self, monkeypatch):
        """ADR-0020: the donation key is f"{session_id}-{platform_name.lower()}".
        Fixing the config lookup must not touch platform_name or that contract.
        """
        with patch("port.helpers.flow_builder.load_public_key_pem", return_value=None), \
             patch("port.helpers.flow_builder.load_reference_timezone", return_value=None):
            flow = whatsapp.WhatsAppFlow("test-session")

        assert flow.platform_name == "WhatsApp Group Chat"
