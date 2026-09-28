"""Task 8: TikTok, Netflix and WhatsApp declare their date_columns encodings.

ADR-0042 donates the export's own raw value; ADR-0043 has the front end interpret
it from the per-table ``date_columns`` declaration in each extractor's ``Table
config::`` docstring block. This module checks the declarations themselves
(config-only, no fixtures needed): every table expected to carry a date column
declares exactly the spec this task assigned, and every declared key is an
actual header on that table.

Builds each platform's config in memory the way ``pnpm generate-config`` does
(``scripts/generate_port_config.py:build_config``, AST-based, reads the
``Table config::`` docstrings directly) rather than through
``load_port_config``, which reads the generated ``port/configs/<platform>_config.json``.
That file is gitignored (``*_config.json``) and not staged by this task, and no
test-running path regenerates it first — reading it here would make this suite
depend on a build step that never runs before it (task-8-review.md, Important
finding 1).
"""
import sys
from pathlib import Path

# scripts/ is not a package on sys.path; add the repo root's scripts/ directory
# (computed relative to this file, not the cwd) so `generate_port_config` can be
# imported directly, the same way `pnpm generate-config` invokes it.
_REPO_ROOT = Path(__file__).resolve().parents[3]
_SCRIPTS_DIR = _REPO_ROOT / "scripts"
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

from generate_port_config import build_config  # noqa: E402

from port.helpers.table_extractor import _build_config  # noqa: E402
from port.platforms import netflix, tiktok, whatsapp  # noqa: E402

#: table id -> {column: expected date_columns spec} for each platform.
TIKTOK_EXPECTED: dict[str, dict[str, dict]] = {
    "tiktok_watch_history": {"Date": {"encoding": "tiktok"}},
    "tiktok_favorite_videos": {"Date": {"encoding": "tiktok"}},
    "tiktok_follower": {"Date": {"encoding": "tiktok"}},
    "tiktok_following": {"Date": {"encoding": "tiktok"}},
    "tiktok_like_list": {"Date": {"encoding": "tiktok"}},
    "tiktok_searches": {"Date": {"encoding": "tiktok"}},
    "tiktok_share_history": {"Date": {"encoding": "tiktok"}},
    "tiktok_comments": {"Date": {"encoding": "tiktok"}},
    "tiktok_off_tiktok": {"Date": {"encoding": "tiktok"}},
}

#: Tables with a "Table config::" block but no date column (left alone).
TIKTOK_NO_DATE = {"tiktok_activity_summary", "tiktok_settings", "tiktok_hashtag"}

#: Netflix's three date columns are UTC wall-clock text without a zone marker
#: (the header names UTC; the value does not) — declared with utcOffsetMinutes 0
#: per the fix-round ruling so a zone-less value places as an instant, not `local`.
NETFLIX_EXPECTED: dict[str, dict[str, dict]] = {
    "netflix_ratings": {"Event Utc Ts": {"encoding": "iso-8601", "utcOffsetMinutes": 0}},
    "netflix_viewing_activity": {"Start Time": {"encoding": "iso-8601", "utcOffsetMinutes": 0}},
    "netflix_search_history": {"Utc Timestamp": {"encoding": "iso-8601", "utcOffsetMinutes": 0}},
}

WHATSAPP_EXPECTED: dict[str, dict[str, dict]] = {
    "whatsapp_group_chat": {"Timestamp": {"encoding": "iso-8601"}},
}

#: Tables with a "Table config::" block but no date column (left alone).
WHATSAPP_NO_DATE = {"whatsapp_emoji_usage", "whatsapp_user_statistics"}


def _load_config(platform_module, platform_name: str):
    """Build TableConfigs straight from the docstrings, no generated JSON on disk."""
    raw = build_config(platform_name)
    return _build_config(raw, platform_module.EXTRACTOR_REGISTRY)


def _by_id(config):
    return {table.id: table for table in config}


def _assert_declarations(config, expected: dict[str, dict[str, dict]]) -> None:
    tables = _by_id(config)
    for table_id, columns in expected.items():
        assert table_id in tables, f"{table_id}: no such table in config"
        table = tables[table_id]
        assert table.date_columns, f"{table_id}: expected date_columns, found none"
        assert set(table.date_columns) == set(columns), (
            f"{table_id}: date_columns keys {set(table.date_columns)} != expected {set(columns)}"
        )
        for column, spec in columns.items():
            assert column in table.headers, f"{table_id}: declared column {column!r} not in headers"
            assert table.date_columns[column] == spec, (
                f"{table_id}.{column}: spec {table.date_columns[column]!r} != expected {spec!r}"
            )


def test_tiktok_date_columns_declared():
    config = _load_config(tiktok, "tiktok")
    _assert_declarations(config, TIKTOK_EXPECTED)


def test_tiktok_tables_without_dates_are_undeclared():
    tables = _by_id(_load_config(tiktok, "tiktok"))
    for table_id in TIKTOK_NO_DATE:
        assert table_id in tables, f"{table_id}: no such table in config"
        assert not tables[table_id].date_columns, f"{table_id}: unexpectedly declares date_columns"


def test_netflix_date_columns_declared():
    config = _load_config(netflix, "netflix")
    _assert_declarations(config, NETFLIX_EXPECTED)


def test_whatsapp_date_columns_declared():
    config = _load_config(whatsapp, "whatsapp")
    _assert_declarations(config, WHATSAPP_EXPECTED)


def test_whatsapp_tables_without_dates_are_undeclared():
    tables = _by_id(_load_config(whatsapp, "whatsapp"))
    for table_id in WHATSAPP_NO_DATE:
        assert table_id in tables, f"{table_id}: no such table in config"
        assert not tables[table_id].date_columns, f"{table_id}: unexpectedly declares date_columns"
