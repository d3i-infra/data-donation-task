"""Tests for the Google platform's wiring: the extractor registry, the
config-driven ``extraction()`` entry point, and ``GoogleFlow``.

These are the ADR-0029 standard-interface tests every platform module
carries, plus the two behaviors specific to a Takeout archive-set: every
table's extractor needs the detected DDP locale to find its files, and the
best-effort Failed-Files count only ever appears in ``errors`` when Takeout
actually reported failures (never as a zero-valued key). Path-resolution and
per-extractor row-shape tests live in ``test_google_paths.py``; this module
only exercises the platform-interface layer built on top of them.

Every test below that reaches ``extraction()`` builds its table config out of
programmatic ``TableConfig`` objects (monkeypatching ``load_port_config``) so
none of them depend on a hand-curated config file (spec §8 config-independence
rule — the fork's undeployable-Google mistake). The donation-key test is the
one exception: it stubs ``extract_data`` directly and never reaches
``load_port_config`` at all.
"""
import io
import zipfile
from collections import Counter

import pandas as pd
import pytest

import port.api.d3i_props as d3i_props
import port.api.props as props
from port.api.commands import CommandSystemLog, CommandUIRender
from port.api.d3i_props import ExtractionResult
from port.helpers.archive_set import ArchiveSet
from port.helpers.flow_builder import FlowBuilder
from port.helpers.table_extractor import TableConfig
from port.platforms import google

WATCH_JSON = (
    '[{"title": "Watched A video", "titleUrl": "https://www.youtube.com/watch?v=abc", '
    '"time": "2026-06-15T20:30:41Z"}]'
)

#: Same shape as TestFailedFilesDetector.MANIFEST in test_google_paths.py: two
#: non-empty failure-message nodes plus the empty template node that must not count.
FAILED_MANIFEST = (
    '<html><body>'
    '<div class="file-leaf"><div class="extracted-file-name">a</div>'
    '<div class="failure-message">Service failed to retrieve this item</div></div>'
    '<div class="file-leaf"><div class="extracted-file-name">b</div>'
    '<div class="failure-message">Kon dit item niet ophalen</div></div>'
    '<div class="failure-message"></div>'
    '</body></html>'
)


def _named_part(name: str, members: dict[str, str | bytes]) -> io.BytesIO:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for path, content in members.items():
            zf.writestr(path, content)
    buf.seek(0)
    buf.name = name
    buf.size = buf.getbuffer().nbytes
    return buf


def make_set(members: dict[str, str | bytes], part_name: str = "takeout-001.zip") -> ArchiveSet:
    return ArchiveSet([_named_part(part_name, members)])


# ---------------------------------------------------------------------------
# ADR-0029 standard platform interface
# ---------------------------------------------------------------------------


def test_standard_platform_interface():
    assert isinstance(google.EXTRACTOR_REGISTRY, dict)
    assert callable(google.extraction)
    assert issubclass(google.GoogleFlow, FlowBuilder)
    assert callable(google.process)


def test_expects_payload_files():
    assert google.GoogleFlow("s1").expected_file_payload == "PayloadFiles"


def test_registry_has_twelve_extractors_in_priority_order():
    assert list(google.EXTRACTOR_REGISTRY) == [
        "youtube_watch_history_to_df",
        "youtube_search_history_to_df",
        "youtube_subscriptions_to_df",
        "youtube_comments_to_df",
        "search_history_to_df",
        "chrome_history_to_df",
        "video_search_history_to_df",
        "ads_history_to_df",
        "discover_history_to_df",
        "google_news_history_to_df",
        "news_history_to_df",
        "news_items_to_df",
    ]


# ---------------------------------------------------------------------------
# extraction()
# ---------------------------------------------------------------------------


def _fake_table_config(table_id: str) -> TableConfig:
    return TableConfig(
        id=table_id,
        extractor=lambda reader, errors, **kwargs: pd.DataFrame(),
        title=props.Translatable({"en": table_id, "nl": table_id}),
        description=props.Translatable({"en": "", "nl": ""}),
        headers={},
        extractor_kwargs={"existing_kwarg": "kept"},
    )


def test_extraction_passes_ddp_locale_to_every_table(monkeypatch):
    """Every table's extractor_kwargs carries ddp_locale — injected once here
    rather than duplicated in every extractor's config entry — and any
    kwargs the config already carried survive the merge."""
    fake_tables = [_fake_table_config(f"t{i}") for i in range(3)]
    monkeypatch.setattr(google, "load_port_config", lambda registry, platform: fake_tables)

    archive_set = make_set({
        "Takeout/YouTube and YouTube Music/history/watch-history.json": WATCH_JSON,
    })
    validation = google.validate_ddp(archive_set)

    google.extraction(archive_set, validation)

    assert validation.ddp_locale == "en"
    for table in fake_tables:
        assert table.extractor_kwargs["ddp_locale"] == "en"
        assert table.extractor_kwargs["existing_kwarg"] == "kept"


def test_extraction_calls_load_port_config_for_google(monkeypatch):
    """extraction() drives the shared config-loading path with this platform's
    own name and registry, like every other config-driven platform."""
    calls = []

    def fake_load(registry, platform):
        calls.append((registry, platform))
        return []

    monkeypatch.setattr(google, "load_port_config", fake_load)

    archive_set = make_set({
        "Takeout/YouTube and YouTube Music/history/watch-history.json": WATCH_JSON,
    })
    validation = google.validate_ddp(archive_set)
    google.extraction(archive_set, validation)

    assert calls == [(google.EXTRACTOR_REGISTRY, "google")]


def test_failed_files_count_lands_in_errors_only_when_present(monkeypatch):
    """Builds the table config out of programmatic ``TableConfig`` objects
    (monkeypatching ``load_port_config``, like the ddp-locale/interface tests
    above) rather than depending on the generated config file (spec §8
    config-independence rule) — the Failed-Files count this test pins comes
    from ``extraction()``'s own manifest scan, independent of which tables
    the config lists."""
    fake_tables = [_fake_table_config("t0")]
    monkeypatch.setattr(google, "load_port_config", lambda registry, platform: fake_tables)

    with_manifest = make_set({
        "Takeout/archive_browser.html": FAILED_MANIFEST,
        "Takeout/YouTube and YouTube Music/history/watch-history.json": WATCH_JSON,
    })
    validation = google.validate_ddp(with_manifest)
    result = google.extraction(with_manifest, validation)
    assert result.errors["ExportReportedFailedFiles"] == 2

    without_manifest = make_set({
        "Takeout/YouTube and YouTube Music/history/watch-history.json": WATCH_JSON,
    })
    validation2 = google.validate_ddp(without_manifest)
    result2 = google.extraction(without_manifest, validation2)
    assert "ExportReportedFailedFiles" not in result2.errors


# ---------------------------------------------------------------------------
# GoogleFlow / donation key (ADR-0020)
# ---------------------------------------------------------------------------


def _make_payload(type_name, **attrs):
    class _Payload:
        pass
    p = _Payload()
    p.__type__ = type_name
    for k, v in attrs.items():
        setattr(p, k, v)
    return p


def _payload_files(members: dict[str, str | bytes]):
    """A PayloadFiles-shaped payload wrapping one recognizable Takeout part."""
    return _make_payload("PayloadFiles", value=[_named_part("takeout-1-001.zip", members)])


def _advance_past_logs(gen, response=None):
    cmd = gen.send(response)
    while isinstance(cmd, CommandSystemLog):
        cmd = gen.send(_make_payload("PayloadVoid"))
    return cmd


def _start_and_skip_logs(gen):
    cmd = next(gen)
    while isinstance(cmd, CommandSystemLog):
        cmd = gen.send(_make_payload("PayloadVoid"))
    return cmd


def test_flow_donation_key_is_session_google():
    """Mirrors test_flow_builder.py::TestDonateKeyFormat: the donate key
    derives as '{session_id}-{platform_name.lower()}' (ADR-0020). Google's
    platform_name is 'Google', so the key is 'session-google', not
    'session-Google' — pinning ADR-0020's lowercasing for this platform."""
    flow = google.GoogleFlow("sess-1")
    flow.extract_data = lambda archive_set, validation: ExtractionResult(
        tables=[d3i_props.PropsUIPromptConsentFormTableViz(
            id="t",
            data_frame=pd.DataFrame({"c": [1]}),
            title=props.Translatable({"en": "T", "nl": "T"}),
        )],
        errors=Counter(),
    )

    gen = flow.start_flow()
    cmd = _start_and_skip_logs(gen)
    assert isinstance(cmd, CommandUIRender)

    cmd = _advance_past_logs(gen, _payload_files({
        "Takeout/YouTube and YouTube Music/history/watch-history.json": WATCH_JSON,
    }))
    assert isinstance(cmd, CommandUIRender)  # consent form

    cmd = _advance_past_logs(gen, _make_payload("PayloadJSON", value="{}"))
    assert cmd.key == "sess-1-google"


def test_single_zip_through_multi_file_flow_completes(monkeypatch):
    """N=1 through the PayloadFiles pipeline is a mainstream case, not an
    edge case: a small Google export legitimately arrives as a single zip,
    and the multi-select prompt with exactly one file selected must
    validate, extract, and reach the consent stage exactly like a
    multi-part set — ArchiveSet unions N>=1 parts identically (ADR-0040), so
    nothing in validate_file/extract_data branches on part count.

    Runs the real validate_file/extract_data (no monkeypatching of those,
    unlike test_flow_donation_key_is_session_google above, which stubs
    extract_data to isolate the ADR-0020 donate-key concern and may evolve
    separately) — this test exercises the real pipeline end to end. Only
    ``load_port_config`` is monkeypatched, to a table wired to the real
    ``youtube_watch_history_to_df`` extractor, so the run never depends on
    the generated config file (spec §8 config-independence rule)."""
    fake_tables = [TableConfig(
        id="youtube_watch_history",
        extractor=google.EXTRACTOR_REGISTRY["youtube_watch_history_to_df"],
        title=props.Translatable({"en": "t", "nl": "t"}),
        description=props.Translatable({"en": "", "nl": ""}),
        headers={},
    )]
    monkeypatch.setattr(google, "load_port_config", lambda registry, platform: fake_tables)

    flow = google.GoogleFlow("sess-n1")

    gen = flow.start_flow()
    cmd = _start_and_skip_logs(gen)
    assert isinstance(cmd, CommandUIRender)

    cmd = _advance_past_logs(gen, _payload_files({
        "Takeout/YouTube and YouTube Music/history/watch-history.json": WATCH_JSON,
    }))

    # Reaching the consent-form body (as opposed to a retry/error/no-data
    # page, which all render props.PropsUIPromptConfirm instead) proves
    # validation passed and extraction produced at least one table.
    assert isinstance(cmd, CommandUIRender)
    assert isinstance(cmd.page.body, d3i_props.PropsUIPromptConsentFormViz)
    assert len(cmd.page.body.tables) >= 1
