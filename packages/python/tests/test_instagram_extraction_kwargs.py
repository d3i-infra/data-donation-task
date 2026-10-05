"""``extraction()`` passes ``validation`` to every extractor that accepts it.

Extractors choose their HTML or JSON parser from ``validation``. One that
accepts it and does not receive it reads JSON, so an HTML export yields an
empty table; one that does not accept it raises ``TypeError`` when handed it.
"""
import inspect
import io
import zipfile
from types import SimpleNamespace

import pytest

from port.helpers.validate import DDPFiletype, Language
from port.platforms import instagram
from test_date_columns_declared import _load_config


@pytest.fixture(autouse=True)
def _config_from_docstrings(monkeypatch):
    """CI has no generated ``port/configs/*_config.json`` (gitignored); build the
    config in memory from the docstrings, as the generator does."""
    monkeypatch.setattr(instagram, "load_port_config", lambda registry, platform: _load_config(instagram, platform))


def _zip(*entries: tuple[str, str]) -> io.BytesIO:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        for name, content in entries:
            z.writestr(name, content)
    buf.seek(0)
    return buf


def test_extraction_runs_every_extractor_without_type_error():
    validation = SimpleNamespace(
        archive_members=["placeholder.txt"],
        current_ddp_category=SimpleNamespace(id="json_en", language=Language.EN, ddp_filetype=None),
        ddp_locale="en",
    )

    result = instagram.extraction(_zip(("placeholder.txt", "")), validation)

    assert result.tables == []  # nothing recognised, and nothing raised


def test_every_extractor_that_accepts_validation_receives_it(monkeypatch):
    seen = {}

    def _capture(reader, errors, config):
        seen["config"] = config
        return instagram.ExtractionResult(tables=[])

    monkeypatch.setattr(instagram, "run_extraction", _capture)
    validation = SimpleNamespace(archive_members=[], current_ddp_category=None, ddp_locale="en")

    instagram.extraction(_zip(("placeholder.txt", "")), validation)

    assert seen["config"]
    for table in seen["config"]:
        accepts = "validation" in inspect.signature(table.extractor).parameters
        assert ("validation" in table.extractor_kwargs) == accepts, table.id


def test_html_export_yields_its_table_through_extraction():
    html = """
    <html><body><main>
      <div class="_a6-g">
        <div>
          <h2>Owner</h2>
          <table><tr><td>Username</td><td>the_liker</td></tr></table>
        </div>
        <div class="_a6-o">Mar 02, 2026 4:57:45 pm</div>
      </div>
    </main></body></html>
    """
    validation = SimpleNamespace(
        archive_members=["story_likes.html"],
        current_ddp_category=SimpleNamespace(id="html_en", language=Language.EN, ddp_filetype=DDPFiletype.HTML),
        ddp_locale="en",
    )

    result = instagram.extraction(_zip(("story_likes.html", html)), validation)

    dates = [list(t.data_frame["Date"]) for t in result.tables if "Date" in t.data_frame]
    assert ["Mar 02, 2026 4:57:45 pm"] in dates
