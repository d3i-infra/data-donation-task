"""Tests for the config-driven table loader in ``table_extractor``.

All fixtures here read the shipped ``example`` platform config (metadata only,
never participant data).
"""

from collections import Counter

import pandas as pd

import port.api.props as props
from port.helpers.table_extractor import TableConfig, run_extraction


def test_load_reference_timezone_reads_example_config(monkeypatch):
    from port.helpers import table_extractor as te

    assert te.load_reference_timezone("example") is None  # example ships null


def test_load_reference_timezone_missing_config_is_none():
    from port.helpers import table_extractor as te

    assert te.load_reference_timezone("no_such_platform") is None


def _stub_extractor(reader, errors):
    return pd.DataFrame({"Timestamp": [1700000000], "Name": ["a"]})


def test_run_extraction_carries_date_columns_into_toDict():
    """A table whose config declares ``date_columns`` carries it through
    ``run_extraction`` into the props and its serialized ``toDict()`` (ADR-0043).

    This is the generic seam ``whatsapp.py``'s hand-rolled ``extraction()``
    missed (final review, Important 1): every config-driven platform goes
    through here, so pinning it here is what would have caught the omission.
    """
    config = [
        TableConfig(
            id="stub_table",
            extractor=_stub_extractor,
            title=props.Translatable({"en": "Stub", "nl": "Stub"}),
            description=props.Translatable({"en": "Stub", "nl": "Stub"}),
            headers={"Timestamp": props.Translatable({"en": "Timestamp", "nl": "Tijdstempel"})},
            date_columns={"Timestamp": {"encoding": "epoch-seconds"}},
        )
    ]
    result = run_extraction(reader=None, errors=Counter(), config=config)
    assert len(result.tables) == 1
    table = result.tables[0]
    assert table.date_columns == {"Timestamp": {"encoding": "epoch-seconds"}}
    assert table.toDict()["date_columns"] == {"Timestamp": {"encoding": "epoch-seconds"}}
