"""Integration tests for ChatGPT extractor functions.

Requires a real ChatGPT DDP zip at::

    tests/ddp/chatgpt_<anything>.zip

Tests skip when no fixture is found — CI runs clean without real data.
"""
import sys
from unittest.mock import MagicMock

# Must precede all port imports — see AD0002.
sys.modules["js"] = MagicMock()

import pytest

from extractor_integration_helpers import ExtractorSpec, find_fixture, make_reader
from port.platforms.chatgpt import DDP_CATEGORIES, conversations_to_df

SPECS = [
    ExtractorSpec(name="conversations_to_df", extractor=conversations_to_df),
]


@pytest.fixture(scope="module")
def chatgpt_reader():
    fixture = find_fixture("chatgpt")
    if fixture is None:
        pytest.skip("No chatgpt_*.zip fixture found in tests/ddp/")
    return make_reader(fixture, DDP_CATEGORIES)


@pytest.mark.parametrize("spec", SPECS, ids=lambda s: s.name)
def test_extractor_not_empty(spec, chatgpt_reader):
    df = spec.run(chatgpt_reader)
    assert not df.empty, (
        f"{spec.name} returned an empty DataFrame — the extractor may have "
        "crashed, found no matching file, or the DDP format changed."
    )
