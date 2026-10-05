"""Tests for new port_helpers functions."""
from unittest.mock import MagicMock

import pandas as pd

import port.helpers.extraction_helpers as eh
import port.helpers.port_helpers as ph
from port.api.commands import CommandUIRender


class TestRenderNoDataPage:
    def test_returns_command_ui_render(self):
        result = ph.render_no_data_page("Instagram")
        assert isinstance(result, CommandUIRender)

    def test_page_type_is_data_submission(self):
        result = ph.render_no_data_page("Instagram")
        d = result.toDict()
        assert d["page"]["__type__"] == "PropsUIPageDataSubmission"


class TestRenderSafetyErrorPage:
    def test_returns_command_ui_render(self):
        error = ValueError("test error")
        result = ph.render_safety_error_page("Facebook", error)
        assert isinstance(result, CommandUIRender)

    def test_page_type_is_data_submission(self):
        error = ValueError("test error")
        result = ph.render_safety_error_page("Facebook", error)
        d = result.toDict()
        assert d["page"]["__type__"] == "PropsUIPageDataSubmission"

    def test_confirm_prompt_has_no_cancel_button(self):
        """ITEM 3: FlowBuilder discards this Confirm's result and always
        raises TaskIncompleteError("upload_rejected") next regardless of
        which button is pressed — a single acknowledging button, like the
        task-incomplete page."""
        error = ValueError("test error")
        result = ph.render_safety_error_page("Facebook", error)
        d = result.toDict()
        body = d["page"]["body"][0]
        assert body["__type__"] == "PropsUIPromptConfirm"
        assert "cancel" not in body


class TestRenderDonateFailurePage:
    def test_returns_command_ui_render(self):
        result = ph.render_donate_failure_page("YouTube")
        assert isinstance(result, CommandUIRender)

    def test_page_type_is_data_submission(self):
        result = ph.render_donate_failure_page("YouTube")
        d = result.toDict()
        assert d["page"]["__type__"] == "PropsUIPageDataSubmission"

    def test_confirm_prompt_has_no_cancel_button(self):
        """ITEM 3: FlowBuilder discards this Confirm's result and always
        raises TaskIncompleteError("donation_failed") next regardless of
        which button is pressed — donation is never retried from here, so
        this is a single acknowledging button, like the task-incomplete
        page."""
        result = ph.render_donate_failure_page("YouTube")
        d = result.toDict()
        body = d["page"]["body"][0]
        assert body["__type__"] == "PropsUIPromptConfirm"
        assert "cancel" not in body


class TestRenderTaskIncompletePage:
    def test_returns_command_ui_render(self):
        result = ph.render_task_incomplete_page("TikTok")
        assert isinstance(result, CommandUIRender)

    def test_page_type_is_data_submission(self):
        result = ph.render_task_incomplete_page("TikTok")
        d = result.toDict()
        assert d["page"]["__type__"] == "PropsUIPageDataSubmission"

    def test_confirm_prompt_has_no_cancel_button(self):
        """The task-incomplete page is an acknowledge page: a single OK button."""
        result = ph.render_task_incomplete_page("TikTok")
        d = result.toDict()
        body = d["page"]["body"]
        prompts = body if isinstance(body, list) else [body]
        confirm = next(p for p in prompts if p["__type__"] == "PropsUIPromptConfirm")
        assert confirm.get("cancel") in (None, {})


class TestHandleDonateResult:
    def test_success_response(self):
        """PayloadResponse with value.success=True → True."""
        result = MagicMock()
        result.__type__ = "PayloadResponse"
        result.value = MagicMock(success=True, key="k", status=200)
        assert ph.handle_donate_result(result) is True

    def test_failure_response(self):
        """PayloadResponse with value.success=False → False."""
        result = MagicMock()
        result.__type__ = "PayloadResponse"
        result.value = MagicMock(success=False, key="k", status=500, error="server error")
        assert ph.handle_donate_result(result) is False

    def test_payload_void_is_success(self):
        """PayloadVoid (dev mode / backward-compat) → True."""
        result = MagicMock()
        result.__type__ = "PayloadVoid"
        assert ph.handle_donate_result(result) is True

    def test_none_is_success(self):
        """None (legacy fire-and-forget) → True."""
        assert ph.handle_donate_result(None) is True

    def test_unknown_type_is_failure(self):
        result = MagicMock()
        result.__type__ = "PayloadWeird"
        assert ph.handle_donate_result(result) is False


class TestFindItemRaw:
    """ADR-0042: an epoch is a JSON number and must stay one — find_item_raw
    returns the matched value in its own type, unlike find_item which stringifies."""

    def test_returns_int_for_an_int(self):
        d = {"a-b-timestamp": 1781548241}
        assert eh.find_item_raw(d, "timestamp") == 1781548241
        assert isinstance(eh.find_item_raw(d, "timestamp"), int)

    def test_none_when_absent(self):
        d = {"a-b-other": 1}
        assert eh.find_item_raw(d, "timestamp") is None

    def test_zero_is_not_falsy_collapsed(self):
        """A zero epoch must survive, not be treated as missing."""
        d = {"a-timestamp": 0}
        result = eh.find_item_raw(d, "timestamp")
        assert result == 0
        assert result is not None

    def test_picks_least_nested_match(self):
        d = {"a-b-timestamp": 111, "a-timestamp": 222, "unrelated": 3}
        assert eh.find_item_raw(d, "timestamp") == 222

    def test_does_not_stringify(self):
        d = {"a-timestamp": 1781548241}
        result = eh.find_item_raw(d, "timestamp")
        assert not isinstance(result, str)
        assert result == 1781548241


class TestRawTimestamp:
    """ADR-0042: `raw_timestamp` is the shared wrapper around `find_item_raw`
    that every platform's timestamp column goes through — donate the export's
    own value, never `find_item_raw(...) or ""`, which would collapse a
    legitimate epoch of `0` into the same `""` used for absence."""

    def test_int_survives(self):
        d = {"a-timestamp": 1781548241}
        result = eh.raw_timestamp(d, "timestamp")
        assert result == 1781548241
        assert isinstance(result, int)

    def test_zero_survives(self):
        d = {"a-timestamp": 0}
        result = eh.raw_timestamp(d, "timestamp")
        assert result == 0
        assert result is not None
        assert result != ""

    def test_absent_is_empty_string(self):
        d = {"a-other": 1}
        assert eh.raw_timestamp(d, "timestamp") == ""


class TestTimestampColumnPrecision:
    """A DataFrame column mixing native ints with missing values must not
    upcast to float64 (ADR-0042): that would silently lose integer precision
    for large epoch values. Pandas keeps `object` dtype when the missing
    value is `""`, but upcasts to float64/NaN when it's `None` — so every
    extractor normalizes a missing raw timestamp to `""` before building the
    frame, via the shared `eh.raw_timestamp`.
    """

    def test_raw_timestamp_keeps_int_precision_with_missing_rows(self):
        present = {"a-timestamp": 1781548241}
        absent = {"a-other": 1}

        df = pd.DataFrame(
            [(eh.raw_timestamp(present, "timestamp"),), (eh.raw_timestamp(absent, "timestamp"),)],
            columns=["Date"],
        )
        assert df["Date"].iloc[0] == 1781548241
        assert isinstance(df["Date"].iloc[0], int)
        assert df["Date"].iloc[1] == ""

    def test_none_in_the_column_would_upcast_and_lose_precision(self):
        """Documents the failure mode the helpers avoid: mixing a bare `None`
        into the column (instead of `""`) upcasts the whole column to
        float64, so a large epoch is no longer an exact int."""
        df = pd.DataFrame([(1781548241,), (None,)], columns=["Date"])
        assert df["Date"].dtype == "float64"
        assert not isinstance(df["Date"].iloc[0], int)
