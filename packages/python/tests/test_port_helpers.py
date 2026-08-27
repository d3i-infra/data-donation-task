"""Tests for new port_helpers functions."""
from unittest.mock import MagicMock

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


class TestRenderDonateFailurePage:
    def test_returns_command_ui_render(self):
        result = ph.render_donate_failure_page("YouTube")
        assert isinstance(result, CommandUIRender)

    def test_page_type_is_data_submission(self):
        result = ph.render_donate_failure_page("YouTube")
        d = result.toDict()
        assert d["page"]["__type__"] == "PropsUIPageDataSubmission"


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

    def test_copy_points_at_the_host_close_control(self):
        """After the nonzero exit the host paints nothing (verified on live
        Next 2026-08-27): the participant's way back to the task list is the
        host's own Close control, so the page must name it — not tell them
        to refresh into the void. The e2e spec pins the opening sentence."""
        import json

        result = ph.render_task_incomplete_page("TikTok")
        serialized = json.dumps(result.toDict())

        assert "This task could not be completed" in serialized
        assert "Close" in serialized
        assert "refreshing" not in serialized


class TestGenerateRetryPrompt:
    def _confirm(self):
        return ph.generate_retry_prompt("Facebook").toDict()

    def test_cancel_button_is_not_labelled_continue(self):
        """'Continue' was a lie: declining the retry ends the attempt and
        leads to the task-incomplete page. The label must say so."""
        confirm = self._confirm()
        assert confirm["cancel"]["translations"]["en"] == "Stop for now"
        assert all(
            label != "Continue" for label in confirm["cancel"]["translations"].values()
        )

    def test_ok_button_stays_try_again(self):
        confirm = self._confirm()
        assert confirm["ok"]["translations"]["en"] == "Try again"

    def test_text_no_longer_promises_continuing(self):
        """The old copy told a participant who was sure about their file to
        'Continue' — but the file is never processed either way."""
        confirm = self._confirm()
        en = confirm["text"]["translations"]["en"]
        assert "Continue, if you are sure" not in en
        assert "stop for now" in en

    def test_all_five_locales_present_on_text_and_buttons(self):
        confirm = self._confirm()
        for part in ("text", "ok", "cancel"):
            assert set(confirm[part]["translations"].keys()) == {"en", "nl", "de", "it", "es"}


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
