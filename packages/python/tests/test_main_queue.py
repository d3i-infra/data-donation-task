"""
Tests for ScriptWrapper command processing.

Verifies that commands yielded by the script generator are correctly
processed and returned, that error handling works, and that
CommandSystemLog milestones pass through the command protocol.
"""
import logging
import pytest
from unittest.mock import MagicMock

from port.main import ScriptWrapper
from port.api.commands import CommandSystemLog


def test_script_command_returned():
    """ScriptWrapper returns the script command directly."""
    def simple_script():
        yield CommandSystemLog(level="info", message="first")

    wrapper = ScriptWrapper(simple_script())
    result = wrapper.send(None)
    assert result["__type__"] == "CommandSystemLog"


def test_log_command_passes_through():
    """CommandSystemLog yielded by script passes through like any other command."""
    def script_with_log():
        _ = yield CommandSystemLog(level="info", message="test milestone")
        yield CommandSystemLog(level="info", message="second milestone")

    wrapper = ScriptWrapper(script_with_log())

    # First command: the log
    result1 = wrapper.send(None)
    assert result1["__type__"] == "CommandSystemLog"
    assert result1["message"] == "test milestone"

    # PayloadVoid response to log → script receives it, yields next command
    result2 = wrapper.send({"__type__": "PayloadVoid", "value": None})
    assert result2["__type__"] == "CommandSystemLog"
    assert result2["message"] == "second milestone"


def test_error_handler_still_works():
    """Error handling still works correctly — uncaught exceptions route to error_flow."""
    def crashing():
        data = yield
        raise RuntimeError("test explosion")

    wrapper = ScriptWrapper(crashing(), platform="X")
    result = wrapper.send(None)

    assert result["__type__"] == "CommandUIRender"
    page = result["page"]
    assert page["__type__"] == "PropsUIPageDataSubmission"


def test_stop_iteration_returns_exit():
    """Generator exhaustion produces CommandSystemExit."""
    def finite_script():
        return
        yield  # make it a generator

    wrapper = ScriptWrapper(finite_script())
    result = wrapper.send(None)
    assert result["__type__"] == "CommandSystemExit"


class _Payload:
    """Minimal stand-in for a JS payload object with a __type__ attribute."""

    def __init__(self, type_: str):
        self.__type__ = type_


def _crashing_wrapper() -> ScriptWrapper:
    def crashing():
        data = yield
        raise RuntimeError("test explosion")

    return ScriptWrapper(crashing(), platform="X")


def test_success_exit_code_is_zero():
    """Normal generator exhaustion keeps exit code 0 (flow-end contract)."""
    def finite_script():
        return
        yield  # make it a generator

    wrapper = ScriptWrapper(finite_script())
    result = wrapper.send(None)
    assert result["__type__"] == "CommandSystemExit"
    assert result["code"] == 0


def test_error_flow_skip_renders_incomplete_page_then_exits_nonzero():
    """After skipping the error report, the participant lands on a task-incomplete page
    and the flow terminates with a nonzero exit (Issue #123)."""
    import json

    wrapper = _crashing_wrapper()

    error_page = wrapper.send(None)
    assert error_page["__type__"] == "CommandUIRender"

    incomplete_page = wrapper.send(_Payload("PayloadFalse"))
    assert incomplete_page["__type__"] == "CommandUIRender"
    assert incomplete_page["page"]["__type__"] == "PropsUIPageDataSubmission"
    assert "could not be completed" in json.dumps(incomplete_page)

    exit_command = wrapper.send(_Payload("PayloadTrue"))
    assert exit_command["__type__"] == "CommandSystemExit"
    assert exit_command["code"] != 0


def test_error_flow_report_donates_then_renders_incomplete_page_then_exits_nonzero():
    """Reporting the error donates under 'error-report', then shows the task-incomplete
    page, then terminates with a nonzero exit."""
    import json

    wrapper = _crashing_wrapper()

    error_page = wrapper.send(None)
    assert error_page["__type__"] == "CommandUIRender"

    donate = wrapper.send(_Payload("PayloadTrue"))
    assert donate["__type__"] == "CommandSystemDonate"
    assert donate["key"] == "error-report"

    incomplete_page = wrapper.send(_Payload("PayloadVoid"))
    assert incomplete_page["__type__"] == "CommandUIRender"
    assert "could not be completed" in json.dumps(incomplete_page)

    exit_command = wrapper.send(_Payload("PayloadTrue"))
    assert exit_command["__type__"] == "CommandSystemExit"
    assert exit_command["code"] != 0


def test_error_exit_info_contains_no_exception_text():
    """The exit info crossing the bridge is PII-free: no traceback or
    exception text (ADR-0022 / ADR-0023)."""
    wrapper = _crashing_wrapper()

    wrapper.send(None)  # error page
    wrapper.send(_Payload("PayloadFalse"))  # task-incomplete page
    exit_command = wrapper.send(_Payload("PayloadTrue"))

    assert exit_command["__type__"] == "CommandSystemExit"
    assert "test explosion" not in exit_command["info"]
    assert "RuntimeError" not in exit_command["info"]
    assert "Traceback" not in exit_command["info"]


def test_task_incomplete_renders_page_then_exits_with_flow_code():
    """A TaskIncompleteError from the flow skips the error-report consent page:
    the participant lands directly on the task-incomplete page, and the exit
    carries the exception's own code/info instead of the error-flow defaults."""
    import json

    from port.helpers.flow_builder import TaskIncompleteError

    def abandoning():
        _ = yield
        raise TaskIncompleteError("abandoned")

    wrapper = ScriptWrapper(abandoning(), platform="X")

    incomplete_page = wrapper.send(None)
    assert incomplete_page["__type__"] == "CommandUIRender"
    assert incomplete_page["page"]["__type__"] == "PropsUIPageDataSubmission"
    assert "could not be completed" in json.dumps(incomplete_page)

    exit_command = wrapper.send(_Payload("PayloadTrue"))
    assert exit_command["__type__"] == "CommandSystemExit"
    assert exit_command["code"] == 2
    assert exit_command["info"] == "Participant abandoned the task"


def test_task_incomplete_exit_uses_each_reasons_own_literal():
    """Every TaskIncompleteError reason crosses the bridge with its own fixed
    PII-free (code, info) pair from the EXITS table — nothing from the raise
    site leaks, and no reason maps to the success exit."""
    from port.helpers.flow_builder import TaskIncompleteError

    for reason, (code, info) in TaskIncompleteError.EXITS.items():
        def incomplete():
            _ = yield
            raise TaskIncompleteError(reason)

        wrapper = ScriptWrapper(incomplete(), platform="X")
        wrapper.send(None)  # task-incomplete page
        exit_command = wrapper.send(_Payload("PayloadTrue"))
        assert exit_command["__type__"] == "CommandSystemExit"
        assert exit_command["code"] == code
        assert exit_command["code"] != 0
        assert exit_command["info"] == info


def test_task_incomplete_rejects_unknown_reason():
    """Raise sites cannot invent (code, info) pairs — an unknown reason fails
    at the raise site instead of carrying arbitrary text across the bridge."""
    from port.helpers.flow_builder import TaskIncompleteError

    with pytest.raises(KeyError):
        TaskIncompleteError("bogus reason with participant data")


def test_start_function_creates_wrapper(monkeypatch):
    """start() returns a ScriptWrapper."""
    def fake_process(session_id, platform):
        return iter([])

    monkeypatch.setattr("port.main.process", fake_process)

    from port.main import start
    wrapper = start({"sessionId": "session123", "platform": "LinkedIn"})
    assert isinstance(wrapper, ScriptWrapper)


class TestPayloadFilesWrapping:
    def test_each_reader_wrapped_in_adapter(self):
        from port.api.file_utils import AsyncFileAdapter
        import port.main as main
        readers = []
        for name, size in (("takeout-1-001.zip", 10), ("takeout-2-001.zip", 20)):
            r = MagicMock(); r.size = size; r.name = name; readers.append(r)
        data = MagicMock()
        data.__type__ = "PayloadFiles"
        data.value = readers
        script = MagicMock()
        script.send.return_value = MagicMock(toDict=lambda: {"__type__": "CommandUIRender"})
        wrapper = main.ScriptWrapper(script, platform="example")
        wrapper.send(data)
        sent = script.send.call_args[0][0]
        assert [type(a) for a in sent.value] == [AsyncFileAdapter, AsyncFileAdapter]
        assert [a.size for a in sent.value] == [10, 20]


class _MultiFileStubFlow:
    """Minimal PayloadFiles flow for driving the multi-file terminal paths
    end-to-end through ScriptWrapper.

    Built as a factory rather than a module-level subclass so the import
    stays inside the test (conftest shims `js` before `port` is imported).
    """

    @staticmethod
    def build():
        from port.helpers.flow_builder import FlowBuilder

        class _Flow(FlowBuilder):
            expected_file_payload = "PayloadFiles"

            def validate_file(self, file):
                raise AssertionError("validation is not reached on these paths")

            def extract_data(self, file, validation):
                raise AssertionError("extraction is not reached on these paths")

        return _Flow("session123", "Example")


def _advance_past_logs(wrapper, payload):
    """Send `payload` and skip the CommandSystemLog milestones that follow."""
    command = wrapper.send(payload)
    while command["__type__"] == "CommandSystemLog":
        command = wrapper.send(None)
    return command


class TestMultiFileIncompleteExits:
    """The multi-file pipeline's own terminal pages are incomplete endings:
    each reaches the host as the task-incomplete page followed by a nonzero
    exit, never as a completion (ADR-0039)."""

    def test_protocol_error_page_exits_upload_rejected(self):
        """A payload that is neither the expected PayloadFiles nor one of the
        established decline shapes shows the protocol-error page, then exits 4
        — a version-skew dead end is not a completed donation."""
        import json

        wrapper = ScriptWrapper(_MultiFileStubFlow.build().start_flow(), platform="Example")

        file_prompt = _advance_past_logs(wrapper, None)
        assert file_prompt["__type__"] == "CommandUIRender"

        protocol_error = _advance_past_logs(wrapper, _Payload("PayloadBogus"))
        assert protocol_error["__type__"] == "CommandUIRender"
        assert "out of sync" in json.dumps(protocol_error)

        incomplete_page = wrapper.send(_Payload("PayloadTrue"))
        assert incomplete_page["__type__"] == "CommandUIRender"
        assert "could not be completed" in json.dumps(incomplete_page)

        exit_command = wrapper.send(_Payload("PayloadTrue"))
        assert exit_command["__type__"] == "CommandSystemExit"
        assert exit_command["code"] == 4
        assert exit_command["info"] == "Upload rejected"

    def test_too_many_files_safety_page_exits_upload_rejected(self):
        """A set over uploads.MAX_UPLOAD_FILES shows the safety-error page,
        then exits 4 — the count guard shares the size guard's terminal path."""
        import json

        from port.helpers.uploads import MAX_UPLOAD_FILES

        wrapper = ScriptWrapper(_MultiFileStubFlow.build().start_flow(), platform="Example")

        file_prompt = _advance_past_logs(wrapper, None)
        assert file_prompt["__type__"] == "CommandUIRender"

        readers = []
        for i in range(MAX_UPLOAD_FILES + 1):
            reader = MagicMock()
            reader.size = 10
            reader.name = f"takeout-{i + 1}-001.zip"
            readers.append(reader)
        payload = MagicMock()
        payload.__type__ = "PayloadFiles"
        payload.value = readers

        safety_error = _advance_past_logs(wrapper, payload)
        assert safety_error["__type__"] == "CommandUIRender"
        assert "cannot be processed" in json.dumps(safety_error)

        incomplete_page = wrapper.send(_Payload("PayloadTrue"))
        assert incomplete_page["__type__"] == "CommandUIRender"
        assert "could not be completed" in json.dumps(incomplete_page)

        exit_command = wrapper.send(_Payload("PayloadTrue"))
        assert exit_command["__type__"] == "CommandSystemExit"
        assert exit_command["code"] == 4
