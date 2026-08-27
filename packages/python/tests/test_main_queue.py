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
