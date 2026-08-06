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


def test_start_function_creates_wrapper(monkeypatch):
    """start() returns a ScriptWrapper."""
    def fake_process(session_id, platform):
        return iter([])

    monkeypatch.setattr("port.main.process", fake_process)

    from port.main import start
    wrapper = start({"sessionId": "session123", "platform": "LinkedIn"})
    assert isinstance(wrapper, ScriptWrapper)
